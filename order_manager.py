# order_manager.py
import logging
import requests
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Tuple

logger = logging.getLogger("topstepx-client")

# Working statuses we keep in memory (TopstepX numeric -> meaning you mapped in app.py)
_WORKING_STATUSES = {0, 1, 6}  # new, working, partially filled


class OrderPositionManager:
    """
    Centralizes order/position state management.

    - Keeps an up-to-date open position snapshot (or None)
    - Keeps a dict of working orders (keyed by order id)
    - Writes trade records to Firestore when fills happen
    - On startup, attempts to snapshot from REST (if available),
      otherwise relies on incoming SignalR updates.
    """

    def __init__(
        self,
        account_id: int,
        symbol_id: str,
        jwt_token_supplier,  # callable -> str (current JWT)
        map_order_type,      # callable(int) -> str
        map_order_status,    # callable(int) -> str
        write_trade_record,  # callable(dict) -> None  (from sync_firestore)
        # Optional REST endpoints for baseline snapshot (leave as "" to skip)
        orders_snapshot_url: str = "",
        positions_snapshot_url: str = "",
    ):
        self.account_id = account_id
        self.symbol_id = symbol_id
        self._jwt = jwt_token_supplier
        self._map_order_type = map_order_type
        self._map_order_status = map_order_status
        self._write_trade_record = write_trade_record

        # Live state
        self._open_position: Optional[Dict[str, Any]] = None
        self._working_orders: Dict[int, Dict[str, Any]] = {}

        # Snapshot URLs (if you have them)
        self._orders_url = orders_snapshot_url
        self._positions_url = positions_snapshot_url

    # ---------- Public getters ----------
    def get_open_position(self) -> Optional[Dict[str, Any]]:
        return self._open_position

    def get_working_orders(self) -> List[Dict[str, Any]]:
        return list(self._working_orders.values())

    # ---------- Startup snapshot ----------
    def startup_snapshot(self) -> None:
        """
        Try REST baseline snapshot (orders + positions).
        If URLs are not configured or fail, we simply proceed and rely on SignalR updates.
        """
        token = self._jwt()
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        try:
            # Positions first
            if self._positions_url:
                pos = requests.get(self._positions_url, headers=headers, timeout=8)
                if pos.ok:
                    self._ingest_positions_snapshot(pos.json())
                else:
                    logger.warning(f"⚠️ Positions snapshot HTTP {pos.status_code}: {pos.text[:200]}")

            # Orders
            if self._orders_url:
                od = requests.get(self._orders_url, headers=headers, timeout=8)
                if od.ok:
                    self._ingest_orders_snapshot(od.json())
                else:
                    logger.warning(f"⚠️ Orders snapshot HTTP {od.status_code}: {od.text[:200]}")

            self._log_current_state(source="startup-snapshot")
        except Exception as e:
            logger.warning(f"⚠️ Startup snapshot failed (fallback to live streams): {e}")

    # ---------- Ingest REST snapshots ----------
    def _ingest_positions_snapshot(self, payload: Any) -> None:
        """
        Accepts whatever the REST returns; normalize to:
        { id, accountId, contractId, size, averagePrice, side(optional), updateTimestamp(optional) }
        """
        # Try common shapes: list of dicts or dict with 'positions'
        items = []
        if isinstance(payload, list):
            items = payload
        elif isinstance(payload, dict) and "positions" in payload:
            items = payload.get("positions") or []
        else:
            items = [payload] if isinstance(payload, dict) else []

        # For our symbol/account only; pick the latest non-zero size as open
        latest: Optional[Tuple[datetime, Dict[str, Any]]] = None
        for p in items:
            if not isinstance(p, dict):
                continue
            if str(p.get("contractId")) != self.symbol_id:
                continue
            if int(p.get("accountId", -1)) != self.account_id:
                continue

            size = p.get("size", 0)
            ts = p.get("updateTimestamp") or p.get("creationTimestamp")
            try:
                updated = (
                    datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
                    if ts else datetime.min.replace(tzinfo=timezone.utc)
                )
            except Exception:
                updated = datetime.min.replace(tzinfo=timezone.utc)

            if size and size != 0:
                if latest is None or updated > latest[0]:
                    latest = (updated, p)

        self._open_position = latest[1] if latest else None

    def _ingest_orders_snapshot(self, payload: Any) -> None:
        """
        Accepts snapshot of orders; keep only working (new/working/partial) for our account + symbol.
        """
        items = []
        if isinstance(payload, list):
            items = payload
        elif isinstance(payload, dict) and "orders" in payload:
            items = payload["orders"] or []
        else:
            items = [payload] if isinstance(payload, dict) else []

        for od in items:
            if not isinstance(od, dict):
                continue
            if int(od.get("accountId", -1)) != self.account_id:
                continue
            if str(od.get("symbolId", od.get("contractId"))) != self.symbol_id:
                continue

            status = od.get("status")
            if status in _WORKING_STATUSES:
                order_id = int(od.get("id"))
                self._working_orders[order_id] = self._normalize_order(od)

    # ---------- SignalR ingestion (from app.py callbacks) ----------
    def on_user_position_event(self, entry: Dict[str, Any]) -> None:
        """
        Ingest a single 'data' dict from GatewayUserPosition.
        """
        p = entry or {}
        if int(p.get("accountId", -1)) != self.account_id:
            return
        if str(p.get("contractId")) != self.symbol_id:
            return

        size = p.get("size", 0)
        if not size or size == 0:
            # position closed
            if self._open_position is not None:
                logger.info("✅ Position now flat (size=0).")
            self._open_position = None
            return

        # update open position
        self._open_position = p

    def on_user_order_event(self, entry: Dict[str, Any]) -> None:
        """
        Ingest a single 'data' dict from GatewayUserOrder.
        - Track working orders
        - If fills occur, write trade record
        """
        o = entry or {}
        if int(o.get("accountId", -1)) != self.account_id:
            return
        if str(o.get("symbolId", o.get("contractId"))) != self.symbol_id:
            return

        order_id = int(o.get("id"))
        status_num = o.get("status")
        order_status = self._map_order_status(status_num)

        # If we see fill or partial fill, record it
        fill_vol = o.get("fillVolume", 0) or 0
        filled_price = o.get("filledPrice")
        if fill_vol and float(fill_vol) > 0:
            # Write a trade record
            self._write_trade_record(self._build_trade_record(o))

        # Maintain working orders dictionary
        if status_num in _WORKING_STATUSES:
            self._working_orders[order_id] = self._normalize_order(o)
        else:
            # remove non-working
            if order_id in self._working_orders:
                self._working_orders.pop(order_id, None)

    # ---------- Helpers ----------
    def _normalize_order(self, o: Dict[str, Any]) -> Dict[str, Any]:
        side_num = o.get("side")
        side = "buy" if side_num == 1 else "sell"
        return {
            "id": int(o.get("id")),
            "symbol": str(o.get("symbolId", o.get("contractId"))),
            "side": side,
            "type": self._map_order_type(o.get("type")),
            "size": o.get("size"),
            "filled": o.get("fillVolume", 0) or 0,
            "price": o.get("limitPrice") or o.get("filledPrice"),
            "status": self._map_order_status(o.get("status")),
            "timestamp": o.get("updateTimestamp") or o.get("creationTimestamp"),
        }

    def _build_trade_record(self, o: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build a Firestore trade record from an order fill event.
        You can enrich this with strategy context if you have it in scope.
        """
        ts = o.get("updateTimestamp") or o.get("creationTimestamp")
        try:
            ts_iso = (
                datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
                if ts else datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
            )
        except Exception:
            ts_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

        return {
            "trade_id": str(o.get("id")),
            "timestamp": ts_iso,
            "accountId": self.account_id,
            "contractId": str(o.get("symbolId", o.get("contractId"))),
            "side": "buy" if o.get("side") == 1 else "sell",
            "size": o.get("fillVolume", 0) or 0,
            "price": o.get("filledPrice"),
            "order_type": self._map_order_type(o.get("type")),
            "status": self._map_order_status(o.get("status")),
            # slots for your strategy metadata if available:
            "strategy": "Strategy-1",
            "signal": o.get("clientTag") or "N/A",
        }

    def _log_current_state(self, source: str) -> None:
        pos = self._open_position
        w = self.get_working_orders()
        if pos:
            side = "long" if pos.get("side") == 1 else "short"
            logger.info(
                f"🔎 [{source}] Open position: {self.symbol_id} | {side} | "
                f"size={pos.get('size')} @ {pos.get('averagePrice')}"
            )
        else:
            logger.info(f"🔎 [{source}] Open position: none")

        if w:
            for o in w:
                logger.info(
                    f"🔎 [{source}] Working order: {o['symbol']} | {o['side']} | "
                    f"{o['size']} | type={o['type']} | filled={o['filled']} "
                    f"| price={o['price']} | status={o['status']} (id={o['id']})"
                )
        else:
            logger.info(f"🔎 [{source}] Working orders: none")
