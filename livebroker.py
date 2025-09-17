# livebroker.py  — latency-instrumented + parallel SL/TP + reconnect helpers

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Optional, Dict, Any, Tuple, List
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

logger = logging.getLogger("LiveBroker")


class LiveBroker:
    """
    Handles REST calls to TopStepX API.

    Notes
    -----
    - All endpoints require a valid JWT bearer token (24h lifetime).
    - Token is obtained/refreshed by app.py and passed here.
    - Synchronous calls; SL/TP are placed concurrently via threads.

    New in this version
    -------------------
    - warmup(): pre-opens TCP/TLS so the next POST reuses a hot socket.
    - place_exit_orders(): unique tag nonce to avoid tag collisions on server.
    - close_position(account_id=None, contract_id=None): backward compatible.
    - fetch_account_positions_orders(): explicit pull of acct/pos/orders for reconcile.
    - cancel_all_for_contract(): convenience to purge stale working orders after reconnect.
    """

    API_BASE = "https://api.topstepx.com/api"

    # TopStepX order type enums
    TYPE_LIMIT = 1
    TYPE_MARKET = 2
    TYPE_STOP = 4

    # TopStepX side enums
    SIDE_BUY = 0   # Bid
    SIDE_SELL = 1  # Ask

    # Observed order statuses
    STATUS_NEW = 0
    STATUS_WORKING = 1
    STATUS_FILLED = 2
    STATUS_CANCELLED = 3
    STATUS_REJECTED = 4
    STATUS_EXPIRED = 5
    STATUS_PARTIAL = 6

    def __init__(
        self,
        jwt_token: str,
        account_id: int,
        contract_id: str,
        tick_size: float,
        timeout: float = 10.0,
        api_base: Optional[str] = None,
    ) -> None:
        self.token = jwt_token
        self.account_id = int(account_id)
        self.contract_id = str(contract_id)
        self.tick_size = float(tick_size)
        self.timeout = float(timeout)
        self.api_base = api_base or self.API_BASE

        # Reusable session + headers + tuned adapter (normal session, with light retries)
        self._session = requests.Session()
        self._session.headers.update(
            {
                "accept": "text/plain",
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.token}",
                "Connection": "keep-alive",
            }
        )

        # Increase pool size; optional light retry for transient 5xx
        retries = Retry(
            total=2,
            backoff_factor=0.1,
            status_forcelist=(500, 502, 503, 504),
            allowed_methods=frozenset(["POST"]),
        )
        adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=retries)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

        # ---------------------------
        # HOT session for entry orders
        # - Used only for market-entry orders to minimize client-side overhead
        # - No automatic retries (so no retry/backoff delays)
        # - Short timeout (fail fast)
        # - Separate, small connection pool so hot requests aren't queued behind other calls
        # ---------------------------
        self._hot_timeout = 3.0  # seconds; small so caller fails fast instead of blocking
        self._hot_session = requests.Session()
        # copy critical headers (authorization MUST be present)
        self._hot_session.headers.update(
            {
                "accept": "text/plain",
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.token}",
                "Connection": "keep-alive",
            }
        )
        # no retries for hot session
        hot_retries = Retry(total=0, backoff_factor=0, allowed_methods=frozenset(["POST"]))
        hot_adapter = HTTPAdapter(pool_connections=5, pool_maxsize=10, max_retries=hot_retries)
        self._hot_session.mount("https://", hot_adapter)
        self._hot_session.mount("http://", hot_adapter)

        # Executor for parallel SL/TP placements (if class uses it elsewhere)
        # keep small and shared — avoids creating threads on hot path
        self._executor = ThreadPoolExecutor(max_workers=6)


    def _log(self, msg: str, level: str = "info") -> None:
        fn = getattr(logger, level, logger.info)
        fn(msg)

    # --------------------------
    # Connection warmup
    # --------------------------
    def warmup(self) -> None:
        """
        Pre-open TCP+TLS so the next POST reuses a hot connection.
        Safe even if HEAD returns non-200; we only care about the handshake.
        """
        try:
            self._session.head(self.api_base, timeout=1.5)
            logger.debug("Warmup HEAD completed (TLS ready).")
        except Exception as e:
            logger.debug(f"Warmup HEAD error (ignored): {e}")

    def _hot_post(self, path: str, payload: Dict[str, Any]) -> Tuple[Dict[str, Any], float]:
        """Minimal, fast POST for hot orders — uses _hot_session, no retries."""
        url = f"{self.api_base}{path}"
        start = time.perf_counter()
        # using json= is slightly faster and simpler
        r = self._hot_session.post(url, json=payload, timeout=3.0)
        r.raise_for_status()
        data = r.json()
        if not data.get("success", False):
            code = data.get("errorCode")
            msg = data.get("errorMessage")
            raise RuntimeError(f"API error {code}: {msg}")
        return data, time.perf_counter() - start

    # --- Add near your existing _hot_post/_post helpers in livebroker.py / livebroker2.py ---

    def _hot_place_order(self, path: str, payload: dict, timeout: float = 5.0):
        """
        Hot helper: post using the hot socket path if available, otherwise fallback to normal _post.
        Returns the parsed order response (same as _post would return).
        """
        try:
            # If _hot_post exists and should be used, prefer it.
            if hasattr(self, "_hot_post"):
                return self._hot_post(path, json=payload, timeout=timeout)
            # fallback
            return self._post(path, json=payload, timeout=timeout)
        except Exception:
            # keep existing behaviour (and let caller handle exceptions/logging)
            raise


    def search_trades(self, start: datetime, end: Optional[datetime] = None) -> List[Dict[str, Any]]:
            """
            Fetch trade history for this account between start and (optional) end time.
            Returns a list of trade dicts.
            """
            url = f"{self.api_base}/Trade/search"
            payload = {
                "accountId": self.account_id,
                "startTimestamp": start.isoformat() + "Z",
            }
            if end:
                payload["endTimestamp"] = end.isoformat() + "Z"

            try:
                r = self._session.post(url, json=payload, timeout=self.timeout)
                r.raise_for_status()
                data = r.json()
                if not data.get("success"):
                    logger.warning(f"[LiveBroker] search_trades failed: {data}")
                    return []
                return data.get("trades", [])
            except Exception as e:
                logger.error(f"[LiveBroker] Error in search_trades: {e}", exc_info=True)
                return []

    def _unique_suffix(self) -> str:
        """Generate a short nonce for unique order tags (avoids server dedup)."""
        ms = int(time.time() * 1000) % 1_000_000
        rnd = uuid.uuid4().hex[:4]
        return f"{ms:06d}-{rnd}"

    # --------------------------
    # Core helpers
    # --------------------------
    def _post(self, path: str, payload: Dict[str, Any]) -> Tuple[Dict[str, Any], float]:
        """
        POST helper with precise RTT (seconds) returned for latency logging.
        Raises RuntimeError if the API reports success == false.
        """
        url = f"{self.api_base}{path}"
        start = time.perf_counter()
        r = self._session.post(url, data=json.dumps(payload), timeout=self.timeout)
        r.raise_for_status()
        data = r.json()
        if not data.get("success", False):
            code = data.get("errorCode")
            msg = data.get("errorMessage")
            raise RuntimeError(f"API error {code}: {msg}")
        rtt = time.perf_counter() - start
        return data, rtt

    def _post_soft(self, path: str, payload: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], float]:
        """
        Like _post, but returns (None, rtt) on 404 without noisy logs.
        Any other HTTP error still raises.
        """
        url = f"{self.api_base}{path}"
        start = time.perf_counter()
        r = self._session.post(url, data=json.dumps(payload), timeout=self.timeout)
        if r.status_code == 404:
            return None, time.perf_counter() - start
        r.raise_for_status()
        data = r.json()
        if not data.get("success", False):
            code = data.get("errorCode")
            msg = data.get("errorMessage")
            raise RuntimeError(f"API error {code}: {msg}")
        return data, time.perf_counter() - start


    def _round_to_tick(self, price: float) -> float:
        """Round a price to the nearest valid tick increment."""
        t = self.tick_size
        return round(round(float(price) / t) * t, 10)

    def _side_to_api(self, side: str) -> int:
        """Convert 'BUY'/'SELL' -> API side enum."""
        s = side.upper()
        if s == "BUY":
            return self.SIDE_BUY
        if s == "SELL":
            return self.SIDE_SELL
        raise ValueError(f"Invalid side: {side}")

    # --------------------------
    # Order placement primitives
    # --------------------------
    def place_market_order(
        self,
        side: str,
        size: int,
        custom_tag: Optional[str] = None,
        linked_order_id: Optional[int] = None,
    ) -> Tuple[int, float]:
        """
        Place a market order.
        Returns: (orderId, rtt_seconds)
        """
        payload = {
            "accountId": self.account_id,
            "contractId": self.contract_id,
            "type": self.TYPE_MARKET,
            "side": self._side_to_api(side),
            "size": int(size),
            "limitPrice": None,
            "stopPrice": None,
            "trailPrice": None,
            "customTag": custom_tag,
            "linkedOrderId": linked_order_id,
        }
        data, rtt = self._post("/Order/place", payload)
        order_id = int(data["orderId"])
        logger.info(f"✅ Market order placed: id={order_id}, side={side}, size={size}, rtt_ms={int(rtt*1000)}")
        return order_id, rtt

    def place_market_hot(
        self,
        side: str,
        size: int,
        custom_tag: Optional[str] = None,
        linked_order_id: Optional[int] = None,
    ) -> Tuple[int, float]:
        """
        Hot path for market entry orders. Use this from strategy on candle close.
        Returns: (orderId, rtt_seconds)
        """
        payload = {
            "accountId": self.account_id,
            "contractId": self.contract_id,
            "type": self.TYPE_MARKET,
            "side": self._side_to_api(side),
            "size": int(size),
            "limitPrice": None,
            "stopPrice": None,
            "trailPrice": None,
            "customTag": custom_tag,
            "linkedOrderId": linked_order_id,
        }

        data, rtt = self._hot_post("/Order/place", payload)   # uses hot session & short timeout
        order_id = int(data["orderId"])
        # Keep log short and avoid heavy formatting in hot path
        logger.info(f"✅ Market order placed (hot): id={order_id}, side={side}, size={size}, rtt_ms={int(rtt*1000)}")
        return order_id, rtt


    def place_limit_order(
        self,
        side: str,
        price: float,
        size: int,
        custom_tag: Optional[str] = None,
        linked_order_id: Optional[int] = None,
    ) -> Tuple[int, float]:
        """Place a limit order at a tick-rounded price. Returns (orderId, rtt_seconds)."""
        p = self._round_to_tick(float(price))
        payload = {
            "accountId": self.account_id,
            "contractId": self.contract_id,
            "type": self.TYPE_LIMIT,
            "side": self._side_to_api(side),
            "size": int(size),
            "limitPrice": p,
            "stopPrice": None,
            "trailPrice": None,
            "customTag": custom_tag,
            "linkedOrderId": linked_order_id,
        }
        data, rtt = self._post("/Order/place", payload)
        order_id = int(data["orderId"])
        logger.info(f"✅ Limit order placed: id={order_id}, side={side}, price={p}, size={size}, rtt_ms={int(rtt*1000)}")
        return order_id, rtt

    def place_stop_order(
        self,
        side: str,
        stop_price: float,
        size: int,
        custom_tag: Optional[str] = None,
        linked_order_id: Optional[int] = None,
    ) -> Tuple[int, float]:
        """Place a stop order at a tick-rounded stop price. Returns (orderId, rtt_seconds)."""
        sp = self._round_to_tick(float(stop_price))
        payload = {
            "accountId": self.account_id,
            "contractId": self.contract_id,
            "type": self.TYPE_STOP,
            "side": self._side_to_api(side),
            "size": int(size),
            "limitPrice": None,
            "stopPrice": sp,
            "trailPrice": None,
            "customTag": custom_tag,
            "linkedOrderId": linked_order_id,
        }
        data, rtt = self._post("/Order/place", payload)
        order_id = int(data["orderId"])
        logger.info(f"✅ Stop order placed: id={order_id}, side={side}, stop={sp}, size={size}, rtt_ms={int(rtt*1000)}")
        return order_id, rtt

    # --------------------------
    # Higher-level utilities
    # --------------------------

    def place_exit_orders_from_signal(
        self,
        entry_side: str,
        engulfing_close: float,
        sl_ticks: int,
        tp_ticks: int,
        size: int = 1,
        link_to_entry_id: Optional[int] = None,
        tag_prefix: str = "Strategy",
    ) -> dict:
        """
        Place SL/TP based on the *signal close price* (legacy Strategy 1 behavior).
        This is separate from the OCO bracket-based place_exit_orders() used by reconcile.
        """

        entry = entry_side.upper()
        base = float(engulfing_close)

        if entry == "BUY":
            sl_price = base - (sl_ticks * self.tick_size)
            tp_price = base + (tp_ticks * self.tick_size)
            sl_side, tp_side = "SELL", "SELL"
        elif entry == "SELL":
            sl_price = base + (sl_ticks * self.tick_size)
            tp_price = base - (tp_ticks * self.tick_size)
            sl_side, tp_side = "BUY", "BUY"
        else:
            raise ValueError("entry_side must be BUY or SELL")

        sl_tag = f"{tag_prefix}:SL:{self._unique_suffix()}"
        tp_tag = f"{tag_prefix}:TP:{self._unique_suffix()}"

        results = {}

        with ThreadPoolExecutor(max_workers=2) as ex:
            futs = {
                ex.submit(
                    self.place_stop_order,
                    side=sl_side,
                    stop_price=sl_price,
                    size=size,
                    linked_order_id=link_to_entry_id,
                    custom_tag=sl_tag,
                ): "sl",
                ex.submit(
                    self.place_limit_order,
                    side=tp_side,
                    price=tp_price,
                    size=size,
                    linked_order_id=link_to_entry_id,
                    custom_tag=tp_tag,
                ): "tp",
            }
            for fut in as_completed(futs):
                kind = futs[fut]
                oid, _ = fut.result()
                results[f"{kind}_order_id"] = int(oid)

        logger.info(
            f"⏱️ Exit orders placed (from signal): "
            f"SL={results.get('sl_order_id')} TP={results.get('tp_order_id')} "
            f"base={base}"
        )
        return results

    def place_exit_orders(
        self,
        entry_side: str,
        engulfing_close: float,
        sl_ticks: int,
        tp_ticks: int,
        size: int = 1,
        link_to_entry_id: Optional[int] = None,
        tag_prefix: str = "Strategy1",
    ) -> Dict[str, int]:
        """
        Place SL & TP from the *signal close* (engulfing close) IN PARALLEL.
        Returns: {"sl_order_id": int, "tp_order_id": int}
        """
        entry = entry_side.upper()
        base = float(engulfing_close)

        if entry == "BUY":
            # long entry -> SL/TP are SELL side
            sl_price = base - (sl_ticks * self.tick_size)
            tp_price = base + (tp_ticks * self.tick_size)
            sl_side = "SELL"
            tp_side = "SELL"
        elif entry == "SELL":
            # short entry -> SL/TP are BUY side
            sl_price = base + (sl_ticks * self.tick_size)
            tp_price = base - (tp_ticks * self.tick_size)
            sl_side = "BUY"
            tp_side = "BUY"
        else:
            raise ValueError("entry_side must be BUY or SELL")

        # Unique nonce per pair so server doesn't de-dupe by identical tags
        ms = int(time.time() * 1000) % 1_000_000
        rnd = uuid.uuid4().hex[:4]
        nonce = f"{ms:06d}-{rnd}"
        sl_tag = f"{tag_prefix}:SL:{nonce}"
        tp_tag = f"{tag_prefix}:TP:{nonce}"

        results: Dict[str, int] = {}
        latencies_ms: Dict[str, int] = {}

        with ThreadPoolExecutor(max_workers=2) as ex:
            futures = {
                ex.submit(
                    self.place_stop_order,
                    side=sl_side,
                    stop_price=sl_price,
                    size=size,
                    linked_order_id=link_to_entry_id,
                    custom_tag=sl_tag,
                ): "sl",
                ex.submit(
                    self.place_limit_order,
                    side=tp_side,
                    price=tp_price,
                    size=size,
                    linked_order_id=link_to_entry_id,
                    custom_tag=tp_tag,
                ): "tp",
            }
            for fut in as_completed(futures):
                kind = futures[fut]
                oid, rtt = fut.result()
                results[f"{kind}_order_id"] = int(oid)
                latencies_ms[kind] = int(rtt * 1000)

        logger.info(
            f"⏱️ Exit orders placed in parallel: SL={results.get('sl_order_id')} ({latencies_ms.get('sl')} ms), "
            f"TP={results.get('tp_order_id')} ({latencies_ms.get('tp')} ms) | tags sl='{sl_tag}' tp='{tp_tag}'"
        )
        return results

    # --------------------------
    # Order/position management
    # --------------------------
    def cancel_order(self, order_id: int) -> None:
        payload = {"accountId": self.account_id, "orderId": int(order_id)}
        _, rtt = self._post("/Order/cancel", payload)
        # Tolerate "already cancelled/expired" by API but keep log simple here
        logger.info(f"🛑 Order canceled id={order_id}, rtt_ms={int(rtt*1000)}")

    def modify_order(
        self,
        order_id: int,
        size: Optional[int] = None,
        limit_price: Optional[float] = None,
        stop_price: Optional[float] = None,
        trail_price: Optional[float] = None,
    ) -> None:
        payload: Dict[str, Any] = {
            "accountId": self.account_id,
            "orderId": int(order_id),
            "size": int(size) if size is not None else None,
            "limitPrice": self._round_to_tick(limit_price) if limit_price is not None else None,
            "stopPrice": self._round_to_tick(stop_price) if stop_price is not None else None,
            "trailPrice": trail_price,
        }
        _, rtt = self._post("/Order/modify", payload)
        logger.info(
            f"✏️ Order modified id={order_id} size={size} "
            f"limit={limit_price} stop={stop_price} rtt_ms={int(rtt*1000)}"
        )

    def close_position(self, account_id: Optional[int] = None, contract_id: Optional[str] = None) -> None:
        """
        Close *all* size for a contract on an account.
        Backward-compatible: defaults to the LiveBroker's own account/contract.
        """
        acct = int(account_id if account_id is not None else self.account_id)
        cid = str(contract_id if contract_id is not None else self.contract_id)
        payload = {"accountId": acct, "contractId": cid}
        _, rtt = self._post("/Position/closeContract", payload)
        logger.info(f"❎ Position close request sent (accountId={acct}, contractId={cid}). rtt_ms={int(rtt*1000)}")

    # --------------------------
    # Reconnect helpers (safe to call only on cold start / reconnection)
    # --------------------------
    def fetch_account_positions_orders(self) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Fetch latest account info, open positions, and open orders for this account.
        Returns: (account_info | None, positions[], orders[])
        """
        acct_info: Optional[Dict[str, Any]] = None
        positions: List[Dict[str, Any]] = []
        orders: List[Dict[str, Any]] = []

        # --- Account ---
        try:
            url = f"{self.api_base}/Account/search"
            payload = {"onlyActiveAccounts": False}
            r = self._session.post(url, json=payload, timeout=self.timeout)
            r.raise_for_status()
            data = r.json()
            if data.get("success"):
                accounts = data.get("accounts") or []
                acct_info = next((a for a in accounts if int(a.get("id", -1)) == self.account_id), None)
        except Exception as e:
            logger.debug(f"[LiveBroker] fetch account error: {e}", exc_info=True)

        # --- Positions ---
        try:
            url = f"{self.api_base}/Position/searchOpen"
            payload = {"accountId": self.account_id}
            r = self._session.post(url, json=payload, timeout=self.timeout)
            r.raise_for_status()
            data = r.json()
            if data.get("success"):
                positions = data.get("positions", []) or []
        except Exception as e:
            logger.error(f"[LiveBroker] fetch positions error: {e}", exc_info=True)

        # --- Orders ---
        try:
            url = f"{self.api_base}/Order/searchOpen"
            payload = {"accountId": self.account_id}
            r = self._session.post(url, json=payload, timeout=self.timeout)
            r.raise_for_status()
            data = r.json()
            if data.get("success"):
                orders = data.get("orders", []) or []
        except Exception as e:
            logger.error(f"[LiveBroker] fetch orders error: {e}", exc_info=True)

        return acct_info, positions, orders

    def cancel_all_for_contract(self) -> None:
        """
        Convenience helper: fetch working orders and cancel those matching this.contract_id.
        Intended for use during reconcile after reconnect (not in the hot path).
        """
        try:
            _, _, orders = self.fetch_account_positions_orders()
            if not orders:
                return
            to_cancel: List[int] = []
            for o in orders:
                try:
                    status = o.get("status")
                    is_active = status in (self.STATUS_NEW, self.STATUS_WORKING, self.STATUS_PARTIAL)
                    contract_id = str(o.get("contractId") or "")
                    if is_active and contract_id == self.contract_id:
                        to_cancel.append(int(o.get("id")))
                except Exception:
                    continue

            for oid in to_cancel:
                try:
                    self.cancel_order(oid)
                except Exception as e:
                    msg = str(e)
                    # API error 5 ("already cancelled/expired") is benign
                    if "error 5" in msg or '"errorCode":5' in msg:
                        logger.info(f"[reconcile] Order {oid} already inactive (ok).")
                    else:
                        logger.warning(f"[reconcile] Cancel failed for {oid}: {e}")
        except Exception as e:
            logger.warning(f"[reconcile] cancel_all_for_contract failed: {e}")

    def fetch_open_positions(self) -> list[dict]:
        """
        Fetch all open positions for this broker's account.
        Returns a list of normalized dicts with keys:
        {id, accountId, contractId, size, averagePrice, raw}
        """
        url = f"{self.api_base}/Position/searchOpen"
        payload = {"accountId": self.account_id}
        try:
            r = self._session.post(url, json=payload, timeout=self.timeout)
            r.raise_for_status()
            raw_positions = r.json().get("positions", []) or []
        except Exception as e:
            logger.error(f"❌ fetch_open_positions failed: {e}", exc_info=True)
            return []

        normalized = []
        for p in raw_positions:
            try:
                normalized.append({
                    "id": int(p.get("id")),
                    "accountId": int(p.get("accountId", self.account_id)),
                    "contractId": str(p.get("contractId")),
                    "size": int(p.get("size", 0)),
                    "averagePrice": float(p.get("averagePrice", 0.0)),
                    "raw": p
                })
            except Exception:
                continue
        return normalized

    def fetch_open_orders(self) -> list[dict]:
        """
        Fetch all open orders for this broker's account.
        Returns a list of normalized dicts with keys:
        {id, accountId, contractId, status, side, size, price, raw}
        """
        url = f"{self.api_base}/Order/searchOpen"
        payload = {"accountId": self.account_id}
        try:
            r = self._session.post(url, json=payload, timeout=self.timeout)
            r.raise_for_status()
            raw_orders = r.json().get("orders", []) or []
        except Exception as e:
            logger.error(f"❌ fetch_open_orders failed: {e}", exc_info=True)
            return []

        normalized = []
        for o in raw_orders:
            try:
                side_val = o.get("side")
                side = "buy" if side_val in (0, "BUY", "buy") else "sell"
                price = (
                    o.get("price")
                    or o.get("limitPrice")
                    or o.get("stopPrice")
                    or o.get("filledPrice")
                )
                normalized.append({
                    "id": int(o.get("id")),
                    "accountId": int(o.get("accountId", self.account_id)),
                    "contractId": str(o.get("contractId")),
                    "status": int(o.get("status", 0)),
                    "side": side,
                    "size": int(o.get("size", 0)),
                    "price": float(price) if price else None,
                    "raw": o
                })
            except Exception:
                continue
        return normalized

    def recon_fetch_positions(self, account_id: int) -> List[Dict[str, Any]]:
        """
        [RECONCILE] Return all open positions for the given account_id.
        API: POST /Position/searchOpen

        Adds:
          - 'type' (raw API field, 1=Long, 2=Short)
          - 'side' normalized from 'type' -> 'BUY' or 'SELL'
        """
        try:
            payload = {"accountId": int(account_id)}
            data, _ = self._post("/Position/searchOpen", payload)
            positions = data.get("positions", []) or []
            out = []
            for p in positions:
                ptype = p.get("type")  # 1=Long, 2=Short
                if ptype == 1:
                    pos_side = "BUY"
                elif ptype == 2:
                    pos_side = "SELL"
                else:
                    pos_side = None  # unknown/unset

                out.append({
                    "id": int(p["id"]),
                    "accountId": int(p["accountId"]),
                    "contractId": str(p["contractId"]),
                    "size": int(p["size"]),
                    "averagePrice": float(p["averagePrice"]),
                    "type": ptype,                       # keep raw
                    "side": pos_side,                    # normalized
                    "creationTimestamp": p.get("creationTimestamp"),
                })
            self._log(f"[recon_fetch_positions] acct={account_id} -> {len(out)}")
            return out
        except Exception as e:
            self._log(f"[recon_fetch_positions] ERROR acct={account_id}: {e}", level="error")
            return []


    def recon_fetch_orders(self, account_id: int, contract_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        [RECONCILE] Return all OPEN orders for the given account_id (optional contract filter).
        API: POST /Order/searchOpen

        Adds:
          - 'sideStr' normalized from numeric side -> 'BUY'/'SELL' (0/1)
        """
        try:
            payload = {"accountId": int(account_id)}
            data, _ = self._post("/Order/searchOpen", payload)
            orders = data.get("orders", []) or []

            out = []
            for o in orders:
                if contract_id and str(o.get("contractId")) != str(contract_id):
                    continue
                side_num = int(o["side"])
                side_str = "BUY" if side_num == self.SIDE_BUY else "SELL"

                out.append({
                    "id": int(o["id"]),
                    "accountId": int(o["accountId"]),
                    "contractId": str(o["contractId"]),
                    "status": int(o["status"]),
                    "type": int(o["type"]),
                    "side": side_num,
                    "sideStr": side_str,  # normalized for convenience
                    "size": int(o["size"]),
                    "limitPrice": (float(o["limitPrice"]) if o.get("limitPrice") is not None else None),
                    "stopPrice": (float(o["stopPrice"]) if o.get("stopPrice") is not None else None),
                    "trailPrice": (float(o["trailPrice"]) if o.get("trailPrice") is not None else None),
                    "creationTimestamp": o.get("creationTimestamp"),
                    "updateTimestamp": o.get("updateTimestamp"),
                })
            self._log(f"[recon_fetch_orders] acct={account_id} contract={contract_id or '*'} -> {len(out)}")
            return out
        except Exception as e:
            self._log(f"[recon_fetch_orders] ERROR acct={account_id} contract={contract_id}: {e}", level="error")
            return []


    def recon_cancel_all_orders(self, account_id: int, contract_id: Optional[str] = None) -> int:
        """
        [RECONCILE] Cancel all OPEN orders for the account (optionally limited to a contract).
        API: POST /Order/cancel
        Returns: number of successfully cancelled orders
        """
        try:
            open_orders = self.recon_fetch_orders(account_id=account_id, contract_id=contract_id)
            cancelled = 0
            for o in open_orders:
                try:
                    # cancel_order uses self.account_id already; only pass order id
                    self.cancel_order(o["id"])  # <-- fixed
                    cancelled += 1
                except Exception as inner:
                    self._log(
                        f"[recon_cancel_all_orders] cancel failed acct={account_id} "
                        f"order_id={o['id']}: {inner}",
                        level="error",
                    )

            self._log(
                f"[recon_cancel_all_orders] acct={account_id} contract={contract_id or '*'} "
                f"cancelled={cancelled}/{len(open_orders)}"
            )
            return cancelled
        except Exception as e:
            self._log(f"[recon_cancel_all_orders] ERROR acct={account_id} contract={contract_id}: {e}", level="error")
            return 0



    def recon_place_exit_orders(
        self,
        entry_side: str,
        engulfing_close: float,
        sl_ticks: int,
        tp_ticks: int,
        size: int = 1,
        link_to_entry_id: Optional[int] = None,
        tag_prefix: str = "S1",
    ) -> Dict[str, int]:
        """
        Place SL & TP from the *signal close* (engulfing close) IN PARALLEL
        during reconciliation period.

        Returns: {"sl_order_id": int, "tp_order_id": int}
        """
        entry = entry_side.upper()
        base = float(engulfing_close)

        if entry == "BUY":
            sl_price = base - (sl_ticks * self.tick_size)
            tp_price = base + (tp_ticks * self.tick_size)
            sl_side, tp_side = "SELL", "SELL"
        elif entry == "SELL":
            sl_price = base + (sl_ticks * self.tick_size)
            tp_price = base - (tp_ticks * self.tick_size)
            sl_side, tp_side = "BUY", "BUY"
        else:
            raise ValueError("entry_side must be BUY or SELL")

        # Unique nonce so server doesn’t dedupe by identical tags
        ms = int(time.time() * 1000) % 1_000_000
        rnd = uuid.uuid4().hex[:4]
        nonce = f"{ms:06d}-{rnd}"
        sl_tag = f"{tag_prefix}:RECON:SL:{nonce}"
        tp_tag = f"{tag_prefix}:RECON:TP:{nonce}"

        results: Dict[str, int] = {}
        latencies_ms: Dict[str, int] = {}

        logger.info(
            f"🧩 Livebroker1-[RECON] placing exits: entry_side={entry_side}, close={base}, "
            f"SL={sl_price}, TP={tp_price}, size={size}, link_to={link_to_entry_id}"
        )

        with ThreadPoolExecutor(max_workers=2) as ex:
            futures = {
                ex.submit(
                    self.place_stop_order,
                    side=sl_side,
                    stop_price=sl_price,
                    size=size,
                    linked_order_id=link_to_entry_id,
                    custom_tag=sl_tag,
                ): "sl",
                ex.submit(
                    self.place_limit_order,
                    side=tp_side,
                    price=tp_price,
                    size=size,
                    linked_order_id=link_to_entry_id,
                    custom_tag=tp_tag,
                ): "tp",
            }
            for fut in as_completed(futures):
                kind = futures[fut]
                oid, rtt = fut.result()
                results[f"{kind}_order_id"] = int(oid)
                latencies_ms[kind] = int(rtt * 1000)

        logger.info(
            f"🧩 [RECON] exits placed: SL={results.get('sl_order_id')} "
            f"({latencies_ms.get('sl')} ms), TP={results.get('tp_order_id')} "
            f"({latencies_ms.get('tp')} ms) | tags sl='{sl_tag}' tp='{tp_tag}'"
        )
        return results
