# livebroker2.py — Broker isolated for Strategy 2
# - Keeps the same public interface as livebroker.LiveBroker
# - Thin wrappers: place_market / place_limit / place_stop -> (ok, order_id, rtt)
# - Convenience: place_exit_orders_from_fill (prices from actual *fill* price)
# - Adds reconnect helpers: fetch_account_positions_orders, cancel_all_for_contract

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Optional, Dict, Any, Tuple, List
from datetime import datetime

import threading


import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger("LiveBroker2")


class LiveBroker:
    """
    Handles REST calls to TopStepX API for Strategy 2.

    Notes
    -----
    - All endpoints require a valid JWT bearer token (24h lifetime).
    - Token is obtained in app.py and passed in here.
    - Synchronous class; SL/TP placements are done concurrently via threads.

    New in this version
    -------------------
    - warmup(): pre-opens TCP/TLS so the next POST reuses a hot connection.
    - place_exit_orders(): unique tag nonce to avoid collisions on server.
    - close_position(account_id=None, contract_id=None): backward compatible.
    - fetch_account_positions_orders(): explicit pull for reconnect reconcile.
    - cancel_all_for_contract(): purge stale working orders after reconnect.
    """

    API_BASE = "https://api.topstepx.com/api"

    # TopStepX enums
    TYPE_LIMIT = 1
    TYPE_MARKET = 2
    TYPE_STOP = 4

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

        # Reusable session + headers + tuned adapter
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

    # --------------------------
    # Connection warmup
    # --------------------------
    def warmup(self) -> None:
        """
        Pre-open TCP+TLS so the next POST reuses a hot connection.
        Safe even if HEAD returns 404; we only care about the handshake.
        """
        try:
            self._session.head(self.api_base, timeout=1.5)
            logger.debug("Warmup HEAD completed (TLS ready).")
        except Exception as e:
            logger.debug(f"Warmup HEAD error (ignored): {e}")

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




    def search_trades(self, start: datetime, end: Optional[datetime] = None):
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
            r = self._session.post(
                url,
                data=json.dumps(payload),
                timeout=self.timeout,
            )
            r.raise_for_status()
            data = r.json()
            if not data.get("success"):
                logger.warning(f"[LiveBroker2] search_trades failed: {data}")
                return []
            return data.get("trades", [])
        except Exception as e:
            logger.error(f"[LiveBroker2] Error in search_trades: {e}", exc_info=True)
            return []


    # --------------------------
    # Helpers
    # --------------------------
    def _post(self, path: str, payload: Dict[str, Any]) -> Tuple[Dict[str, Any], float]:
        url = f"{self.api_base}{path}"
        start = time.perf_counter()
        try:
            r = self._session.post(url, data=json.dumps(payload), timeout=self.timeout)
            r.raise_for_status()
            data = r.json()
            if not data.get("success", False):
                code = data.get("errorCode")
                msg = data.get("errorMessage")
                logger.error(f"API error on {path} (code={code}): {msg} | payload={payload}")
                raise RuntimeError(f"API error {code}: {msg}")
            rtt = time.perf_counter() - start
            return data, rtt
        except Exception as e:
            # Log raw text too for non-JSON errors
            try:
                body = r.text[:500] if 'r' in locals() else '(no response)'
            except Exception:
                body = '(unavailable)'
            logger.error(f"_post failed {path} payload={payload} error={e} body={body}", exc_info=True)
            raise

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

    def _unique_suffix(self) -> str:
        """
        Generate a short, unique suffix for customTag to satisfy
        TopStepX's per-account uniqueness requirement.
        Example: "483921-3f7a"
        """
        ms = int(time.time() * 1000) % 1_000_000  # last 6 digits of ms
        rnd = uuid.uuid4().hex[:4]
        return f"{ms:06d}-{rnd}"

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
        Place a market order with preference for a hot (pre-warmed) POST if available.

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

        # Prefer _hot_post if provided (hot socket reuse). Fall back to _post for compatibility.
        post_fn = getattr(self, "_hot_post", None)
        used = "_hot_post"
        try:
            if post_fn is None:
                # no _hot_post available, use standard _post
                data, rtt = self._post("/Order/place", payload)
                used = "_post"
            else:
                # try common signature: (path, payload)
                try:
                    data, rtt = post_fn("/Order/place", payload)
                except TypeError:
                    # some implementations may accept just payload, or different args:
                    # try payload-only as last resort
                    data, rtt = post_fn(payload)
        except Exception as exc:
            logger.exception(f"❗ Failed to place market order via {used}: {exc}")
            raise

        order_id = int(data["orderId"])
        logger.info(
            f"✅ [S2]Market order placed: id={order_id}, side={side}, size={size}, rtt_ms={int(rtt*1000)} (via={used})"
        )
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
    def place_exit_orders(
        self,
        entry_side: str,
        engulfing_close: float,
        sl_ticks: int,
        tp_ticks: int,
        size: int = 1,
        link_to_entry_id: Optional[int] = None,
        tag_prefix: str = "Strategy",
    ) -> Dict[str, int]:
        """
        Place SL & TP from a provided 'close' (legacy helper, used by Strategy 1).
        Returns: {"sl_order_id": int, "tp_order_id": int}

        NOTE: customTag must be unique per account. We auto-append a short nonce to avoid collisions.
        """
        entry = entry_side.upper()
        base = float(engulfing_close)

        if entry == "BUY":
            sl_price = base - (sl_ticks * self.tick_size)
            tp_price = base + (tp_ticks * self.tick_size)
            sl_side = "SELL"
            tp_side = "SELL"
        elif entry == "SELL":
            sl_price = base + (sl_ticks * self.tick_size)
            tp_price = base - (tp_ticks * self.tick_size)
            sl_side = "BUY"
            tp_side = "BUY"
        else:
            raise ValueError("entry_side must be BUY or SELL")

        # Unique nonce to keep tags distinct on server
        nonce = self._unique_suffix()
        sl_tag = f"{tag_prefix}:SL:{nonce}"
        tp_tag = f"{tag_prefix}:TP:{nonce}"

        results: Dict[str, int] = {}
        lat_ms: Dict[str, int] = {}

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
                oid, rtt = fut.result()
                results[f"{kind}_order_id"] = int(oid)
                lat_ms[kind] = int(rtt * 1000)

        logger.info(
            f"⏱️ Exit orders placed in parallel: SL={results.get('sl_order_id')} ({lat_ms.get('sl')} ms), "
            f"TP={results.get('tp_order_id')} ({lat_ms.get('tp')} ms) | "
            f"tags sl='{sl_tag}' tp='{tp_tag}' base={base}"
        )
        return results

    def place_exit_orders_from_fill(
        self,
        entry_side: str,
        fill_price: float,
        sl_ticks: int,
        tp_ticks: int,
        size: int = 1,
        link_to_entry_id: Optional[int] = None,
        tag_prefix: str = "Strategy2",
    ) -> Dict[str, int]:
        """
        Place SL & TP computed from the *actual fill price*.
        Exactly like place_exit_orders, but the semantic makes it clear for Strategy 2.
        """
        return self.place_exit_orders(
            entry_side=entry_side,
            engulfing_close=float(fill_price),
            sl_ticks=sl_ticks,
            tp_ticks=tp_ticks,
            size=size,
            link_to_entry_id=link_to_entry_id,
            tag_prefix=tag_prefix,
        )

    # --------------------------
    # Order/position management
    # --------------------------
    def cancel_order(self, order_id: int) -> None:
        payload = {"accountId": self.account_id, "orderId": int(order_id)}
        _, rtt = self._post("/Order/cancel", payload)
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
        Backward-compatible: defaults to this broker's own account/contract.
        """
        acct = int(account_id if account_id is not None else self.account_id)
        cid = str(contract_id if contract_id is not None else self.contract_id)
        payload = {"accountId": acct, "contractId": cid}
        _, rtt = self._post("/Position/closeContract", payload)
        logger.info(f"❎ Position close request sent (accountId={acct}, contractId={cid}). rtt_ms={int(rtt*1000)}")

    # --------------------------
    # Reconnect helpers
    # --------------------------
    def fetch_account_positions_orders(
        self
    ) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
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
            logger.debug(f"[LiveBroker2] fetch account error: {e}", exc_info=True)

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
            logger.error(f"[LiveBroker2] fetch positions error: {e}", exc_info=True)

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
            logger.error(f"[LiveBroker2] fetch orders error: {e}", exc_info=True)

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
                    if "error 5" in msg or '"errorCode":5' in msg:
                        logger.info(f"[reconcile] Order {oid} already inactive (ok).")
                    else:
                        logger.warning(f"[reconcile] Cancel failed for {oid}: {e}")
        except Exception as e:
            logger.warning(f"[reconcile] cancel_all_for_contract failed: {e}")

    # --------------------------
    # Thin wrappers for Strategy 2
    # --------------------------
    def place_market(self, side: str, size: int, custom_tag: Optional[str] = None) -> Tuple[bool, int, float]:
        """
        Strategy 2 wrapper: place a market order.
        Returns: (ok, order_id, rtt_seconds)
        """
        try:
            order_id, rtt = self.place_market_order(side=side, size=size, custom_tag=custom_tag)
            return True, int(order_id), float(rtt)
        except Exception as e:
            logger.error(f"[Broker2] place_market failed: {e}", exc_info=True)
            return False, -1, 0.0

    def place_limit(
        self,
        side: str,
        price: float,
        size: int,
        custom_tag: Optional[str] = None,
        linked_order_id: Optional[int] = None,
    ) -> Tuple[bool, int, float]:
        """
        Strategy 2 wrapper: place a limit order.
        Returns: (ok, order_id, rtt_seconds)
        """
        try:
            order_id, rtt = self.place_limit_order(
                side=side,
                price=price,
                size=size,
                custom_tag=custom_tag,
                linked_order_id=linked_order_id,
            )
            return True, int(order_id), float(rtt)
        except Exception as e:
            logger.error(f"[Broker2] place_limit failed: {e}", exc_info=True)
            return False, -1, 0.0

    def place_stop(
        self,
        side: str,
        stop_price: float,
        size: int,
        custom_tag: Optional[str] = None,
        linked_order_id: Optional[int] = None,
    ) -> Tuple[bool, int, float]:
        """
        Strategy 2 wrapper: place a stop order.
        Returns: (ok, order_id, rtt_seconds)
        """
        try:
            order_id, rtt = self.place_stop_order(
                side=side,
                stop_price=stop_price,
                size=size,
                custom_tag=custom_tag,
                linked_order_id=linked_order_id,
            )
            return True, int(order_id), float(rtt)
        except Exception as e:
            logger.error(f"[Broker2] place_stop failed: {e}", exc_info=True)
            return False, -1, 0.0




    def wait_for_fill(
        self,
        entry_order_id: int,
        timeout: float = 5.0,
        poll_interval: float = 0.02,
        account_id: Optional[int] = None,
        contract_id: Optional[str] = None,
    ) -> Optional[Dict]:
        """
        Robust wait_for_fill that strictly honors account_id and contract_id hints.

        Returns dict like {"avgPrice": float, "filled": int, "source": "gateway|rest|position_field", "raw_pos": {...}}
        or None on timeout.

        Important behavior:
         - If account_id is provided, positions without an accountId field are rejected (conservative).
         - If contract_id is provided, contractId must equal it.
         - Normalizes several field names produced by various brokers.
        """
        deadline = time.time() + float(timeout)
        result_event = threading.Event()
        result_holder = {"result": None}
        lock = threading.Lock()

        # Normalize hints early
        acct_hint = None
        if account_id is not None:
            try:
                acct_hint = int(account_id)
            except Exception:
                try:
                    acct_hint = int(str(account_id))
                except Exception:
                    acct_hint = None

        contract_hint = None
        if contract_id is not None:
            contract_hint = str(contract_id)

        # Helper: normalize a single incoming position dict -> result dict or None
        def _pos_to_result(pos):
            if not isinstance(pos, dict):
                return None
            # Some gateway wrappers embed raw payload under 'raw'
            pdata = pos.get("raw", pos) if isinstance(pos, dict) else pos

            # Enforce account match if hint provided. Conservative: reject if accountId missing.
            if acct_hint is not None:
                p_acct_val = pdata.get("accountId")
                if p_acct_val is None:
                    # skip positions missing accountId when we were asked to filter by account
                    try:
                        self._log_info(f"wait_for_fill: skipping pos without accountId while looking for acct={acct_hint}")
                    except Exception:
                        pass
                    return None
                try:
                    p_acct_i = int(p_acct_val)
                except Exception:
                    return None
                if p_acct_i != acct_hint:
                    try:
                        self._log_info(f"wait_for_fill: skipping pos acct={p_acct_i} (looking for {acct_hint})")
                    except Exception:
                        pass
                    return None

            # Enforce contract match if hint provided. If contractId missing -> reject.
            if contract_hint is not None:
                p_contract = pdata.get("contractId")
                if p_contract is None or str(p_contract) != contract_hint:
                    return None

            # Extract average/size fields with flexible keys
            avg = (
                pdata.get("averagePrice")
                or pdata.get("avgPrice")
                or pdata.get("avg_price")
                or pdata.get("average_price")
            )
            size = pdata.get("size") or pdata.get("filled") or pdata.get("filled_qty") or pdata.get("quantity")

            if avg is None:
                return None
            try:
                avg_f = float(avg)
            except Exception:
                return None
            try:
                filled_i = int(size) if size is not None else None
            except Exception:
                filled_i = None

            # Provide a helpful source hint when available
            src = pdata.get("source") or pdata.get("type") or "position_field"
            return {"avgPrice": avg_f, "filled": filled_i, "source": src, "raw_pos": pdata}

        # FAST PATH: poll app.latest_positions (if app attached to broker)
        def _gateway_poll():
            while not result_event.is_set() and time.time() < deadline:
                try:
                    app = getattr(self, "app", None)
                    # some brokers attach app to live_broker rather than self
                    if not app:
                        app = getattr(self, "live_broker", None)
                        if app:
                            app = getattr(app, "app", None) or getattr(app, "latest_positions", None) and getattr(self, "app", None)
                    if not app:
                        return
                    latest = getattr(app, "latest_positions", None)
                    if latest:
                        lst = latest if isinstance(latest, list) else [latest]
                        for p in lst:
                            try:
                                res = _pos_to_result(p)
                            except Exception:
                                res = None
                            if res:
                                with lock:
                                    if not result_event.is_set():
                                        # attach a stable source label
                                        if "source" not in res:
                                            res["source"] = "gateway"
                                        result_holder["result"] = res
                                        result_event.set()
                                        return
                    time.sleep(poll_interval)
                except Exception:
                    time.sleep(poll_interval)

        # FALLBACK: use REST fetchers already present in this class
        def _rest_fetch():
            while not result_event.is_set() and time.time() < deadline:
                try:
                    positions = None
                    # prefer fetch_open_positions (returns normalized list)
                    if hasattr(self, "fetch_open_positions"):
                        try:
                            positions = self.fetch_open_positions()
                        except Exception:
                            positions = None

                    # second option: fetch_account_positions_orders -> (acct, positions, orders)
                    if not positions and hasattr(self, "fetch_account_positions_orders"):
                        try:
                            acct_info, pos_list, orders = self.fetch_account_positions_orders()
                            positions = pos_list or []
                        except Exception:
                            positions = None

                    # last resort recon helper (recon_fetch_positions)
                    if not positions and hasattr(self, "recon_fetch_positions"):
                        try:
                            # prefer passing acct_hint if available, else self.account_id
                            acct_for_recon = acct_hint if acct_hint is not None else getattr(self, "account_id", None)
                            positions = self.recon_fetch_positions(acct_for_recon)
                        except Exception:
                            positions = None

                    if not positions:
                        time.sleep(max(poll_interval, 0.05))
                        continue

                    pos_list = (
                        positions
                        if isinstance(positions, list)
                        else (positions.get("positions") if isinstance(positions, dict) else [positions])
                    )
                    for p in pos_list:
                        try:
                            res = _pos_to_result(p)
                        except Exception:
                            res = None
                        if res:
                            with lock:
                                if not result_event.is_set():
                                    # mark source as rest when coming from REST
                                    if "source" not in res or not res.get("source"):
                                        res["source"] = "rest"
                                    result_holder["result"] = res
                                    result_event.set()
                                    return
                    time.sleep(max(poll_interval, 0.05))
                except Exception:
                    time.sleep(max(poll_interval, 0.05))

        # Start threads
        t1 = threading.Thread(target=_gateway_poll, name="wait_for_fill_gateway", daemon=True)
        t2 = threading.Thread(target=_rest_fetch, name="wait_for_fill_rest", daemon=True)
        t1.start()
        t2.start()

        # Wait until deadline or found
        remaining = deadline - time.time()
        while remaining > 0 and not result_event.is_set():
            result_event.wait(timeout=min(0.1, remaining))
            remaining = deadline - time.time()

        if result_event.is_set():
            return result_holder.get("result")
        return None


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
            logger.error(f"❌ [LB2] fetch_open_positions failed: {e}", exc_info=True)
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
        {id, accountId, contractId, symbol, status, side, size, price, raw}
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
                    "symbol": str(o.get("contractId")),   # <-- add symbol for logging compatibility
                    "status": int(o.get("status", 0)),
                    "side": side,
                    "size": int(o.get("size", 0)),
                    "price": float(price) if price else None,
                    "raw": o
                })
            except Exception:
                continue
        return normalized

    # --------------------------
    # RECONNECT HELPERS (RECONCILE-ONLY API)
    # --------------------------
    # --- Reconnect helpers (LB2) -----------------------------------------------

    def _log(self, msg: str, level: str = "info"):
        getattr(logger, level)(f"[LB2]{msg}")

    def recon_fetch_positions(self, account_id: int) -> List[Dict[str, Any]]:
        """
        [RECONCILE] Return all open positions for the given account_id.
        API: POST /Position/searchOpen
        """
        try:
            payload = {"accountId": int(account_id)}
            data, _ = self._post("/Position/searchOpen", payload)
            positions = data.get("positions", []) or []

            out: List[Dict[str, Any]] = []
            for p in positions:
                ptype = p.get("type")  # 1=Long, 2=Short
                side = "BUY" if ptype == 1 else ("SELL" if ptype == 2 else None)
                out.append({
                    "id": int(p["id"]),
                    "accountId": int(p["accountId"]),
                    "contractId": str(p["contractId"]),
                    "size": int(p["size"]),
                    "averagePrice": float(p["averagePrice"]),
                    "type": ptype,
                    "side": side,  # normalized from type
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
        """
        try:
            payload = {"accountId": int(account_id)}
            data, _ = self._post("/Order/searchOpen", payload)
            orders = data.get("orders", []) or []

            out: List[Dict[str, Any]] = []
            for o in orders:
                if contract_id and str(o.get("contractId")) != str(contract_id):
                    continue
                out.append({
                    "id": int(o["id"]),
                    "accountId": int(o["accountId"]),
                    "contractId": str(o["contractId"]),
                    "status": int(o["status"]),
                    "type": int(o["type"]),
                    "side": int(o["side"]),   # API enum (0=BUY, 1=SELL)
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
                    self.cancel_order(o["id"])
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
        tag_prefix: str = "S2",
    ) -> Dict[str, int]:
        """
        [RECONCILE] Place SL & TP from the provided base price (use avg fill price on cold start).
        Executes SL/TP in parallel with unique tags. Returns: {"sl_order_id": int, "tp_order_id": int}
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

        nonce = self._unique_suffix()
        sl_tag = f"{tag_prefix}:RECON:SL:{nonce}"
        tp_tag = f"{tag_prefix}:RECON:TP:{nonce}"

        results: Dict[str, int] = {}
        latencies_ms: Dict[str, int] = {}

        logger.info(
            f"🧩 [LB2][RECON] placing exits: entry_side={entry_side}, base={base}, "
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
            f"🧩 [LB2][RECON] exits placed: SL={results.get('sl_order_id')} ({latencies_ms.get('sl')} ms), "
            f"TP={results.get('tp_order_id')} ({latencies_ms.get('tp')} ms) | "
            f"tags sl='{sl_tag}' tp='{tp_tag}'"
        )
        return results



