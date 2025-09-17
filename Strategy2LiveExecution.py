# Strategy2LiveExecution.py
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
import threading
from collections import deque

# If you use these locks elsewhere keep the import; harmless if unused here.
from global_locks import acquire_reconcile_lock, release_reconcile_lock

from livebroker2 import LiveBroker as LiveBroker2

logger = logging.getLogger("Strategy2")


# =========================
# Local config & thresholds
# =========================
S2_DEFAULTS = {
    "ACCOUNT_ID":     11503107,
    "CONTRACT_ID":    "CON.F.US.MNQ.Z25",
    "TIMEFRAME":      "5m",
    "TICK_SIZE":      0.25,
    "CONTRACT_SIZE":  2,
    "SL_TICKS_LONG":  100,
    "TP_TICKS_LONG":  40,
    "SL_TICKS_SHORT": 100,
    "TP_TICKS_SHORT": 80,
    "MIN_ENGULF_TICKS_BULL": 20,
    "MIN_ENGULF_TICKS_BEAR": 0,
}


def _side_to_api(side: str) -> str:
    """Return API-side string for a strategy side ('long'|'short')."""
    return "BUY" if side.lower() == "long" else "SELL"


class Strategy2LiveExecution:
    """
    Strategy2 - copy/paste-ready:
      - Engulfing signals -> market entries
      - Place SL/TP from average fill price (avgPrice)
      - Fast reversal: double-sized market to flip
      - Recon kept as originally provided (unchanged logic, only logging annotated)
    """

    # Mirror some order status codes for reconcile logic
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
        name: str = "S2",
        account_id: Optional[int] = None,
        contract_id: Optional[str] = None,
        timeframe: Optional[str] = None,
        tick_size: Optional[float] = None,
        contract_size: Optional[int] = None,
        sl_ticks_long: Optional[int] = None,
        tp_ticks_long: Optional[int] = None,
        sl_ticks_short: Optional[int] = None,
        tp_ticks_short: Optional[int] = None,
        min_engulf_bull: Optional[int] = None,
        min_engulf_bear: Optional[int] = None,
        api_base: Optional[str] = None,
    ) -> None:
        # Strategy identity
        self.name = name

        # Config
        self.account_id = int(account_id or S2_DEFAULTS["ACCOUNT_ID"])
        self.contract_id = str(contract_id or S2_DEFAULTS["CONTRACT_ID"])
        self.strategy_timeframe = str(timeframe or S2_DEFAULTS["TIMEFRAME"])
        self.tick_size = float(tick_size or S2_DEFAULTS["TICK_SIZE"])
        self.contract_size = int(contract_size or S2_DEFAULTS["CONTRACT_SIZE"])

        self.sl_long = int(sl_ticks_long if sl_ticks_long is not None else S2_DEFAULTS["SL_TICKS_LONG"])
        self.tp_long = int(tp_ticks_long if tp_ticks_long is not None else S2_DEFAULTS["TP_TICKS_LONG"])
        self.sl_short = int(sl_ticks_short if sl_ticks_short is not None else S2_DEFAULTS["SL_TICKS_SHORT"])
        self.tp_short = int(tp_ticks_short if tp_ticks_short is not None else S2_DEFAULTS["TP_TICKS_SHORT"])

        self.min_engulf_bull = int(min_engulf_bull if min_engulf_bull is not None else S2_DEFAULTS["MIN_ENGULF_TICKS_BULL"])
        self.min_engulf_bear = int(min_engulf_bear if min_engulf_bear is not None else S2_DEFAULTS["MIN_ENGULF_TICKS_BEAR"])

        # Broker
        self.live_broker = LiveBroker2(
            jwt_token=jwt_token,
            account_id=self.account_id,
            contract_id=self.contract_id,
            tick_size=self.tick_size,
            api_base=api_base,
        )
        self.live_broker.warmup()

        # Active trade (single) state
        self.active_trade: Dict[str, Any] = {
            "entry_order_id": None,
            "sl_order_id": None,
            "tp_order_id": None,
            "side": None,                # "long"/"short"
            "size": 0,                   # net confirmed size from hub
            "entry_signal_price": None,  # candle close that triggered the entry
            "fill_price": None,          # position.averagePrice (used for SL/TP)
            "tag_base": None,            # unique per trade (scopes SL/TP tags)
        }

        # Flags kept in sync by hub updates
        self.has_open_position = False
        self.has_open_orders = False

        # Concurrency primitives
        self.trade_lock = threading.RLock()
        self.pending_exits = deque()
        self.pending_exits_lock = threading.Lock()

        #
        # concurrency / proactive worker tracking (add to __init__)
        self.active_proactive_workers = set()
        self.active_proactive_workers_lock = threading.Lock()
        self.proactive_worker_events = {}              # entry_id -> threading.Event (heads-up 'working' flag)
        self.proactive_worker_events_lock = threading.Lock()


        # Coordination condition (uses self.trade_lock as its lock so checks+waits are atomic)
        self.exits_condition = threading.Condition(self.trade_lock)

        # Optional housekeeping flags in active_trade (ensure consistent keys exist)
        # active_trade already created; ensure keys exist:
        self.active_trade.setdefault("exits_in_progress", False)
        self.active_trade.setdefault("exits_in_progress_by", None)
        self.active_trade.setdefault("exits_in_progress_started_at", None)
        self.active_trade.setdefault("exits_placement_result", None)


        # Recon helpers
        self.in_recon = False
        self.recon_until_ts = 0.0
        self._recon_lock = None

        # Hubs readiness flags (may be set by app)
        self.userhub_ready = getattr(self, "userhub_ready", True)
        self.markethub_ready = getattr(self, "markethub_ready", True)

        #procative worker threads
        self.active_proactive_workers = set()
        self.active_proactive_workers_lock = threading.Lock()


        # Defaults for proactive worker timing (can be adjusted externally)
        self.fill_wait_timeout = getattr(self, "fill_wait_timeout", 5.0)
        self.fill_poll_interval = getattr(self, "fill_poll_interval", 0.02)
        self.proactive_exit = getattr(self, "proactive_exit", True)

        self._log_info(
            f"Init | acct={self.account_id} contract={self.contract_id} "
            f"tf={self.strategy_timeframe} tick={self.tick_size} size={self.contract_size} "
            f"SL/TP ticks long={self.sl_long}/{self.tp_long} short={self.sl_short}/{self.tp_short}"
        )

    # -------------------------
    # Small logging helpers to always include thread & strategy
    # -------------------------
    def _prefix(self) -> str:
        return f"[{self.name}] | thread={threading.current_thread().name} - "

    def _log_info(self, msg: str, *args, **kwargs) -> None:
        logger.info(self._prefix() + msg, *args, **kwargs)

    def _log_warning(self, msg: str, *args, **kwargs) -> None:
        logger.warning(self._prefix() + msg, *args, **kwargs)

    def _log_error(self, msg: str, *args, **kwargs) -> None:
        logger.error(self._prefix() + msg, *args, **kwargs)

    def _log_exception(self, msg: str, *args, **kwargs) -> None:
        logger.exception(self._prefix() + msg, *args, **kwargs)

    # =========================
    # Utility helpers
    # =========================

    def _now_tag(self, suffix: str = "") -> str:
        # Tag scopes all orders per trade; suffix for reversals
        t = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")
        base = f"{self.name}:{self.contract_id}:{t}"
        return f"{base}:{suffix}" if suffix else base

    def _cancel_if_exists(self, order_id: Optional[int]) -> None:
        if not order_id:
            return
        try:
            self.live_broker.cancel_order(int(order_id))
            self._log_info(f"Cancelled order {order_id}")
        except Exception as e:
            msg = str(e)
            if "error 5" in msg or "errorCode\":5" in msg:
                self._log_info(f"Cancel order {order_id}: already inactive (ok).")
            else:
                self._log_error(f"cancel_order error for {order_id}: {e}")
                self._log_exception("exception detail")

    def _compute_engulf(self, prev: Dict[str, Any], curr: Dict[str, Any]) -> Tuple[bool, bool]:
        o1, h1, l1, c1 = float(prev["open"]), float(prev["high"]), float(prev["low"]), float(prev["close"])
        c2 = float(curr["close"])
        prev_bearish = c1 < o1
        prev_bullish = c1 > o1

        bullish_engulfing = (prev_bearish and (c2 > (h1 + self.min_engulf_bull * self.tick_size)))
        bearish_engulfing = (prev_bullish and (c2 < (l1 - self.min_engulf_bear * self.tick_size)))

        self._log_info(
            f"Check Engulfing | prev_bull={prev_bullish} prev_bear={prev_bearish} "
            f"Bearish engulfing={bearish_engulfing} Bullish engulfing={bullish_engulfing}"
        )
        return bearish_engulfing, bullish_engulfing

    def _make_tag_base_from_entry(self, entry_id: Optional[int]) -> str:
        """
        Build canonical tag base. If entry_id available, include it; otherwise fallback to timestamp.
        Format: "{strategy_prefix}:{contract_id}:{entry_id_or_timestamp}"
        Example: "S2:CON.F.US.MNQ.Z25:1604524627" or "S2:CON.F.US.MNQ.Z25:20250916144700009888"
        """
        try:
            strategy = getattr(self, "name", "S2")
        except Exception:
            strategy = "S2"
        try:
            cid = str(getattr(self, "contract_id", getattr(self.live_broker, "contract_id", "UNKNOWN")))
        except Exception:
            cid = "UNKNOWN"
        if entry_id:
            return f"{strategy}:{cid}:{int(entry_id)}"
        # fallback deterministic timestamp string (same format as previous _now_tag maybe used)
        ts = int(time.time() * 1000)
        return f"{strategy}:{cid}:{ts}"

    def _normalize_tag_base(self, raw_tag: Optional[str]) -> Optional[str]:
        """
        Normalize a tag for prefix comparison.
        Removes known suffixes like :REPAIRED, :RECON, and strips trailing :SL:nonce or :TP:nonce or -SL-... variants.
        Returns the canonical prefix portion up to the entry id (if present).
        """
        if not raw_tag:
            return raw_tag
        try:
            s = str(raw_tag)
            # remove suffix markers we add in code
            s = s.split(":REPAIRED")[0]
            s = s.split(":RECON")[0]
            s = s.split(":REPAIRED")[0]
            # remove known "-SL-" or "-TP-" patterns -> split on those hyphen patterns
            # but first handle colon style suffixes like ":SL:nonce" or ":TP:nonce"
            # For colon-style, split and take up to the token that looks like the entry id or the canonical base
            parts = re.split(r"(:SL:|:TP:|-SL-|-TP-)", s, maxsplit=1)
            if parts:
                s = parts[0]
            # strip trailing nonce fragments after final colon if it's clearly not part of base (nonce contains '-')
            # e.g. "S2:CON...:1604524627:SL:026965-ecd9" -> parts[0] yields "S2:CON...:1604524627"
            s = s.rstrip(":").rstrip("-")
            return s
        except Exception:
            try:
                return str(raw_tag)
            except Exception:
                return raw_tag

    def _tag_with_suffix(self, base: str, suffix: str) -> str:
        """
        Build tag for passing to broker if you want explicit suffix style.
        Not required with LB2 (it will append :SL/:TP and a nonce), but useful if you need a consistent tag format.
        """
        if not base:
            return f"{getattr(self, 'name', 'S2')}:{getattr(self, 'contract_id', 'UNK')}:{int(time.time())}:{suffix}"
        # keep simple; broker will still append its nonce so don't duplicate
        return f"{base}:{suffix}"

    def _extract_entry_id_from_any(self, obj: Any) -> Optional[int]:
        """
        Try to extract an entry order id from a variety of objects:
          - If obj is dict and has numeric fields (orderId, order_id, linkedOrderId, linked_order_id, linkedOrder)
          - If obj is a tag string that includes a numeric token at the end or near end (split by ":" or "-")
        Returns int or None.
        """
        try:
            # if dict-like, check common numeric keys
            if isinstance(obj, dict):
                for k in ("orderId", "order_id", "id", "linkedOrderId", "linked_order_id", "linkedOrder", "entryOrderId", "entry_order_id"):
                    if obj.get(k) is not None:
                        try:
                            return int(obj.get(k))
                        except Exception:
                            # some APIs return stringable ints
                            try:
                                return int(str(obj.get(k)))
                            except Exception:
                                continue
                # sometimes the linked id lives in nested raw payload
                raw = obj.get("raw") if isinstance(obj.get("raw"), dict) else None
                if raw:
                    return self._extract_entry_id_from_any(raw)
            # if string-like, parse numeric tokens
            if obj is None:
                return None
            s = str(obj)
            # attempt to find a numeric token of reasonable length (>=5 digits)
            tokens = re.split(r"[:\-_\s]", s)
            for tok in reversed(tokens):  # prefer rightmost tokens (where entry id typically is)
                if tok.isdigit() and len(tok) >= 4:
                    try:
                        return int(tok)
                    except Exception:
                        continue
            # fallback: find any group of digits
            m = re.search(r"(\d{4,})", s)
            if m:
                try:
                    return int(m.group(1))
                except Exception:
                    return None
        except Exception:
            return None
        return None

    # =========================
    # Central Fill Worker (used by entry, position update, order update)
    # =========================
    def _place_exits_from_fill_worker(self, entry_id_local: int, entry_side_local: str, tag_base_local: str, signal_price_local: float):
        """
        Proactive exits from fill worker (updated to set canonical tag_base including entry id).
        """
        entry_key = int(entry_id_local) if entry_id_local is not None else int(time.time())
        event = threading.Event()

        # register event so handle_position_update can see we're working
        try:
            with self.proactive_worker_events_lock:
                self.proactive_worker_events[entry_key] = event
                event.set()
        except Exception:
            try:
                self.proactive_worker_events[entry_key] = event
                event.set()
            except Exception:
                pass

        try:
            th_name = threading.current_thread().name
            self._log_info(f"_place_exits_from_fill_worker started entry={entry_id_local} signal={signal_price_local}")

            # Defensive: entry id may be None or invalid; worker will still attempt probing & fallback.
            filled_price = None
            filled_size = None
            filled_source = None

            timeout = float(getattr(self, "fill_wait_timeout", 5.0))
            poll_interval = float(getattr(self, "fill_poll_interval", 0.02))

            # Step 1: broker.wait_for_fill (authoritative, fast)
            if hasattr(self.live_broker, "wait_for_fill"):
                try:
                    acct_hint = getattr(self.live_broker, "account_id", None) or self.account_id
                    contract_hint = getattr(self, "contract_id", None)
                    wf = getattr(self.live_broker, "wait_for_fill")
                    info = wf(entry_id_local, timeout=timeout, poll_interval=poll_interval, account_id=acct_hint, contract_id=contract_hint)
                    if isinstance(info, dict):
                        filled_price = info.get("avgPrice") or info.get("averagePrice") or info.get("price")
                        filled_size = info.get("filled") or info.get("size")
                        if filled_price is not None:
                            filled_source = info.get("source") if info.get("source") else "broker.wait_for_fill"
                            self._log_info(f"Got avgPrice from broker.wait_for_fill -> {filled_price} (src={filled_source})")
                except Exception as e:
                    self._log_info(f"broker.wait_for_fill failed/returned nothing for entry {entry_id_local}: {e}")
                    filled_price = None
                    filled_size = None
                    filled_source = None

            # Step 2: fallback probing (gateway cache + REST) if still empty
            if filled_price is None:
                deadline = time.time() + timeout
                result_event = threading.Event()
                result_holder = {"result": None}
                lock = threading.Lock()

                acct_hint = int(getattr(self.live_broker, "account_id", self.account_id))
                contract_hint = str(getattr(self, "contract_id"))

                def _probe_gateway():
                    app = getattr(self.live_broker, "app", None)
                    while not result_event.is_set() and time.time() < deadline:
                        try:
                            if not app:
                                return
                            latest = getattr(app, "latest_positions", None)
                            if latest:
                                iterable = latest if isinstance(latest, list) else [latest]
                                for p in iterable:
                                    pdata = p.get("raw", p) if isinstance(p, dict) else p
                                    try:
                                        p_acct = int(pdata.get("accountId", -1)) if pdata.get("accountId") is not None else -1
                                    except Exception:
                                        p_acct = -1
                                    p_contract = pdata.get("contractId")
                                    if p_acct != acct_hint:
                                        continue
                                    if str(p_contract) != str(contract_hint):
                                        continue
                                    avg = pdata.get("averagePrice") or pdata.get("avgPrice")
                                    sz = pdata.get("size")
                                    if avg not in (None, 0):
                                        with lock:
                                            if not result_event.is_set():
                                                result_holder["result"] = {"avgPrice": float(avg), "filled": int(sz) if sz else None, "source": "gateway", "raw_pos": pdata}
                                                result_event.set()
                                                self._log_info(f"🟢 Got avgPrice {avg} from GatewayUserPosition")
                                                return
                            time.sleep(poll_interval)
                        except Exception:
                            time.sleep(poll_interval)

                def _probe_rest():
                    while not result_event.is_set() and time.time() < deadline:
                        try:
                            pos_list = None
                            if hasattr(self.live_broker, "fetch_open_positions"):
                                try:
                                    pos_list = self.live_broker.fetch_open_positions()
                                except Exception:
                                    pos_list = None
                            if not pos_list and hasattr(self.live_broker, "fetch_account_positions_orders"):
                                try:
                                    _acct_info, pos_list, _ords = self.live_broker.fetch_account_positions_orders()
                                except Exception:
                                    pos_list = None
                            if not pos_list and hasattr(self.live_broker, "recon_fetch_positions"):
                                try:
                                    pos_list = self.live_broker.recon_fetch_positions(acct_hint)
                                except Exception:
                                    pos_list = None

                            if pos_list:
                                iterable = pos_list if isinstance(pos_list, list) else (pos_list.get("positions") if isinstance(pos_list, dict) else [pos_list])
                                for p in iterable:
                                    try:
                                        p_acct = int(p.get("accountId", -1)) if p.get("accountId") is not None else -1
                                    except Exception:
                                        p_acct = -1
                                    p_contract = p.get("contractId")
                                    if p_acct != acct_hint:
                                        continue
                                    if str(p_contract) != str(contract_hint):
                                        continue
                                    avg = p.get("averagePrice") or p.get("avgPrice")
                                    sz = p.get("size")
                                    if avg not in (None, 0):
                                        with lock:
                                            if not result_event.is_set():
                                                result_holder["result"] = {"avgPrice": float(avg), "filled": int(sz) if sz else None, "source": "rest", "raw_pos": p}
                                                result_event.set()
                                                self._log_info(f"🟢 Got avgPrice {avg} from REST fetch")
                                                return
                            time.sleep(max(poll_interval, 0.05))
                        except Exception:
                            time.sleep(max(poll_interval, 0.05))

                th_g = threading.Thread(target=_probe_gateway, name=f"wait_gateway_{entry_id_local}", daemon=True)
                th_r = threading.Thread(target=_probe_rest, name=f"wait_rest_{entry_id_local}", daemon=True)
                th_g.start(); th_r.start()

                remaining = deadline - time.time()
                while remaining > 0 and not result_event.is_set():
                    result_event.wait(timeout=min(0.1, remaining))
                    remaining = deadline - time.time()

                if result_event.is_set():
                    r = result_holder.get("result") or {}
                    filled_price = r.get("avgPrice")
                    filled_size = r.get("filled")
                    filled_source = r.get("source", "probe")
                    self._log_info(f"Probing result: avgPrice={filled_price} (src={filled_source})")

            # Step 3: Trade/search CROSS-CHECK (prefer trade avg if possible)
            def _compute_weighted_avg_from_trades(trades):
                total_qty = 0.0
                weighted_sum = 0.0
                found = 0
                for t in trades:
                    try:
                        p = t.get("price") if isinstance(t.get("price"), (int, float)) else float(t.get("price"))
                        q = t.get("size") or t.get("quantity") or 1
                        qf = float(q)
                        weighted_sum += float(p) * qf
                        total_qty += qf
                        found += 1
                    except Exception:
                        continue
                if total_qty == 0:
                    return None, 0
                return (weighted_sum / total_qty), found

            def _fetch_trades_via_broker(acct, contract, start_ts_iso, end_ts_iso=None):
                try_methods = [
                    ("fetch_trades", (acct, start_ts_iso, end_ts_iso)),
                    ("trade_search", (acct, start_ts_iso, end_ts_iso)),
                    ("fetch_account_trades", (acct, start_ts_iso, end_ts_iso)),
                    ("fetch_recent_trades", (acct, contract, start_ts_iso, end_ts_iso)),
                ]
                for m, args in try_methods:
                    if hasattr(self.live_broker, m):
                        try:
                            fn = getattr(self.live_broker, m)
                            res = fn(*args) if isinstance(args, tuple) else fn(args)
                            if res:
                                if isinstance(res, dict) and res.get("trades"):
                                    return res.get("trades")
                                if isinstance(res, list):
                                    return res
                                if isinstance(res, dict):
                                    for k in ("data", "items", "trades"):
                                        if isinstance(res.get(k), list):
                                            return res.get(k)
                        except Exception:
                            continue

                # Last-resort low-level REST attempts
                try:
                    payload = {
                        "accountId": int(getattr(self.live_broker, "account_id", acct)),
                        "startTimestamp": start_ts_iso,
                    }
                    if end_ts_iso:
                        payload["endTimestamp"] = end_ts_iso
                    if hasattr(self.live_broker, "api_post"):
                        try:
                            resp = self.live_broker.api_post("/api/Trade/search", json=payload)
                            if isinstance(resp, dict) and resp.get("trades"):
                                return resp.get("trades")
                            if hasattr(resp, "json"):
                                data = resp.json()
                                if isinstance(data, dict) and data.get("trades"):
                                    return data.get("trades")
                        except Exception:
                            pass
                    if hasattr(self.live_broker, "session") and getattr(self.live_broker, "base_url", None):
                        try:
                            sess = getattr(self.live_broker, "session")
                            url = getattr(self.live_broker, "base_url").rstrip("/") + "/api/Trade/search"
                            r = sess.post(url, json=payload, timeout=3.0)
                            data = r.json()
                            if isinstance(data, dict) and data.get("trades"):
                                return data.get("trades")
                        except Exception:
                            pass
                except Exception:
                    pass

                return None

            try:
                now_dt = datetime.utcnow()
                start_dt = now_dt - timedelta(seconds=30)
                end_dt = now_dt + timedelta(seconds=5)
                start_iso = start_dt.isoformat() + "Z"
                end_iso = end_dt.isoformat() + "Z"

                acct_for_trades = int(getattr(self.live_broker, "account_id", self.account_id))
                contract_for_trades = str(getattr(self, "contract_id", getattr(self.live_broker, "contract_id", None) or ""))

                trades = _fetch_trades_via_broker(acct_for_trades, contract_for_trades, start_iso, end_iso)
                if trades is None:
                    self._log_info("Trade.search cross-check not available (no broker trade fetch).")
                else:
                    filtered = []
                    for t in trades:
                        try:
                            t_order = t.get("orderId") or t.get("order_id") or t.get("order")
                            t_acct = int(t.get("accountId", acct_for_trades)) if t.get("accountId") is not None else acct_for_trades
                            t_contract = t.get("contractId") or t.get("contract")
                            if t_acct != acct_for_trades:
                                continue
                            if entry_id_local and t_order and int(t_order) == int(entry_id_local):
                                filtered.append(t)
                            else:
                                filtered.append(t)
                        except Exception:
                            continue

                    if filtered:
                        trade_avg, cnt = _compute_weighted_avg_from_trades(filtered)
                        if trade_avg is not None and cnt > 0:
                            prev_filled = filled_price
                            filled_price = float(trade_avg)
                            filled_size = sum((int(t.get("size") or 0) for t in filtered if t.get("size"))) if filtered else filled_size
                            filled_source = "trade_search"
                            self._log_info(f"🟢 Trade-based avg computed={filled_price} from {cnt} trade records (prev_candidate={prev_filled})")
            except Exception as e:
                self._log_info(f"Trade cross-check failed/ignored: {e}")

            # Step 4: Last resort: use signal price
            if filled_price is None:
                filled_price = float(signal_price_local)
                filled_source = "fallback_signal_price"
                self._log_warning(f"Fill not observed within timeout; using signal_price {signal_price_local} as fallback for entry {entry_id_local}.")

            # choose ticks
            if entry_side_local is None:
                entry_side_local = "UNKNOWN"
            if entry_side_local.upper() in ("BUY", "LONG"):
                sl_ticks = self.sl_long
                tp_ticks = self.tp_long
            else:
                sl_ticks = self.sl_short
                tp_ticks = self.tp_short

            # dedupe & place exits (check local state under lock)
            try:
                with self.trade_lock:
                    if self.active_trade.get("sl_order_id") or self.active_trade.get("tp_order_id"):
                        self._log_info(f"Exits already present for entry {entry_id_local}; skipping proactive placement.")
                        return
            except Exception:
                if self.active_trade.get("sl_order_id") or self.active_trade.get("tp_order_id"):
                    self._log_info(f"Exits already present for entry {entry_id_local}; skipping proactive placement.")
                    return

            # Build canonical tag_base including entry id when available
            try:
                canonical_tag_base = self._make_tag_base_from_entry(entry_id_local)
                # persist canonical base locally BEFORE placing orders so handle_position_update can match
                try:
                    with self.trade_lock:
                        self.active_trade["tag_base"] = canonical_tag_base
                except Exception:
                    self.active_trade["tag_base"] = canonical_tag_base
                tag_prefix_local = canonical_tag_base
            except Exception:
                tag_prefix_local = (self.active_trade.get("tag_base") or tag_base_local) + ":REPAIRED"

            # --- START placement with discrepancy guard ---
            try:
                es = str(entry_side_local).lower() if entry_side_local is not None else ""
                is_long = es in ("long", "buy", "b", "buy_market", "buy-limit", "buy-limit")
                is_short = es in ("short", "sell", "s", "sell_market", "sell-limit", "sell-limit")

                tick_size = float(getattr(self, "tick_size", getattr(self, "contract_tick", 0.25)))

                base_fill = float(filled_price) if filled_price is not None else float(signal_price_local)
                sl_ticks_i = int(sl_ticks)
                tp_ticks_i = int(tp_ticks)

                try:
                    ticks_diff = abs(base_fill - float(signal_price_local)) / (tick_size or 1.0)
                    max_mismatch = float(getattr(self, "max_fill_signal_ticks_mismatch", 10))
                    if ticks_diff > max_mismatch:
                        self._log_warning(
                            f"Large fill vs signal mismatch: fill={base_fill}, signal={signal_price_local} ({ticks_diff:.1f} ticks) "
                            f"for entry {entry_id_local}. Queuing pending_exits instead of placing exits."
                        )
                        payload = {
                            "tag_prefix": tag_prefix_local,
                            "base_price": base_fill,
                            "sl_ticks": sl_ticks,
                            "tp_ticks": tp_ticks,
                            "entry_id": entry_id_local,
                            "entry_side": entry_side_local,
                            "attempted_at": datetime.utcnow().isoformat(),
                            "reason": "mismatch_fill_vs_signal",
                            "fill_source": filled_source,
                        }
                        try:
                            with self.pending_exits_lock:
                                self.pending_exits.append(payload)
                            self._log_info(f"Queued pending exit payload for entry {entry_id_local} due to mismatch.")
                        except Exception as exq:
                            self._log_exception(f"Failed to queue pending exit for entry {entry_id_local}: {exq}")
                        return
                except Exception:
                    self._log_info("Discrepancy guard check failed; continuing placement (best-effort).")

                if is_long:
                    computed_sl = base_fill - sl_ticks_i * tick_size
                    computed_tp = base_fill + tp_ticks_i * tick_size
                elif is_short:
                    computed_sl = base_fill + sl_ticks_i * tick_size
                    computed_tp = base_fill - tp_ticks_i * tick_size
                else:
                    self._log_exception(f"Unknown entry_side for proactive placement: {entry_side_local!r}. Queuing for retry.")
                    payload = {
                        "tag_prefix": tag_prefix_local,
                        "base_price": base_fill,
                        "sl_ticks": sl_ticks,
                        "tp_ticks": tp_ticks,
                        "entry_id": entry_id_local,
                        "entry_side": entry_side_local,
                        "attempted_at": datetime.utcnow().isoformat(),
                    }
                    try:
                        with self.pending_exits_lock:
                            self.pending_exits.append(payload)
                        self._log_info(f"Queued pending exit payload for entry {entry_id_local} (unknown side).")
                    except Exception as exq:
                        self._log_exception(f"Failed to queue pending exit for entry {entry_id_local}: {exq}")
                    raise RuntimeError("Unknown entry_side - queued pending exit")

                try:
                    filled_src_val = filled_source
                except NameError:
                    filled_src_val = "worker_probe"

                self._log_info(
                    f"[WORKER-PLACER] computed exits (src={filled_src_val}) | side={'long' if is_long else 'short'} "
                    f"fill={base_fill} tick={tick_size} sl_ticks={sl_ticks_i} tp_ticks={tp_ticks_i} "
                    f"computed_sl={computed_sl} computed_tp={computed_tp} tag_prefix={tag_prefix_local}"
                )

                if is_long:
                    ok = (computed_sl < base_fill) and (computed_tp > base_fill)
                else:
                    ok = (computed_tp < base_fill) and (computed_sl > base_fill)

                res = None
                if not ok:
                    self._log_exception(
                        f"Invalid exit prices computed (not monotonic) for side={'long' if is_long else 'short'}; "
                        f"computed_sl={computed_sl} computed_tp={computed_tp} fill={base_fill}. Queuing pending exit."
                    )
                    try:
                        payload = {
                            "tag_prefix": tag_prefix_local,
                            "base_price": base_fill,
                            "sl_ticks": sl_ticks,
                            "tp_ticks": tp_ticks,
                            "entry_id": entry_id_local,
                            "entry_side": entry_side_local,
                            "attempted_at": datetime.utcnow().isoformat(),
                        }
                        with self.pending_exits_lock:
                            self.pending_exits.append(payload)
                        self._log_info(f"Queued pending exit payload for entry {entry_id_local} (invalid computed prices).")
                    except Exception as exq:
                        self._log_exception(f"Failed to queue pending exit for entry {entry_id_local}: {exq}")
                    res = None
                else:
                    try:
                        # Prefer the broker helper that takes fill_price
                        if hasattr(self.live_broker, "place_exit_orders_from_fill"):
                            res = self.live_broker.place_exit_orders_from_fill(
                                entry_side=entry_side_local,
                                fill_price=float(filled_price),
                                sl_ticks=int(sl_ticks),
                                tp_ticks=int(tp_ticks),
                                size=int(getattr(self, "contract_size", self.contract_size)),
                                link_to_entry_id=entry_id_local,
                                tag_prefix=tag_prefix_local,
                            )
                        else:
                            # fallback to place_exit_orders (legacy)
                            res = self.live_broker.place_exit_orders(
                                entry_side=entry_side_local,
                                engulfing_close=float(filled_price),
                                sl_ticks=int(sl_ticks),
                                tp_ticks=int(tp_ticks),
                                size=int(getattr(self, "contract_size", self.contract_size)),
                                link_to_entry_id=entry_id_local,
                                tag_prefix=tag_prefix_local,
                            )
                    except Exception as e:
                        self._log_exception(f"Broker exit placement call failed: {e}")
                        res = None

                    # Latch ids, set fill price, set metadata (under trade_lock if available)
                    try:
                        with self.trade_lock:
                            if isinstance(res, dict):
                                self.active_trade["sl_order_id"] = res.get("sl_order_id") or self.active_trade.get("sl_order_id")
                                self.active_trade["tp_order_id"] = res.get("tp_order_id") or self.active_trade.get("tp_order_id")
                            try:
                                if filled_price is not None:
                                    self.active_trade["fill_price"] = float(filled_price)
                            except Exception:
                                pass
                            self.active_trade["exits_placed_at"] = time.time()
                            self.active_trade["exits_placed_from"] = filled_src_val
                            self.has_open_orders = bool(self.active_trade.get("sl_order_id") or self.active_trade.get("tp_order_id"))
                    except Exception:
                        try:
                            if isinstance(res, dict):
                                self.active_trade["sl_order_id"] = res.get("sl_order_id") or self.active_trade.get("sl_order_id")
                                self.active_trade["tp_order_id"] = res.get("tp_order_id") or self.active_trade.get("tp_order_id")
                            try:
                                if filled_price is not None:
                                    self.active_trade["fill_price"] = float(filled_price)
                            except Exception:
                                pass
                            self.active_trade["exits_placed_at"] = time.time()
                            self.active_trade["exits_placed_from"] = filled_src_val
                            self.has_open_orders = bool(self.active_trade.get("sl_order_id") or self.active_trade.get("tp_order_id"))
                        except Exception:
                            pass

                if res:
                    self._log_info(f"✅ Proactive SL/TP submitted: {res} (src={filled_src_val})")
                else:
                    pass

            except Exception as e:
                self._log_exception(f"Proactive exit placement failed, queuing for retry: {e}")
                try:
                    payload = {
                        "tag_prefix": tag_prefix_local,
                        "base_price": float(filled_price) if filled_price is not None else float(signal_price_local),
                        "sl_ticks": sl_ticks,
                        "tp_ticks": tp_ticks,
                        "entry_id": entry_id_local,
                        "entry_side": entry_side_local,
                        "attempted_at": datetime.utcnow().isoformat(),
                    }
                    with self.pending_exits_lock:
                        self.pending_exits.append(payload)
                    self._log_info(f"Queued pending exit payload for entry {entry_id_local}")
                except Exception as exq:
                    self._log_exception(f"Failed to queue pending exit for entry {entry_id_local}: {exq}")

        except Exception as e:
            self._log_exception(f"Unexpected error in proactive exit worker for entry {entry_id_local}: {e}")
            try:
                payload = {
                    "tag_prefix": f"{self.name}:{self.contract_id}:{int(time.time())}:FAILED",
                    "base_price": signal_price_local,
                    "sl_ticks": getattr(self, "sl_long", None),
                    "tp_ticks": getattr(self, "tp_long", None),
                    "entry_id": entry_id_local,
                    "entry_side": entry_side_local,
                    "attempted_at": datetime.utcnow().isoformat(),
                }
                with self.pending_exits_lock:
                    self.pending_exits.append(payload)
                self._log_info(f"Queued failure payload for entry {entry_id_local}")
            except Exception:
                pass

        finally:
            # signal finished: clear and remove the event so handle_position_update can proceed
            try:
                with self.proactive_worker_events_lock:
                    ev = self.proactive_worker_events.get(entry_key)
                    if ev:
                        try:
                            ev.clear()
                        except Exception:
                            pass
                        try:
                            del self.proactive_worker_events[entry_key]
                        except Exception:
                            self.proactive_worker_events.pop(entry_key, None)
            except Exception:
                try:
                    ev = self.proactive_worker_events.get(entry_key)
                    if ev:
                        try:
                            ev.clear()
                        except Exception:
                            pass
                        try:
                            del self.proactive_worker_events[entry_key]
                        except Exception:
                            self.proactive_worker_events.pop(entry_key, None)
                except Exception:
                    pass


    # =========================
    # Candle entry point
    # =========================
    def on_new_candles(self, symbol: str, timeframe: str, candles: List[Dict[str, Any]]) -> None:
        """
        Called by app.py with a short history of closed candles.
        Mapping:
            bearish_engulfing => LONG entry
            bullish_engulfing => SHORT entry
        """
        th_name = threading.current_thread().name
        self._log_info(f"on_new_candles invoked | symbol={symbol} tf={timeframe} | candles={len(candles)}")

        if symbol != self.contract_id or timeframe != self.strategy_timeframe:
            return
        if len(candles) < 2:
            return

        prev, curr = candles[-2], candles[-1]
        try:
            bearish_engulfing, bullish_engulfing = self._compute_engulf(prev, curr)
        except Exception as e:
            self._log_exception(f"Bad candle data: {e}")
            return

        # Reversal path when already in a position
        if self.has_open_position and self.active_trade.get("side"):
            if self.active_trade["side"] == "long" and bullish_engulfing:
                self._log_info(f"[Reversal] Long -> Short via double-sized market.")
                self.flip_by_double_market(new_side="short", signal_price=float(curr["close"]))
                return
            if self.active_trade["side"] == "short" and bearish_engulfing:
                self._log_info(f"[Reversal] Short -> Long via double-sized market.")
                self.flip_by_double_market(new_side="long", signal_price=float(curr["close"]))
                return

        # Flat entry (skip if any active orders/position)
        if self.active_trade["size"] > 0 or self.has_open_position or self.has_open_orders:
            return

        if bearish_engulfing:
            self._log_info(f"SIGNAL => LONG (bearish_engulf detected)")
            self.enter_trade("long", float(curr["close"]))
        elif bullish_engulfing:
            self._log_info(f"SIGNAL => SHORT (bullish_engulf detected)")
            self.enter_trade("short", float(curr["close"]))

    # =========================
    # Entry / Reversal
    # =========================
    def enter_trade(self, side: str, signal_price: float) -> None:
        """
        Place a market entry immediately (size = CONTRACT_SIZE).
        Tracks state and — if proactive_exit enabled — spawns the fill worker.
        """
        # guard unchanged from your working version
        if self.active_trade.get("size", 0) > 0 or getattr(self, "has_open_position", False):
            self._log_warning("Attempt to enter while active trade exists; skipping.")
            return

        entry_side_api = "BUY" if side == "long" else "SELL"
        tag_base = self._now_tag()
        th_name = threading.current_thread().name
        self._log_info(f"ENTRY_SEND_START {self.name} at {time.time():.3f}")

        # call the broker and normalize return shapes
        try:
            res = self.live_broker.place_market(
                side=entry_side_api,
                size=self.contract_size,
                custom_tag=f"{tag_base}:ENTRY",
            )
        except Exception as e:
            self._log_exception(f"Error calling place_market: {e}")
            return

        # Normalize (ok, id, rtt) or (id, rtt) or dict or id
        ok = True
        entry_order_id = None
        rtt = 0.0
        try:
            if isinstance(res, (tuple, list)):
                if len(res) == 3:
                    ok, entry_order_id, rtt = res
                elif len(res) == 2:
                    entry_order_id, rtt = res
                elif len(res) == 1:
                    entry_order_id = res[0]
                else:
                    raise ValueError(f"Unexpected tuple/list length from place_market: {len(res)}")
            elif isinstance(res, dict):
                ok = res.get("ok", True)
                entry_order_id = res.get("orderId") or res.get("id") or res.get("order_id")
                rtt = res.get("rtt", 0.0)
            else:
                # single primitive (order id)
                entry_order_id = int(res)
                rtt = 0.0
        except Exception as e:
            self._log_exception(f"Unable to interpret place_market response: {res!r} error={e}")
            return

        if ok is False:
            self._log_error("ENTRY market failed.")
            return

        try:
            entry_order_id = int(entry_order_id)
        except Exception:
            self._log_error(f"entry_order_id not coercible to int: {entry_order_id!r}")
            return

        try:
            rtt = float(rtt)
        except Exception:
            rtt = 0.0

        self._log_info(
            f"ENTRY MARKET sent id={entry_order_id} rtt_ms={int(rtt*1000)} @signal_close={signal_price}"
        )

        # Track state exactly as your original function did (size=0 / fill_price=None)
        with self.trade_lock:
            self.active_trade.update({
                "entry_order_id": int(entry_order_id),
                "sl_order_id": None,
                "tp_order_id": None,
                "side": side,
                "size": 0,
                "entry_signal_price": float(signal_price),
                "fill_price": None,
                "tag_base": tag_base,
            })
            # Keep same semantics: has_open_orders False means "entry outstanding, exits not placed yet"
            self.has_open_orders = False

        # If proactive mode is OFF, return and let your handle_position_update do exit placement exactly as before.
        if not getattr(self, "proactive_exit", False):
            return

        # Spawn worker thread (centralized worker method)
        t = threading.Thread(
            target=self._place_exits_from_fill_worker,
            args=(entry_order_id, entry_side_api, tag_base, float(signal_price)),
            daemon=True,
            name=f"{self.name}-ProactiveExits-{entry_order_id}"
        )
        t.start()

    def flip_by_double_market(self, new_side: str, signal_price: float) -> None:
        """
        Reversal:
          - Send double-sized market in the opposite direction to flatten+flip.
          - Cancel old exits.
          - Wait for the new position fill to place SL/TP from *averagePrice*.
        """
        assert new_side in ("long", "short")
        entry_side_api = "BUY" if new_side == "long" else "SELL"

        # Latest known size from hub (fallback to CONTRACT_SIZE)
        curr_size = int(self.active_trade.get("size") or self.contract_size)
        if curr_size < 1:
            curr_size = 1
        send_size = curr_size * 2

        tag_base = self._now_tag("REV")
        th_name = threading.current_thread().name

        try:
            res = self.live_broker.place_market(
                side=entry_side_api,
                size=send_size,
                custom_tag=f"{tag_base}:ENTRY",
            )
        except Exception as e:
            self._log_exception(f"[Reversal] double market call failed: {e}")
            return

        ok = True
        entry_order_id = None
        rtt = 0.0
        try:
            if isinstance(res, (tuple, list)):
                if len(res) == 3:
                    ok, entry_order_id, rtt = res
                elif len(res) == 2:
                    entry_order_id, rtt = res
                else:
                    entry_order_id = res[0]
            elif isinstance(res, dict):
                ok = res.get("ok", True)
                entry_order_id = res.get("orderId") or res.get("id") or res.get("order_id")
                rtt = res.get("rtt", 0.0)
            else:
                entry_order_id = int(res)
        except Exception:
            entry_order_id = None

        if not ok:
            self._log_error(f"[Reversal] double market failed.")
            return

        try:
            entry_order_id = int(entry_order_id)
        except Exception:
            self._log_warning(f"[Reversal] entry_order_id not coercible to int: {entry_order_id!r}")

        self._log_info(
            f"[Reversal] Sent {send_size} {entry_side_api} to flip. entry_order_id={entry_order_id} rtt_ms={int(rtt*1000) if rtt else 0}"
        )

        # Cancel prior exits immediately
        old_sl = self.active_trade.get("sl_order_id")
        old_tp = self.active_trade.get("tp_order_id")
        for oid in (old_sl, old_tp):
            self._cancel_if_exists(oid)

        # Re-arm state for the new leg; exits placed on fill via position handler or proactive worker
        self.active_trade.update({
            "entry_order_id": int(entry_order_id) if entry_order_id is not None else None,
            "sl_order_id": None,
            "tp_order_id": None,
            "side": new_side,
            "entry_signal_price": float(signal_price),
            "fill_price": None,
            "tag_base": tag_base,
        })
        # We keep has_open_position True; size will be updated by hub.
        self.has_open_orders = False
        self._log_info(f"[Reversal] Flipped to {new_side}. Waiting for fill to place SL/TP from averagePrice.")

        # Spawn a worker to place exits after fill (same worker handles detection)
        t = threading.Thread(
            target=self._place_exits_from_fill_worker,
            args=(entry_order_id, entry_side_api, tag_base, float(signal_price)),
            daemon=True,
            name=f"{self.name}-ReversalExits-{entry_order_id}"
        )
        t.start()

    # =========================
    # Place exits helper (synchronous)
    # =========================
    def _place_exits_from_fill(self, side: str, fill_price: float, size: int, link_id: Optional[int]) -> Optional[Dict[str, int]]:
        """
        Synchronous helper used by handle_position_update and police.
        Places SL/TP immediately using broker helper place_exit_orders_from_fill if available,
        otherwise falls back to place_exit_orders. Returns the broker response dict or None.
        Does not mutate active_trade (caller should latch ids under trade_lock).

        Adds price-sanity checks: computed SL/TP absolute prices must make sense for the given side.
        """
        entry_side_api = "BUY" if (str(side).lower() == "long" or str(side).upper() == "BUY") else "SELL"
        if str(side).lower() == "long":
            sl_ticks = self.sl_long
            tp_ticks = self.tp_long
        else:
            sl_ticks = self.sl_short
            tp_ticks = self.tp_short

        # canonical tag_base — reuse what's in active_trade if present to avoid mismatch
        tag_prefix = self.active_trade.get("tag_base") or self._now_tag("REPAIRED")
        self._log_info(
            f"[SYNC-PLACER] Place exits from FILL | side={side} base={fill_price} "
            f"ticks SL/TP={sl_ticks}/{tp_ticks} size={size} tag='{tag_prefix}'"
        )

        # Determine tick size
        tick_size = float(getattr(self, "tick_size", getattr(self, "contract_tick", 0.25)))
        base_fill = float(fill_price)

        # compute absolute prices using correct sign depending on side
        is_long = str(side).lower() == "long" or str(side).lower() == "buy"
        try:
            sl_ticks_i = int(sl_ticks)
            tp_ticks_i = int(tp_ticks)
        except Exception:
            sl_ticks_i = int(float(sl_ticks or 0))
            tp_ticks_i = int(float(tp_ticks or 0))

        if is_long:
            computed_sl = base_fill - sl_ticks_i * tick_size
            computed_tp = base_fill + tp_ticks_i * tick_size
        else:
            computed_sl = base_fill + sl_ticks_i * tick_size
            computed_tp = base_fill - tp_ticks_i * tick_size

        # Audit log
        self._log_info(
            f"[SYNC-PLACER] computed_sl={computed_sl} computed_tp={computed_tp} tick_size={tick_size} "
            f"sl_ticks={sl_ticks_i} tp_ticks={tp_ticks_i}"
        )

        # Sanity check: ensure ordering of prices
        if is_long:
            valid = (computed_sl < base_fill) and (computed_tp > base_fill)
        else:
            valid = (computed_tp < base_fill) and (computed_sl > base_fill)

        if not valid:
            # Do not place; queue for retry + log critical
            self._log_exception(
                f"[SYNC-PLACER] Invalid computed exit prices for side={'long' if is_long else 'short'}; "
                f"sl={computed_sl} tp={computed_tp} fill={base_fill} - queuing pending exit instead of placing."
            )
            try:
                payload = {
                    "tag_prefix": tag_prefix,
                    "base_price": base_fill,
                    "sl_ticks": sl_ticks,
                    "tp_ticks": tp_ticks,
                    "entry_id": link_id,
                    "entry_side": entry_side_api,
                    "attempted_at": datetime.utcnow().isoformat(),
                }
                with self.pending_exits_lock:
                    self.pending_exits.append(payload)
                self._log_info(f"[SYNC-PLACER] Queued pending exit payload for entry {link_id}")
            except Exception as qex:
                self._log_exception(f"[SYNC-PLACER] Failed to queue pending exit: {qex}")
            return None

        # Try placing exits normally if sanity ok
        try:
            if hasattr(self.live_broker, "place_exit_orders_from_fill"):
                res = self.live_broker.place_exit_orders_from_fill(
                    entry_side=entry_side_api,
                    fill_price=float(fill_price),
                    sl_ticks=int(sl_ticks_i),
                    tp_ticks=int(tp_ticks_i),
                    size=int(max(1, size)),
                    link_to_entry_id=link_id,
                    tag_prefix=tag_prefix,
                )
            else:
                # fallback for older brokers
                res = self.live_broker.place_exit_orders(
                    entry_side=entry_side_api,
                    engulfing_close=float(fill_price),
                    sl_ticks=int(sl_ticks_i),
                    tp_ticks=int(tp_ticks_i),
                    size=int(max(1, size)),
                    link_to_entry_id=link_id,
                    tag_prefix=tag_prefix,
                )
            self._log_info(f"[SYNC-PLACER] SL/TP submitted: {res} (computed_sl={computed_sl} computed_tp={computed_tp})")
            return res
        except Exception as e:
            self._log_exception(f"[SYNC-PLACER] Sync exit placement failed: {e}")
            # queue for retry
            try:
                payload = {
                    "tag_prefix": tag_prefix,
                    "base_price": base_fill,
                    "sl_ticks": sl_ticks,
                    "tp_ticks": tp_ticks,
                    "entry_id": link_id,
                    "entry_side": entry_side_api,
                    "attempted_at": datetime.utcnow().isoformat(),
                }
                with self.pending_exits_lock:
                    self.pending_exits.append(payload)
                self._log_info(f"[SYNC-PLACER] Queued pending exit payload for entry {link_id} after exception.")
            except Exception:
                pass
            return None



    # =========================
    # Hub / app.py forwarded events
    # =========================

    def handle_account_update(self, account_payload: Optional[Dict[str, Any]]) -> None:
        # For future use (balance etc.)
        self._log_info("handle_account_update invoked (no-op)")

    def handle_order_update(self, orders: List[Dict[str, Any]]) -> None:
        """
        Latch fills/cancels and detect which orders belong to the *current* trade only.
        Responsibilities:
          - Fast detection of entry fills (update size + has_open_position)
          - Latch SL/TP order ids when they appear (by tag_prefix)
          - Implement OCO: when an exit (SL/TP) is FILLED, cancel the opposite exit and update local state
        NOTE: This handler does NOT spawn placement workers or place SL/TP itself — position_update is
        authoritative for placing exits from gateway avgPrice.
        """
        try:
            curr_tag_base = self.active_trade.get("tag_base")
            for o in orders:
                try:
                    oid = int(o.get("id"))
                except Exception:
                    # malformed id; skip
                    continue

                status = o.get("status")   # 0=new,1=working,2=filled,3=cancelled,4=rejected,5=expired,6=partial
                tag = o.get("customTag") or o.get("tag") or o.get("custom_tag")
                contract = o.get("contractId") or o.get("contract_id")

                # Ignore orders for other symbols
                if contract and str(contract) != str(self.contract_id):
                    continue

                # ---------- ENTRY order filled (fast update only) ----------
                try:
                    entry_oid = self.active_trade.get("entry_order_id")
                    if entry_oid and oid == int(entry_oid):
                        if status == self.STATUS_FILLED:
                            # determine filled size (best-effort)
                            try:
                                filled_size = int(
                                    o.get("filled")
                                    or o.get("size")
                                    or self.active_trade.get("size")
                                    or self.contract_size
                                )
                            except Exception:
                                try:
                                    filled_size = int(float(o.get("filled") or o.get("size") or self.active_trade.get("size", 0) or self.contract_size))
                                except Exception:
                                    filled_size = self.contract_size

                            # update active_trade size and flags under lock if possible
                            try:
                                with self.trade_lock:
                                    self.active_trade["size"] = abs(filled_size)
                                    self.has_open_position = True
                                    # record that entry filled time
                                    self.active_trade["entry_filled_at"] = time.time()
                            except Exception:
                                # best-effort fallback without lock
                                self.active_trade["size"] = abs(filled_size)
                                self.has_open_position = True
                                self.active_trade["entry_filled_at"] = time.time()

                            self._log_info(
                                f"[ORDER] Entry order {entry_oid} FILLED -> size={self.active_trade.get('size')} (fast update). "
                                "Awaiting gateway position (handle_position_update) for authoritative avgPrice and exit placement."
                            )

                            # IMPORTANT: Do NOT spawn workers or place exits here.
                            # Placement will be done by handle_position_update (authoritative).
                except Exception as e:
                    self._log_exception(f"Error processing entry order update for oid={oid}: {e}")

                # ---------- Latch SL/TP by tag (only if tag belongs to current trade) ----------
                try:
                    if tag and isinstance(tag, str) and curr_tag_base and tag.startswith(curr_tag_base):
                        try:
                            with self.trade_lock:
                                if ":SL" in tag and not self.active_trade.get("sl_order_id"):
                                    self.active_trade["sl_order_id"] = oid
                                    self.active_trade["exits_placed_at"] = time.time()
                                    self.active_trade["exits_placed_from"] = "order_update"
                                    self._log_info(f"[ORDER] Latched SL order id={oid} from tag={tag}")
                                if ":TP" in tag and not self.active_trade.get("tp_order_id"):
                                    self.active_trade["tp_order_id"] = oid
                                    self.active_trade["exits_placed_at"] = time.time()
                                    self.active_trade["exits_placed_from"] = "order_update"
                                    self._log_info(f"[ORDER] Latched TP order id={oid} from tag={tag}")
                        except Exception:
                            # fallback without lock
                            if ":SL" in tag and not self.active_trade.get("sl_order_id"):
                                self.active_trade["sl_order_id"] = oid
                                self.active_trade["exits_placed_at"] = time.time()
                                self.active_trade["exits_placed_from"] = "order_update"
                                self._log_info(f"[ORDER] Latched SL order id={oid} from tag={tag} (no-lock fallback)")
                            if ":TP" in tag and not self.active_trade.get("tp_order_id"):
                                self.active_trade["tp_order_id"] = oid
                                self.active_trade["exits_placed_at"] = time.time()
                                self.active_trade["exits_placed_from"] = "order_update"
                                self._log_info(f"[ORDER] Latched TP order id={oid} from tag={tag} (no-lock fallback)")
                except Exception as e:
                    self._log_exception(f"Error latching exit order from tag for oid={oid}: {e}")

                # ---------- OCO behavior: if an exit is FILLED, cancel the opposite exit ----------
                try:
                    if status == self.STATUS_FILLED and tag and isinstance(tag, str) and curr_tag_base and tag.startswith(curr_tag_base):
                        # An exit for the current trade has been filled
                        if ":SL" in tag:
                            self._log_info(f"[OCO] SL order {oid} FILLED; cancelling TP (if any).")
                            other = None
                            try:
                                with self.trade_lock:
                                    other = self.active_trade.get("tp_order_id")
                            except Exception:
                                other = self.active_trade.get("tp_order_id")

                            if other:
                                try:
                                    self._log_info(f"[OCO] Cancelling TP order id={other} due to SL fill.")
                                    self._cancel_if_exists(other)
                                except Exception as e:
                                    self._log_exception(f"[OCO] Failed to cancel TP {other}: {e}")

                                # clear locally
                                try:
                                    with self.trade_lock:
                                        if self.active_trade.get("tp_order_id") == other:
                                            self.active_trade["tp_order_id"] = None
                                except Exception:
                                    if self.active_trade.get("tp_order_id") == other:
                                        self.active_trade["tp_order_id"] = None

                            # Update state: SL filled -> position likely closed
                            try:
                                with self.trade_lock:
                                    self.active_trade["sl_order_id"] = oid
                                    self.has_open_orders = bool(self.active_trade.get("sl_order_id") or self.active_trade.get("tp_order_id"))
                                    self.has_open_position = False
                            except Exception:
                                self.active_trade["sl_order_id"] = oid
                                self.has_open_orders = bool(self.active_trade.get("sl_order_id") or self.active_trade.get("tp_order_id"))
                                self.has_open_position = False

                            self._log_info(f"[OCO] SL fill processed for oid={oid}. State updated.")

                        elif ":TP" in tag:
                            self._log_info(f"[OCO] TP order {oid} FILLED; cancelling SL (if any).")
                            other = None
                            try:
                                with self.trade_lock:
                                    other = self.active_trade.get("sl_order_id")
                            except Exception:
                                other = self.active_trade.get("sl_order_id")

                            if other:
                                try:
                                    self._log_info(f"[OCO] Cancelling SL order id={other} due to TP fill.")
                                    self._cancel_if_exists(other)
                                except Exception as e:
                                    self._log_exception(f"[OCO] Failed to cancel SL {other}: {e}")

                                # clear locally
                                try:
                                    with self.trade_lock:
                                        if self.active_trade.get("sl_order_id") == other:
                                            self.active_trade["sl_order_id"] = None
                                except Exception:
                                    if self.active_trade.get("sl_order_id") == other:
                                        self.active_trade["sl_order_id"] = None

                            # Update state: TP filled -> position likely closed
                            try:
                                with self.trade_lock:
                                    self.active_trade["tp_order_id"] = oid
                                    self.has_open_orders = bool(self.active_trade.get("sl_order_id") or self.active_trade.get("tp_order_id"))
                                    self.has_open_position = False
                            except Exception:
                                self.active_trade["tp_order_id"] = oid
                                self.has_open_orders = bool(self.active_trade.get("sl_order_id") or self.active_trade.get("tp_order_id"))
                                self.has_open_position = False

                            self._log_info(f"[OCO] TP fill processed for oid={oid}. State updated.")
                except Exception as e:
                    self._log_exception(f"Error during OCO handling for oid={oid}: {e}")

                # ---------- Clear local latch on cancelled/rejected/expired exits ----------
                try:
                    if status in (self.STATUS_CANCELLED, self.STATUS_REJECTED, self.STATUS_EXPIRED):
                        changed = False
                        try:
                            with self.trade_lock:
                                if self.active_trade.get("sl_order_id") == oid:
                                    self.active_trade["sl_order_id"] = None
                                    changed = True
                                if self.active_trade.get("tp_order_id") == oid:
                                    self.active_trade["tp_order_id"] = None
                                    changed = True
                                if changed:
                                    self.active_trade["exits_placed_at"] = None
                                    self.active_trade["exits_placed_from"] = None
                                    self.has_open_orders = bool(self.active_trade.get("sl_order_id") or self.active_trade.get("tp_order_id"))
                        except Exception:
                            # fallback without lock
                            if self.active_trade.get("sl_order_id") == oid:
                                self.active_trade["sl_order_id"] = None
                                changed = True
                            if self.active_trade.get("tp_order_id") == oid:
                                self.active_trade["tp_order_id"] = None
                                changed = True
                            if changed:
                                self.active_trade["exits_placed_at"] = None
                                self.active_trade["exits_placed_from"] = None
                                self.has_open_orders = bool(self.active_trade.get("sl_order_id") or self.active_trade.get("tp_order_id"))
                        if changed:
                            self._log_info(f"[ORDER] Exit order {oid} {status} -> cleared local latch(s).")
                except Exception as e:
                    self._log_exception(f"Error clearing cancelled/rejected exit latch for oid={oid}: {e}")

            # End for orders

            # Working exits?
            try:
                with self.trade_lock:
                    self.has_open_orders = bool(self.active_trade.get("sl_order_id") or self.active_trade.get("tp_order_id"))
            except Exception:
                self.has_open_orders = bool(self.active_trade.get("sl_order_id") or self.active_trade.get("tp_order_id"))

        except Exception as e:
            self._log_exception(f"Error in handle_order_update: {e}")


    def handle_trade_update(self, trades_payload: Any) -> None:
        # Optional: for diagnostics to prevent AttributeError from app
        self._log_info("handle_trade_update invoked (ignored)")



    def handle_position_update(self, positions_payload: Any) -> None:
        """
        Position update handler (watchman) — updated to robustly match tags which include entry order id.
        """
        try:
            # Normalize payload; accept dict or list
            positions: List[Dict[str, Any]] = []
            if isinstance(positions_payload, list):
                for item in positions_payload:
                    d = item.get("data", item)
                    if isinstance(d, dict):
                        positions.append(d)
            elif isinstance(positions_payload, dict):
                d = positions_payload.get("data", positions_payload)
                if isinstance(d, dict):
                    positions.append(d)

            # Find our contract (and filter by account)
            found_open = False
            net_size = 0
            avg_price = None
            pos_side_type = None  # 1=Long, 2=Short
            for p in positions:
                pdata = p.get("raw", p) if isinstance(p, dict) else p
                try:
                    p_acct = int(pdata.get("accountId", -1)) if pdata.get("accountId") is not None else -1
                except Exception:
                    p_acct = -1
                if str(pdata.get("contractId")) != str(self.contract_id):
                    continue
                if p_acct != int(getattr(self.live_broker, "account_id", self.account_id)):
                    continue

                try:
                    sz = int(pdata.get("size", 0))
                except Exception:
                    try:
                        sz = int(float(pdata.get("size", 0) or 0))
                    except Exception:
                        sz = 0
                if sz != 0:
                    found_open = True
                    net_size = abs(sz)
                    avg_price = pdata.get("averagePrice") or pdata.get("avgPrice") or pdata.get("avg_price")
                    pos_side_type = pdata.get("type")
                    break

            # Update "has_open_position" and synchronized size (use lock if possible)
            try:
                with self.trade_lock:
                    self.has_open_position = found_open
                    self.active_trade["size"] = net_size if found_open else 0
            except Exception:
                self.has_open_position = found_open
                self.active_trade["size"] = net_size if found_open else 0

            # If FLAT → cancel any tracked exits and reset local state
            if not found_open:
                for oid in (self.active_trade.get("sl_order_id"), self.active_trade.get("tp_order_id")):
                    try:
                        if oid:
                            self._cancel_if_exists(oid)
                    except Exception:
                        pass

                if any(self.active_trade.get(k) for k in ("sl_order_id", "tp_order_id", "entry_order_id", "size")):
                    self._reset_state(reason="Flat; state cleared.")
                return

            # OPEN: determine side
            if pos_side_type == 1:
                side = "long"
            elif pos_side_type == 2:
                side = "short"
            else:
                side = self.active_trade.get("side")

            # Ensure canonical tag_base exists — DO NOT re-generate if already present
            try:
                with self.trade_lock:
                    existing_tag = self.active_trade.get("tag_base")
                    if not existing_tag:
                        # If entry_order_id present, use it; else use fallback tag passed earlier
                        entry_for_tag = None
                        try:
                            entry_for_tag = int(self.active_trade.get("entry_order_id")) if self.active_trade.get("entry_order_id") else None
                        except Exception:
                            entry_for_tag = None
                        if entry_for_tag:
                            existing_tag = self._make_tag_base_from_entry(entry_for_tag)
                        else:
                            existing_tag = self._now_tag("ENTRY") if not self.active_trade.get("tag_base") else self.active_trade.get("tag_base")
                        self.active_trade["tag_base"] = existing_tag
                        self._log_info(f"Assigned canonical tag_base={existing_tag} in handle_position_update (was absent).")
                    tag_base = existing_tag
            except Exception:
                tag_base = self.active_trade.get("tag_base") or self._now_tag("ENTRY")
                if not self.active_trade.get("tag_base"):
                    self._log_info(f"Assigned canonical tag_base={tag_base} (fallback path).")
                    self.active_trade["tag_base"] = tag_base

            curr_tag_base = tag_base

            def _normalize_tag(tb: Optional[str]) -> Optional[str]:
                return self._normalize_tag_base(tb)

            # If a proactive worker is in-flight, wait up to position_update_initial_wait for it to finish.
            try:
                wait_seconds = float(getattr(self, "position_update_initial_wait", 3.0))
            except Exception:
                wait_seconds = 3.0

            try:
                entry_for_worker = None
                try:
                    entry_for_worker = int(self.active_trade.get("entry_order_id")) if self.active_trade.get("entry_order_id") else None
                except Exception:
                    entry_for_worker = None

                waited = False
                start_wait = time.time()
                if entry_for_worker is not None:
                    with self.proactive_worker_events_lock:
                        ev = self.proactive_worker_events.get(entry_for_worker)
                    if ev and ev.is_set():
                        self._log_info(f"[WATCHMAN] Detected in-flight proactive worker for entry {entry_for_worker}; waiting up to {wait_seconds}s for it to finish.")
                        while ev.is_set() and (time.time() - start_wait) < wait_seconds:
                            time.sleep(0.05)
                        waited = True
                else:
                    any_ev_set = False
                    with self.proactive_worker_events_lock:
                        for ev in list(self.proactive_worker_events.values()):
                            try:
                                if ev.is_set():
                                    any_ev_set = True
                                    break
                            except Exception:
                                continue
                    if any_ev_set:
                        self._log_info(f"[WATCHMAN] Detected in-flight proactive worker(s); waiting up to {wait_seconds}s for them to finish.")
                        start_wait = time.time()
                        while (time.time() - start_wait) < wait_seconds:
                            with self.proactive_worker_events_lock:
                                still_set = any((ev.is_set() for ev in self.proactive_worker_events.values()))
                            if not still_set:
                                break
                            time.sleep(0.05)
                        waited = True

                if waited:
                    self._log_info(f"[WATCHMAN] Waited {(time.time() - start_wait):.3f}s for proactive worker(s) to finish or timeout.")
            except Exception:
                pass

            # Ownership verification: if our state mistakenly holds exit IDs from an older trade, verify ownership before cancelling
            if self.active_trade.get("sl_order_id") or self.active_trade.get("tp_order_id"):
                for oid in (self.active_trade.get("sl_order_id"), self.active_trade.get("tp_order_id")):
                    if not oid:
                        continue
                    try:
                        order_remote = None
                        lb = getattr(self, "live_broker", None)

                        # Attempt multiple fetch methods to get the remote order
                        if lb is not None:
                            if hasattr(lb, "fetch_order"):
                                try:
                                    order_remote = lb.fetch_order(int(oid))
                                except Exception:
                                    order_remote = None
                            if order_remote is None and hasattr(lb, "fetch_order_by_id"):
                                try:
                                    order_remote = lb.fetch_order_by_id(int(oid))
                                except Exception:
                                    order_remote = None

                            if order_remote is None and hasattr(lb, "fetch_open_orders"):
                                try:
                                    open_orders = lb.fetch_open_orders()
                                    iterable = open_orders if isinstance(open_orders, list) else (open_orders.get("orders") if isinstance(open_orders, dict) else [open_orders])
                                    for oo in iterable:
                                        try:
                                            if int(oo.get("id")) == int(oid):
                                                order_remote = oo
                                                break
                                        except Exception:
                                            continue
                                except Exception:
                                    order_remote = None

                            if order_remote is None and hasattr(lb, "fetch_account_positions_orders"):
                                try:
                                    _acct_info, _pos_list, ords = lb.fetch_account_positions_orders()
                                    iterable = ords if isinstance(ords, list) else (ords.get("orders") if isinstance(ords, dict) else [ords])
                                    for oo in iterable:
                                        try:
                                            if int(oo.get("id")) == int(oid):
                                                order_remote = oo
                                                break
                                        except Exception:
                                            continue
                                except Exception:
                                    order_remote = None

                        if order_remote is not None:
                            # Try to find remote tag and normalize
                            order_tag = ""
                            if isinstance(order_remote, dict):
                                order_tag = (order_remote.get("customTag") or order_remote.get("tag") or "") or ""
                                # if nested raw
                                if not order_tag and isinstance(order_remote.get("raw"), dict):
                                    order_tag = (order_remote.get("raw").get("customTag") or order_remote.get("raw").get("tag") or "") or ""

                            norm_remote = _normalize_tag(order_tag)
                            norm_local = _normalize_tag(curr_tag_base or "")

                            # If normalized remote tag starts with normalized local base -> skip cancelling
                            if norm_local and isinstance(norm_remote, str) and norm_remote.startswith(norm_local):
                                self._log_info(f"Skipping cancel of order {oid}: normalized remote tag matches local tag_base")
                                continue

                            # Additional cross-check: if remote has linkedOrderId or linked_order_id or raw.linked etc
                            remote_entry_id = None
                            try:
                                remote_entry_id = self._extract_entry_id_from_any(order_remote)
                            except Exception:
                                remote_entry_id = None

                            local_entry_id = None
                            try:
                                local_entry_id = int(self.active_trade.get("entry_order_id")) if self.active_trade.get("entry_order_id") else None
                            except Exception:
                                local_entry_id = None

                            if remote_entry_id is not None and local_entry_id is not None and int(remote_entry_id) == int(local_entry_id):
                                self._log_info(f"Skipping cancel of order {oid}: remote linked entry id matches local entry id ({local_entry_id})")
                                continue

                            now_ts = time.time()
                            grace_seconds = float(getattr(self, "reconcile_grace_seconds", 3.0))
                            exits_ts = self.active_trade.get("exits_placed_at")

                            remote_created = None
                            for k in ("creationTimestamp", "createdAt", "created_at", "timestamp"):
                                try:
                                    if order_remote.get(k) is not None:
                                        remote_created = float(order_remote.get(k))
                                        break
                                except Exception:
                                    remote_created = None

                            recent_by_local = (exits_ts is not None and (now_ts - float(exits_ts) <= grace_seconds))
                            recent_by_remote = (remote_created is not None and (now_ts - float(remote_created) <= grace_seconds))

                            if recent_by_local or recent_by_remote:
                                self._log_info(
                                    f"Skipping cancel of order {oid}: recent placement detected "
                                    f"(age_local={(now_ts - float(exits_ts)) if exits_ts else 'NA'}s, age_remote={(now_ts - remote_created) if remote_created else 'NA'}s)"
                                )
                                continue

                            self._log_info(f"Cancelling order {oid}: remote order tag does not match current tag_base (remote_tag={order_tag!r} local_base={curr_tag_base!r})")
                            try:
                                self._cancel_if_exists(oid)
                            except Exception:
                                self._log_info(f"Failed to cancel remote order {oid} (clearing local state anyway)")
                            try:
                                with self.trade_lock:
                                    if self.active_trade.get("sl_order_id") == oid:
                                        self.active_trade["sl_order_id"] = None
                                    if self.active_trade.get("tp_order_id") == oid:
                                        self.active_trade["tp_order_id"] = None
                                    self.has_open_orders = bool(self.active_trade.get("sl_order_id") or self.active_trade.get("tp_order_id"))
                            except Exception:
                                if self.active_trade.get("sl_order_id") == oid:
                                    self.active_trade["sl_order_id"] = None
                                if self.active_trade.get("tp_order_id") == oid:
                                    self.active_trade["tp_order_id"] = None
                                self.has_open_orders = bool(self.active_trade.get("sl_order_id") or self.active_trade.get("tp_order_id"))

                            continue

                        # If we couldn't fetch remote order info at all, rely on local exits_placed_at grace window
                        now_ts = time.time()
                        grace_seconds = float(getattr(self, "reconcile_grace_seconds", 3.0))
                        exits_ts = self.active_trade.get("exits_placed_at")
                        if exits_ts is not None and (now_ts - exits_ts) <= grace_seconds:
                            self._log_info(f"Skipping cancel of order {oid} due to recent local placement (age={now_ts - exits_ts:.3f}s)")
                            continue
                        else:
                            self._log_info(f"Cancelling order {oid} because ownership could not be verified and it's older than grace window")
                            try:
                                self._cancel_if_exists(oid)
                            except Exception:
                                self._log_info(f"Failed to cancel order {oid}; clearing local state anyway")
                            try:
                                with self.trade_lock:
                                    if self.active_trade.get("sl_order_id") == oid:
                                        self.active_trade["sl_order_id"] = None
                                    if self.active_trade.get("tp_order_id") == oid:
                                        self.active_trade["tp_order_id"] = None
                                    self.has_open_orders = bool(self.active_trade.get("sl_order_id") or self.active_trade.get("tp_order_id"))
                            except Exception:
                                if self.active_trade.get("sl_order_id") == oid:
                                    self.active_trade["sl_order_id"] = None
                                if self.active_trade.get("tp_order_id") == oid:
                                    self.active_trade["tp_order_id"] = None
                                self.has_open_orders = bool(self.active_trade.get("sl_order_id") or self.active_trade.get("tp_order_id"))

                    except Exception as exc:
                        self._log_exception(f"Error while verifying remote order {oid}: {exc}. Skipping cancel for safety.")
                        continue

            # Only place if we actually have a fill price
            if found_open and net_size > 0 and avg_price is not None:
                has_local_exits = bool(self.active_trade.get("sl_order_id") or self.active_trade.get("tp_order_id"))

                if not has_local_exits:
                    lb = getattr(self, "live_broker", None)
                    open_orders = []
                    if lb and hasattr(lb, "fetch_open_orders"):
                        try:
                            open_orders = lb.fetch_open_orders() or []
                            if isinstance(open_orders, dict):
                                open_orders = open_orders.get("orders", [])
                        except Exception:
                            open_orders = []

                    remote_has_protective = False
                    norm_local = self._normalize_tag_base(curr_tag_base or "")
                    for oo in open_orders:
                        try:
                            o_tag = oo.get("customTag") or oo.get("tag") or ""
                            norm_o = self._normalize_tag_base(o_tag)
                            if isinstance(norm_o, str) and norm_local and norm_o.startswith(norm_local):
                                remote_has_protective = True
                                break
                            # also check linked/linkedOrderId fields
                            remote_linked = self._extract_entry_id_from_any(oo)
                            try:
                                local_entry_id = int(self.active_trade.get("entry_order_id")) if self.active_trade.get("entry_order_id") else None
                            except Exception:
                                local_entry_id = None
                            if remote_linked is not None and local_entry_id is not None and int(remote_linked) == int(local_entry_id):
                                remote_has_protective = True
                                break
                        except Exception:
                            continue

                    if remote_has_protective:
                        self._log_info(
                            f"[WATCHMAN] Open position {net_size}@{avg_price}, but exits already found remotely "
                            f"(tag_base={curr_tag_base}); skipping placement."
                        )
                    else:
                        fill = float(avg_price)
                        link_id = int(self.active_trade["entry_order_id"]) if self.active_trade.get("entry_order_id") else None

                        # Call the synchronous placement path (this will place exits and return dict of order ids)
                        res = None
                        try:
                            res = self._place_exits_from_fill(side=side, fill_price=fill, size=net_size, link_id=link_id)
                        except Exception as ex:
                            self._log_exception(f"Sync place_exits_from_fill failed: {ex}")
                            res = None

                        if res:
                            try:
                                with self.trade_lock:
                                    self.active_trade["sl_order_id"] = res.get("sl_order_id")
                                    self.active_trade["tp_order_id"] = res.get("tp_order_id")
                                    self.active_trade["exits_placed_at"] = time.time()
                                    self.active_trade["exits_placed_from"] = "position_update"
                                    self.active_trade["fill_price"] = fill
                                    self.has_open_orders = bool(res.get("sl_order_id") or res.get("tp_order_id"))
                            except Exception:
                                pass

                            self._log_info(
                                f"✅ [WATCHMAN-FIX] {self.contract_id} | size={net_size} | avgPrice={fill} | "
                                f"side={side} | SLticks={self.sl_long if side=='long' else self.sl_short} "
                                f"TPticks={self.tp_long if side=='long' else self.tp_short} "
                                f"→ tag={curr_tag_base}"
                            )
                else:
                    self._log_info(
                        f"[WATCHMAN] Open position {net_size}@{avg_price}, exits already latched "
                        f"(sl={self.active_trade.get('sl_order_id')} tp={self.active_trade.get('tp_order_id')}); no action."
                    )

            # Else → no avgPrice yet, spawn worker fallback (non-blocking) — guarded to avoid duplicates
            elif found_open and net_size > 0 and avg_price is None and getattr(self, "proactive_exit", True):
                entry_id = int(self.active_trade["entry_order_id"]) if self.active_trade.get("entry_order_id") else None
                self._log_info(f"Entry detected OPEN but avgPrice missing; spawning fill-worker for entry {entry_id}")

                try:
                    with self.trade_lock:
                        worker_tag_base = self.active_trade.get("tag_base")
                except Exception:
                    worker_tag_base = self.active_trade.get("tag_base")

                entry_for_worker = entry_id if entry_id is not None else int(time.time())
                spawned = False
                try:
                    with self.active_proactive_workers_lock:
                        if entry_for_worker in self.active_proactive_workers:
                            self._log_info(f"Proactive worker already running for entry {entry_for_worker}; skipping spawn.")
                            spawned = False
                        else:
                            self.active_proactive_workers.add(entry_for_worker)
                            spawned = True
                except Exception:
                    self._log_info("active_proactive_workers lock not present or failed; spawning worker without guard.")
                    spawned = True

                if spawned:
                    def _worker_wrapper(eid=entry_for_worker, tb=worker_tag_base):
                        try:
                            self._place_exits_from_fill_worker(eid, "BUY" if side == "long" else "SELL", tb, float(self.active_trade.get("entry_signal_price", 0)))
                        finally:
                            try:
                                with self.active_proactive_workers_lock:
                                    self.active_proactive_workers.discard(eid)
                            except Exception:
                                try:
                                    self.active_proactive_workers.discard(eid)
                                except Exception:
                                    pass

                    t = threading.Thread(
                        target=_worker_wrapper,
                        daemon=True,
                        name=f"{self.name}-ProactiveExits-{entry_for_worker}"
                    )
                    t.start()

        except Exception as e:
            self._log_exception(f"Error in handle_position_update: {e}")


    # ----------------------------------------------------------------------
    # Reconcile (cold start or reconnect)
    # ----------------------------------------------------------------------
    def reconcile_on_reconnect(self) -> None:
        """
        Strategy 2 reconciliation:
          - Waits briefly for hubs to be ready.
          - Pulls fresh positions/orders via LB2.recon_* helpers.
          - If flat -> cancel all pending orders, reset state.
          - If open -> infer entry side from POSITION.TYPE (1=Long, 2=Short),
                       cancel stale exits, and place new SL/TP from averagePrice.
          - Arms recon OCO watchers (recon_handle_* one-shot pass).
        """
        import time, threading

        acct = int(self.account_id)
        ctid = str(self.contract_id)
        print(f"[RECON-TRACE] 🛑 🛑 🛑 🛑 Strategy--2 reconcile_on_reconnect START — 🛑 🛑 🛑 🛑 ")

        # one-shot guard
        if getattr(self, "_recon_lock", None) is None:
            self._recon_lock = threading.Lock()
        if not self._recon_lock.acquire(blocking=False):
            self._log_info("reconcile_on_reconnect already running; skip.")
            return

        # small helper to choose ticks for BUY/SELL using whatever fields are present
        def _get_recon_ticks_for(side_api: str) -> tuple[int, int]:
            # Try common attribute names first; fall back to sane defaults
            candidates = [
                ("sl_ticks_long", "tp_ticks_long", "sl_ticks_short", "tp_ticks_short"),
                ("stop_ticks_long", "target_ticks_long", "stop_ticks_short", "target_ticks_short"),
                ("sl_long", "tp_long", "sl_short", "tp_short"),
            ]
            slL = tpL = slS = tpS = None
            for a,b,c,d in candidates:
                slL = getattr(self, a, None) if slL is None else slL
                tpL = getattr(self, b, None) if tpL is None else tpL
                slS = getattr(self, c, None) if slS is None else slS
                tpS = getattr(self, d, None) if tpS is None else tpS
            if slL is None: slL = 100
            if tpL is None: tpL = 40
            if slS is None: slS = 100
            if tpS is None: tpS = 80
            return (int(slL), int(tpL)) if side_api == "BUY" else (int(slS), int(tpS))

        try:
            # brief readiness wait to avoid racing hubs
            ready_deadline = time.monotonic() + 2.0
            sleep = 0.05
            while time.monotonic() < ready_deadline:
                uh = getattr(self, "userhub_ready", True)
                mh = getattr(self, "markethub_ready", True)
                if uh and mh:
                    break
                time.sleep(sleep); sleep = min(sleep*1.8, 0.25)

            self._log_info(f"[RECON] start acct={acct} contract={ctid} hubs_ready={getattr(self,'userhub_ready',True) and getattr(self,'markethub_ready',True)}")

            # fresh snapshot
            positions = self.live_broker.recon_fetch_positions(acct)
            orders = self.live_broker.recon_fetch_orders(acct, contract_id=ctid)
            my_pos = next((p for p in positions if str(p.get("contractId")) == ctid), None)
            my_orders = [o for o in orders if str(o.get("contractId")) == ctid]

            # FLAT path
            if not my_pos or int(my_pos.get("size", 0)) == 0:
                if my_orders:
                    self.live_broker.recon_cancel_all_orders(acct, contract_id=ctid)
                # sweep any locally-tracked exits just in case
                self._cancel_if_exists(self.active_trade.get("sl_order_id"))
                self._cancel_if_exists(self.active_trade.get("tp_order_id"))
                self._reset_state()
                self._log_info("[RECON] reconcile_on_reconnect completed (flat).")
                return

            # OPEN position
            # Prefer side from POSITION.TYPE (1=Long->BUY, 2=Short->SELL)
            side_api = my_pos.get("side")
            if not side_api:
                ptype = my_pos.get("type")
                if ptype == 1:
                    side_api = "BUY"
                elif ptype == 2:
                    side_api = "SELL"
            if not side_api:
                # last resort: infer from size sign (warn)
                sz = int(my_pos.get("size", 0))
                side_api = "BUY" if sz > 0 else "SELL"
                self._log_warning(f"[RECON] Falling back to size-sign to infer side ({sz})->{side_api}")

            avg = float(my_pos["averagePrice"])
            sl_ticks, tp_ticks = _get_recon_ticks_for(side_api)

            # Cancel stale exits then place new ones atomically
            try:
                if my_orders:
                    self.live_broker.recon_cancel_all_orders(acct, contract_id=ctid)

                placed = self.live_broker.recon_place_exit_orders(
                    entry_side=side_api,
                    engulfing_close=avg,          # S2: we still compute off avg on recon
                    sl_ticks=sl_ticks,
                    tp_ticks=tp_ticks,
                    size=int(self.contract_size),
                    link_to_entry_id=None,
                    tag_prefix=f"S2:{acct}",      # include accountId to avoid tag collisions
                )
            except Exception as place_err:
                # safety: try to drop an emergency SL if placement fails after cancels
                try:
                    tick = float(self.live_broker.tick_size)
                    if side_api == "BUY":
                        stop_px = avg - (sl_ticks * tick)
                        stop_side = "SELL"
                    else:
                        stop_px = avg + (sl_ticks * tick)
                        stop_side = "BUY"
                    em_id, _ = self.live_broker.place_stop_order(
                        stop_side, stop_px, size=int(self.contract_size),
                        custom_tag=f"S2:{acct}:RECON:EMERGENCY_SL:{int(time.time())}"
                    )
                    self._log_warning(f"[RECON] Emergency SL placed after placement failure. id={em_id}")
                except Exception as em:
                    self._log_exception(f"[RECON] Emergency SL placement failed too: {em}")
                raise

            # seed state & arm recon OCO watcher
            sl_id = placed.get("sl_order_id"); tp_id = placed.get("tp_order_id")
            self.active_trade["sl_order_id"] = sl_id
            self.active_trade["tp_order_id"] = tp_id
            self.has_open_position = True
            self.has_open_orders = bool(sl_id or tp_id)

            self.in_recon = True
            self.recon_until_ts = time.monotonic() + 30.0

            # One-shot pass through recon handlers with fresh snapshots
            try:
                snap_orders = self.live_broker.recon_fetch_orders(acct, contract_id=ctid)
                self.recon_handle_order_update(snap_orders)
            except Exception as e:
                logger.debug(f"[{self.name}][RECON] recon_handle_order_update pass error: {e}")

            try:
                snap_positions = self.live_broker.recon_fetch_positions(acct)
                self.recon_handle_position_update(snap_positions)
            except Exception as e:
                logger.debug(f"[{self.name}][RECON] recon_handle_position_update pass error: {e}")

            self._log_info("[RECON] armed OCO watcher (recon mode ON).")

        except Exception as e:
            self._log_exception(f"[RECON] reconcile_on_reconnect error: {e}")
            self.in_recon = False
            self.recon_until_ts = 0.0
        finally:
            try:
                self._recon_lock.release()
            except Exception:
                pass

    # =========================
    # Recon helpers (left as function names referenced by reconcile methods)
    # They were part of your original codebase; keep as no-op placeholders if not defined elsewhere.
    # =========================
    def recon_handle_order_update(self, orders: Any) -> None:
        # keep lightweight: your existing recon logic will call this if you have it defined elsewhere
        self._log_info("recon_handle_order_update invoked (no-op)")

    def recon_handle_position_update(self, positions: Any) -> None:
        # keep lightweight placeholder
        self._log_info("recon_handle_position_update invoked (no-op)")


    def position_police_loop(
            self,
            interval: float = 10.0,
            stale_order_age: float = 120.0,
            protection_wait: float = 15.0,   # <- default changed to 15s as requested
            orphan_wait: float = 10.0,
            stop_event: Optional[threading.Event] = None
        ) -> None:
            """
            Enhanced policeman loop with wait timers before taking action.

            - protection_wait: seconds to wait for SL/TP to appear for an observed open position before forcing flatten.
            - orphan_wait: seconds to wait after first seeing an unlinked order before cancelling it.
            """
            self._log_info(f"[POLICE] Starting position_police_loop interval={interval}s protection_wait={protection_wait}s orphan_wait={orphan_wait}s")
            if stop_event is None:
                stop_event = threading.Event()

            account_hint = int(getattr(self.live_broker, "account_id", self.account_id))
            contract_hint = str(getattr(self, "contract_id", self.contract_id))

            # trackers
            pos_missing_protection_since: dict[str, float] = {}  # key -> timestamp when missing protection first observed (key: f"{acct}:{contract}")
            orphan_first_seen: dict[int, float] = {}              # order_id -> first seen timestamp
            pos_size_abnormal_since: dict[str, float] = {}       # key -> timestamp when pos size abnormality first observed

            def _extract_tag(o: dict) -> str:
                return (o.get("customTag") or o.get("tag") or o.get("custom_tag") or "") if isinstance(o, dict) else ""

            def _extract_linked(o: dict) -> Optional[int]:
                link = o.get("link_to_entry_id") or o.get("linkOrderId") or o.get("linkedOrderId") or o.get("linked_order_id")
                try:
                    return int(link) if link is not None else None
                except Exception:
                    return None

            def _order_id(o: dict) -> Optional[int]:
                try:
                    return int(o.get("id"))
                except Exception:
                    try:
                        return int(o.get("orderId"))
                    except Exception:
                        return None

            def order_belongs_to_entry(o: dict, local_entry_id: Optional[int], local_tag_base: Optional[str]) -> bool:
                """
                Return True if order 'o' can be reasonably considered belonging to the current active trade.
                Criteria (in order):
                 - linkedOrderId matches local_entry_id
                 - tag contains the local_entry_id as substring
                 - tag startswith local_tag_base
                 - recon-style tag: contains 'RECON' and account_hint (and optionally entry id)
                """
                if not isinstance(o, dict):
                    return False

                # linked field
                linked = _extract_linked(o)
                if linked is not None and local_entry_id is not None and linked == int(local_entry_id):
                    return True

                tag = _extract_tag(o)
                # if we have an explicit local_entry_id, accept tags containing it
                if local_entry_id is not None:
                    try:
                        if str(int(local_entry_id)) in tag:
                            return True
                    except Exception:
                        pass

                # if tag_base exists, check prefix match (tag_base is the prefix used when placing orders)
                if local_tag_base:
                    try:
                        if tag.startswith(local_tag_base):
                            return True
                    except Exception:
                        pass

                # recon case: accept tags that contain RECON and the account id (these are recon-placed orders)
                if tag and "RECON" in tag:
                    if str(account_hint) in tag:
                        return True
                    if local_entry_id is not None and str(local_entry_id) in tag:
                        return True

                return False

            while not stop_event.is_set():
                try:
                    # --- FETCH positions (authoritative) ---
                    positions = None
                    try:
                        if hasattr(self.live_broker, "fetch_open_positions"):
                            positions = self.live_broker.fetch_open_positions()
                        if not positions and hasattr(self.live_broker, "fetch_account_positions_orders"):
                            _acct_info, pos_list, _ords = self.live_broker.fetch_account_positions_orders()
                            positions = pos_list
                        if not positions and hasattr(self.live_broker, "recon_fetch_positions"):
                            positions = self.live_broker.recon_fetch_positions(account_hint)
                    except Exception as e:
                        self._log_info(f"[POLICE] Failed to fetch positions from broker: {e}")
                        positions = None

                    # Normalize positions
                    pos_iter = []
                    if positions:
                        if isinstance(positions, list):
                            pos_iter = positions
                        elif isinstance(positions, dict) and "positions" in positions:
                            pos_iter = positions.get("positions", [])
                        elif isinstance(positions, dict):
                            pos_iter = [positions]
                    else:
                        pos_iter = []

                    relevant_positions = []
                    for p in pos_iter:
                        try:
                            p_acct = int(p.get("accountId", -1)) if p.get("accountId") is not None else -1
                        except Exception:
                            p_acct = -1
                        p_contract = p.get("contractId") or p.get("contract_id")
                        if p_acct != account_hint:
                            continue
                        if str(p_contract) != str(contract_hint):
                            continue
                        try:
                            p_size = int(p.get("size") or 0)
                        except Exception:
                            try:
                                p_size = int(float(p.get("size") or 0))
                            except Exception:
                                p_size = 0
                        p_avg = p.get("averagePrice") or p.get("avgPrice") or p.get("avg_price")
                        relevant_positions.append({"raw": p, "size": p_size, "avg": p_avg})

                    # --- FETCH open orders ---
                    open_orders = []
                    try:
                        if hasattr(self.live_broker, "fetch_open_orders"):
                            open_orders = self.live_broker.fetch_open_orders() or []
                        elif hasattr(self.live_broker, "fetch_account_positions_orders"):
                            _acct_info, _pos_list, ords = self.live_broker.fetch_account_positions_orders()
                            open_orders = ords or []
                        else:
                            open_orders = []
                    except Exception as e:
                        self._log_info(f"[POLICE] Failed to fetch open orders from broker: {e}")
                        open_orders = []

                    if isinstance(open_orders, dict) and "orders" in open_orders:
                        open_orders = open_orders.get("orders", []) or []
                    if not isinstance(open_orders, list):
                        open_orders = [open_orders] if open_orders else []

                    # Build helpful indices
                    orders_by_id = {}
                    orders_by_entry_link = {}
                    now_ts = time.time()
                    for o in open_orders:
                        try:
                            oid = _order_id(o)
                        except Exception:
                            oid = None
                        if oid is not None:
                            orders_by_id[oid] = o
                        linked = _extract_linked(o)
                        if linked is not None:
                            orders_by_entry_link.setdefault(int(linked), []).append(o)

                    # ---------- CASE: open position(s) exist ----------
                    if relevant_positions:
                        rp = relevant_positions[0]
                        pos_size = abs(int(rp.get("size") or 0))
                        pos_avg = rp.get("avg")
                        self._log_info(f"[POLICE] Detected open position size={pos_size} avg={pos_avg}")

                        # local tag & entry id (read under lock)
                        try:
                            with self.trade_lock:
                                local_tag_base = self.active_trade.get("tag_base")
                                local_entry_id = int(self.active_trade.get("entry_order_id")) if self.active_trade.get("entry_order_id") else None
                                local_side = self.active_trade.get("side")
                                configured_expected_size = int(self.active_trade.get("size") or getattr(self, "contract_size", None) or 0)
                        except Exception:
                            local_tag_base = self.active_trade.get("tag_base")
                            local_entry_id = self.active_trade.get("entry_order_id")
                            local_side = self.active_trade.get("side")
                            configured_expected_size = int(self.active_trade.get("size") or getattr(self, "contract_size", None) or 0)

                        # ---- explicit position-size-abnormality detection ----
                        # If observed position size does not equal expected strategy size, treat as abnormal.
                        try:
                            expected_size = int(configured_expected_size or getattr(self, "contract_size", 0) or 0)
                        except Exception:
                            expected_size = 0

                        pos_key = f"{account_hint}:{contract_hint}"
                        if expected_size > 0 and pos_size != expected_size:
                            # start / continue abnormal timer
                            if pos_key not in pos_size_abnormal_since:
                                pos_size_abnormal_since[pos_key] = now_ts
                                self._log_info(f"[POLICE] Position size abnormality observed (pos={pos_size} expected={expected_size}). Starting {protection_wait}s timer.")
                            else:
                                elapsed_size = now_ts - pos_size_abnormal_since[pos_key]
                                self._log_info(f"[POLICE] Position size abnormality for {pos_key} observed for {elapsed_size:.1f}s (threshold={protection_wait}s)")
                                if elapsed_size >= protection_wait:
                                    self._log_info(f"[POLICE] Size abnormality persisted >= {protection_wait}s. Forcing flatten & cleanup for {pos_key}.")
                                    # cancel all orders for this contract + flatten (same flow as missing protection)
                                    for o in open_orders:
                                        try:
                                            oid = _order_id(o)
                                        except Exception:
                                            oid = None
                                        if not oid:
                                            continue
                                        try:
                                            o_acct = int(o.get("accountId", account_hint)) if o.get("accountId") is not None else account_hint
                                        except Exception:
                                            o_acct = account_hint
                                        o_contract = o.get("contractId") or o.get("contract_id")
                                        if o_acct != account_hint or str(o_contract) != str(contract_hint):
                                            continue
                                        try:
                                            self._log_info(f"[POLICE] Cancelling order {oid} as part of forced flatten for size abnormality {pos_key}")
                                            self._cancel_if_exists(oid)
                                        except Exception as e:
                                            self._log_exception(f"[POLICE] Failed to cancel order {oid} during forced flatten: {e}")
                                    # attempt close
                                    try:
                                        closed = False
                                        if hasattr(self.live_broker, "close_position"):
                                            try:
                                                self.live_broker.close_position(account_hint, contract_hint, pos_size)
                                                closed = True
                                            except Exception as e:
                                                self._log_info(f"[POLICE] live_broker.close_position failed during size-abnormal flatten: {e}")
                                                closed = False
                                        if not closed:
                                            # best-effort market flatten
                                            side_api = "SELL" if (local_side == "long" or pos_size > 0) else "BUY"
                                            try:
                                                if hasattr(self.live_broker, "place_market_order"):
                                                    try:
                                                        self.live_broker.place_market_order(side_api, size=pos_size)
                                                    except TypeError:
                                                        self.live_broker.place_market_order(side=side_api, size=pos_size)
                                                elif hasattr(self.live_broker, "place_order"):
                                                    self.live_broker.place_order({"type": "MARKET", "side": side_api, "size": int(pos_size), "account_id": account_hint, "contract_id": contract_hint})
                                                self._log_info("[POLICE] Market flatten attempt made for size-abnormality (check broker).")
                                            except Exception as e:
                                                self._log_info(f"[POLICE] Market flatten attempt failed (size-abnormality): {e}")
                                    except Exception as e:
                                        self._log_exception(f"[POLICE] Error while attempting to close position during size-abnormal forced flatten: {e}")
                                    # reset local state
                                    try:
                                        self._log_info("[POLICE] Calling _reset_state after size-abnormal forced flatten.")
                                        self._reset_state(reason="police:forced_flatten_size_abnormal")
                                    except Exception as e:
                                        self._log_exception(f"[POLICE] _reset_state failed after size-abnormal forced flatten: {e}")
                                    pos_size_abnormal_since.pop(pos_key, None)
                                    # continue to next loop iteration
                                    continue
                        else:
                            # if everything OK, clear any size abnormal timer
                            pos_size_abnormal_since.pop(pos_key, None)

                        # find SL/TP using robust order_belongs_to_entry
                        protective_sl = None
                        protective_tp = None

                        for o in open_orders:
                            try:
                                o_contract = o.get("contractId") or o.get("contract_id")
                                o_acct = int(o.get("accountId", account_hint)) if o.get("accountId") is not None else account_hint
                            except Exception:
                                o_contract = o.get("contractId") or o.get("contract_id")
                                o_acct = account_hint
                            if o_acct != account_hint or str(o_contract) != str(contract_hint):
                                continue
                            tag = _extract_tag(o)
                            # check membership
                            belongs = order_belongs_to_entry(o, local_entry_id, local_tag_base)
                            if not belongs:
                                # Not our order (skip for protective detection)
                                continue
                            # classify protective SL/TP by tag or heuristic
                            if ":SL" in tag and protective_sl is None:
                                protective_sl = o
                            if ":TP" in tag and protective_tp is None:
                                protective_tp = o
                            # fallback detection using API type hints
                            typ = (o.get("type") or o.get("orderType") or "").upper()
                            if protective_sl is None and typ and "STOP" in typ:
                                protective_sl = protective_sl or o
                            if protective_tp is None and typ and "LIMIT" in typ:
                                protective_tp = protective_tp or o
                            # linked fallback
                            linked = _extract_linked(o)
                            if linked is not None and linked == local_entry_id:
                                if protective_sl is None and "STOP" in typ:
                                    protective_sl = protective_sl or o
                                if protective_tp is None and "LIMIT" in typ:
                                    protective_tp = protective_tp or o

                        # if missing protections, start/continue timer
                        missing_protection = (protective_sl is None or protective_tp is None)

                        if missing_protection:
                            if pos_key not in pos_missing_protection_since:
                                pos_missing_protection_since[pos_key] = now_ts
                                self._log_info(f"[POLICE] Missing protection observed for position {pos_key}. Starting {protection_wait}s timer.")
                            else:
                                elapsed = now_ts - pos_missing_protection_since[pos_key]
                                self._log_info(f"[POLICE] Missing protection for {pos_key} observed for {elapsed:.1f}s (threshold={protection_wait}s).")
                                if elapsed >= protection_wait:
                                    # After wait: force flatten & clean
                                    self._log_info(f"[POLICE] Protection still missing after {protection_wait}s — forcing flatten and reset for {pos_key}.")
                                    # 1) cancel any protective orders found (none expected) and any orphans for this contract
                                    for o in open_orders:
                                        try:
                                            oid = _order_id(o)
                                        except Exception:
                                            oid = None
                                        if not oid:
                                            continue
                                        try:
                                            o_acct = int(o.get("accountId", account_hint)) if o.get("accountId") is not None else account_hint
                                        except Exception:
                                            o_acct = account_hint
                                        o_contract = o.get("contractId") or o.get("contract_id")
                                        if o_acct != account_hint or str(o_contract) != str(contract_hint):
                                            continue
                                        # cancel everything for this contract as a final cleanup
                                        try:
                                            self._log_info(f"[POLICE] Cancelling order {oid} as part of forced flatten for {pos_key}")
                                            self._cancel_if_exists(oid)
                                        except Exception as e:
                                            self._log_exception(f"[POLICE] Failed to cancel order {oid} during forced flatten: {e}")

                                    # 2) attempt to close position: prefer broker helper close_position, else place market opposite
                                    try:
                                        closed = False
                                        # try close_position helper
                                        if hasattr(self.live_broker, "close_position"):
                                            try:
                                                self._log_info(f"[POLICE] Attempting live_broker.close_position acct={account_hint} contract={contract_hint} size={pos_size}")
                                                try:
                                                    self.live_broker.close_position(account_hint, contract_hint, pos_size)
                                                except TypeError:
                                                    self.live_broker.close_position(account_id=account_hint, contract_id=contract_hint)
                                                closed = True
                                            except Exception as e:
                                                self._log_info(f"[POLICE] live_broker.close_position failed: {e}")
                                                closed = False
                                        # fallback: try place_market_order helper(s)
                                        if not closed:
                                            tried = False
                                            if hasattr(self.live_broker, "place_market_order"):
                                                try:
                                                    side_api = "SELL" if (local_side == "long" or pos_size > 0) else "BUY"
                                                    self._log_info(f"[POLICE] Attempting place_market_order to flatten: side={side_api} size={pos_size}")
                                                    try:
                                                        self.live_broker.place_market_order(side_api, size=pos_size)
                                                    except TypeError:
                                                        self.live_broker.place_market_order(side=side_api, size=pos_size)
                                                    closed = True
                                                    tried = True
                                                except Exception as e:
                                                    self._log_info(f"[POLICE] place_market_order failed: {e}")
                                            if not tried and hasattr(self.live_broker, "place_order"):
                                                try:
                                                    side_api = "SELL" if (local_side == "long" or pos_size > 0) else "BUY"
                                                    self._log_info(f"[POLICE] Attempting place_order(market) to flatten: side={side_api} size={pos_size}")
                                                    self.live_broker.place_order({"type": "MARKET", "side": side_api, "size": int(pos_size), "account_id": account_hint, "contract_id": contract_hint})
                                                    closed = True
                                                except Exception as e:
                                                    self._log_info(f"[POLICE] place_order(market) failed: {e}")

                                        if not closed:
                                            self._log_info("[POLICE] Could not close position via broker helpers; marking state reset and queuing manual intervention.")
                                        else:
                                            self._log_info("[POLICE] Position close attempt made (check broker).")
                                    except Exception as e:
                                        self._log_exception(f"[POLICE] Error while attempting to close position during forced flatten: {e}")

                                    # 3) finally reset local state to clean
                                    try:
                                        self._log_info("[POLICE] Calling _reset_state after forced flatten.")
                                        self._reset_state(reason="police:forced_flatten")
                                    except Exception as e:
                                        self._log_exception(f"[POLICE] _reset_state failed after forced flatten: {e}")

                                    # remove timer
                                    pos_missing_protection_since.pop(pos_key, None)
                        else:
                            # protections present -> ensure sizes match; if mismatch cancel+replace (as earlier)
                            try:
                                def _order_size(o):
                                    try:
                                        return abs(int(o.get("size") or o.get("qty") or o.get("quantity") or 0))
                                    except Exception:
                                        try:
                                            return abs(int(float(o.get("size") or o.get("qty") or o.get("quantity") or 0)))
                                        except Exception:
                                            return 0

                                # compute sizes (if found)
                                sl_size = _order_size(protective_sl) if protective_sl else None
                                tp_size = _order_size(protective_tp) if protective_tp else None
                                mismatch = False
                                if protective_sl and sl_size != pos_size:
                                    self._log_info(f"[POLICE] Protective SL size mismatch (order={sl_size} vs pos={pos_size}), will cancel & replace")
                                    mismatch = True
                                if protective_tp and tp_size != pos_size:
                                    self._log_info(f"[POLICE] Protective TP size mismatch (order={tp_size} vs pos={pos_size}), will cancel & replace")
                                    mismatch = True
                                if mismatch:
                                    for o in (protective_sl, protective_tp):
                                        if not o:
                                            continue
                                        try:
                                            oid = _order_id(o)
                                        except Exception:
                                            oid = None
                                        if oid:
                                            try:
                                                self._log_info(f"[POLICE] Cancelling protective order {oid} due to size mismatch (police loop).")
                                                self._cancel_if_exists(oid)
                                            except Exception as e:
                                                self._log_exception(f"[POLICE] Failed to cancel protective order {oid}: {e}")
                                    # enqueue placement to fix (use same logic as earlier)
                                    try:
                                        with self.trade_lock:
                                            link_to_entry = int(self.active_trade.get("entry_order_id")) if self.active_trade.get("entry_order_id") else None
                                    except Exception:
                                        link_to_entry = self.active_trade.get("entry_order_id")
                                    try:
                                        res = self._place_exits_from_fill(side=(self.active_trade.get("side") or ("long" if pos_size>0 else "short")),
                                                                          fill_price=float(pos_avg) if pos_avg not in (None, 0) else float(self.active_trade.get("entry_signal_price") or 0),
                                                                          size=pos_size,
                                                                          link_id=link_to_entry)
                                        if res:
                                            with self.trade_lock:
                                                self.active_trade["sl_order_id"] = res.get("sl_order_id") or self.active_trade.get("sl_order_id")
                                                self.active_trade["tp_order_id"] = res.get("tp_order_id") or self.active_trade.get("tp_order_id")
                                                self.active_trade["exits_placed_at"] = time.time()
                                                self.active_trade["exits_placed_from"] = "police:fix_mismatch"
                                                self.has_open_orders = bool(self.active_trade.get("sl_order_id") or self.active_trade.get("tp_order_id"))
                                            self._log_info("[POLICE] Replaced protective exits via direct sync call.")
                                        else:
                                            # spawn guarded worker to place
                                            eid = int(self.active_trade.get("entry_order_id")) if self.active_trade.get("entry_order_id") else None
                                            if eid:
                                                try:
                                                    with self.active_proactive_workers_lock:
                                                        if eid not in self.active_proactive_workers:
                                                            self.active_proactive_workers.add(eid)
                                                            def _wrap_police_worker(eid_local=eid):
                                                                try:
                                                                    self._place_exits_from_fill_worker(eid_local, _side_to_api(self.active_trade.get("side") or "long"), self.active_trade.get("tag_base"), float(self.active_trade.get("entry_signal_price", 0)))
                                                                finally:
                                                                    try:
                                                                        with self.active_proactive_workers_lock:
                                                                            self.active_proactive_workers.discard(eid_local)
                                                                    except Exception:
                                                                        try:
                                                                            self.active_proactive_workers.discard(eid_local)
                                                                        except Exception:
                                                                            pass
                                                            threading.Thread(target=_wrap_police_worker, daemon=True, name=f"{self.name}-police-worker-fix-{eid}").start()
                                                            self._log_info(f"[POLICE] Spawned police worker to fix mismatch for entry {eid}")
                                                except Exception:
                                                    self._log_info("[POLICE] Failed to spawn worker to fix mismatch; enqueuing pending payload")
                                                    try:
                                                        payload = {
                                                            "tag_prefix": (self.active_trade.get("tag_base") or self._now_tag("POLICE")) + ":POLICE",
                                                            "base_price": float(pos_avg) if pos_avg not in (None, 0) else float(self.active_trade.get("entry_signal_price") or 0),
                                                            "sl_ticks": self.sl_long if (self.active_trade.get("side") == "long") else self.sl_short,
                                                            "tp_ticks": self.tp_long if (self.active_trade.get("side") == "long") else self.tp_short,
                                                            "entry_id": self.active_trade.get("entry_order_id"),
                                                            "entry_side": _side_to_api(self.active_trade.get("side") or "long"),
                                                            "attempted_at": datetime.utcnow().isoformat(),
                                                        }
                                                        with self.pending_exits_lock:
                                                            self.pending_exits.append(payload)
                                                        self._log_info("[POLICE] queued pending payload for mismatch fix")
                                                    except Exception:
                                                        pass
                                    except Exception as e:
                                        self._log_exception(f"[POLICE] Error while replacing protective exits: {e}")

                            except Exception as e:
                                self._log_exception(f"[POLICE] Error validating protective sizes: {e}")

                            # if present and everything OK, ensure any pos_missing_protection timer is cleared
                            pos_missing_protection_since.pop(pos_key, None)

                    # ---------- CASE: no open position -> scan for orphan orders and cancel after orphan_wait ----------
                    else:
                        self._log_info("[POLICE] No open position found; scanning for orphan/unlinked orders to possibly cancel.")
                        for o in open_orders:
                            try:
                                oid = _order_id(o)
                            except Exception:
                                oid = None
                            if not oid:
                                continue

                            try:
                                o_acct = int(o.get("accountId", account_hint)) if o.get("accountId") is not None else account_hint
                            except Exception:
                                o_acct = account_hint
                            o_contract = o.get("contractId") or o.get("contract_id")
                            if o_acct != account_hint or str(o_contract) != str(contract_hint):
                                continue

                            tag = _extract_tag(o)
                            linked_id = _extract_linked(o)

                            # decide if orphan: order does not belong to current active trade
                            try:
                                with self.trade_lock:
                                    current_tag_base = self.active_trade.get("tag_base")
                                    current_entry_id = int(self.active_trade.get("entry_order_id")) if self.active_trade.get("entry_order_id") else None
                            except Exception:
                                current_tag_base = self.active_trade.get("tag_base")
                                current_entry_id = self.active_trade.get("entry_order_id")

                            is_for_current = order_belongs_to_entry(o, current_entry_id, current_tag_base)
                            is_orphan = not is_for_current

                            if not is_orphan:
                                # reset any existing first-seen clock for this order
                                orphan_first_seen.pop(oid, None)
                                continue

                            # record first-seen if not present
                            first_seen = orphan_first_seen.get(oid)
                            if first_seen is None:
                                orphan_first_seen[oid] = now_ts
                                self._log_info(f"[POLICE] Orphan order {oid} first seen; will wait up to {orphan_wait}s before cancelling.")
                                continue
                            else:
                                age = now_ts - first_seen
                                if age < orphan_wait:
                                    self._log_info(f"[POLICE] Orphan order {oid} age={age:.1f}s (<{orphan_wait}s) - skipping cancel for now.")
                                    continue
                                # age >= orphan_wait -> cancel it
                                try:
                                    self._log_info(f"[POLICE] Cancelling orphan order {oid} after {age:.1f}s.")
                                    self._cancel_if_exists(oid)
                                except Exception as e:
                                    self._log_exception(f"[POLICE] Failed to cancel orphan order {oid}: {e}")
                                orphan_first_seen.pop(oid, None)

                    # small loop-end sleep
                except Exception as e:
                    self._log_exception(f"[POLICE] Unexpected error in police loop: {e}")

                # sleep with stop_event awareness
                stop_event.wait(timeout=interval)

            self._log_info("[POLICE] position_police_loop stopped by stop_event.")


    # -------------------------
    # State reset helpers
    # -------------------------
    def _reset_state(self, reason: str = "") -> None:
        """
        Reset local active trade state and internal flags.
        Safe to call even if attributes are missing or partially initialized.
        """
        # Defensive default shape for active_trade
        try:
            if not hasattr(self, "active_trade") or not isinstance(self.active_trade, dict):
                self.active_trade = {}
        except Exception:
            # last resort
            self.active_trade = {}

        # Use a canonical active_trade shape
        try:
            self.active_trade.update({
                "entry_order_id": None,
                "sl_order_id": None,
                "tp_order_id": None,
                "side": None,
                "size": 0,
                "entry_signal_price": None,
                "fill_price": None,
                "tag_base": None,
                "exits_placed_at": None,
                "exits_placed_from": None,
            })
        except Exception:
            # if .update fails, replace whole dict
            try:
                self.active_trade = {
                    "entry_order_id": None,
                    "sl_order_id": None,
                    "tp_order_id": None,
                    "side": None,
                    "size": 0,
                    "entry_signal_price": None,
                    "fill_price": None,
                    "tag_base": None,
                    "exits_placed_at": None,
                    "exits_placed_from": None,
                }
            except Exception:
                # last-possible fallback; shouldn't happen
                self.active_trade = {}

        # boolean flags
        try:
            self.has_open_orders = False
        except Exception:
            setattr(self, "has_open_orders", False)
        try:
            self.has_open_position = False
        except Exception:
            setattr(self, "has_open_position", False)

        # additional recon flags if present
        try:
            self.in_recon = False
        except Exception:
            setattr(self, "in_recon", False)
        try:
            self.recon_until_ts = 0.0
        except Exception:
            setattr(self, "recon_until_ts", 0.0)

        if reason:
            # Use your existing logging helper if available
            try:
                self._log_info(f"[RESET] {reason} - state cleaned.")
            except Exception:
                print(f"[RESET] {reason} - state cleaned.")


    def _reset_state_safe(self, reason: str = "") -> None:
        """
        Wrapper that avoids raising if, for some reason, _reset_state is not present.
        This allows older derived classes to call safely.
        """
        try:
            # prefer the real implementation
            return self._reset_state(reason=reason)
        except AttributeError:
            # Shouldn't happen since we just defined _reset_state, but defensive anyway
            try:
                # attempt naive cleanup
                self.active_trade = getattr(self, "active_trade", {}) or {}
                for k in ["entry_order_id","sl_order_id","tp_order_id","side","size","entry_signal_price","fill_price","tag_base","exits_placed_at","exits_placed_from"]:
                    self.active_trade.setdefault(k, None)
                self.has_open_orders = False
                self.has_open_position = False
                self.in_recon = False
                self.recon_until_ts = 0.0
                try:
                    self._log_info(f"[RESET-SAFE] {reason} - fallback state cleaned.")
                except Exception:
                    print(f"[RESET-SAFE] {reason} - fallback state cleaned.")
            except Exception as e:
                # last resort
                try:
                    self._log_exception(f"[RESET-SAFE] failed to reset state: {e}")
                except Exception:
                    print(f"[RESET-SAFE] failed to reset state: {e}")

