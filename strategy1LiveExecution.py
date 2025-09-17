# strategy1LiveExecution.py
# Close-based exits, per-strategy self-configuration (local defaults or env or constructor).
# - Data and execution are for the SAME contract (no cross-mapping).
# - Exits (SL/TP) computed from the *signal candle close* (not the fill) during normal flow.
# - Strategy reacts only to candles matching (contractId, timeframe).
#
# This version includes two key safety fixes:
#   1) app.py routes only this strategy's (accountId, contractId) position updates.
#   2) S1 marks FLAT only on an EXPLICIT size==0 update for its own (account, contract);
#      if a delta does not include this strategy's position, it ignores the delta
#      (prevents accidental "flat sweep" from unrelated deltas).
#
# Plus:
#   3) reconcile_on_reconnect(): self-heals after cold start / reconnect:
#      - If flat -> cancels any leftover SL/TP (by tag prefix) and clears state.
#      - If open -> latches existing SL/TP (by tag) or re-places protective SL/TP from avgPrice.

import os
import logging

from typing import List, Dict, Any, Optional
from global_locks import acquire_reconcile_lock, release_reconcile_lock
import threading
import time                # stdlib module, use time.time(), time.sleep(), etc.
from datetime import datetime, timedelta, time as dt_time

import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Local, in-file defaults you control here (no env needed) ---
S1_DEFAULTS = {
    "ACCOUNT_ID":     10729026,
    "CONTRACT_ID":    "CON.F.US.MNQ.Z25",
    "TIMEFRAME":      "5m",
    "TICK_SIZE":      0.25,
    "SL_TICKS_LONG":  100,
    "TP_TICKS_LONG":  40,
    "SL_TICKS_SHORT": 100,
    "TP_TICKS_SHORT": 80,
    "CONTRACT_SIZE":  2,
}

# Optional: if you want the strategy to find the token from env instead of constructor
_TOPSTEPX_TOKEN_ENV = os.getenv("TOPSTEPX_TOKEN")

logger = logging.getLogger("Strategy1")
# ✅ define timezone here
TIMEZONE = pytz.timezone("US/Eastern")

try:
    from livebroker import LiveBroker
except Exception:
    LiveBroker = None  # type: ignore


def _side_to_api(side: str) -> str:
    return "BUY" if side.lower() == "long" else "SELL"


class Strategy1LiveExecution:
    """
    Strategy 1 (close-based exits), self-contained config.

    - Uses its OWN config for:
        account_id, contract_id, timeframe, tick_size, SL/TP (long/short), contract_size
      (constructor args > env S1_* > in-file S1_DEFAULTS)

    - Data reservoir contract (candles it listens to) == execution contract.
      It will ignore candles for any other symbol or timeframe.

    - ENTRY market is sent at signal time; SL/TP are placed *immediately*,
      and prices are computed from the *signal candle close*.
    """

    def __init__(
        self,
        # Either provide a broker (must match config) ...
        live_broker: Optional["LiveBroker"] = None,
        # ... or allow the strategy to construct its own broker from token+ids.
        jwt_token: Optional[str] = None,

        # Per-strategy configuration (constructor wins > env > in-file defaults)
        account_id: Optional[int] = None,
        contract_id: Optional[str] = None,
        strategy_timeframe: Optional[str] = None,
        tick_size: Optional[float] = None,
        sl_long: Optional[int] = None,
        tp_long: Optional[int] = None,
        sl_short: Optional[int] = None,
        tp_short: Optional[int] = None,
        contract_size: Optional[int] = None,
        name: str = "S1"

    ):
        # --- Resolve config: constructor > env > in-file defaults
        # Behavior toggles

        self.account_id: int = int(
            account_id if account_id is not None
            else (os.getenv("S1_ACCOUNT_ID") or S1_DEFAULTS["ACCOUNT_ID"])
        )
        self.contract_id: str = str(
            contract_id if contract_id is not None
            else (os.getenv("S1_CONTRACT_ID") or S1_DEFAULTS["CONTRACT_ID"])
        )
        self.strategy_timeframe: str = (
            strategy_timeframe
            or os.getenv("S1_TIMEFRAME")
            or S1_DEFAULTS["TIMEFRAME"]
        )
        self.tick_size: float = float(
            tick_size if tick_size is not None
            else (os.getenv("S1_TICK_SIZE") or S1_DEFAULTS["TICK_SIZE"])
        )
        self.sl_long: int = int(
            sl_long if sl_long is not None
            else (os.getenv("S1_SL_TICKS_LONG") or S1_DEFAULTS["SL_TICKS_LONG"])
        )
        self.tp_long: int = int(
            tp_long if tp_long is not None
            else (os.getenv("S1_TP_TICKS_LONG") or S1_DEFAULTS["TP_TICKS_LONG"])
        )
        self.sl_short: int = int(
            sl_short if sl_short is not None
            else (os.getenv("S1_SL_TICKS_SHORT") or S1_DEFAULTS["SL_TICKS_SHORT"])
        )
        self.tp_short: int = int(
            tp_short if tp_short is not None
            else (os.getenv("S1_TP_TICKS_SHORT") or S1_DEFAULTS["TP_TICKS_SHORT"])
        )
        self.contract_size: int = int(
            contract_size if contract_size is not None
            else (os.getenv("S1_CONTRACT_SIZE") or S1_DEFAULTS["CONTRACT_SIZE"])
        )

        self.name = name

        self.trading_start = dt_time(0, 0)
        self.trading_end   = dt_time(23, 59)

        # Sanity: require contract/account
        if not self.contract_id:
            raise ValueError(
                "Strategy1LiveExecution requires a contract_id. "
                "Set S1_CONTRACT_ID env or edit S1_DEFAULTS['CONTRACT_ID'] or pass contract_id=..."
            )
        if not self.account_id:
            raise ValueError(
                "Strategy1LiveExecution requires an account_id. "
                "Set S1_ACCOUNT_ID env or edit S1_DEFAULTS['ACCOUNT_ID'] or pass account_id=..."
            )
        if self.contract_size <= 0:
            raise ValueError("contract_size must be >= 1")

        # Execution broker: use the provided one or construct from token
        if live_broker is not None:
            self.live_broker = live_broker
            # Hard guard: broker must match strategy config
            broker_acc = getattr(self.live_broker, "account_id", None)
            broker_sym = getattr(self.live_broker, "contract_id", None)
            if int(broker_acc) != int(self.account_id) or str(broker_sym) != self.contract_id:
                raise ValueError(
                    f"[{self.name}] Provided LiveBroker (account={broker_acc}, contract={broker_sym}) "
                    f"does not match strategy config (account={self.account_id}, contract={self.contract_id})."
                )
        else:
            if LiveBroker is None:
                raise RuntimeError("LiveBroker class is not importable, cannot auto-construct broker.")
            token = jwt_token or _TOPSTEPX_TOKEN_ENV
            if not token:
                raise ValueError(
                    "JWT token required to build LiveBroker. "
                    "Pass jwt_token=... from app.py or set TOPSTEPX_TOKEN env."
                )
            self.live_broker = LiveBroker(
                jwt_token=token,
                account_id=self.account_id,
                contract_id=self.contract_id,
                tick_size=self.tick_size,
            )
            self.live_broker.warmup()  # pre-heat TCP/TLS immediately

        # ---- Runtime state (per trade) ----
        self.active_trade: Dict[str, Any] = {
            "entry_order_id": None,
            "sl_order_id": None,
            "tp_order_id": None,
            "side": None,       # "long"/"short"
            "size": 0,          # S1 sizes exits immediately (doesn't wait for fill sizing)
            "entry_signal_price": None,
        }
        self.has_open_position = False
        self.has_open_orders = False

        # Engulfing thresholds (align with your previous defaults; make env-driven later if you want)
        self.min_engulf_bull = 20  # ticks above prev high for bullish
        self.min_engulf_bear = 0   # ticks below prev low for bearish

        # Ensure trade_lock exists
        self.trade_lock = threading.RLock()

        logger.info(
            f"[{self.name}] Initialized | acct={self.account_id} contract={self.contract_id} "
            f"tf={self.strategy_timeframe} tick={self.tick_size} size={self.contract_size} "
            f"SL(tp) long={self.sl_long}/{self.tp_long} short={self.sl_short}/{self.tp_short}"
        )

    def is_within_trading_hours(self, now: datetime) -> bool:
        """Return True if strategy is inside its trading hours (local TZ)."""
        local_time = now.astimezone(TIMEZONE).time()
        return self.trading_start <= local_time <= self.trading_end

    # ----------------------------------------------------------------------
    # Candle feed: only react to my (contract_id, timeframe)
    # ----------------------------------------------------------------------
    def on_new_candles(self, symbol: str, timeframe: str, candles: List[Dict[str, Any]]):
        # Only consume data for our configured contract & timeframe
        if symbol != self.contract_id or timeframe != self.strategy_timeframe:
            return
        if len(candles) < 2:
            return

        # Keep one active sequence at a time
        if self.has_open_orders:
            return

        prev = candles[-2]
        curr = candles[-1]

        o1, h1, l1, c1 = prev.get("open"), prev.get("high"), prev.get("low"), prev.get("close")
        c2 = curr.get("close")
        if any(v is None for v in (o1, h1, l1, c1, c2)):
            return

        prev_bearish = c1 < o1
        prev_bullish = c1 > o1

        min_engulf_range_bull = self.min_engulf_bull * self.tick_size
        diff_bear_eng_condition = self.min_engulf_bear * self.tick_size

        bullish_engulfing = (prev_bearish and (c2 > (h1 + min_engulf_range_bull)))
        bearish_engulfing = (prev_bullish and (c2 < (l1 - diff_bear_eng_condition)))

        logger.info(
            f"[{self.name}] Check Engulfing | symbol={symbol} tf={timeframe} "
            f"prev_bull={prev_bullish} prev_bear={prev_bearish} "
            f"Bearish engulfing={bearish_engulfing} Bullish engulfing={bullish_engulfing}"
        )

        # mapping: bearish_engulfing => LONG, bullish_engulfing => SHORT
        if bearish_engulfing:
            self.enter_trade("long", float(c2))
        elif bullish_engulfing:
            self.enter_trade("short", float(c2))

    # ----------------------------------------------------------------------
    # Entry (exits placed immediately from *signal close*)
    # ----------------------------------------------------------------------
    # in your Strategy __init__:
    # self.trade_lock = threading.Lock()

    def enter_trade(self, side: str, signal_price: float):
        """
        Place a market entry quickly, then place SL/TP exits in a background thread.
        Preserves Strategy1 behavior: exits computed from signal_close, and active_trade.size
        is set immediately to contract_size (intent).
        """
        if self.has_open_orders:
            logger.warning(f"[{self.name}] Attempt to enter while orders active; skipping.")
            return

        logger.info(f"[{self.name}] Initiating ENTRY: side={side} signal_price={signal_price}")
        entry_side_api = _side_to_api(side)

        tag_base = f"{self.name}:{self.contract_id}:{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}"
        logger.info(f"[Timing] Strategy→Send: sending market order NOW ({self.name})")

        # Call the entry method: try your original place_market_order, fall back to hot variant
        try:
            if hasattr(self.live_broker, "place_market_order"):
                res = self.live_broker.place_market_order(
                    side=entry_side_api,
                    size=self.contract_size,
                    custom_tag=f"{tag_base}:ENTRY",
                )
            else:
                # fallback to hot method if present
                res = self.live_broker.place_market_hot(
                    side=entry_side_api,
                    size=self.contract_size,
                    custom_tag=f"{tag_base}:ENTRY",
                )
        except Exception as e:
            logger.error(f"[{self.name}] Market entry call failed: {e}", exc_info=True)
            return

        # Normalize returns: accept (id, rtt) or (ok,id,rtt) or dict
        entry_order_id = None
        entry_rtt = 0.0
        ok = True
        try:
            if isinstance(res, (tuple, list)):
                if len(res) == 2:
                    entry_order_id, entry_rtt = res
                elif len(res) == 3:
                    possible_ok, entry_order_id, entry_rtt = res
                    if isinstance(possible_ok, bool):
                        ok = possible_ok
            elif isinstance(res, dict):
                ok = res.get("ok", True)
                entry_order_id = res.get("orderId") or res.get("id") or res.get("order_id")
                entry_rtt = res.get("rtt", 0.0)
            else:
                entry_order_id = int(res)
        except Exception as e:
            logger.error(f"[{self.name}] Unable to normalize broker entry result: {e} (res={res!r})", exc_info=True)
            return

        if ok is False:
            logger.error(f"[{self.name}] ENTRY market failed (ok=False). res={res!r}")
            return

        try:
            entry_order_id = int(entry_order_id)
        except Exception:
            logger.error(f"[{self.name}] entry_order_id is not coercible to int: {entry_order_id!r}")
            return
        try:
            entry_rtt = float(entry_rtt)
        except Exception:
            entry_rtt = 0.0

        logger.info(f"[Timing] ENTRY ({self.name}) orderId={entry_order_id} entry_rtt_ms={int(entry_rtt*1000)}")

        # Immediately mark orders active & set the intended size (preserve old semantic)
        with getattr(self, "trade_lock", threading.RLock()):
            self.has_open_orders = True
            self.active_trade.update({
                "entry_order_id": entry_order_id,
                "sl_order_id": None,
                "tp_order_id": None,
                "side": "long" if side == "long" else "short",
                "size": self.contract_size,   # S1 intent: place exits sized immediately
                "entry_signal_price": float(signal_price),
                "entry_time_utc": datetime.utcnow().isoformat(),
                "tag_base": tag_base,
            })

        # compute sl/tp ticks off the *signal close*
        if side == "long":
            sl_ticks = self.sl_long
            tp_ticks = self.tp_long
        else:
            sl_ticks = self.sl_short
            tp_ticks = self.tp_short

        # background worker to place exits (so we don't block)
        def place_exits_bg(tag_prefix_local, base_price_local, sl_ticks_local, tp_ticks_local, entry_id_local, entry_side_local):
            try:
                start_wall = time.time()
                results = {}

                # If broker offers low-level stop/limit placement, place them concurrently here
                if hasattr(self.live_broker, "place_stop_order") and hasattr(self.live_broker, "place_limit_order"):
                    logger.debug(f"[{self.name}] Placing SL/TP via direct stop/limit helpers (parallel).")
                    # compute sl/tp price as in livebroker.place_exit_orders_from_signal semantics
                    entry_upper = entry_side_local.upper()
                    base = float(base_price_local)
                    if entry_upper == "BUY":
                        sl_price = base - (int(sl_ticks_local) * float(self.live_broker.tick_size))
                        tp_price = base + (int(tp_ticks_local) * float(self.live_broker.tick_size))
                        sl_side, tp_side = "SELL", "SELL"
                    else:
                        sl_price = base + (int(sl_ticks_local) * float(self.live_broker.tick_size))
                        tp_price = base - (int(tp_ticks_local) * float(self.live_broker.tick_size))
                        sl_side, tp_side = "BUY", "BUY"

                    sl_tag = f"{tag_prefix_local}:SL:{self.name}-{int(time.time()*1000)}"
                    tp_tag = f"{tag_prefix_local}:TP:{self.name}-{int(time.time()*1000)}"

                    with ThreadPoolExecutor(max_workers=2) as ex:
                        futs = {
                            ex.submit(
                                self.live_broker.place_stop_order,
                                sl_side,
                                sl_price,
                                size=int(self.contract_size),
                                linked_order_id=int(entry_id_local),
                                custom_tag=sl_tag,
                            ): "sl",
                            ex.submit(
                                self.live_broker.place_limit_order,
                                tp_side,
                                tp_price,
                                size=int(self.contract_size),
                                linked_order_id=int(entry_id_local),
                                custom_tag=tp_tag,
                            ): "tp",
                        }

                        per_results = {}
                        for fut in as_completed(futs):
                            kind = futs[fut]
                            try:
                                oid, rtt = fut.result()
                            except Exception as e:
                                logger.error(f"[{self.name}] Exit {kind} placement errored: {e}", exc_info=True)
                                oid, rtt = None, 0.0
                            per_results[kind] = {"id": oid, "rtt": rtt}

                        # Normalize to same keys we used previously
                        results["sl_order_id"] = int(per_results.get("sl", {}).get("id")) if per_results.get("sl", {}).get("id") else None
                        results["tp_order_id"] = int(per_results.get("tp", {}).get("id")) if per_results.get("tp", {}).get("id") else None

                        # logging individual timings if present
                        try:
                            sl_rtt = per_results.get("sl", {}).get("rtt", 0.0) or 0.0
                            tp_rtt = per_results.get("tp", {}).get("rtt", 0.0) or 0.0
                            logger.info(f"[{self.name}] stop placed rtt_ms={int(sl_rtt*1000)} tp placed rtt_ms={int(tp_rtt*1000)}")
                        except Exception:
                            pass

                else:
                    # Fallback: reuse broker helper(s) — keep OCO behaviour intact
                    if hasattr(self.live_broker, "place_exit_orders_from_signal"):
                        results = self.live_broker.place_exit_orders_from_signal(
                            entry_side="BUY" if entry_side_local.upper() == "BUY" else "SELL",
                            engulfing_close=float(base_price_local),
                            sl_ticks=int(sl_ticks_local),
                            tp_ticks=int(tp_ticks_local),
                            size=int(self.contract_size),
                            link_to_entry_id=int(entry_id_local),
                            tag_prefix=tag_prefix_local,
                        )
                    else:
                        # Final fallback to generic place_exit_orders if older broker
                        results = self.live_broker.place_exit_orders(
                            entry_side=entry_side_local,
                            engulfing_close=float(base_price_local),
                            sl_ticks=int(sl_ticks_local),
                            tp_ticks=int(tp_ticks_local),
                            size=int(self.contract_size),
                            link_to_entry_id=int(entry_id_local),
                            tag_prefix=tag_prefix_local,
                        )

                # thread-safe update of active_trade with exit order ids / results
                with getattr(self, "trade_lock", threading.RLock()):
                    try:
                        sl_id = results.get("sl_order_id") if isinstance(results, dict) else results.get("sl_order_id")
                        tp_id = results.get("tp_order_id") if isinstance(results, dict) else results.get("tp_order_id")
                    except Exception:
                        sl_id, tp_id = None, None

                    self.active_trade.update({
                        "sl_order_id": sl_id,
                        "tp_order_id": tp_id,
                        "exit_results": results,
                        "exits_placed_time_utc": datetime.utcnow().isoformat(),
                    })
                    # ensure has_open_orders remains True if exits were placed
                    self.has_open_orders = bool(sl_id or tp_id)

                wall_ms = int((time.time() - start_wall) * 1000)
                logger.info(f"[{self.name}] Exit orders placed in parallel: SL={self.active_trade.get('sl_order_id')} TP={self.active_trade.get('tp_order_id')} | wall_ms={wall_ms} | base={base_price_local}")

            except Exception as e:
                logger.error(f"[{self.name}] Background exit placement failed: {e}", exc_info=True)
                # on failure consider enqueueing to pending_exits for retry (optional)

        # spawn daemon thread to place exits
        t = threading.Thread(
            target=place_exits_bg,
            args=(
                tag_base,
                signal_price,
                sl_ticks,
                tp_ticks,
                entry_order_id,
                entry_side_api,
            ),
            daemon=True,
            name=f"{self.name}-Exits-{entry_order_id}"
        )
        t.start()

    # ----------------------------------------------------------------------
    # User hub: account/order/position/trade updates
    # (app.py forwards only matching (accountId, contractId) events)
    # ----------------------------------------------------------------------
    def handle_account_update(self, account_payload: Optional[Dict[str, Any]]):
        logger.debug(f"[{self.name}] Account update: {account_payload}")

    def handle_order_update(self, orders: List[Dict[str, Any]]):
        """
        Expect normalized order dicts from app.py with:
          id, symbol, side, type, size, filled, price, status, raw
        We'll:
          - Watch for SL/TP fills -> cancel sibling -> reset state
          - Cleanup on cancel/reject/expire
        """
        try:
            for o in orders:
                oid = int(o["id"])
                status = o.get("status")

                # EXIT fills -> cancel sibling & reset
                if self.active_trade.get("sl_order_id") and oid == int(self.active_trade["sl_order_id"]) and status == 2:
                    logger.info(f"[{self.name}] SL filled; cancelling TP and resetting.")
                    self._cancel_if_exists(self.active_trade.get("tp_order_id"))
                    self._reset_all_state()
                    return

                if self.active_trade.get("tp_order_id") and oid == int(self.active_trade["tp_order_id"]) and status == 2:
                    logger.info(f"[{self.name}] TP filled; cancelling SL and resetting.")
                    self._cancel_if_exists(self.active_trade.get("sl_order_id"))
                    self._reset_all_state()
                    return

                # Clean up if exits cancelled/rejected/expired
                if status in (3, 4, 5):
                    if self.active_trade.get("sl_order_id") == oid:
                        self.active_trade["sl_order_id"] = None
                    if self.active_trade.get("tp_order_id") == oid:
                        self.active_trade["tp_order_id"] = None

            self.has_open_orders = bool(self.active_trade.get("sl_order_id") or self.active_trade.get("tp_order_id"))

        except Exception as e:
            logger.error(f"[{self.name}] Error in handle_order_update: {e}", exc_info=True)

    def handle_position_update(self, positions_payload: Any):
        """
        Keep open/flat state aligned. IMPORTANT:
        - Only act on records for THIS strategy's (accountId, contractId).
        - If the delta does NOT contain any record for me, IGNORE it (do not sweep).
        - Mark flat ONLY on an explicit size == 0 update for my (account, contract).
        """
        try:
            acct_id = int(self.account_id)
            ctid = self.contract_id

            saw_my_update = False
            saw_flat_for_me = False
            saw_open_for_me = False

            # Normalize to list
            items = positions_payload if isinstance(positions_payload, list) else [positions_payload]

            for item in items:
                data = item.get("data", item) if isinstance(item, dict) else None
                if not data:
                    continue
                if int(data.get("accountId", -1)) != acct_id:
                    continue
                if str(data.get("contractId")) != str(ctid):
                    continue

                # From here, it's an update about ME
                saw_my_update = True
                size = int(data.get("size", 0))
                if size == 0:
                    saw_flat_for_me = True
                else:
                    saw_open_for_me = True

            if not saw_my_update:
                # No info about me in this delta; keep state and return
                logger.debug(f"[{self.name}] Ignoring unrelated position delta.")
                return

            if saw_open_for_me:
                if not self.has_open_position:
                    logger.info(f"[{self.name}] Position marked OPEN from position delta.")
                self.has_open_position = True
                return

            if saw_flat_for_me:
                if self.has_open_position:
                    logger.info(f"[{self.name}] Position marked FLAT from explicit size=0. Sweeping exits.")
                self.has_open_position = False
                # Flat sweep: cancel any dangling exits and reset
                self._cancel_if_exists(self.active_trade.get("sl_order_id"))
                self._cancel_if_exists(self.active_trade.get("tp_order_id"))
                if any([self.active_trade.get("sl_order_id"), self.active_trade.get("tp_order_id")]):
                    self._reset_trade_only()
                    logger.info(f"[{self.name}] Flat sweep: cleared leftover state.")

        except Exception as e:
            logger.error(f"[{self.name}] Error in handle_position_update: {e}", exc_info=True)

    def handle_trade_update(self, trades_payload: Any):
        logger.debug(f"[{self.name}] Trade update: {trades_payload}")


    def _get_recon_ticks(self, side: str) -> tuple[int, int]:
        """
        Return (sl_ticks, tp_ticks) for 'BUY' or 'SELL' using whatever
        attribute names you actually have. Fall back to sane defaults.
        """
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

        if side.upper() == "BUY":
            return int(slL), int(tpL)
        else:
            return int(slS), int(tpS)


    # ----------------------------------------------------------------------
    # Reconcile (cold start / reconnect)
    # ----------------------------------------------------------------------

    def reconcile_on_reconnect(self) -> None:
        import time, threading
        acct = int(self.account_id); ctid = str(self.contract_id)
        print(f"[RECON-TRACE] 🛑 🛑 🛑 🛑 Strategy--1 reconcile_on_reconnect START — 🛑 🛑 🛑 🛑 ")

        # ---- one-shot guard
        if getattr(self, "_recon_lock", None) is None:
            self._recon_lock = threading.Lock()
        if not self._recon_lock.acquire(blocking=False):
            logger.info(f"[{self.name}][RECON] already running; skip.")
            return

        try:
            # short readiness wait to avoid racing the hubs
            ready_deadline = time.monotonic() + 2.0
            sleep = 0.05
            while time.monotonic() < ready_deadline:
                uh = getattr(self, "userhub_ready", True)
                mh = getattr(self, "markethub_ready", True)
                if uh and mh:
                    break
                time.sleep(sleep); sleep = min(sleep*1.8, 0.25)

            logger.info(f"[{self.name}][RECON] start acct={acct} contract={ctid} "
                        f"hubs_ready={(getattr(self,'userhub_ready',True) and getattr(self,'markethub_ready',True))}")

            # --- snapshot
            positions = self.live_broker.recon_fetch_positions(acct)
            orders = self.live_broker.recon_fetch_orders(acct, contract_id=ctid)
            my_pos = next((p for p in positions if str(p.get("contractId")) == ctid), None)
            my_orders = [o for o in orders if str(o.get("contractId")) == ctid]

            # --- flat path
            if not my_pos or int(my_pos.get("size", 0)) == 0:
                if my_orders:
                    self.live_broker.recon_cancel_all_orders(acct, contract_id=ctid)
                self._cancel_if_exists(self.active_trade.get("sl_order_id"))
                self._cancel_if_exists(self.active_trade.get("tp_order_id"))
                self._reset_all_state()
                logger.info(f"[{self.name}][RECON] Completed (flat).")
                return

            # --- open position: determine direction robustly
            side = my_pos.get("side")
            if not side:
                ptype = my_pos.get("type")
                if ptype == 1:         # long
                    side = "BUY"
                elif ptype == 2:       # short
                    side = "SELL"
                else:
                    side = "BUY" if int(my_pos.get("size", 0)) > 0 else "SELL"

            avg = float(my_pos["averagePrice"])
            sl_ticks, tp_ticks = self._get_recon_ticks(side)

            # cancel stale exits, then place fresh ones
            try:
                if my_orders:
                    self.live_broker.recon_cancel_all_orders(acct, contract_id=ctid)

                placed = self.live_broker.recon_place_exit_orders(
                    entry_side=side,
                    engulfing_close=avg,
                    sl_ticks=sl_ticks,
                    tp_ticks=tp_ticks,
                    size=int(self.contract_size),
                    link_to_entry_id=None,
                    tag_prefix=f"S1:{acct}"
                )
            except Exception as place_err:
                # last-resort protective SL
                try:
                    tick = float(self.live_broker.tick_size)
                    if side == "BUY":
                        stop_px = avg - (sl_ticks * tick)
                        stop_side = "SELL"
                    else:
                        stop_px = avg + (sl_ticks * tick)
                        stop_side = "BUY"
                    ok_id, _ = self.live_broker.place_stop_order(
                        stop_side, stop_px, size=int(self.contract_size),
                        custom_tag=f"S1:{acct}:RECON:EMERGENCY_SL:{int(time.time())}"
                    )
                    logger.warning(f"[{self.name}][RECON] Emergency SL placed after placement failure. id={ok_id}")
                except Exception as em:
                    logger.error(f"[{self.name}][RECON] Emergency SL placement failed too: {em}", exc_info=True)
                raise  # bubble original error

            # --- seed state + recon watch
            sl_id = placed.get("sl_order_id"); tp_id = placed.get("tp_order_id")
            self.active_trade["sl_order_id"] = sl_id
            self.active_trade["tp_order_id"] = tp_id
            self.active_trade["side"] = "long" if side == "BUY" else "short"
            self.active_trade["size"] = int(self.contract_size)
            self.has_open_position = True
            self.has_open_orders = bool(sl_id or tp_id)

            self.in_recon = True
            self.recon_until_ts = time.monotonic() + 30.0

            # one-shot OCO sweep with fresh snapshots
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

            logger.info(f"[{self.name}][RECON] armed OCO watcher (recon mode ON).")

        except Exception as e:
            logger.error(f"[{self.name}][RECON] reconcile_on_reconnect error: {e}", exc_info=True)
            self.in_recon = False
            self.recon_until_ts = 0.0
        finally:
            try:
                self._recon_lock.release()
            except Exception:
                pass


    # ----------------------------------------------------------------------
    # Helpers
    # ----------------------------------------------------------------------
    def _cancel_if_exists(self, oid: Optional[int]):
        if oid:
            try:
                self.live_broker.cancel_order(int(oid))
                logger.info(f"[{self.name}] Cancelled order {oid}")
            except Exception as e:
                logger.error(f"[{self.name}] Error cancelling {oid}: {e}", exc_info=True)

    def _reset_trade_only(self):
        self.active_trade = {
            "entry_order_id": None,
            "sl_order_id": None,
            "tp_order_id": None,
            "side": None,
            "size": 0,
            "entry_signal_price": None,
        }
        self.has_open_orders = False
        self.has_open_position = False

    def _reset_all_state(self):
        self._reset_trade_only()
        logger.info(f"[{self.name}] State reset.")


    # --- Strategy1: reconciliation-only OCO helpers -----------------------------

    def recon_handle_order_update(self, orders: list[dict]) -> bool:
        """
        Reconciliation OCO watcher (poll-based).
        Returns True if we handled a terminal action (reset/sweep) and the
        recon loop can stop. Uses numeric statuses: 2=FILLED, 3=CANCELLED, 4=REJECTED, 5=EXPIRED.
        """
        try:
            sl_id = self.active_trade.get("sl_order_id")
            tp_id = self.active_trade.get("tp_order_id")

            for o in orders:
                oid = int(o["id"])
                status = int(o.get("status", -1))

                # If SL filled -> cancel TP and reset
                if sl_id and oid == int(sl_id) and status == 2:
                    logger.info(f"[{self.name}][RECON] SL filled; cancelling TP & resetting.")
                    self._cancel_if_exists(tp_id)
                    self._reset_all_state()
                    return True

                # If TP filled -> cancel SL and reset
                if tp_id and oid == int(tp_id) and status == 2:
                    logger.info(f"[{self.name}][RECON] TP filled; cancelling SL & resetting.")
                    self._cancel_if_exists(sl_id)
                    self._reset_all_state()
                    return True

                # If either exit was cancelled/rejected/expired during recon, clear its slot
                if status in (3, 4, 5):
                    if sl_id and oid == int(sl_id):
                        self.active_trade["sl_order_id"] = None
                    if tp_id and oid == int(tp_id):
                        self.active_trade["tp_order_id"] = None

            # keep a cheap flag for UI/logic
            self.has_open_orders = bool(self.active_trade.get("sl_order_id") or self.active_trade.get("tp_order_id"))
            return False
        except Exception as e:
            logger.error(f"[{self.name}] Error in recon_handle_order_update: {e}", exc_info=True)
            return False


    def recon_handle_position_update(self, positions: list[dict]) -> bool:
        """
        Reconciliation flat-sweep: if we observe our (accountId, contractId) is FLAT (size==0),
        cancel any exits and reset. Returns True if we swept and caller can stop the recon loop.
        """
        try:
            acct = int(self.account_id)
            ctid = str(self.contract_id)

            # find my position (if any) in the polled list
            mine = next((p for p in positions
                         if int(p.get("accountId", -1)) == acct and str(p.get("contractId")) == ctid), None)

            if mine is None:
                # no position record for me — don’t assume flat; keep looping
                return False

            size = int(mine.get("size", 0))
            if size == 0:
                if self.has_open_position:
                    logger.info(f"[{self.name}][RECON] Detected FLAT; sweeping dangling exits.")
                self.has_open_position = False
                self._cancel_if_exists(self.active_trade.get("sl_order_id"))
                self._cancel_if_exists(self.active_trade.get("tp_order_id"))
                self._reset_trade_only()
                return True

            # still open
            if not self.has_open_position:
                logger.info(f"[{self.name}][RECON] Detected OPEN during recon.")
            self.has_open_position = True
            return False
        except Exception as e:
            logger.error(f"[{self.name}] Error in recon_handle_position_update: {e}", exc_info=True)
            return False
