# app.py — multi-strategy data reservoir (event-driven restarts + armed market watchdog)
import os
import time
import logging
import threading
import requests
import sys
import json
import signal
import random
from datetime import datetime, timezone, timedelta, time as dt_time, date
from typing import Optional, Dict, Any, List, Set

import pytz
from signalrcore.hub_connection_builder import HubConnectionBuilder
from signalrcore.hub.errors import HubConnectionError

from aggregator import CandleAggregator
from sync_firestore import initialize_firestore, write_candles
from mockbroker import MockBroker

from livebroker import LiveBroker
from strategy1LiveExecution import Strategy1LiveExecution
from Strategy2LiveExecution import Strategy2LiveExecution
from global_locks import acquire_reconcile_lock, release_reconcile_lock

# === Auth config ===
TOPSTEPX_API_URL = "https://api.topstepx.com/api/"
TOPSTEPX_AUTH_URL = TOPSTEPX_API_URL + "Auth/loginKey"

TOPSTEPX_USERNAME = os.getenv("TOPSTEPX_USERNAME", "Snowballpanda")
TOPSTEPX_API_KEY = os.getenv("TOPSTEPX_API_KEY", "V3jxJ0cv6UMTW53aQvg2KEY6D7XxPxb9Gdeu2yC3WaU=")

# === User hub account (subscription identity for user feed) ===
ACCOUNT_ID = int(os.getenv("ACCOUNT_ID", "10729026"))

# Non-trading window (US/Eastern) — unchanged (only used if you decide to block during settle)
TIMEZONE = pytz.timezone("US/Eastern")
CLOSEOUT_TIME = dt_time(15, 59, 56)
TRADING_RESUME_TIME = dt_time(18, 0, 0)

# Diagnostics
DIAGNOSTIC_TICK_LOG = False

# Backoff & token refresh
RESTART_BACKOFF_BASE = float(os.getenv("RESTART_BACKOFF_BASE", "2.0"))  # seconds (exponential with jitter)
RESTART_BACKOFF_MAX = float(os.getenv("RESTART_BACKOFF_MAX", "20.0"))   # cap
JWT_REFRESH_AFTER_MIN = int(os.getenv("JWT_REFRESH_AFTER_MIN", "30"))   # refresh token if older than this on restart
RESTART_COOLDOWN_SECS = float(os.getenv("RESTART_COOLDOWN_SECS", "20.0"))  # min gap between restarts per hub

# --- Armed market watchdog tuning ---
# Only active *after* a real interruption (close/error), for up to WD_ARM_MINUTES
WD_ARM_MINUTES = int(os.getenv("WD_ARM_MINUTES", "60"))
# Primary fast signal: no ticks for N seconds while armed
WD_NO_TICK_SECS = float(os.getenv("WD_NO_TICK_SECS", "7"))
# Backup slower signal: no candle finalization for N seconds while armed
WD_NO_CANDLE_SECS = float(os.getenv("WD_NO_CANDLE_SECS", "90"))
# Watchdog poll interval
WD_POLL_SECS = float(os.getenv("WD_POLL_SECS", "2.5"))

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("topstepx-client")


class TradingApp:
    """
    - Aggregates ticks for all subscribed contracts (union of strategies' contractIds)
    - Writes candles to Firestore once per (symbol,timeframe)
    - Dispatches finalized candles ONLY to strategies with matching (contract_id, timeframe)
    - Forwards user-hub events ONLY to strategies whose broker (accountId, contractId) matches
    - Schedules TLS warmups per strategy broker
    - Event-driven restarts only; armed market watchdog to detect quiet-dead states post-interruption
    """

    def __init__(self):
        self.token: Optional[str] = None
        self._token_obtained_at: Optional[datetime] = None

        # Hub connection placeholders (set by setup_*_hub)
        self.market_hub_connection = None
        self.user_hub_connection = None

        # Events to coordinate readiness
        self.market_ready_event = threading.Event()
        self.user_ready_event = threading.Event()

        # Single-flight + state flags
        self._user_subscribe_once_lock = threading.Lock()
        self._user_subscribed_flag = False  # set true once a subscribe succeeds after open
        self._user_restart_lock = threading.Lock()
        self._market_restart_lock = threading.Lock()
        self._user_start_stop_lock = threading.Lock()
        self._market_start_stop_lock = threading.Lock()
        self._user_starting = False
        self._market_starting = False

        # Heartbeats (for info; watchdog now uses last tick / last candle)
        now = datetime.now(timezone.utc)
        self._last_user_event_at = now
        self._last_market_event_at = now

        # Backoff & cooldown state
        self._user_backoff_attempt = 0
        self._market_backoff_attempt = 0
        self._last_user_restart_at = 0.0
        self._last_market_restart_at = 0.0

        # Armed watchdog state
        self._wd_armed_until: Optional[datetime] = None
        self._last_interruption_at: Optional[datetime] = None
        self._last_tick_at: datetime = now
        self._last_candle_finalized_at: datetime = now

        # --- Cold start / reconcile state ---
        self._user_hub_ready = False
        self._market_hub_ready = False
        self._did_cold_start_reconcile = False
        self._last_reconcile_time = 0.0
        self._reconcile_cooldown = 10  # seconds

        # Confirmed-connected flags (set by on_*_open; cleared on close/error)
        self._market_confirmed_connected = False
        self._user_confirmed_connected = False

        # App state
        self.aggregator = CandleAggregator(
            timeframes=["1m", "2m", "3m", "5m", "15m", "30m", "1h"],
        )

        # Strategy registry
        self.strategies: List[object] = []

        # --- persistent per-strategy worker state ---
        # mapping queue-key -> Queue object (jobs to be consumed by the worker thread)
        self._strategy_queues: Dict[str, object] = {}
        # mapping queue-key -> Thread object (the worker thread)
        self._strategy_workers: Dict[str, threading.Thread] = {}
        # mapping queue-key -> Event (pause flag: if cleared the worker idles)
        self._strategy_pause_flags: Dict[str, threading.Event] = {}


        # Tracking for candle history & debounce per (symbol, timeframe)
        self.candle_history: Dict[tuple, list] = {}
        self._last_candle_started: Dict[tuple, datetime] = {}

        # Snapshots for optional logging
        self.latest_account_info: Optional[Any] = None
        self.latest_orders: List[Dict[str, Any]] = []
        self.latest_positions: Optional[List[Dict[str, Any]]] = None
        self._active_orders: Dict[int, Dict[str, Any]] = {}

        # --- Reconcile (cold-start & reconnect) guards ---
        self._reconcile_lock = threading.Lock()
        self._last_reconcile_time = 0.0
        self._reconcile_cooldown = 5.0  # seconds

        # Stop signal for watchdog
        self._shutdown = False

        # --- NEW: safe defaults for reconnect loop and cooldown/backoff tuning ---
        # how many seconds between hub health checks performed by the reconnect loop
        self._hub_reconnect_interval = 5.0

        # Add restart backoff state already initialized above; kept for clarity
        self._user_backoff_attempt = getattr(self, "_user_backoff_attempt", 0)
        self._market_backoff_attempt = getattr(self, "_market_backoff_attempt", 0)

        # last restart timestamps already initialized above, but ensure existence
        self._last_user_restart_at = getattr(self, "_last_user_restart_at", 0.0)
        self._last_market_restart_at = getattr(self, "_last_market_restart_at", 0.0)

        #get positions
        self.latest_positions_by_acct: dict[int, list[dict]] = {}

    # ---------------------------
    # Auth
    # ---------------------------
    def get_jwt_token(self) -> bool:
        try:
            payload = {"userName": TOPSTEPX_USERNAME, "apiKey": TOPSTEPX_API_KEY}
            r = requests.post(TOPSTEPX_AUTH_URL, json=payload, timeout=10)
            r.raise_for_status()
            data = r.json()
            if not data.get("success"):
                logger.error(f"Auth failed: {data}")
                return False
            self.token = data["token"]
            self._token_obtained_at = datetime.now(timezone.utc)
            logger.info("✅ JWT token obtained.")
            return True
        except Exception as e:
            logger.error(f"Auth error: {e}", exc_info=True)
            return False

    def _refresh_token_always(self):
            """
            Always refresh the JWT and rebuild hub URLs.
            This ensures reconnects use a fresh token, avoiding stale-socket issues.
            """
            logger.info("🔐 Forcing JWT refresh...")
            if self.get_jwt_token():
                self.setup_market_hub()
                self.setup_user_hub()


    # ---------------------------
    # Helpers
    # ---------------------------
    def _tf_to_seconds(self, tf: str) -> int:
        tf = (tf or "").lower().strip()
        if tf.endswith("m"):
            return int(tf[:-1]) * 60
        if tf.endswith("s"):
            return int(tf[:-1])
        if tf.endswith("h"):
            return int(tf[:-1]) * 3600
        return 60

    def name_broker_connection(self, broker, strategy_name: str):
        """
        Assign a readable connection_name on the broker so logs can show
        which strategy/broker is warming / placing orders.
        """
        try:
            acct = getattr(broker, "account_id", None)
            if acct is not None:
                broker.connection_name = f"{strategy_name}-acct{acct}"
            else:
                broker.connection_name = f"{strategy_name}-obj{hex(id(broker))}"
            return broker.connection_name
        except Exception:
            # best-effort
            broker.connection_name = f"{strategy_name}-obj{hex(id(broker))}"
            return broker.connection_name

    def _schedule_broker_warmups(self, broker: LiveBroker, tf_seconds: int, lead_ms: int = 800):
        """
        Persistent single-runner warmup scheduler per-broker.

        Behavior:
        - One daemon thread per broker (named WarmRunner-<conn>) that loops forever.
        - For each TF boundary it performs two warmups in the SAME runner thread:
            * prewarm1: ~prewarm1_offset seconds before boundary
            * prewarm2: ~prewarm2_offset seconds before boundary
        - Does NOT spawn a new Python Thread/Timer per warmup, so thread names remain stable.
        - Logs conn, thread name, warmup label and elapsed ms.
        """
        import time as _time
        from datetime import datetime as _dt, timezone as _tz, timedelta as _td
        import traceback as _trace

        # offsets in seconds before the boundary
        prewarm1_offset = 2.5
        prewarm2_offset = 0.8
        jitter_range = 0.12  # seconds of +/- jitter for timing

        def _do_warm_inline(broker_local, which_label: str):
            th = threading.current_thread()
            conn_name = getattr(broker_local, "connection_name", getattr(broker_local, "account_id", "unknown"))
            start = _time.perf_counter()
            try:
                broker_local.warmup()
                result = "ok"
            except Exception as exc:
                result = f"err:{exc}"
            dur_ms = (_time.perf_counter() - start) * 1000.0
            logger.info(
                "[WARMUP] conn=%s thread=%s warmup=%s result=%s took=%.1fms",
                conn_name,
                th.name,
                which_label,
                result,
                dur_ms,
            )

        def runner():
            th = threading.current_thread()
            conn_name = getattr(broker, "connection_name", getattr(broker, "account_id", "unknown"))
            logger.debug("WarmRunner started for conn=%s thread=%s tf_seconds=%s", conn_name, th.name, tf_seconds)

            while not self._shutdown:
                try:
                    now = _dt.now(_tz.utc)
                    epoch = int(now.timestamp())
                    # compute next TF boundary
                    next_epoch = ((epoch // tf_seconds) + 1) * tf_seconds
                    next_boundary = _dt.fromtimestamp(next_epoch, _tz.utc)

                    # schedule prewarm1
                    target1 = next_boundary - _td(seconds=prewarm1_offset)
                    delay1 = (target1 - _dt.now(_tz.utc)).total_seconds()
                    if delay1 > 0:
                        # add small jitter so many brokers don't fire exactly same ms
                        delay1 += random.uniform(-jitter_range, jitter_range)
                        if delay1 > 0:
                            _time.sleep(delay1)
                    # run prewarm1 inline on this runner thread
                    _do_warm_inline(broker, "prewarm1")

                    # compute time remaining to prewarm2
                    target2 = next_boundary - _td(seconds=prewarm2_offset)
                    delay2 = (target2 - _dt.now(_tz.utc)).total_seconds()
                    if delay2 > 0:
                        # sleep until prewarm2
                        _time.sleep(delay2)
                    # run prewarm2 inline on this runner thread
                    _do_warm_inline(broker, "prewarm2")

                    # sleep until just after the boundary to avoid tight-loop
                    rem = (next_boundary - _dt.now(_tz.utc)).total_seconds()
                    if rem > 0:
                        # small cushion after boundary
                        _time.sleep(rem + 0.01)

                except Exception as e:
                    logger.exception("WarmRunner for conn=%s encountered error: %s", getattr(broker, "connection_name", "unknown"), e)
                    # On unexpected error, back off briefly then continue loop
                    _time.sleep(0.5)

        # start exactly one persistent runner thread per broker
        runner_name = f"WarmRunner-{getattr(broker, 'connection_name', getattr(broker, 'account_id', 'unknown'))}"
        t = threading.Thread(target=runner, daemon=True, name=runner_name)
        t.start()


    def _parse_timestamp(self, ts) -> datetime:
        try:
            if ts is None:
                return datetime.utcnow().replace(tzinfo=timezone.utc)
            if isinstance(ts, str):
                return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
            if isinstance(ts, datetime):
                return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
        except Exception:
            return datetime.utcnow().replace(tzinfo=timezone.utc)

    def _diagnostic_log_tick(self, source: str, sym: str, price: float, volume: float, ts: datetime):
        if not DIAGNOSTIC_TICK_LOG:
            return
        try:
            ts_utc = ts.astimezone(timezone.utc) if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
            parts = []
            for tf in self.aggregator.timeframes:
                start = self.aggregator._get_candle_start(ts_utc, tf)
                parts.append(f"{tf}:{start.isoformat()}")
            logger.debug(f"[TICK MAP] {sym} src={source} p={price} v={volume} ts={ts_utc.isoformat()} -> {' | '.join(parts)}")
        except Exception as e:
            logger.error(f"[TICK MAP ERROR] {e}", exc_info=True)

    def _start_strategy_worker(self, strat):
        """
        Create a persistent worker thread for strat if not already started.
        Worker name will be Worker-<StrategyName> (or Worker-Strategy-<id>).
        The worker consumes jobs of the form (sym, tf, window) or (fn, args, kwargs).
        """
        import queue as _queue

        name = getattr(strat, "name", f"Strategy-{id(strat)}")
        qkey = f"WorkerQueue-{name}"

        # idempotent: don't recreate if exists
        if qkey in self._strategy_queues:
            return

        q = _queue.Queue(maxsize=1000)  # bounded to avoid runaway memory (tune if needed)
        self._strategy_queues[qkey] = q

        pause_ev = threading.Event()
        pause_ev.set()
        self._strategy_pause_flags[qkey] = pause_ev

        def _worker_loop():
            th = threading.current_thread()
            logger.info("[WORKER START] assigned=%s thread=%s", name, th.name)
            # attach monitor fields on strategy instance for visibility
            try:
                setattr(strat, "_worker_last_run", None)
                setattr(strat, "_worker_thread_name", th.name)
            except Exception:
                pass

            while not self._shutdown:
                if not pause_ev.is_set():
                    # paused — small sleep loop
                    time.sleep(0.05)
                    continue
                try:
                    job = q.get(timeout=0.5)
                except _queue.Empty:
                    continue

                try:
                    # --- support both candle jobs and generic function jobs ---
                    if isinstance(job, tuple) and len(job) == 3 and isinstance(job[0], str):
                        # candle job: (sym, tf, window)
                        sym, tf, window = job
                        setattr(strat, "_worker_last_run", datetime.now(timezone.utc))
                        strat.on_new_candles(sym, tf, window)

                    elif isinstance(job, tuple) and callable(job[0]):
                        # function job: (fn, args, kwargs)
                        fn, args, kwargs = job
                        setattr(strat, "_worker_last_run", datetime.now(timezone.utc))
                        logger.info("[WORKER] Executing job %s for %s", fn.__name__, name)
                        fn(*args, **kwargs)

                    else:
                        logger.warning("[WORKER] Unknown job format for %s: %r", name, job)

                except Exception:
                    logger.exception("Uncaught exception in worker for %s", name)
                finally:
                    try:
                        q.task_done()
                    except Exception:
                        pass

            logger.info("[WORKER EXIT] assigned=%s thread=%s", name, th.name)

        t = threading.Thread(target=_worker_loop, daemon=True, name=f"Worker-{name}")
        t.start()
        self._strategy_workers[qkey] = t

    def enqueue_task(self, strat, fn, *args, **kwargs):
            """
            Push a function into the given strategy’s worker thread.
            Example: app.enqueue_task(strategy2, strategy2.reconcile_on_reconnect)
            """
            name = getattr(strat, "name", f"Strategy-{id(strat)}")
            qkey = f"WorkerQueue-{name}"
            q = self._strategy_queues[qkey]
            q.put((fn, args, kwargs))

    def enqueue_reconcile(self, strat):
            """
            Convenience wrapper: enqueue reconcile_on_reconnect into strat's worker.
            """
            self.enqueue_task(strat, strat.reconcile_on_reconnect)



    # ---------------------------
    # Market Hub
    # ---------------------------
    def setup_market_hub(self):
        url = f"wss://rtc.topstepx.com/hubs/market?access_token={self.token}"
        self.market_hub_connection = (
            HubConnectionBuilder()
            .with_url(url)
            .configure_logging(logging.INFO)
            .with_automatic_reconnect({"keep_alive_interval": 10, "reconnect_interval": 5, "max_attempts": 50})
            .build()
        )
        self.market_hub_connection.on_open(self.on_market_hub_open)
        self.market_hub_connection.on_close(self.on_market_hub_close)
        self.market_hub_connection.on_error(self.on_market_hub_error)


        self.market_hub_connection.on("GatewayQuote", self.on_gateway_quote)
        self.market_hub_connection.on("GatewayTrade", self.on_gateway_trade)
        self.market_hub_connection.on("GatewayDepth", self.on_gateway_depth)

    def _watched_contracts(self) -> Set[str]:
        # Union of all strategies' contract IDs
        return {getattr(s, "contract_id") for s in self.strategies if getattr(s, "contract_id", None)}

    def on_market_hub_open(self):
        logger.info("✅ SignalR Market Hub connection started.")
        try:
            symbols = list(self._watched_contracts())
            if not symbols:
                logger.warning("No strategies registered yet; not subscribing market hub.")
            else:
                for sym in symbols:
                    self.market_hub_connection.send("SubscribeContractQuotes", [sym])
                    self.market_hub_connection.send("SubscribeContractTrades", [sym])
                logger.info(f"✅ Subscribed to Quotes/Trades for {symbols}.")
        except Exception as e:
            logger.error(f"Subscribe error: {e}", exc_info=True)

        # Mark the hub as confirmed-connected from our side (drives reconnect loop)
        self._market_confirmed_connected = True

        # library-ready event + backoff reset
        self.market_ready_event.set()
        self._market_backoff_attempt = 0

        # Force full reconcile after reconnect
        self._market_hub_ready = True
        self._maybe_cold_start_reconcile()

    def on_market_hub_close(self):
        logger.warning("❌ SignalR Market Hub connection closed.")
        # Clear ready event and our confirmed flag immediately
        self.market_ready_event.clear()
        self._market_confirmed_connected = False

        # We do NOT manually restart here (let auto-reconnect try first).
        # Arm watchdog to catch any quiet-dead after reconnect.
        self._arm_watchdog("market_hub_close")
        self._market_hub_ready = False
        self._did_cold_start_reconcile = False

    def on_market_hub_error(self, data):
        logger.error(f"🔴 SignalR Market Hub error: {data}")
        # Clear our confirmed flag so the reconnect loop will notice
        self._market_confirmed_connected = False

        # Arm watchdog on errors as well.
        self._arm_watchdog("market_hub_error")
        # try to reconcile (no-op if throttled)
        self._reconcile_if_needed("reconnect")


    def on_gateway_quote(self, args):
        # update fast tick heartbeat for watchdog
        self._last_tick_at = datetime.now(timezone.utc)
        self._last_market_event_at = self._last_tick_at  # informational

        try:
            if not args:
                return
            if isinstance(args, list) and len(args) >= 2:
                contract_id = str(args[0])
                data = args[1]
            else:
                contract_id = None
                data = args[0] if isinstance(args, list) else args

            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except Exception:
                    return
            if not isinstance(data, dict):
                return

            price = data.get("lastPrice") or data.get("price") or data.get("p")
            volume = data.get("lastSize") or data.get("size") or 0
            ts = data.get("timestamp") or data.get("t")
            if price is None or contract_id is None:
                return

            dt = self._parse_timestamp(ts)
            self._diagnostic_log_tick("QUOTE", contract_id, float(price), float(volume or 0), dt)
            self.aggregator.ingest_tick(contract_id, float(price), float(volume or 0), dt)
        except Exception as e:
            logger.error(f"Error processing GatewayQuote: {e}", exc_info=True)

    def on_gateway_trade(self, args):
        # update fast tick heartbeat for watchdog
        self._last_tick_at = datetime.now(timezone.utc)
        self._last_market_event_at = self._last_tick_at  # informational

        try:
            if not args:
                return
            if isinstance(args, list) and len(args) >= 2:
                contract_id = str(args[0])
                data = args[1]
            else:
                contract_id = None
                data = args[0] if isinstance(args, list) else args

            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except Exception:
                    return

            trades = [data] if isinstance(data, dict) else (data if isinstance(data, list) else [])
            for tick in trades:
                if isinstance(tick, str):
                    try:
                        tick = json.loads(tick)
                    except Exception:
                        continue
                if not isinstance(tick, dict):
                    continue

                price = tick.get("price") or tick.get("lastPrice") or tick.get("p")
                volume = tick.get("volume") or tick.get("lastSize") or tick.get("s") or 1
                ts = tick.get("timestamp") or tick.get("t")
                if price is None or contract_id is None:
                    continue

                dt = self._parse_timestamp(ts)
                self._diagnostic_log_tick("TRADE", contract_id, float(price), float(volume or 0), dt)
                self.aggregator.ingest_tick(contract_id, float(price), float(volume or 0), dt)
        except Exception as e:
            logger.error(f"Error processing GatewayTrade: {e}", exc_info=True)

    def on_gateway_depth(self, args):
        logger.debug(f"GatewayDepth raw args: {args}")

    # ---------------------------
    # User Hub (single account session)
    # ---------------------------
    def setup_user_hub(self):
        url = f"wss://rtc.topstepx.com/hubs/user?access_token={self.token}"
        self.user_hub_connection = (
            HubConnectionBuilder()
            .with_url(url)
            .configure_logging(logging.INFO)
            .with_automatic_reconnect({"keep_alive_interval": 10, "reconnect_interval": 5, "max_attempts": 50})
            .build()
        )

        self.user_hub_connection.on_open(self.on_user_hub_open)
        self.user_hub_connection.on_close(self.on_user_hub_close)
        self.user_hub_connection.on_error(self.on_user_hub_error)


        self.user_hub_connection.on("GatewayUserAccount", self.on_gateway_user_account)
        self.user_hub_connection.on("GatewayUserOrder", self.on_gateway_user_order)
        self.user_hub_connection.on("GatewayUserPosition", self.on_gateway_user_position)
        self.user_hub_connection.on("GatewayUserTrade", self.on_gateway_user_trade)

    def _resubscribe_user(self):
        try:
            logger.info("🔄 Resubscribing to user hub...")
            self.user_hub_connection.send("SubscribeAccounts", [])
            accounts = {
                int(getattr(getattr(s, "live_broker", None), "account_id", -1))
                for s in self.strategies if getattr(s, "live_broker", None)
            }
            for acc in accounts:
                if acc > 0:
                    self.user_hub_connection.send("SubscribeOrders", [acc])
                    self.user_hub_connection.send("SubscribePositions", [acc])
                    self.user_hub_connection.send("SubscribeTrades", [acc])
        except Exception as e:
            logger.error(f"User hub resubscribe failed: {e}", exc_info=True)

    def _resubscribe_market(self):
        try:
            logger.info("🔄 Resubscribing to market hub...")
            for contract in self._watched_contracts():
                self.market_hub_connection.send("SubscribeContractQuotes", [contract])
                self.market_hub_connection.send("SubscribeContractTrades", [contract])
                self.market_hub_connection.send("SubscribeContractMarketDepth", [contract])
        except Exception as e:
            logger.error(f"Market hub resubscribe failed: {e}", exc_info=True)


    def _subscribe_user_once(self):
        """
        Attempt to subscribe after on_open. Retries for a short window if the hub is still starting.
        Single-flight guarded.
        """
        if self._user_subscribed_flag:
            return

        with self._user_subscribe_once_lock:
            if self._user_subscribed_flag:
                return

            accounts = sorted({
                int(getattr(getattr(s, "live_broker", None), "account_id", -1))
                for s in self.strategies
                if getattr(s, "live_broker", None) is not None
            })

            attempts = 0
            max_attempts = 10  # ~20–30s depending on sleep
            while attempts < max_attempts and not self._user_subscribed_flag:
                attempts += 1
                try:
                    # Subscribe per-account feeds; Skip SubscribeAccounts (not required).
                    for acc in accounts:
                        if acc > 0:
                            self.user_hub_connection.send("SubscribeOrders", [acc])
                            self.user_hub_connection.send("SubscribePositions", [acc])
                            self.user_hub_connection.send("SubscribeTrades", [acc])
                    self._user_subscribed_flag = True
                    logger.info(f"✅ Subscribed to user data for accounts {accounts}.")
                    break
                except HubConnectionError as e:
                    logger.error(f"Subscribe user hub error (attempt {attempts}): {e}", exc_info=True)
                    time.sleep(2.0)
                except Exception as e:
                    logger.error(f"Unexpected user subscribe error (attempt {attempts}): {e}", exc_info=True)
                    time.sleep(2.0)

    def on_user_hub_open(self):
        logger.info("✅ SignalR User Hub connection started.")

        def _subscribe_user_with_retries(max_retries=10, delay=2):
            """
            Try to subscribe with retries, in case the hub handshake isn't fully complete yet.
            """
            accounts = sorted({
                int(getattr(getattr(s, "live_broker", None), "account_id", -1))
                for s in self.strategies
                if getattr(s, "live_broker", None) is not None
            })

            for attempt in range(1, max_retries + 1):
                try:
                    self.user_hub_connection.send("SubscribeAccounts", [])
                    for acc in accounts:
                        if acc > 0:
                            self.user_hub_connection.send("SubscribeOrders", [acc])
                            self.user_hub_connection.send("SubscribePositions", [acc])
                            self.user_hub_connection.send("SubscribeTrades", [acc])
                    logger.info(f"✅ Subscribed to user data for accounts {accounts}.")
                    return True
                except Exception as e:
                    logger.error(f"Subscribe user hub error (attempt {attempt}): {e}", exc_info=True)
                    time.sleep(delay)

            logger.error("❌ Failed to subscribe user hub after retries.")
            return False

        if _subscribe_user_with_retries():
            # Mark the hub as confirmed-connected from our side
            self._user_confirmed_connected = True

            self.user_ready_event.set()
            # 🔄 reconcile here to clean up orphaned SL/TP etc.
            self._user_hub_ready = True
            self._maybe_cold_start_reconcile()
        else:
            # If we could not subscribe, keep cleared
            self._user_confirmed_connected = False
            self.user_ready_event.clear()

    def _reconnect_loop(self):
        """
        Manages hub connections and triggers a full process exit (so run.sh / container restart can recover).
        Runs in a background thread.

        This version:
          - Prefers explicit confirmed-connected flags set by our on_open callbacks.
          - Uses a confirmation timer so transient startup races won't force a restart.
          - Falls back to inspecting library internals if our flags are not set.
        """
        logger.info("Starting reconnect loop...")
        interval = float(getattr(self, "_hub_reconnect_interval", 2.0))
        # how long the reconnect loop will require a persistent "not connected"
        CONFIRM_SECS = float(getattr(self, "_disconnect_confirm_secs", 3.0))

        # markers: timestamp when we first observed "not connected"
        market_mark = None
        user_mark = None

        def _is_connected_library_way(conn) -> bool:
            """Resilient check of the underlying library's connection state (fallback)."""
            if not conn:
                return False
            hub = getattr(conn, "hub", None)
            if hub is not None:
                try:
                    return bool(getattr(hub, "is_connected", False))
                except Exception:
                    pass
            try:
                return bool(getattr(conn, "is_connected", False))
            except Exception:
                return False

        while not getattr(self, "_shutdown", False):
            try:
                market_conn = getattr(self, "market_hub_connection", None)
                user_conn = getattr(self, "user_hub_connection", None)

                # Prefer our confirmed-connected flags (set in on_*_open)
                market_ok = bool(getattr(self, "_market_confirmed_connected", False)) or _is_connected_library_way(market_conn)
                user_ok = bool(getattr(self, "_user_confirmed_connected", False)) or _is_connected_library_way(user_conn)

                now = time.time()

                # MARKET
                if not market_ok:
                    if market_mark is None:
                        # first observation of not-connected -> start timer
                        market_mark = now
                        logger.info(f"🔍 Market Hub not connected according to reconnect loop — starting confirmation timer (confirm_secs={CONFIRM_SECS}).")
                    else:
                        age = now - market_mark
                        # If confirmed not-connected for longer than CONFIRM_SECS -> exit
                        if age >= CONFIRM_SECS:
                            logger.critical(f"🔴 Market Hub disconnect CONFIRMED by reconnect loop. age={int(age)}s reason=detected-by-reconnect-loop-not-connected. Triggering full shutdown.")
                            try:
                                self.flush_memory(reason="market-hub-confirmed-disconnect")
                            except Exception:
                                logger.exception("Error while flushing memory during market-hub-confirmed-disconnect")
                            # Force immediate exit so run.sh / Docker restarts the container
                            os._exit(1)
                else:
                    # connected -> reset marker
                    if market_mark is not None:
                        logger.info("✅ Market Hub connection confirmed by reconnect loop; clearing confirmation timer.")
                    market_mark = None

                # USER
                if not user_ok:
                    if user_mark is None:
                        user_mark = now
                        logger.info(f"🔍 User Hub not connected according to reconnect loop — starting confirmation timer (confirm_secs={CONFIRM_SECS}).")
                    else:
                        age = now - user_mark
                        if age >= CONFIRM_SECS:
                            logger.critical(f"🔴 User Hub disconnect CONFIRMED by reconnect loop. age={int(age)}s reason=detected-by-reconnect-loop-not-connected.")
                            try:
                                self.flush_memory(reason="user-hub-confirmed-disconnect")
                            except Exception:
                                logger.exception("Error while flushing memory during user-hub-confirmed-disconnect")
                            os._exit(1)
                else:
                    if user_mark is not None:
                        logger.info("✅ User Hub connection confirmed by reconnect loop; clearing confirmation timer.")
                    user_mark = None

                # Sleep until next iteration (allow dynamic interval)
                interval = float(getattr(self, "_hub_reconnect_interval", interval))
                time.sleep(interval)

            except Exception:
                logger.exception("Error in reconnect loop; will retry after interval")
                interval = float(getattr(self, "_hub_reconnect_interval", interval))
                time.sleep(interval)

        logger.info("Reconnect loop shutting down.")



    def on_user_hub_close(self):
        logger.warning("❌ SignalR User Hub connection closed.")
        # clear user-ready and confirmed flags
        self.user_ready_event.clear()
        self._user_confirmed_connected = False

        # Do not restart on a timer; let automatic reconnect do its thing.
        # Arm watchdog to watch the market stream post-interruption.
        self._arm_watchdog("user_hub_close")
        # Always reset so next on_open runs reconcile
        self._user_hub_ready = False
        self._did_cold_start_reconcile = False

    def on_user_hub_error(self, data):
        logger.error(f"🔴 SignalR User Hub error: {data}")
        # Clear confirmed flag so reconnect loop will treat it as disconnected
        self._user_confirmed_connected = False

        # Arm watchdog on errors too (in case reconnect leads to quiet-dead on market stream).
        self._reconcile_if_needed("cold-start")
        self._user_hub_ready = False
        self._did_cold_start_reconcile = False



    # ---------- Normalize + route user payloads ----------
    def _touch_user_heartbeat(self):
        self._last_user_event_at = datetime.now(timezone.utc)

    def on_gateway_user_account(self, args):
        self._touch_user_heartbeat()
        try:
            logger.debug(f"RAW GatewayUserAccount: {args}")
            data = None
            if isinstance(args, dict):
                data = args.get("data", args)
            elif isinstance(args, list) and isinstance(args[0], dict):
                data = args[0].get("data", args[0])

            # Accept either list of accounts or single
            if isinstance(data, list):
                mapping = {}
                for acc in data:
                    if not acc:
                        continue
                    mapping[int(acc.get("id"))] = acc
                self.latest_account_info = mapping
            elif isinstance(data, dict):
                if isinstance(self.latest_account_info, dict) and any(isinstance(v, dict) for v in self.latest_account_info.values()):
                    self.latest_account_info[int(data.get("id"))] = data
                else:
                    self.latest_account_info = {int(data.get("id")): data}

            logger.info("✅ Account info updated.")

            # Broadcast to all strategies (they'll ignore if not relevant)
            for strat in self.strategies:
                try:
                    strat.handle_account_update(
                        self.latest_account_info.get(getattr(getattr(strat, "live_broker", None), "account_id", -1))
                        if isinstance(self.latest_account_info, dict) else self.latest_account_info
                    )
                except Exception as e:
                    logger.error(f"Error forwarding account update: {e}", exc_info=True)

            # show snapshot
            try:
                self._log_account_snapshot()
            except Exception:
                logger.debug("Snapshot log failed during account update.", exc_info=True)

        except Exception as e:
            logger.error(f"Error handling GatewayUserAccount: {e}", exc_info=True)

    def on_gateway_user_order(self, args):
        self._touch_user_heartbeat()
        """
        Normalize *all* order updates and forward them to the right strategy.
        Match by (accountId) and either (contractId == strategy.contract_id) OR (symbolId fallback).
        """
        try:
            logger.debug(f"RAW GatewayUserOrder: {args}")
            raw_list = args if isinstance(args, list) else [args]
            changed_orders = []

            for entry in raw_list:
                data = entry.get("data") if isinstance(entry, dict) else None
                if not data:
                    continue

                order_id = int(data.get("id"))
                status = data.get("status")  # 0=new,1=working,2=filled,3=cancelled,4=rejected,5=expired,6=partial

                contract_id = str(data.get("contractId")) if data.get("contractId") is not None else None
                symbol_id   = str(data.get("symbolId"))   if data.get("symbolId")   is not None else None

                normalized = {
                    "id": order_id,
                    "symbol": contract_id or symbol_id,
                    "contractId": contract_id,
                    "symbolId": symbol_id,
                    "side": "buy" if data.get("side") == 0 else "sell",
                    "type": data.get("type"),
                    "size": data.get("size"),
                    "filled": data.get("fillVolume", 0) or 0,
                    "price": data.get("filledPrice") or data.get("limitPrice") or data.get("stopPrice"),
                    "status": status,
                    "raw": data,
                }
                changed_orders.append(normalized)

                if status in (0, 1, 6):  # new, working, partial
                    self._active_orders[order_id] = normalized
                else:
                    self._active_orders.pop(order_id, None)

            self.latest_orders = list(self._active_orders.values())

            # Route to strategies
            for strat in self.strategies:
                strat_contract = getattr(strat, "contract_id", None)
                strat_broker   = getattr(strat, "live_broker", None)
                strat_acct     = int(getattr(strat_broker, "account_id", -1)) if strat_broker else -1

                subset = []
                for o in changed_orders:
                    acc_ok = int((o["raw"] or {}).get("accountId", -1)) == strat_acct
                    cid_ok = (o.get("contractId") == str(strat_contract))
                    sid_ok = False
                    if o.get("symbolId") and str(strat_contract):
                        sid_ok = o["symbolId"] in str(strat_contract)
                    if acc_ok and (cid_ok or sid_ok):
                        subset.append(o)

                if subset:
                    try:
                        strat.handle_order_update(subset)
                    except Exception as e:
                        logger.error(f"Error forwarding order update: {e}", exc_info=True)

            try:
                self._log_account_snapshot()
            except Exception:
                logger.debug("Snapshot log failed during order update.", exc_info=True)

        except Exception as e:
            logger.error(f"Error handling GatewayUserOrder: {e}", exc_info=True)



    def on_gateway_user_position(self, args):
        self._touch_user_heartbeat()
        logger.debug(f"RAW GatewayUserPosition args (type={type(args)}): {args!r}")

        try:
            # normalize raw_entries
            raw_entries = []
            if isinstance(args, dict):
                raw_entries.append(args)
            elif isinstance(args, list):
                if len(args) >= 2 and isinstance(args[1], dict) and not isinstance(args[0], dict):
                    raw_entries.append(args[1])
                else:
                    for item in args:
                        if isinstance(item, dict):
                            raw_entries.append(item)
                        elif isinstance(item, list) and len(item) >= 2 and isinstance(item[1], dict):
                            raw_entries.append(item[1])
                        else:
                            logger.debug("Skipping unexpected GatewayUserPosition item: %r", item)
            else:
                logger.debug("on_gateway_user_position: unexpected args type %s", type(args))
                return

            changed_positions = []
            for entry in raw_entries:
                data = entry.get("data", entry) if isinstance(entry, dict) else entry
                if not isinstance(data, dict):
                    continue

                normalized = {
                    "raw": data,
                    "id": data.get("id"),
                    "accountId": int(data.get("accountId", -1)) if data.get("accountId") is not None else -1,
                    "contractId": data.get("contractId"),
                    "creationTimestamp": data.get("creationTimestamp"),
                    "type": data.get("type"),
                    "size": data.get("size"),
                    "averagePrice": data.get("averagePrice"),
                }

                logger.info(
                    "[POS-IN] acct=%s contract=%s size=%s avgPrice=%s id=%s",
                    normalized["accountId"],
                    normalized["contractId"],
                    normalized["size"],
                    normalized["averagePrice"],
                    normalized["id"],
                )
                changed_positions.append(normalized)

            if not changed_positions:
                return

            # group by accountId
            grouped: dict[int, list[dict]] = {}
            for p in changed_positions:
                grouped.setdefault(p["accountId"], []).append(p)

            # update cache
            for acct, plist in grouped.items():
                self.latest_positions_by_acct[acct] = plist

            # forward to strategies
            for strat in self.strategies:
                if not hasattr(strat, "handle_position_update"):
                    continue
                strat_contract = getattr(strat, "contract_id", None)
                strat_broker = getattr(strat, "live_broker", None)
                strat_acct = int(getattr(strat_broker, "account_id", -1)) if strat_broker else -1

                subset = [
                    p for p in changed_positions
                    if p["accountId"] == strat_acct and str(p["contractId"]) == str(strat_contract)
                ]

                if subset:
                    try:
                        strat.handle_position_update(subset)
                    except Exception as e:
                        logger.error(f"Error forwarding position update to {getattr(strat, 'name', strat)}: {e}", exc_info=True)

            try:
                self._log_account_snapshot()
            except Exception:
                logger.debug("Snapshot log failed during position update.", exc_info=True)

        except Exception as e:
            logger.error(f"Error handling GatewayUserPosition: {e}", exc_info=True)


    def _position_printer(self):
        """Background thread: print latest GatewayUserPosition every 1s."""
        while not self._shutdown:
            try:
                if self.latest_positions:
                    logger.info(f"🔎 Cached GatewayUserPosition snapshot: {self.latest_positions}")
                else:
                    logger.info("🔎 No cached GatewayUserPosition yet.")
                time.sleep(1.0)
            except Exception:
                logger.exception("Error in position printer loop")
                time.sleep(1.0)



    def on_gateway_user_trade(self, args):
        self._touch_user_heartbeat()
        """
        Forward only trades for the strategy's (accountId, contractId).
        Accept symbolId as a fallback if contractId isn't present.
        """
        try:
            logger.debug(f"RAW GatewayUserTrade: {args}")
            items = args if isinstance(args, list) else [args]

            for strat in self.strategies:
                strat_contract = getattr(strat, "contract_id", None)
                strat_broker   = getattr(strat, "live_broker", None)
                strat_acct     = int(getattr(strat_broker, "account_id", -1)) if strat_broker else -1

                rel = []
                for it in items:
                    data = it.get("data") if isinstance(it, dict) else None
                    if not data:
                        continue
                    if int(data.get("accountId", -1)) != strat_acct:
                        continue

                    cid = str(data.get("contractId")) if data.get("contractId") is not None else None
                    sid = str(data.get("symbolId"))   if data.get("symbolId")   is not None else None

                    cid_ok = (cid == str(strat_contract))
                    sid_ok = (sid in str(strat_contract)) if (sid and strat_contract) else False

                    if cid_ok or sid_ok:
                        rel.append(data)

                if rel:
                    try:
                        strat.handle_trade_update(rel)
                    except Exception as e:
                        logger.error(f"Error forwarding trade update: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Error handling GatewayUserTrade: {e}", exc_info=True)

    # ---------------------------
    # Candle handling
    # ---------------------------
    def _write_candles_worker(self, sym: str, tf: str, candles: List[dict]):
        """Runs in background so Firestore I/O never delays strategy dispatch."""
        try:
            write_candles(sym, tf, candles)
        except Exception as e:
            logger.error(f"Error writing candles to Firestore: {e}", exc_info=True)

    def main_loop(self):
        """
        Main data loop:
          - Pull newly-finalized candles from the aggregator
          - Maintain a short rolling window per (symbol,timeframe)
          - Debounce and DISPATCH to strategies that match (contract_id, timeframe)
          - Offload Firestore writes so they never block strategy execution
          - Print an account snapshot every ~30s
        """
        logger.info("📊 Starting main data loop.")
        last_snapshot_time = time.time()

        while not self._shutdown:
            try:
                finalized = self.aggregator.collect_newly_finalized()
                had_any_finalization = False

                for tf, by_sym in finalized.items():
                    for sym, candles in by_sym.items():
                        if not candles:
                            continue
                        had_any_finalization = True

                        key = (sym, tf)
                        self.candle_history.setdefault(key, [])
                        self.candle_history[key].extend(candles)
                        if len(self.candle_history[key]) > 5:
                            self.candle_history[key] = self.candle_history[key][-5:]

                        last = candles[-1]
                        curr_start = last.get("start") or last.get("t")
                        if isinstance(curr_start, str):
                            try:
                                curr_start = datetime.fromisoformat(curr_start)
                            except Exception:
                                curr_start = None

                        last_seen = self._last_candle_started.get(key)
                        is_dup = (curr_start is not None and last_seen is not None and curr_start == last_seen)

                        if is_dup:
                            logger.debug(f"[SKIP STRATEGY] Duplicate finalize {sym} {tf} @ {curr_start}")
                        else:
                            self._last_candle_started[key] = curr_start

                            window = self.candle_history[key]
                            last_t = window[-1].get("t") if window else None

                            # Dispatch to matching strategies — use persistent worker queue if available,
                            # otherwise fall back to a short-lived ephemeral thread.
                            for strat in self.strategies:
                                if getattr(strat, "contract_id", None) == sym and getattr(strat, "strategy_timeframe", None) == tf:
                                    try:
                                        # Ensure lb is defined in local scope (avoid NameError)
                                        lb = getattr(strat, "live_broker", None)

                                        # DO NOT block the main loop with synchronous warmup.
                                        # If you want warmup, rely on scheduled warmups + constructor warmup.
                                        if lb:
                                            pass

                                        name = getattr(strat, "name", getattr(strat, "__class__", type(strat)).__name__)
                                        logger.info(
                                            f"→ Dispatch {sym} {tf} to {name} (candles={len(window)}, last_t={last_t})"
                                        )

                                        # prepare job payload
                                        payload = (sym, tf, list(window))
                                        qkey = f"WorkerQueue-{name}"
                                        q = self._strategy_queues.get(qkey)

                                        if q is None:
                                            # worker missing -> fallback to ephemeral thread (rare)
                                            def _run_strat(s, a, b, c):
                                                try:
                                                    s.on_new_candles(a, b, c)
                                                except Exception as e:
                                                    logger.exception(f"Uncaught exception in strategy {getattr(s, 'name', 'strategy')}: {e}")

                                            threading.Thread(
                                                target=_run_strat,
                                                args=(strat, sym, tf, list(window)),
                                                daemon=True,
                                                name=f"Dispatch-{name}-{sym}-{tf}"
                                            ).start()
                                        else:
                                            # non-blocking put; if queue is full, fallback to ephemeral spawn
                                            try:
                                                q.put_nowait(payload)
                                            except Exception:
                                                logger.warning("Strategy queue full or put failed for %s - falling back to ephemeral thread", name)
                                                def _run_strat(s, a, b, c):
                                                    try:
                                                        s.on_new_candles(a, b, c)
                                                    except Exception as e:
                                                        logger.exception(f"Uncaught exception in strategy {getattr(s, 'name', 'strategy')}: {e}")

                                                threading.Thread(
                                                    target=_run_strat,
                                                    args=(strat, sym, tf, list(window)),
                                                    daemon=True,
                                                    name=f"Dispatch-{name}-{sym}-{tf}"
                                                ).start()

                                    except Exception as e:
                                        logger.error(f"Error scheduling strategy.on_new_candles for {sym}: {e}", exc_info=True)


                        ready = self.aggregator.get_candles_to_write(sym, tf)
                        if ready:
                            threading.Thread(
                                target=self._write_candles_worker,
                                args=(sym, tf, ready),
                                daemon=True,
                            ).start()

                # If any timeframe/symbol finalized, update watchdog backup heartbeat
                if had_any_finalization:
                    self._last_candle_finalized_at = datetime.now(timezone.utc)

                if time.time() - last_snapshot_time >= 30:
                    self.print_account_status()
                    try:
                        self._log_account_snapshot()
                    except Exception:
                        logger.debug("Snapshot logging failed on interval.", exc_info=True)
                    last_snapshot_time = time.time()

                time.sleep(0.02)

            except Exception as e:
                logger.error(f"Main loop error: {e}", exc_info=True)
                time.sleep(1.0)


    def print_account_status(self):
        logger.info("===== ACCOUNT STATUS SNAPSHOT =====")
        if self.latest_account_info:
            if isinstance(self.latest_account_info, dict):
                for aid, info in self.latest_account_info.items():
                    logger.info(
                        f"Account {aid}: Balance: {info.get('balance')} | "
                        f"Can Trade: {info.get('canTrade')} | "
                        f"Simulated: {info.get('simulated')}"
                    )
            else:
                acc = self.latest_account_info
                logger.info(
                    f"Balance: {acc.get('balance')} | "
                    f"Can Trade: {acc.get('canTrade')} | "
                    f"Simulated: {acc.get('simulated')}"
                )
        else:
            logger.info("Account info: (no data yet)")

        if self.latest_orders:
            for o in self.latest_orders:
                logger.info(
                    f"Order: {o.get('symbol') or o.get('contractId') or o.get('symbolId') or '?'} | "
                    f"{o.get('side', '?')} | "
                    f"{o.get('size', '?')} | "
                    f"status={o.get('status', '?')} | "
                    f"price={o.get('price', '?')} (id={o.get('id', '?')})"
                )

        if self.latest_positions:
            for p in self.latest_positions:
                logger.info(
                    f"Position: {p.get('contractId', '?')} | "
                    f"size={p.get('size', '?')} | "
                    f"avgPrice={p.get('averagePrice', '?')} | "
                    f"accountId={p.get('accountId', '?')} (id={p.get('id', '?')})"
                )
        else:
            logger.info("Positions: (no open positions)")

        logger.info("===================================")

    # ---------------------------
    # Account snapshot logging
    # ---------------------------
    def _log_account_snapshot(self):
        """
        Console-only diagnostic: shows balance, positions and SL/TP orders per account.
        Uses cached SignalR data when available, but falls back to broker /searchOpen
        on reconnect races so we don't print misleading 'No active orders'.
        """
        try:
            OPEN_STATUSES = {1, 6}  # 1=WORKING, 6=NEW/ACCEPTED (treat both as open)

            def _norm_price(o_raw: dict) -> str:
                # prefer limit/stop; fallback to any 'price' field the cache might have
                lp = o_raw.get("limitPrice")
                sp = o_raw.get("stopPrice")
                if lp is not None:
                    return f"{float(lp)}"
                if sp is not None:
                    return f"{float(sp)}"
                # some caches store a unified 'price'
                pr = o_raw.get("price")
                return f"{pr}" if pr is not None else "?"

            def _is_my_sltp(tag: str, strat_name: str, acct_id: int, contract_id: str) -> bool:
                if not tag:
                    return False
                tag = str(tag)
                # Accept normal flow, RECON and REPAIRED patterns for this strategy
                if tag.startswith(f"{strat_name}:{contract_id}:"):
                    return True
                if f"{strat_name}:{acct_id}:RECON:" in tag:
                    return True
                if f"{strat_name}:{contract_id}:REPAIRED:" in tag:
                    return True
                return False

            logger.info("\n════════════════════════════════════════════════════════════")
            logger.info("📌 ACCOUNT SNAPSHOT — Positions, Balance & SL/TP Orders")
            logger.info("════════════════════════════════════════════════════════════")

            # map account -> strategy name and contract (so tags & filters work)
            strat_by_acct = {}
            for s in getattr(self, "strategies", []):
                try:
                    lb = getattr(s, "live_broker", None)
                    acc = int(getattr(lb, "account_id", -1))
                    if acc > 0:
                        strat_by_acct[acc] = {
                            "name": getattr(s, "name", "Strategy"),
                            "contract": str(getattr(s, "contract_id", "")),
                            "broker": lb,
                        }
                except Exception:
                    pass

            # balances
            balances = {}
            if isinstance(self.latest_account_info, dict):
                # could be {acctId: {...}} or a single dict with id
                for k, v in self.latest_account_info.items():
                    if isinstance(v, dict) and str(k).isdigit():
                        balances[int(k)] = v.get("balance") or v.get("availableFunds") or "?"
                if self.latest_account_info.get("id"):
                    balances[int(self.latest_account_info["id"])] = (
                        self.latest_account_info.get("balance")
                        or self.latest_account_info.get("availableFunds")
                        or "?"
                    )
            elif isinstance(self.latest_account_info, list):
                for a in self.latest_account_info:
                    try:
                        balances[int(a.get("id"))] = a.get("balance") or a.get("availableFunds") or "?"
                    except Exception:
                        pass

            # group cached positions by acct
            positions_by_acct = {}
            for pos in (self.latest_positions or []):
                data = pos.get("raw") if isinstance(pos, dict) and pos.get("raw") else pos
                if isinstance(data, dict):
                    acct_id = int(data.get("accountId", -1))
                    if acct_id > 0:
                        positions_by_acct.setdefault(acct_id, []).append(data)

            # group cached orders by acct (raw expected to contain limitPrice/stopPrice/customTag)
            orders_by_acct = {}
            for o in (self.latest_orders or []):
                raw = (o.get("raw") if isinstance(o, dict) else None) or {}
                acct = int(raw.get("accountId", -1)) if raw else -1
                if acct > 0:
                    orders_by_acct.setdefault(acct, []).append(raw)

            known_accounts = sorted(strat_by_acct.keys())

            for acct_id in known_accounts:
                strat_meta = strat_by_acct.get(acct_id, {}) or {}
                strat_name = strat_meta.get("name", "UnknownStrategy")
                contract_id = strat_meta.get("contract", "")
                lb = strat_meta.get("broker")

                logger.info(f"\n🟢 {strat_name} — Account {acct_id}")
                logger.info(f"   Balance: {balances.get(acct_id, '?')}")

                # ---- Positions: prefer cache; if none, fall back to broker snapshot
                pos_list = positions_by_acct.get(acct_id, [])
                if not pos_list and lb:
                    try:
                        # broker recon snapshot is authoritative on reconnect
                        pos_list = lb.recon_fetch_positions(acct_id)
                    except Exception:
                        pos_list = []

                if pos_list:
                    # (optional) filter to this contract so we don't mix symbols
                    mine_pos = [p for p in pos_list if str(p.get("contractId")) == str(contract_id)]
                    if mine_pos:
                        for p in mine_pos:
                            # If 'type' exists, we can annotate direction
                            ptype = p.get("type")
                            dir_txt = "long" if ptype == 1 else ("short" if ptype == 2 else "?")
                            logger.info(
                                f"   Position: {p.get('contractId')} | size={p.get('size', 0)} "
                                f"| avgPrice={p.get('averagePrice')} | type={ptype}({dir_txt}) "
                                f"| accountId={acct_id} (id={p.get('id')})"
                            )
                    else:
                        logger.info("   No open positions.")
                else:
                    logger.info("   No open positions.")

                # ---- Orders: prefer cache; if empty/inconsistent, broker /searchOpen
                open_orders = [o for o in orders_by_acct.get(acct_id, []) if int(o.get("status", -1)) in OPEN_STATUSES]
                if (not open_orders) and lb:
                    try:
                        open_orders = lb.recon_fetch_orders(acct_id, contract_id=contract_id)
                        open_orders = [o for o in open_orders if int(o.get("status", -1)) in OPEN_STATUSES]
                    except Exception:
                        open_orders = []

                # filter to this contract
                open_orders = [o for o in open_orders if str(o.get("contractId")) == str(contract_id)]

                # SL/TP recognition: tag contains SL / TP and belongs to this strategy
                sltp = []
                for o in open_orders:
                    tag = o.get("customTag") or o.get("custom_tag") or o.get("tag") or ""
                    if not tag:
                        continue
                    if _is_my_sltp(tag, strat_name, acct_id, contract_id) and ("SL" in tag or "TP" in tag):
                        sltp.append(o)

                if sltp:
                    logger.info("   Active SL/TP Orders:")
                    for o in sltp:
                        tag = o.get("customTag") or o.get("custom_tag") or o.get("tag") or "?"
                        oid = o.get("id") or o.get("orderId") or "?"
                        status = o.get("status")
                        logger.info(f"     • {tag} | id={oid} | price={_norm_price(o)} | status={status}")
                else:
                    logger.info("   No active SL/TP orders.")

                logger.info("────────────────────────────────────────────────────────────")

        except Exception as e:
            logger.error(f"🔴 Error logging account snapshot: {e}", exc_info=True)


    # ---------------------------
    # Reconcile logic (cold-start & reconnect only)
    # ---------------------------
    def _maybe_cold_start_reconcile(self):
        """Run reconcile once after both hubs are ready."""
        if (
            self._market_hub_ready
            and self._user_hub_ready
            and not self._did_cold_start_reconcile
        ):
            logger.info("🔄 Cold start: both hubs ready, reconciling once.")
            self._reconcile_if_needed("cold-start")

            self._did_cold_start_reconcile = True


    def _reconcile_if_needed(self, reason: str, force: bool = False):
        """
        Guarded + throttled reconciliation trigger:
        - Runs at most once per cooldown (unless force=True).
        - Ensures only one reconcile runs at a time.
        """
        try:
            with self._reconcile_lock:
                now = time.time()
                if not force and (now - self._last_reconcile_time) < self._reconcile_cooldown:
                    logger.info(f"⏸️ Reconcile skipped (cooldown). Trigger={reason}")
                    return
                self._last_reconcile_time = now

            # If we got here, we’re cleared to run
            self._reconcile_all_strategies(reason)
        except Exception:
            logger.exception("Reconcile trigger failed")



    def _reconcile_all_strategies(self, why: str = ""):
        logger.info(f"🔄 Reconcile all strategies [{why}] @ {datetime.now(timezone.utc).isoformat()}")

        # --- 1) warm brokers in parallel (non-blocking, short joins)
        warm_threads = []
        for strat in self.strategies:
            try:
                lb = getattr(strat, "live_broker", None)
                if lb:
                    t = threading.Thread(target=lambda _lb=lb: _lb.warmup(), daemon=True)
                    t.start()
                    warm_threads.append(t)
            except Exception:
                logger.debug("warmup spawn failed", exc_info=True)

        # give warmups a short grace period to complete (they should be quick)
        for t in warm_threads:
            t.join(timeout=1.0)

        # --- 2) fetch consolidated snapshot
        acc_cache: Dict[int, Dict[str, Any]] = {}
        pos_cache: List[Dict[str, Any]] = []
        ord_cache: List[Dict[str, Any]] = []

        for strat in self.strategies:
            try:
                lb = getattr(strat, "live_broker", None)
                if not lb:
                    continue
                acc, positions, orders = lb.fetch_account_positions_orders()
                if isinstance(acc, dict):
                    acc_cache[int(lb.account_id)] = acc
                if isinstance(positions, list):
                    pos_cache.extend(positions)
                if isinstance(orders, list):
                    ord_cache.extend(orders)
            except Exception:
                logger.debug("fetch_account_positions_orders failed", exc_info=True)

        self.latest_account_info = acc_cache or {}
        self.latest_positions = pos_cache or []
        self.latest_orders = ord_cache or []

        # --- 3) enqueue reconcile jobs into each strategy's worker queue
        for strat in self.strategies:
            if hasattr(strat, "reconcile_on_reconnect"):
                try:
                    self.enqueue_reconcile(strat)
                    logger.info(f"🧩 Enqueued reconcile job for {getattr(strat, 'name', strat)}")
                except Exception as e:
                    logger.error(
                        f"Failed to enqueue reconcile for {getattr(strat, 'name', strat)}: {e}",
                        exc_info=True,
                    )

        try:
            self._log_account_snapshot()
        except Exception:
            logger.debug("Snapshot log failed after reconcile.", exc_info=True)



    # ---------------------------
    # Watchdog & restarts (event-driven + armed market watchdog)
    # ---------------------------
    def _exp_backoff_secs(self, attempt: int) -> float:
        if attempt <= 0:
            attempt = 1
        base = min(RESTART_BACKOFF_BASE * (2 ** (attempt - 1)), RESTART_BACKOFF_MAX)
        jitter = base * random.uniform(-0.2, 0.2)  # ±20%
        return max(0.5, base + jitter)

    def _cooldown_ok(self, last_ts: float) -> bool:
        return (time.time() - last_ts) >= RESTART_COOLDOWN_SECS

    def _arm_watchdog(self, reason: str):
        now = datetime.now(timezone.utc)
        self._wd_armed_until = now + timedelta(minutes=WD_ARM_MINUTES)
        self._last_interruption_at = now
        logger.info(f"🛡️ Armed market watchdog for {WD_ARM_MINUTES}m due to {reason}.")

    def _disarm_watchdog_if_expired(self):
        if self._wd_armed_until and datetime.now(timezone.utc) > self._wd_armed_until:
            self._wd_armed_until = None
            logger.info("🛡️ Disarmed market watchdog (window expired).")

    def _hard_restart_user_hub(self):
        with self._user_restart_lock:
            if not self._cooldown_ok(self._last_user_restart_at):
                return
            self._last_user_restart_at = time.time()
            try:
                logger.warning("♻️ Restarting USER hub (hard teardown)…")
                self._refresh_token_always()   # <— always refresh token
                try:
                    if self.user_hub_connection:
                        with self._user_start_stop_lock:
                            self.user_hub_connection.stop()
                except Exception:
                    pass

                self.setup_user_hub()

                self._user_backoff_attempt += 1
                delay = self._exp_backoff_secs(self._user_backoff_attempt)
                time.sleep(delay)

                with self._user_start_stop_lock:
                    if not self._user_starting:
                        self._user_starting = True
                        try:
                            self.user_hub_connection.start()
                        finally:
                            self._user_starting = False

                logger.info("✅ USER hub restarted.")
                self._user_backoff_attempt = 0
            except Exception:
                logger.exception("USER hub restart failed")

    def _hard_restart_market_hub(self):
        with self._market_restart_lock:
            if not self._cooldown_ok(self._last_market_restart_at):
                return
            self._last_market_restart_at = time.time()
            try:
                logger.warning("♻️ Restarting MARKET hub (hard teardown)…")
                self._refresh_token_always()   # <— always refresh token

                try:
                    if self.market_hub_connection:
                        with self._market_start_stop_lock:
                            self.market_hub_connection.stop()
                except Exception:
                    pass

                self.setup_market_hub()

                self._market_backoff_attempt += 1
                delay = self._exp_backoff_secs(self._market_backoff_attempt)
                time.sleep(delay)

                with self._market_start_stop_lock:
                    if not self._market_starting:
                        self._market_starting = True
                        try:
                            self.market_hub_connection.start()
                        finally:
                            self._market_starting = False

                logger.info("✅ MARKET hub restarted.")
                self._market_backoff_attempt = 0
            except Exception:
                logger.exception("MARKET hub restart failed")

    def _watchdog_loop(self):
        """
        Only monitors the MARKET stream, and only while ARMED (post-interruption).
        If both: (no ticks for WD_NO_TICK_SECS) AND (no candle for WD_NO_CANDLE_SECS) while armed,
        we assume a quiet-dead and restart the MARKET hub.
        """
        while not self._shutdown:
            try:
                # auto-disarm if the arming window has elapsed
                self._disarm_watchdog_if_expired()

                if self._wd_armed_until:
                    now = datetime.now(timezone.utc)
                    no_tick = (now - self._last_tick_at).total_seconds()
                    no_cndl = (now - self._last_candle_finalized_at).total_seconds()

                    if no_tick >= WD_NO_TICK_SECS and no_cndl >= WD_NO_CANDLE_SECS:
                        logger.warning(
                            f"🚨 Armed watchdog trigger: no ticks {int(no_tick)}s "
                            f"AND no candle finalize {int(no_cndl)}s → restarting MARKET hub."
                        )
                        # reset timers to avoid thrash while we restart
                        self._last_tick_at = now
                        self._last_candle_finalized_at = now
                        threading.Thread(target=self._hard_restart_market_hub, daemon=True).start()

                time.sleep(WD_POLL_SECS)
            except Exception:
                logger.exception("Watchdog error")
                time.sleep(WD_POLL_SECS)

    def flush_memory(self, reason: str = "manual"):
            """
            Drop all in-memory state (ticks, candles, snapshots).
            Use with care: this wipes RAM clean.
            """
            logger.info(f"🧹 Flushing all in-memory state (reason={reason})")

            self.aggregator = CandleAggregator(
                timeframes=["1m", "2m", "3m", "5m", "15m", "30m", "1h"]
            )
            self.candle_history.clear()
            self._last_candle_started.clear()

            self.latest_account_info = None
            self.latest_orders.clear()
            self.latest_positions = None
            self._active_orders.clear()

    def _periodic_flush(self):
        """
        Flush memory every 1.5 hours.
        """
        while not self._shutdown:
            time.sleep(90 * 60)  # 1.5 hours
            self.flush_memory(reason="periodic")


    # ---------------------------
    # Run & wiring
    # ---------------------------
    def run(self):
        """
        Entry point for the process. Hardens startup so hub start failures or
        stuck waits cause a controlled os._exit(1) (so external supervisor restarts).
        """
        # make sure reconnect loop has a sane default interval
        self._hub_reconnect_interval = getattr(self, "_hub_reconnect_interval", 5)

        try:
            initialize_firestore()
            if not self.get_jwt_token():
                logger.critical("Failed to obtain JWT token on startup.")
                sys.exit(1)

            # --- Register your strategies here ---
            try:
                s1 = Strategy1LiveExecution(jwt_token=self.token)
                self.strategies.append(s1)
                logger.info(
                    f"🧩 Registered Strategy1: acct={s1.account_id} contract={s1.contract_id} "
                    f"tf={s1.strategy_timeframe} size={s1.contract_size}"
                )

                s2 = Strategy2LiveExecution(jwt_token=self.token, name="S2")
                self.strategies.append(s2)
                logger.info(
                    f"🧩 Registered Strategy2: acct={s2.account_id} contract={s2.contract_id} "
                    f"tf={s2.strategy_timeframe} size={s2.contract_size}"
                )

                # --- attach app to brokers so wait_for_fill can read app.latest_positions ---
                for strat in self.strategies:
                    broker = getattr(strat, "live_broker", None)
                    if broker:
                        broker.app = self
                        logger.info(f"🔗 Attached TradingApp to broker {getattr(broker, 'connection_name', broker.account_id)}")


            except Exception as e:
                logger.critical(f"Failed to register strategies: {e}", exc_info=True)
                sys.exit(1)

            # --- start persistent per-strategy worker threads (one worker per strategy) ---
            try:
                for strat in self.strategies:
                    try:
                        self._start_strategy_worker(strat)
                    except Exception:
                        logger.exception("Failed to start worker for strategy %s", getattr(strat, "name", str(strat)))
            except Exception:
                # defensive: shouldn't happen, but don't crash startup for worker failures
                logger.exception("Unexpected error while starting strategy workers")


            if not self.strategies:
                logger.critical("No strategies registered — cannot proceed.")
                sys.exit(1)
            # --- end registry ---

            watched = set()
            for strat in self.strategies:
                broker = getattr(strat, "live_broker", None)
                sym = getattr(strat, "contract_id", None)
                tf = getattr(strat, "strategy_timeframe", "1m")
                strat_name = getattr(strat, "name", getattr(strat, "__class__", type(strat)).__name__)
                if broker and sym:
                    # Give the broker a readable name used in warmup logs
                    try:
                        self.name_broker_connection(broker, strat_name)
                    except Exception:
                        # best-effort: do not crash startup if naming fails
                        pass

                    # schedule the double warmups for this broker (runner thread per broker)
                    self._schedule_broker_warmups(broker, tf_seconds=self._tf_to_seconds(tf), lead_ms=800)
                    watched.add(sym)

            if watched:
                logger.info(f"📡 Warming & watching symbols: {sorted(watched)}")

            # Optional mock broker (unchanged behavior)
            self.mock_broker = MockBroker(
                timeframes_to_watch=list({getattr(s, "strategy_timeframe", "1m") for s in self.strategies}),
                tick_size=float(os.getenv("TICK_SIZE", "0.25")),
                sl_long=int(os.getenv("SL_TICKS_LONG", "10")),
                tp_long=int(os.getenv("TP_TICKS_LONG", "10")),
                sl_short=int(os.getenv("SL_TICKS_SHORT", "10")),
                tp_short=int(os.getenv("TP_TICKS_SHORT", "10")),
            )

            # Hubs
            self.setup_market_hub()
            self.setup_user_hub()

            # Start Market first to ensure ticks flow, then User
            with self._market_start_stop_lock:
                self._market_starting = True
                try:
                    logger.info("Starting MARKET hub...")
                    try:
                        self.market_hub_connection.start()
                    except Exception as e:
                        logger.critical(f"MARKET hub failed to start: {e}", exc_info=True)
                        self.flush_memory(reason="market-start-failed")
                        os._exit(1)
                finally:
                    self._market_starting = False
            # Start position printer thread
            #threading.Thread(target=self._position_printer, daemon=True, name="PositionPrinter").start()


            # Wait for market to signal readiness (use a timeout to avoid hanging)
            if not self.market_ready_event.wait(timeout=20):
                logger.critical("MARKET hub did not become ready within timeout. Exiting to trigger restart.")
                self.flush_memory(reason="market-ready-timeout")
                os._exit(1)

            with self._user_start_stop_lock:
                self._user_starting = True
                try:
                    logger.info("Starting USER hub...")
                    try:
                        self.user_hub_connection.start()
                    except Exception as e:
                        logger.critical(f"USER hub failed to start: {e}", exc_info=True)
                        self.flush_memory(reason="user-start-failed")
                        os._exit(1)
                finally:
                    self._user_starting = False

            # Wait for user hub readiness (timeout as well)
            if not self.user_ready_event.wait(timeout=20):
                logger.critical("USER hub did not become ready within timeout. Exiting to trigger restart.")
                self.flush_memory(reason="user-ready-timeout")
                os._exit(1)

            # Extra safety: run a single reconcile once both hubs are up
            self._did_cold_start_reconcile = True
            try:
                self._reconcile_all_strategies("cold-start-final")
            except Exception:
                logger.exception("cold-start-final reconcile failed", exc_info=True)

            # Start main loop + watchdog + reconnect loop (daemon threads)
            threading.Thread(target=self.main_loop, daemon=True).start()
            threading.Thread(target=self._watchdog_loop, daemon=True).start()
            threading.Thread(target=self._reconnect_loop, daemon=True).start()

            # keep the main thread alive — let background threads manage events
            while True:
                time.sleep(60)

        # --- NEW CODE START ---
        except HubConnectionError as e:
            # Check if the underlying cause is a ConnectionResetError
            cause = getattr(e, "__cause__", None)
            if isinstance(cause, ConnectionResetError):
                logger.critical("ConnectionResetError detected during run(). Forcing application exit to trigger restart.")
                try:
                    self.flush_memory(reason="fatal-connection-error")
                except Exception:
                    logger.exception("flush_memory failed while handling ConnectionResetError")
                os._exit(1)  # Exit with an error code for the external script to catch
            else:
                logger.critical(f"Hub connection failed unexpectedly in run(): {e}", exc_info=True)
                try:
                    self.flush_memory(reason="unhandled-hub-error")
                except Exception:
                    logger.exception("flush_memory failed while handling HubConnectionError")
                os._exit(1)
        except Exception as e:
            # Catch all other unhandled exceptions
            logger.critical(f"Unhandled exception in run(): {e}", exc_info=True)
            try:
                self.flush_memory(reason="unhandled-exception")
            except Exception:
                logger.exception("flush_memory failed while handling unhandled exception")
            os._exit(1)
        # --- NEW CODE END ---

# --- graceful shutdown (module-level, not inside the class) ---

def signal_handler(sig, frame):
    global app
    logger.info("Received termination signal, shutting down gracefully...")
    if app is not None:
        app.flush_memory(reason="shutdown")
        app._shutdown = True  # signal all loops to exit
    time.sleep(1.0)  # give threads a moment to break out
    sys.exit(0)



if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    try:
        signal.signal(signal.SIGTERM, signal_handler)
    except Exception:
        pass  # OK on Windows

    app = TradingApp()
    try:
        app.run()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt — flushing memory & shutting down.")
        if app is not None:
            app.flush_memory(reason="KeyboardInterrupt")
        sys.exit(0)