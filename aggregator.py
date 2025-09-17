# aggregator.py
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import pandas as pd
import logging

logger = logging.getLogger("topstepx-client")


class CandleAggregator:
    """
    Aggregates streaming tick data into OHLCV candles across multiple timeframes,
    with a small delay buffer to catch late ticks.

    Important rule implemented:
    - We DO NOT finalize/persist a candle if the aggregator was NOT active
      at the candle's exact start time. This prevents half/ghost candles created
      when the app was started mid-interval.

    Extra diagnostics added:
    - Log when a tick appears "late" (maps to a candle_start older than current)
    - Count & warn if the same candle is finalized more than once
    """

    def __init__(self, timeframes=None, buffer_ms: int = 30):
        if timeframes is None:
            timeframes = ["1m", "2m", "3m", "5m", "15m", "30m", "1h"]

        self.timeframes = timeframes
        self.buffer_ms = buffer_ms

        # In-progress candle for each symbol & timeframe
        # structure: current_candles[symbol][timeframe] -> dict with keys 'start','open','high','low','close','volume','t'
        self.current_candles = defaultdict(lambda: defaultdict(dict))

        # Finalized candles available immediately for strategy
        # newly_finalized[symbol][timeframe] -> list of candle dicts
        self.newly_finalized = defaultdict(lambda: defaultdict(list))

        # Candles waiting in buffer before being written to Firestore
        # _buffered_candles[symbol][timeframe] -> list of {"candle": {...}, "received_at": datetime}
        self._buffered_candles = defaultdict(lambda: defaultdict(list))

        # Maps timeframe string to duration in seconds
        self._timeframe_seconds = {
            "1m": 60,
            "2m": 120,
            "3m": 180,
            "5m": 300,
            "15m": 900,
            "30m": 1800,
            "1h": 3600,
        }

        # When this aggregator instance started (UTC). Only candles whose start >= this time
        # will be finalized/persisted.
        self.start_time = datetime.now(timezone.utc)

        # Small convenience flag (not used for finalization rule).
        self.has_seen_tick = False

        # Diagnostics: count times we finalize the same (symbol, timeframe, start) more than once
        # shape: counts[symbol][timeframe][start_dt] -> int
        self._finalize_counts = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

    # --- Helpers ---
    def _get_candle_start(self, ts: datetime, timeframe: str) -> datetime:
        """
        Snap a timestamp to its candle start boundary (returns timezone-aware UTC datetime).
        Example: for '5m' a ts at 10:03:45 -> start = 10:00:00 UTC (if seconds epoch indicates that).
        """
        sec_per_candle = self._timeframe_seconds.get(timeframe)
        if sec_per_candle is None:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        # Ensure ts is a timezone-aware UTC datetime for consistency
        if ts.tzinfo is None:
            ts_utc = ts.replace(tzinfo=timezone.utc)
        else:
            ts_utc = ts.astimezone(timezone.utc)

        ts_seconds = ts_utc.timestamp()
        start_seconds = (ts_seconds // sec_per_candle) * sec_per_candle
        return datetime.fromtimestamp(start_seconds, tz=timezone.utc)

    # --- Main ingestion ---
    def ingest_tick(self, symbol: str, price: float, volume: float, timestamp: datetime):
        """
        Process a single tick into all active timeframe candles.
        Only finalizes the previous candle if that candle's start >= aggregator.start_time
        (i.e. aggregator was running at the candle start).

        Diagnostics:
        - If a tick maps to a candle_start older than current_start (for that TF), we log it as late.
        - When finalizing a candle, we count how many times that exact candle_start was finalized.
        """
        ts = pd.to_datetime(timestamp)
        # make tz-aware UTC if possible
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        else:
            ts = ts.tz_convert(timezone.utc) if hasattr(ts, "tz_convert") else ts.astimezone(timezone.utc)

        for timeframe in self.timeframes:
            candle_start = self._get_candle_start(ts, timeframe)

            current = self.current_candles[symbol][timeframe]
            current_start = current.get("start")

            # NEW: guard against late ticks that map to an older candle_start than we're currently building
            if current_start is not None and candle_start < current_start:
                logger.debug(
                    f"[LATE TICK IGNORED] {symbol} {timeframe} "
                    f"tick_start={candle_start.isoformat()} < current_start={current_start.isoformat()}"
                )
                continue

            if current_start is None or candle_start != current_start:
                # We are starting a new candle period for this timeframe -> finalize the old one
                if current_start is not None:
                    # Evaluate whether we should finalize/publish the old candle
                    old_candle = current.copy()
                    if old_candle.get("start") and old_candle["start"] >= self.start_time:
                        # Diagnostics: detect duplicate finalization
                        start_dt = old_candle.get("start")
                        cnt = self._finalize_counts[symbol][timeframe][start_dt]
                        self._finalize_counts[symbol][timeframe][start_dt] = cnt + 1
                        if cnt >= 1:
                            logger.warning(
                                f"[DUP FINALIZE] {symbol} {timeframe} start={start_dt.isoformat()} "
                                f"count={cnt+1}. Likely caused by out-of-order ticks or re-open/finalize loop."
                            )

                        # finalize -> make available to strategy immediately
                        self.newly_finalized[symbol][timeframe].append(old_candle)

                        # buffer for the write delay window
                        self._buffered_candles[symbol][timeframe].append(
                            {"candle": old_candle, "received_at": datetime.now(timezone.utc)}
                        )

                        logger.info(
                            f"📍 [FINALIZED] {symbol} {timeframe} "
                            f"OHLCV=({old_candle['open']}, {old_candle['high']}, "
                            f"{old_candle['low']}, {old_candle['close']}, {old_candle['volume']}) "
                            f"T={old_candle.get('t')}"
                        )
                    else:
                        # ignore this old candle: aggregator was not active from its start
                        logger.info(
                            f"⚪ [IGNORED - STARTED LATE] {symbol} {timeframe} "
                            f"candle_start={old_candle.get('start')} < aggregator_start={self.start_time}. "
                            "This candle will NOT be used for strategy or written to the DB."
                        )

                # create a new in-progress candle for this timeframe starting at candle_start
                self.current_candles[symbol][timeframe] = {
                    "start": candle_start,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": volume,
                    "t": candle_start,  # store the canonical start time
                }
            else:
                # update existing in-progress candle
                c = self.current_candles[symbol][timeframe]
                c["high"] = max(c["high"], price)
                c["low"] = min(c["low"], price)
                c["close"] = price
                c["volume"] = c.get("volume", 0) + (volume or 0)

        self.has_seen_tick = True

    # --- Strategy consumption ---
    def get_newly_finalized_candles(self, symbol: str, timeframe: str):
        """
        Return freshly closed candles for trading decisions (consumes them).
        """
        out = self.newly_finalized[symbol][timeframe]
        self.newly_finalized[symbol][timeframe] = []
        return out

    def collect_newly_finalized(self):
        """
        Collect all newly finalized candles across all symbols & timeframes.
        Returns a dict: results[timeframe][symbol] -> [candles]
        (Matches previous app.py expectation: app.py used to iterate tf,by_sym)
        """
        results = defaultdict(lambda: defaultdict(list))
        for symbol, tf_map in list(self.newly_finalized.items()):
            for timeframe, candles in list(tf_map.items()):
                if candles:
                    # Place into results under timeframe->symbol for compatibility with app.py
                    results[timeframe][symbol].extend(candles)
                    # consume them
                    self.newly_finalized[symbol][timeframe] = []
        return results

    # --- Firestore preparation ---
    def get_candles_to_write(self, symbol: str, timeframe: str):
        """
        Return buffered candles that waited at least `buffer_ms` before being written.
        These are the candles safe to write to Firestore. Does not perform the write.
        """
        ready = []
        remaining = []
        now = datetime.now(timezone.utc)
        delay = timedelta(milliseconds=self.buffer_ms)

        for item in self._buffered_candles[symbol][timeframe]:
            if now - item["received_at"] >= delay:
                ready.append(item["candle"])
            else:
                remaining.append(item)

        self._buffered_candles[symbol][timeframe] = remaining

        if ready:
            logger.info(f"🟢 [READY TO WRITE] {len(ready)} {symbol} candle(s) @ {timeframe}")

        return ready
