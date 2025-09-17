import logging

logger = logging.getLogger("mockbroker")

class MockBroker:
    """
    Mock broker that implements a Pine Script-based engulfing strategy.
    It identifies patterns, manages mock trade positions, and handles
    take-profit, stop-loss, and reversals.
    """

    def __init__(self, timeframes_to_watch: list, tick_size: float, sl_long: int, tp_long: int, sl_short: int, tp_short: int):
        self.trades = []  # store mock trades for debugging
        self.tick_size = tick_size
        self.position = "flat"  # "flat", "long", "short"
        self.entry_price = None
        self.trade_id = 1000

        # Strategy parameters
        self.timeframes_to_watch = timeframes_to_watch
        self.sl_long_ticks = sl_long
        self.tp_long_ticks = tp_long
        self.sl_short_ticks = sl_short
        self.tp_short_ticks = tp_short

        # Pine Script specific constants
        self.min_engulf_ticks = 1
        self.min_engulf_ticks_bear = 0

        self.sl_price = None
        self.tp_price = None

    def on_new_candles(self, symbol: str, timeframe: str, candles: list[dict]):
        """Called by app.py whenever new candles are finalized"""

        # Step 1: Filter by timeframe
        if timeframe not in self.timeframes_to_watch:
            return

        if len(candles) < 2:
            return

        prev = candles[-2]  # second-last (previous closed candle)
        curr = candles[-1]  # last (just-closed candle)

        o1, h1, l1, c1 = prev.get("open"), prev.get("high"), prev.get("low"), prev.get("close")
        o2, h2, l2, c2 = curr.get("open"), curr.get("high"), curr.get("low"), curr.get("close")

        # Step 2: Check for existing position and manage exits (TP/SL/Reversal)
        if self.position != "flat":
            self._check_and_manage_exits(c2)

        # Step 3: Check for new entry signals
        bullish_engulfing, bearish_engulfing = self._check_engulfing_patterns(prev, curr)

        # Step 4: Execute entry logic based on signals and current position
        if self.position == "flat":
            if bullish_engulfing:
                self._enter_trade("short", symbol, timeframe, c2)
            elif bearish_engulfing:
                self._enter_trade("long", symbol, timeframe, c2)
        elif self.position == "long" and bullish_engulfing:
            # Reversal: Close long and enter short
            self._exit_trade("reversal", symbol, timeframe, c2)
            self._enter_trade("short", symbol, timeframe, c2)
        elif self.position == "short" and bearish_engulfing:
            # Reversal: Close short and enter long
            self._exit_trade("reversal", symbol, timeframe, c2)
            self._enter_trade("long", symbol, timeframe, c2)

    def _check_engulfing_patterns(self, prev: dict, curr: dict) -> tuple[bool, bool]:
        """Checks for bullish and bearish engulfing patterns based on Pine Script logic."""
        o1, h1, l1, c1 = prev.get("open"), prev.get("high"), prev.get("low"), prev.get("close")
        o2, h2, l2, c2 = curr.get("open"), curr.get("high"), curr.get("low"), curr.get("close")

        prev_bearish = c1 < o1
        prev_bullish = c1 > o1

        min_engulf_range = self.min_engulf_ticks * self.tick_size
        dif_bear_eng_condition = self.min_engulf_ticks_bear * self.tick_size

        bullish_engulfing = prev_bearish and c2 > h1 + min_engulf_range
        bearish_engulfing = prev_bullish and c2 < l1 - dif_bear_eng_condition

        return bullish_engulfing, bearish_engulfing

    def _enter_trade(self, side: str, symbol: str, timeframe: str, entry_price: float):
        """Logs a mock trade entry."""
        self.trade_id += 1
        self.position = side
        self.entry_price = entry_price

        if side == "long":
            self.sl_price = self.entry_price - (self.sl_long_ticks * self.tick_size)
            self.tp_price = self.entry_price + (self.tp_long_ticks * self.tick_size)
            side_str = "LONG"
        else: # short
            self.sl_price = self.entry_price + (self.sl_short_ticks * self.tick_size)
            self.tp_price = self.entry_price - (self.tp_short_ticks * self.tick_size)
            side_str = "SHORT"

        msg = f"🔥 EXECUTE ENTRY: Trade #{self.trade_id} - {side_str} on {symbol} {timeframe} @ {entry_price}"
        logger.info(msg)
        print(msg)

        msg_orders = f"SL Stop order placed at {self.sl_price:.2f}\nTP Limit order placed at {self.tp_price:.2f}"
        logger.info(msg_orders)
        print(msg_orders)

        self.trades.append({
            "trade_id": self.trade_id,
            "symbol": symbol,
            "tf": timeframe,
            "side": side,
            "entry_price": entry_price,
            "sl_price": self.sl_price,
            "tp_price": self.tp_price,
            "status": "open"
        })

    def _exit_trade(self, reason: str, symbol: str, timeframe: str, exit_price: float):
        """Logs a mock trade exit."""
        trade = next((t for t in self.trades if t['trade_id'] == self.trade_id and t['status'] == 'open'), None)
        if trade:
            trade["status"] = "closed"
            trade["exit_reason"] = reason
            trade["exit_price"] = exit_price

            pnl = (exit_price - trade['entry_price']) if self.position == "long" else (trade['entry_price'] - exit_price)

            msg = f"✅ EXECUTE EXIT: Trade #{self.trade_id} - {reason.upper()} hit at {exit_price:.2f} | P&L: {pnl:.2f}"
            logger.info(msg)
            print(msg)

            if reason == "take-profit":
                msg_cancel = f"Canceled the SL Stop order at {self.sl_price:.2f}"
            elif reason == "stop-loss":
                msg_cancel = f"Canceled the TP Limit order at {self.tp_price:.2f}"
            else:
                msg_cancel = f"Canceled all pending orders"

            logger.info(msg_cancel)
            print(msg_cancel)

        self.position = "flat"
        self.entry_price = None
        self.sl_price = None
        self.tp_price = None

    def _check_and_manage_exits(self, current_price: float):
        """Checks if TP or SL has been hit for the current open position."""
        if self.position == "long":
            if current_price <= self.sl_price:
                self._exit_trade("stop-loss", None, None, current_price)
            elif current_price >= self.tp_price:
                self._exit_trade("take-profit", None, None, current_price)
        elif self.position == "short":
            if current_price >= self.sl_price:
                self._exit_trade("stop-loss", None, None, current_price)
            elif current_price <= self.tp_price:
                self._exit_trade("take-profit", None, None, current_price)