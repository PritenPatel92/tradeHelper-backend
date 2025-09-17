import logging
import time
import requests
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from sync_firestore import (
    write_trade_record,
    write_bot_state,
    read_bot_state,
)
from mockbroker import MockBroker

logger = logging.getLogger("topstepx-client")


@dataclass
class BotState:
    db_present: bool
    strategy_name: str = "Strategy-1"

    # persisted fields
    last_trade_id: int = 0
    open_position: Optional[Dict[str, Any]] = None
    entry_order_id: Optional[int] = None
    sl_order_id: Optional[int] = None
    tp_order_id: Optional[int] = None
    last_candle_ts: Optional[str] = None
    is_trading_active: bool = True
    net_pl_usd: float = 0.0
    # not persisted
    last_candles: list = field(default_factory=list)

    def load(self):
        if not self.db_present:
            logger.error("🔴 [FIRESTORE ERROR] Firestore client not initialized in BotState.")
            return
        data = read_bot_state(self.strategy_name)
        if not data:
            return
        self.last_trade_id = data.get('last_trade_id', 0)
        self.open_position = data.get('open_position')
        self.entry_order_id = data.get('entry_order_id')
        self.sl_order_id = data.get('sl_order_id')
        self.tp_order_id = data.get('tp_order_id')
        self.last_candle_ts = data.get('last_candle_ts')
        self.is_trading_active = data.get('is_trading_active', True)
        self.net_pl_usd = data.get('net_pl_usd', 0.0)

    def persist(self):
        if not self.db_present:
            logger.error("🔴 [FIRESTORE ERROR] Firestore client not initialized in BotState.")
            return
        write_bot_state({
            'last_trade_id': self.last_trade_id,
            'open_position': self.open_position,
            'entry_order_id': self.entry_order_id,
            'sl_order_id': self.sl_order_id,
            'tp_order_id': self.tp_order_id,
            'last_candle_ts': self.last_candle_ts,
            'is_trading_active': self.is_trading_active,
            'net_pl_usd': self.net_pl_usd
        }, self.strategy_name)

    def next_trade_id(self) -> int:
        self.last_trade_id += 1
        return self.last_trade_id


@dataclass
class EngulfingStrategy:
    api_url: str
    jwt_token: str
    account_id: int
    contract_id: str
    order_size: int
    sl_long: int
    tp_long: int
    sl_short: int
    tp_short: int
    min_engulf_ticks: int
    min_engulf_ticks_bear: int
    tick_size: float
    timeframe: str
    db_present: bool

    # 🟡 MOCK integration
    mock_broker: Optional[Any] = None

    # runtime fields
    state: BotState = field(init=False)

    def __post_init__(self):
        self.state = BotState(db_present=self.db_present)
        self.state.load()
        if not self.state.db_present:
            logger.warning("🟡 [WARNING] State is not persisted because Firestore client is not initialized.")
        logger.info(f"✅ Strategy '{self.state.strategy_name}' initialized successfully.")

    # === Utility ===
    def _normalize(self, c: dict) -> dict:
        """Normalize candle dict keys (supports open/high/low/close/volume or o/h/l/c/v)."""
        return {
            "o": c.get("o") or c.get("O") or c.get("open"),
            "h": c.get("h") or c.get("H") or c.get("high"),
            "l": c.get("l") or c.get("L") or c.get("low"),
            "c": c.get("c") or c.get("C") or c.get("close"),
            "v": c.get("v") or c.get("V") or c.get("volume"),
            "t": c.get("t") or c.get("time") or c.get("timestamp"),
        }

    def _engulf_signals(self, prev: dict, cur: dict) -> Dict[str, bool]:
        """
        Engulfing detection:
          Long Entry Condition (Bearish Engulfing in your spec):
            prevBullish and curClose < prevLow - (minEngulfTicksBear * tick)
          Short Entry Condition (Bullish Engulfing in your spec):
            prevBearish and curClose > prevHigh + (minEngulfTicks * tick)
        """
        long_sig = False
        short_sig = False

        prev_bullish = prev["c"] > prev["o"]
        prev_bearish = prev["c"] < prev["o"]

        if prev_bullish and (cur["c"] < (prev["l"] - self.min_engulf_ticks_bear * self.tick_size)):
            long_sig = True
        if prev_bearish and (cur["c"] > (prev["h"] + self.min_engulf_ticks * self.tick_size)):
            short_sig = True

        logger.debug(
            f"[Eval] PrevCandle O={prev['o']} H={prev['h']} L={prev['l']} C={prev['c']} | "
            f"CurCandle O={cur['o']} H={cur['h']} L={cur['l']} C={cur['c']} | "
            f"minBear={self.min_engulf_ticks_bear}, minBull={self.min_engulf_ticks}, tick={self.tick_size} "
            f"=> long={long_sig}, short={short_sig}"
        )

        return {"long": long_sig, "short": short_sig}

    def evaluate_candles(self, candles: list[dict]) -> Optional[Dict[str, bool]]:
        """
        Called when aggregator finalizes a candle for STRATEGY_TIMEFRAME.
        Returns a signal dict {'long': bool, 'short': bool} if pattern detected.
        """
        if len(candles) < 2:
            return None

        prev = self._normalize(candles[-2])
        cur = self._normalize(candles[-1])

        sig = self._engulf_signals(prev, cur)

        now_iso = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
        self.state.last_candle_ts = now_iso
        self.state.persist()

        return sig
