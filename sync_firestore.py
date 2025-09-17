# sync_firestore.py
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import os
import logging
from typing import Optional, Dict, Any, List

# === Logging ===
logger = logging.getLogger("topstepx-client")

# Global client variable to avoid re-initialization
firestore_client: Optional[firestore.Client] = None


def initialize_firestore() -> firestore.Client:
    """
    Initializes the Firestore client and sets the module-level `firestore_client`.
    Must be called once during app startup BEFORE constructing the strategy/state.
    """
    global firestore_client
    if firestore_client is not None:
        return firestore_client

    try:
        cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "firebase-key.json")
        if not os.path.exists(cred_path):
            raise FileNotFoundError(
                f"Firestore credential file not found at: {cred_path}. "
                "Set GOOGLE_APPLICATION_CREDENTIALS or place firebase-key.json in the working dir."
            )
        cred = credentials.Certificate(cred_path)
        firebase_admin.initialize_app(cred)
        firestore_client = firestore.client()
        logger.info("✅ Firestore client initialized successfully.")
        return firestore_client
    except Exception as e:
        logger.critical(f"🔴 [FIREBASE INIT ERROR] {e}", exc_info=True)
        raise


def write_candles(symbol: str, timeframe: str, candles: List[dict]):
    """
    Persist finalized candles for a symbol & timeframe.

    Firestore path:
        {symbol}/{timeframe}/{candle_start_timestamp}

    Example:
        MNQ/1m/2025-08-18T21:00:00Z
    """
    if firestore_client is None:
        logger.error("🔴 [FIRESTORE ERROR] Firestore client not initialized (write_candles).")
        return

    if not candles:
        logger.debug(f"[FIRESTORE] write_candles called with empty list: {symbol} @ {timeframe}")
        return

    try:
        tf_root = firestore_client.collection(symbol).document(timeframe).collection("candles")

        for c in candles:
            ts = c.get("t") or c.get("timestamp")
            if isinstance(ts, datetime):
                doc_id = ts.isoformat()
                ts_str = ts.isoformat()
            else:
                doc_id = str(ts)
                ts_str = str(ts)

            o = c.get("o", c.get("open"))
            h = c.get("h", c.get("high"))
            l = c.get("l", c.get("low"))
            cl = c.get("c", c.get("close"))
            v = c.get("v", c.get("volume"))

            tf_root.document(doc_id).set(c)

            logger.info(
                f"🟢 [FIRESTORE] Written candle: {symbol} {timeframe} "
                f"T={ts_str} O={o} H={h} L={l} C={cl} V={v}"
            )

        logger.info(f"🟢 [FIRESTORE] Written {len(candles)} candles: {symbol} @ {timeframe}")
    except Exception as e:
        logger.error(f"🔴 [FIRESTORE ERROR] Failed to write candles: {e}", exc_info=True)


def write_bot_state(state: Dict[str, Any], strategy_name: str = "Strategy-1"):
    """
    Persist bot state (single document).
    Path: strategies/{strategy_name}
    """
    if firestore_client is None:
        logger.error("🔴 [FIRESTORE ERROR] Firestore client not initialized (write_bot_state).")
        return
    try:
        firestore_client.collection("strategies").document(strategy_name).set(state, merge=True)
        logger.debug("🟢 [FIRESTORE] Bot state updated.")
    except Exception as e:
        logger.error(f"🔴 [FIRESTORE ERROR] Failed to write bot state: {e}", exc_info=True)


def read_bot_state(strategy_name: str = "Strategy-1") -> Dict[str, Any]:
    """
    Load bot state (single document). Returns empty dict if not found.
    """
    if firestore_client is None:
        logger.error("🔴 [FIRESTORE ERROR] Firestore client not initialized (read_bot_state).")
        return {}
    try:
        doc = firestore_client.collection("strategies").document(strategy_name).get()
        return doc.to_dict() or {}
    except Exception as e:
        logger.error(f"🔴 [FIRESTORE ERROR] Failed to read bot state: {e}", exc_info=True)
        return {}


def write_trade_record(trade_data: Dict[str, Any], strategy_name: str = "Strategy-1"):
    """
    Persist trade record under: strategies/{strategy_name}/trades/TRADE-<id>-<ts>
    """
    if firestore_client is None:
        logger.error("🔴 [FIRESTORE ERROR] Firestore client not initialized (write_trade_record).")
        return

    try:
        timestamp_str = trade_data.get("timestamp")
        if isinstance(timestamp_str, datetime):
            timestamp_str = timestamp_str.isoformat()
        if not timestamp_str:
            timestamp_str = datetime.utcnow().isoformat()

        trade_id = trade_data.get("trade_id", "NA")
        doc_id = f"TRADE-{trade_id}-{timestamp_str}"

        root = firestore_client.collection("strategies").document(strategy_name)
        trades = root.collection("trades")
        trades.document(doc_id).set(trade_data)

        logger.info(f"🟢 [FIRESTORE] Trade record written: {doc_id}")
    except Exception as e:
        logger.error(f"🔴 [FIRESTORE ERROR] Failed to write trade record: {e}", exc_info=True)
