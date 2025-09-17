# orders_hotpath.py
import time
import logging
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)
EXEC = ThreadPoolExecutor(max_workers=6)   # single shared executor

def monotonic_ms():
    return int(time.monotonic() * 1000)

def _blocking_place_market(order_client, payload):
    """
    Adapt this to call your real blocking client method.
    Keep it minimal: one HTTP/websocket call, return the raw response/dict.
    """
    t0 = monotonic_ms()
    # Example: resp = order_client.place_market_order(**payload)
    resp = order_client.place_market_order(payload)   # <-- adapt to your API
    t1 = monotonic_ms()
    # try to attach rtt
    try:
        if isinstance(resp, dict):
            resp["_rtt_ms"] = t1 - t0
        else:
            # wrap if response is object
            resp._rtt_ms = t1 - t0
    except Exception:
        pass
    return resp

def place_entry_immediate(order_client, payload, ctx=None, on_done=None):
    """
    MUST be called immediately at candle-close decision time.
    This will enqueue the blocking network call *immediately* in a background thread.
    Return the Future object if caller wants to track it.
    - order_client must be warmed and reused (do not init client here).
    - ctx small dict for logs (strategy, symbol, signal_time)
    - on_done optional callback(future)
    """
    t_submit = monotonic_ms()
    logger.debug("ENTRY QUEUED t_submit=%d ctx=%s", t_submit, ctx or {})

    def worker():
        # minimal per-order work here so send happens ASAP
        try:
            resp = _blocking_place_market(order_client, payload)
            t_done = monotonic_ms()
            rtt = getattr(resp, "_rtt_ms", resp.get("_rtt_ms", t_done - t_submit) if isinstance(resp, dict) else t_done - t_submit)
            total_ms = t_done - t_submit
            logger.info("ENTRY SENT | total_ms=%d rtt_ms=%d ctx=%s id=%s",
                        total_ms, rtt, ctx or {}, (resp.get("id") if isinstance(resp, dict) else getattr(resp, "id", None)))
            return {"resp": resp, "total_ms": total_ms, "rtt_ms": rtt, "ctx": ctx}
        except Exception as e:
            logger.exception("ENTRY SEND EXC ctx=%s exc=%s", ctx or {}, e)
            raise

    fut = EXEC.submit(worker)
    if on_done:
        fut.add_done_callback(on_done)
    return fut
