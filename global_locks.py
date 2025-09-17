# global_locks.py
import threading
import time

reconcile_lock = threading.Lock()
_last_reconcile_ts = 0
RECONCILE_DEBOUNCE_SEC = 3.0  # minimum gap between reconciles

def acquire_reconcile_lock() -> bool:
    """Try to acquire lock + debounce. Returns True if this caller should run reconcile."""
    global _last_reconcile_ts
    now = time.time()
    if now - _last_reconcile_ts < RECONCILE_DEBOUNCE_SEC:
        return False  # too soon since last run
    if reconcile_lock.acquire(blocking=False):
        _last_reconcile_ts = now
        return True
    return False

def release_reconcile_lock() -> None:
    if reconcile_lock.locked():
        reconcile_lock.release()
