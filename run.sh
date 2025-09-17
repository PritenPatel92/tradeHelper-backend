#!/bin/bash
# Robust process manager for app.py with exponential backoff and a restart cap.

set -euo pipefail

echo "Starting TopStepX trading bot..."

# Configurable via env (with sane defaults)
BACKOFF_BASE="${BACKOFF_BASE:-2}"        # seconds
BACKOFF_MAX="${BACKOFF_MAX:-20}"         # cap in seconds
RESTART_CAP="${RESTART_CAP:-15}"         # max restarts in the window
CAP_WINDOW_SEC="${CAP_WINDOW_SEC:-900}"  # 15 minutes
SLEEP_ON_SUCCESS_EXIT="${SLEEP_ON_SUCCESS_EXIT:-0}"

restart_count=0
window_start_epoch="$(date +%s)"

# Handle container stop (SIGTERM) quickly
trap 'echo "Received stop signal. Exiting run.sh"; exit 0' SIGINT SIGTERM

backoff_seconds() {
  local attempt="$1"
  # exponential growth: base * 2^(attempt-1), capped
  local exp=$(( BACKOFF_BASE << (attempt - 1) ))
  if (( exp > BACKOFF_MAX )); then
    exp="$BACKOFF_MAX"
  fi
  echo "$exp"
}

while true; do
  # Reset the sliding window if CAP_WINDOW_SEC has elapsed
  now="$(date +%s)"
  if (( now - window_start_epoch > CAP_WINDOW_SEC )); then
    restart_count=0
    window_start_epoch="$now"
  fi

  # Optional throttle if the previous run exited cleanly
  if (( SLEEP_ON_SUCCESS_EXIT > 0 )); then
    sleep "$SLEEP_ON_SUCCESS_EXIT"
  fi

  # Use exec so python becomes PID 1 and receives signals directly
  set +e
  exec python app.py
  exit_code=$?
  set -e

  if [[ "$exit_code" -eq 0 ]]; then
    echo "✅ app.py exited cleanly (code 0). Not restarting."
    exit 0
  fi

  # Crash path
  restart_count=$((restart_count + 1))
  echo "❌ app.py crashed with exit code ${exit_code}. Restart attempt #${restart_count}."

  # Enforce restart cap
  if (( restart_count > RESTART_CAP )); then
    echo "⛔ Exceeded restart cap (${RESTART_CAP} restarts within ${CAP_WINDOW_SEC}s). Exiting run.sh."
    exit "$exit_code"
  fi

  # Backoff with cap
  delay="$(backoff_seconds "$restart_count")"
  echo "↻ Restarting in ${delay}s..."
  sleep "$delay"
done
