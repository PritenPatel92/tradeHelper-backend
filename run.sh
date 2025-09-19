#!/usr/bin/env bash
set -euo pipefail

echo "Starting TopStepX trading bot..."

# Configurable via env
BACKOFF_BASE="${BACKOFF_BASE:-2}"        # base seconds
BACKOFF_MAX="${BACKOFF_MAX:-20}"         # max backoff
RESTART_CAP="${RESTART_CAP:-15}"         # max restarts in window
CAP_WINDOW_SEC="${CAP_WINDOW_SEC:-900}"  # 15 minutes
SLEEP_ON_SUCCESS_EXIT="${SLEEP_ON_SUCCESS_EXIT:-0}"

restart_count=0
window_start_epoch="$(date +%s)"
child_pid=0

# Forward SIGTERM/SIGINT to python
terminate() {
  echo "Received termination signal; forwarding to child (pid=$child_pid)..."
  if [[ $child_pid -ne 0 ]]; then
    kill -TERM "$child_pid" 2>/dev/null || true
    wait "$child_pid" 2>/dev/null || true
  fi
  exit 0
}
trap 'terminate' SIGINT SIGTERM

backoff_seconds() {
  local attempt="$1"
  local exp=$(( BACKOFF_BASE * (1 << (attempt - 1)) ))
  if (( exp > BACKOFF_MAX )); then
    exp="$BACKOFF_MAX"
  fi
  echo "$exp"
}

while true; do
  now="$(date +%s)"
  if (( now - window_start_epoch > CAP_WINDOW_SEC )); then
    restart_count=0
    window_start_epoch="$now"
  fi

  if (( SLEEP_ON_SUCCESS_EXIT > 0 )); then
    sleep "$SLEEP_ON_SUCCESS_EXIT"
  fi

  # Start python as child
  python app.py &
  child_pid=$!
  echo "▶️ Started python app.py (pid=$child_pid)"

  wait "$child_pid"
  exit_code=$?
  child_pid=0

  if [[ "$exit_code" -eq 0 ]]; then
    echo "✅ app.py exited cleanly (code 0). Not restarting."
    exit 0
  fi

  restart_count=$((restart_count + 1))
  echo "❌ app.py crashed (exit $exit_code). Restart #${restart_count}"

  if (( restart_count > RESTART_CAP )); then
    echo "⛔ Restart cap reached (${RESTART_CAP} in ${CAP_WINDOW_SEC}s). Exiting."
    exit "$exit_code"
  fi

  delay="$(backoff_seconds "$restart_count")"
  echo "↻ Restarting in ${delay}s..."
  sleep "$delay"
done
