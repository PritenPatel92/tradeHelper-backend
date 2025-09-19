#!/usr/bin/env bash
set -euo pipefail

# --- Firebase credentials setup ---
if [ -z "${FIREBASE_KEY_JSON:-}" ]; then
  echo "❌ FIREBASE_KEY_JSON environment variable not set. Exiting."
  exit 1
fi

# Write JSON exactly as-is (printf avoids newline issues)
printf "%s" "$FIREBASE_KEY_JSON" > /app/firebase-key.json
export GOOGLE_APPLICATION_CREDENTIALS=/app/firebase-key.json

echo "✅ Firebase credentials written to /app/firebase-key.json"
echo "DEBUG: FIREBASE_KEY_JSON length = ${#FIREBASE_KEY_JSON}"

# --- Hand off to supervisor ---
exec /bin/bash /app/run.sh
