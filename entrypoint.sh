#!/usr/bin/env bash
set -euo pipefail

# Write firebase-key.json from environment variable
if [ -z "${FIREBASE_KEY_JSON:-}" ]; then
  echo "❌ FIREBASE_KEY_JSON environment variable not set. Exiting."
  exit 1
fi

echo "$FIREBASE_KEY_JSON" > /app/firebase-key.json
export GOOGLE_APPLICATION_CREDENTIALS=/app/firebase-key.json

echo "✅ Firebase credentials written to /app/firebase-key.json"

# Hand off to the process manager (run.sh)
exec /app/run.sh
