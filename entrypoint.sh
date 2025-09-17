#!/usr/bin/env bash
set -euo pipefail

# Check if FIREBASE_KEY_JSON exists
if [ -z "${FIREBASE_KEY_JSON:-}" ]; then
  echo "❌ FIREBASE_KEY_JSON environment variable not set. Exiting."
  exit 1
fi

# If the JSON starts with a quote, strip wrapping quotes
# Example: "{" ... "}"  ->  { ... }
if [[ "$FIREBASE_KEY_JSON" == \"*\" ]]; then
  echo "⚠️ Detected extra wrapping quotes around FIREBASE_KEY_JSON, stripping them..."
  # Remove the leading and trailing double quote
  FIREBASE_KEY_JSON="${FIREBASE_KEY_JSON:1:-1}"
fi

# Write JSON to file
echo "$FIREBASE_KEY_JSON" > /app/firebase-key.json
export GOOGLE_APPLICATION_CREDENTIALS=/app/firebase-key.json

# Debug info (safe: does not print full key)
echo "✅ Firebase credentials written to /app/firebase-key.json"
echo "DEBUG: FIREBASE_KEY_JSON length = ${#FIREBASE_KEY_JSON}"

# Hand off to the process manager (run.sh)
exec /app/run.sh
