#!/usr/bin/env bash
set -euo pipefail

DIR=$(cd "$(dirname "$0")" && pwd)
STATE_FILE="$DIR/.state.json"

require() { command -v "$1" >/dev/null 2>&1 || { echo "Missing command: $1"; exit 1; }; }
require jq
require curl

if [[ -z "${STRIPE_SECRET_KEY:-}" ]]; then echo "Set STRIPE_SECRET_KEY (sk_test_...)" >&2; exit 1; fi

stripe() {
  curl -fsS https://api.stripe.com/v1/$1 \
    -u "$STRIPE_SECRET_KEY:" \
    "$@"
}

if [[ -s "$STATE_FILE" ]]; then
  TEST_CLOCK_ID=$(jq -r .testClockId "$STATE_FILE")
  if [[ -n "$TEST_CLOCK_ID" && "$TEST_CLOCK_ID" != "null" ]]; then
    echo "[*] Deleting test clock $TEST_CLOCK_ID (cascades test resources)"
    stripe "test_helpers/test_clocks/$TEST_CLOCK_ID" -X DELETE | jq . || true
  fi
fi

echo "[*] Cleanup complete"




