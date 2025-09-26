#!/usr/bin/env bash
set -euo pipefail

DIR=$(cd "$(dirname "$0")" && pwd)
STATE_FILE="$DIR/.state.json"

require() { command -v "$1" >/dev/null 2>&1 || { echo "Missing command: $1"; exit 1; }; }
require jq
require curl

if [[ -z "${STRIPE_SECRET_KEY:-}" ]]; then echo "Set STRIPE_SECRET_KEY (sk_test_...)" >&2; exit 1; fi
if [[ ! -s "$STATE_FILE" ]]; then echo "Run ./run.sh first" >&2; exit 1; fi

TEST_CLOCK_ID=$(jq -r .testClockId "$STATE_FILE")

stripe() {
  curl -fsS https://api.stripe.com/v1/$1 \
    -u "$STRIPE_SECRET_KEY:" \
    "$@"
}

echo "[*] Advancing test clock to next month boundary..."

# Retrieve current frozen time
CUR=$(stripe "test_helpers/test_clocks/$TEST_CLOCK_ID")
CUR_TS=$(printf '%s' "$CUR" | jq -r .frozen_time)

# Add ~35 days to guarantee next billing cycle
TARGET=$(( CUR_TS + 35*24*3600 ))

stripe "test_helpers/test_clocks/$TEST_CLOCK_ID/advance" \
  -d frozen_time=$TARGET | jq .

echo "[*] Waiting for invoices to finalize..."
sleep 5

echo "[*] Done"




