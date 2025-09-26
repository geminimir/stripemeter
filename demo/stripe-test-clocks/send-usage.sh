#!/usr/bin/env bash
set -euo pipefail

DIR=$(cd "$(dirname "$0")" && pwd)
STATE_FILE="$DIR/.state.json"
API="http://localhost:3000"

require() { command -v "$1" >/dev/null 2>&1 || { echo "Missing command: $1"; exit 1; }; }
require jq
require curl

if [[ ! -s "$STATE_FILE" ]]; then echo "Run ./run.sh first" >&2; exit 1; fi

TENANT_ID=$(jq -r .tenantId "$STATE_FILE")
CUSTOMER_ID=$(jq -r .customerId "$STATE_FILE")

now_iso() { date -u +%Y-%m-%dT%H:%M:%SZ; }
iso_minus_minutes() { date -u -v-"$1"M +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || python - <<PY
import sys,datetime
print((datetime.datetime.utcnow()-datetime.timedelta(minutes=int(sys.argv[1]))).strftime('%Y-%m-%dT%H:%M:%SZ'))
PY
; }

echo "[*] Sending usage events (normal + duplicate + late)"

EVENTS=$(jq -n \
  --arg tenant "$TENANT_ID" \
  --arg cust "$CUSTOMER_ID" \
  --arg ts1 "$(iso_minus_minutes 30)" \
  --arg ts2 "$(iso_minus_minutes 5)" \
  --arg tsLate "$(iso_minus_minutes 1440)" \
  '{events: [
    {tenantId:$tenant, metric:"api_calls", customerRef:$cust, quantity: 100, ts:$ts1, idempotencyKey:"demo-evt-1"},
    {tenantId:$tenant, metric:"api_calls", customerRef:$cust, quantity: 250, ts:$ts2, idempotencyKey:"demo-evt-2"},
    {tenantId:$tenant, metric:"api_calls", customerRef:$cust, quantity: 100, ts:$ts1, idempotencyKey:"demo-evt-1"},
    {tenantId:$tenant, metric:"api_calls", customerRef:$cust, quantity: 75, ts:$tsLate, idempotencyKey:"demo-evt-late"}
  ]}')

curl -fsS -X POST "$API/v1/events/ingest" \
  -H 'content-type: application/json' \
  -H 'Idempotency-Key: demo-batch-1' \
  -d "$EVENTS" | jq .

echo "[*] Done"




