# Stripe Test Clocks Demo — Invoice Parity

This reproducible demo proves StripeMeter achieves invoice parity at the end of a billing cycle using Stripe Test Clocks.

## What it does

- Creates Stripe resources on a Test Clock: customer, product, metered price, subscription
- Inserts a `price_mappings` row so StripeMeter maps `api_calls` → your Stripe subscription item
- Sends normal + duplicate + late usage to StripeMeter (`/v1/events/ingest`)
- Advances the Test Clock to the next invoice cycle
- Runs reconciliation to show drift within epsilon (ideally 0)

## Prerequisites

- Docker and Docker Compose
- Node 20+ and pnpm 8+ (for running StripeMeter locally)
- `jq` installed (for parsing JSON)
- Stripe Secret Key (test mode): `sk_test_...`

## Environment

Create `.env` at repo root (or export in shell):

```
# Required for demo
STRIPE_SECRET_KEY=sk_test_...
BYPASS_AUTH=1

# Optional: API/Workers defaults
API_HOST=localhost:3000
RECONCILIATION_INTERVAL_MS=60000
STRIPE_WRITER_INTERVAL_MS=5000
```

Notes:
- `BYPASS_AUTH=1` lets demo scripts call the API without provisioning API keys.
- This demo only uses Stripe Test mode. No secrets are committed.

## Quickstart (fresh clone)

```bash
# From repo root
pnpm i -w
docker compose up -d
pnpm -r build
pnpm db:migrate
pnpm dev
# API: http://localhost:3000/health/ready should be healthy/degraded
```

## Run the demo

All scripts live in this folder. They are idempotent where possible and write state to `.state.json`.

```bash
cd stripemeter/demo/stripe-test-clocks

# 1) Create Stripe resources on a Test Clock and map to StripeMeter
chmod +x run.sh send-usage.sh advance-clock.sh validate.sh cleanup.sh
./run.sh

# 2) Send usage (normal + duplicate + late)
./send-usage.sh

# 3) Advance the Stripe Test Clock to the next billing cycle (invoice should finalize)
./advance-clock.sh

# 4) Validate parity (drift_abs/drift_pct within epsilon)
./validate.sh

# 5) Optional: Cleanup (deletes Stripe Test Clock and DB mapping)
./cleanup.sh
```

## Outputs to expect

- `.state.json`: IDs for `tenantId`, `customerId`, `priceId`, `subscriptionItemId`, `testClockId`
- `validate.log`: JSON summaries including reconciliation output
- Final Stripe invoice JSON in `invoice.json` (status should be `finalized` or `paid` in test).

## Epsilon / Acceptance

The validator checks reconciliation summary and enforces:
- `overall.drift_abs` == 0 (ideal) or within tolerance
- `overall.drift_pct` ≤ `RECONCILIATION_EPSILON` (default 0.005 i.e., 0.5%)

## Troubleshooting

- API 401: Ensure `BYPASS_AUTH=1` and server restarted
- Missing mapping: Re-run `./run.sh` to upsert the `price_mappings` row
- No Stripe updates: Ensure workers are running and `STRIPE_SECRET_KEY` is set; writer pushes deltas
- Invoice not finalizing: Re-run `./advance-clock.sh` to move Test Clock further into next month

## Cleanup behavior

`./cleanup.sh` deletes the Test Clock, which also deletes associated test resources created under that clock. It also removes the `price_mappings` row for this demo’s `tenantId`.




