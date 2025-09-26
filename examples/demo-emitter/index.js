#!/usr/bin/env node
/*
 * Demo Emitter: emits usage, triggers reconciliation, saves drift CSV
 * Requirements: Node 20+ (built-in fetch)
 */

import { writeFile, mkdir } from 'fs/promises';
import { existsSync } from 'fs';

const API_URL = process.env.API_URL || 'http://localhost:3000';
const TENANT_ID = process.env.TENANT_ID || globalThis.crypto?.randomUUID?.() || (await import('crypto')).randomUUID();
const METRIC = process.env.METRIC || 'requests';
const CUSTOMER = process.env.CUSTOMER || 'cus_demo_1';
const OUT_DIR = new URL('./output/', import.meta.url).pathname;
const OUT_PATH = new URL('./output/drift_report.csv', import.meta.url).pathname;

async function wait(ms) { return new Promise(r => setTimeout(r, ms)); }

async function waitForApiReady(timeoutMs = 60000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const res = await fetch(`${API_URL}/health/ready`);
      if (res.ok) return true;
    } catch {}
    await wait(1000);
  }
  throw new Error('API not ready in time');
}

async function upsertMapping() {
  const body = {
    tenantId: TENANT_ID,
    metric: METRIC,
    aggregation: 'sum',
    stripeAccount: 'acct_demo',
    priceId: 'price_demo',
    subscriptionItemId: 'si_demo',
    currency: 'USD',
    active: true,
  };
  await fetch(`${API_URL}/v1/mappings`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  }).catch(() => {}); // idempotent enough for demo
}

async function sendEvent(idemKey, quantity, ts) {
  const body = {
    events: [
      { tenantId: TENANT_ID, metric: METRIC, customerRef: CUSTOMER, quantity, ts },
    ],
  };
  const res = await fetch(`${API_URL}/v1/events/ingest`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'Idempotency-Key': idemKey },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`Ingest failed: ${res.status}`);
}

async function triggerRecon() {
  const res = await fetch(`${API_URL}/v1/reconciliation/run`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ tenantId: TENANT_ID }),
  });
  if (!res.ok && res.status !== 202) throw new Error(`Recon trigger failed: ${res.status}`);
}

function currentPeriodYYYYMM(date = new Date()) {
  return date.toISOString().slice(0, 7);
}

async function downloadCsv(period) {
  const url = `${API_URL}/v1/reconciliation/${period}?tenantId=${encodeURIComponent(TENANT_ID)}&format=csv`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`CSV download failed: ${res.status}`);
  const csv = await res.text();
  if (!existsSync(OUT_DIR)) await mkdir(OUT_DIR, { recursive: true });
  await writeFile(OUT_PATH, csv, 'utf8');
}

async function main() {
  console.log(`[demo] API_URL=${API_URL}`);
  console.log(`[demo] TENANT_ID=${TENANT_ID}`);
  await waitForApiReady();
  console.log('[demo] API ready');

  await upsertMapping();
  console.log('[demo] mapping upserted');

  const now = new Date();
  const ts1 = new Date(now.getTime() - 60 * 60 * 1000).toISOString();
  const tsLate = new Date(now.getTime() - 2 * 60 * 60 * 1000).toISOString();

  await sendEvent('demo-event-1', 100, ts1);
  await sendEvent('demo-event-1', 100, ts1); // duplicate
  await sendEvent('demo-event-late', 50, tsLate); // late
  console.log('[demo] events sent');

  await triggerRecon();
  console.log('[demo] reconciliation triggered');

  await wait(3000);
  const period = currentPeriodYYYYMM();
  await downloadCsv(period);
  console.log(`[demo] wrote CSV: ${OUT_PATH}`);
}

main().catch((err) => {
  console.error('[demo] failed:', err);
  process.exit(1);
});


