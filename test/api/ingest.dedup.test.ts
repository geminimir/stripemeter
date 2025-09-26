import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { buildServer } from '../../apps/api/src/server';

describe('Ingest API - deterministic server-generated idempotency dedup', () => {
  let server: Awaited<ReturnType<typeof buildServer>>;

  beforeAll(async () => {
    process.env.BYPASS_AUTH = '1';
    server = await buildServer();
  });

  afterAll(async () => {
    await server.close();
  });

  it('should accept first event and mark duplicate on second with no provided key', async () => {
    const event = {
      tenantId: '123e4567-e89b-12d3-a456-426614174000',
      metric: 'api_calls',
      customerRef: 'cus_X',
      ts: '2025-01-16T14:30:00.000Z',
      quantity: 1,
      source: 'test',
    };

    const res1 = await server.inject({
      method: 'POST',
      url: '/v1/events/ingest',
      payload: { events: [event] },
    });
    expect(res1.statusCode).toBe(200);
    const body1 = res1.json();
    expect(body1.accepted).toBe(1);
    expect(body1.duplicates).toBe(0);
    expect(body1.results[0].status).toBe('accepted');
    const key = body1.results[0].idempotencyKey;
    expect(typeof key).toBe('string');

    const res2 = await server.inject({
      method: 'POST',
      url: '/v1/events/ingest',
      payload: { events: [event] },
    });
    expect(res2.statusCode).toBe(200);
    const body2 = res2.json();
    expect(body2.accepted).toBe(0);
    expect(body2.duplicates).toBe(1);
    expect(body2.results[0].status).toBe('duplicate');
    expect(body2.results[0].idempotencyKey).toBe(key);
  });
});




