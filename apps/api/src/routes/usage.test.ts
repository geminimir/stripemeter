/**
 * Integration tests for usage API
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { FastifyInstance } from 'fastify';
import { buildServer } from '../server';

describe('Usage API', () => {
  let server: FastifyInstance;

  beforeAll(async () => {
    process.env.BYPASS_AUTH = '1';
    server = await buildServer();
    await server.ready();
  });

  afterAll(async () => {
    await server.close();
  });

  it('GET /v1/usage/current returns shape', async () => {
    const response = await server.inject({
      method: 'GET',
      url: '/v1/usage/current',
      query: {
        tenantId: '00000000-0000-0000-0000-000000000000',
        customerRef: 'cus_TEST',
      } as any,
    });

    expect(response.statusCode).toBe(200);
    const body = JSON.parse(response.body);
    expect(body).toHaveProperty('customerRef');
    expect(body).toHaveProperty('period');
    expect(body).toHaveProperty('metrics');
    expect(Array.isArray(body.metrics)).toBe(true);
  });

  it('POST /v1/usage/projection returns shape', async () => {
    const response = await server.inject({
      method: 'POST',
      url: '/v1/usage/projection',
      payload: {
        tenantId: '00000000-0000-0000-0000-000000000000',
        customerRef: 'cus_TEST',
      },
    });

    expect(response.statusCode).toBe(200);
    const body = JSON.parse(response.body);
    expect(body).toHaveProperty('customerRef');
    expect(body).toHaveProperty('lineItems');
    expect(Array.isArray(body.lineItems)).toBe(true);
    expect(body).toHaveProperty('subtotal');
    expect(body).toHaveProperty('total');
  });

  describe('POST /v1/usage/history', () => {
    const tenantId = '00000000-0000-0000-0000-000000000000';
    const customerRef = 'cus_TEST';
    const metric = 'api_calls';
    const timestamps = [
      '2025-09-03T03:03:03Z',
    ];
    const events: any = [];
    const quantities = [100];
    quantities.forEach(q => {
      timestamps.forEach(ts => {
        events.push({
          tenantId,
          metric: 'api_calls',
          customerRef: 'cus_TEST001',
          quantity: q,
          ts: new Date(ts).toISOString(),
        });
      });
    });


    beforeAll(async () => {
      await server.inject({
        method: 'POST',
        url: '/v1/events/ingest',
        payload: { events },
      });
    });
    it('should return usage history correct response shape', async () => {
      const response = await server.inject({
        method: 'GET',
        url: '/v1/usage/history',
        query: {
          tenantId,
          customerRef,
          metric,
          periodStart: '2025-09-01T00:00:00Z',
          periodEnd: '2025-10-31T23:59:59.999Z',
          step: 'month',
        },
      });
      expect(response.statusCode).toBe(200);
      const body = JSON.parse(response.body);
      expect(body).toHaveProperty('usage');
      expect(Array.isArray(body.usage)).toBe(true);
    });
  });
});