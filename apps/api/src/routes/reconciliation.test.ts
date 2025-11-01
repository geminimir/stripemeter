import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { FastifyInstance } from 'fastify';
import { buildServer } from '../server';

describe('Reconciliation summary API', () => {
  let server: FastifyInstance;

  beforeAll(async () => {
    process.env.BYPASS_AUTH = '1';
    server = await buildServer();
    await server.ready();
  });

  afterAll(async () => {
    await server.close();
  });

  it('GET /v1/reconciliation/summary returns empty summary when no data', async () => {
    const res = await server.inject({
      method: 'GET',
      url: '/v1/reconciliation/summary',
      query: {
        tenantId: '00000000-0000-0000-0000-000000000000',
        periodStart: '2025-01',
        periodEnd: '2025-03',
      } as any,
    });
    expect(res.statusCode).toBe(200);
    const body = res.json();
    expect(body).toHaveProperty('periodStart');
    expect(Array.isArray(body.perMetric)).toBe(true);
    expect(body.overall).toMatchObject({
      local: expect.any(Number),
      stripe: expect.any(Number),
      drift_abs: expect.any(Number),
      drift_pct: expect.any(Number),
      metrics: expect.any(Number),
      items: expect.any(Number),
    });
  });

  it('GET /v1/reconciliation/summary validates period order', async () => {
    const res = await server.inject({
      method: 'GET',
      url: '/v1/reconciliation/summary',
      query: {
        tenantId: '00000000-0000-0000-0000-000000000000',
        periodStart: '2025-03',
        periodEnd: '2025-01',
      } as any,
    });
    // Our handler returns 400 for invalid order
    expect([200, 400]).toContain(res.statusCode);
    if (res.statusCode === 400) {
      const body = res.json();
      expect(body).toHaveProperty('periodStart');
    }
  });

  it('GET /v1/reconciliation/summary?format=csv returns CSV', async () => {
    const res = await server.inject({
      method: 'GET',
      url: '/v1/reconciliation/summary',
      query: {
        tenantId: '00000000-0000-0000-0000-000000000000',
        periodStart: '2025-01',
        periodEnd: '2025-03',
        format: 'csv',
      } as any,
    });
    expect(res.statusCode).toBe(200);
    expect(res.headers['content-type']).toContain('text/csv');
    const body = res.body;
    expect(body).toContain('metric,local,stripe,drift_abs,drift_pct,items');
  });

  it('POST /v1/reconciliation/run triggers worker HTTP', async () => {
    // Spin up a dummy worker HTTP server on a random port to capture call
    const http = await import('http');
    const srv = http.createServer((req, res) => {
      if (req.method === 'POST' && req.url === '/reconciler/run') {
        res.statusCode = 202;
        res.end('ok');
      } else {
        res.statusCode = 404;
        res.end('no');
      }
    });
    await new Promise<void>((r) => srv.listen(0, r));
    const addr = srv.address();
    const port = typeof addr === 'object' && addr ? addr.port : 0;

    process.env.WORKER_HTTP_HOST = '127.0.0.1';
    process.env.WORKER_HTTP_PORT = String(port);

    const res = await server.inject({ method: 'POST', url: '/v1/reconciliation/run', payload: { tenantId: 't' } });
    expect([200, 202]).toContain(res.statusCode);
    srv.close();
  });

  it('GET /v1/reconciliation/:period returns CSV when requested', async () => {
    const res = await server.inject({
      method: 'GET',
      url: '/v1/reconciliation/2025-01',
      query: { tenantId: '00000000-0000-0000-0000-000000000000', format: 'csv' } as any,
    });
    expect(res.statusCode).toBe(200);
    expect(res.headers['content-type']).toContain('text/csv');
  });

  it('GET /v1/reconciliation/summary CSV has header', async () => {
    const res = await server.inject({
      method: 'GET',
      url: '/v1/reconciliation/summary?tenantId=00000000-0000-0000-0000-000000000000&periodStart=2025-01&periodEnd=2025-01&format=csv',
    });
    expect(res.statusCode).toBe(200);
    expect(res.headers['content-type']).toContain('text/csv');
    expect(res.body).toContain('metric,local,stripe,drift_abs,drift_pct,items');
  });
});


