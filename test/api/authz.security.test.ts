import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { buildServer } from '../../apps/api/src/server';

describe('AuthN/AuthZ & tenant scoping', () => {
  let server: Awaited<ReturnType<typeof buildServer>>;

  beforeAll(async () => {
    delete process.env.BYPASS_AUTH;
    server = await buildServer();
  });

  afterAll(async () => {
    await server.close();
  });

  it('returns 401 for missing API key', async () => {
    const res = await server.inject({ method: 'GET', url: '/v1/usage/current?tenantId=t&customerRef=c' });
    expect(res.statusCode).toBe(401);
  });

  it('Swagger includes ApiKey security definitions', async () => {
    const res = await server.inject({ method: 'GET', url: '/json' });
    expect(res.statusCode).toBe(200);
    const doc = res.json();
    expect(doc.securityDefinitions.ApiKeyAuth).toBeTruthy();
  });
});


