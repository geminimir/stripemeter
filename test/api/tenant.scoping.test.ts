import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { buildServer } from '../../apps/api/src/server';
import { db } from '@stripemeter/database';
import { apiKeys } from '@stripemeter/database';
import { createHmac } from 'crypto';

function makeApiKey(prefixBase: string, organisationId: string, scopes = 'usage:read,mappings:read,events:write') {
  const prefix = `${prefixBase}_test`;
  const secret = 'secretsecretsecretsecret';
  const apiKey = `${prefix}.${secret}`;
  const last4 = secret.slice(-4);
  const secretHash = createHmac('sha256', process.env.API_KEY_SALT || 'dev-api-key-salt').update(apiKey).digest('base64url');
  return { apiKey, record: { organisationId, name: 'test', prefix, lastFour: last4, secretHash, scopes } } as const;
}

describe('Tenant scoping', () => {
  let server: Awaited<ReturnType<typeof buildServer>>;
  let apiKey: string;
  const orgId = '00000000-0000-0000-0000-000000000000';

  beforeAll(async () => {
    delete process.env.BYPASS_AUTH;
    server = await buildServer();
    const { apiKey: key, record } = makeApiKey('sm', orgId);
    apiKey = key;
    await db.insert(apiKeys).values(record as any).onConflictDoNothing();
  });

  afterAll(async () => {
    await server.close();
  });

  it('blocks cross-tenant access (GET /v1/usage/current)', async () => {
    const res = await server.inject({
      method: 'GET',
      url: '/v1/usage/current?tenantId=11111111-1111-1111-1111-111111111111&customerRef=c',
      headers: { 'Authorization': `Bearer ${apiKey}` },
    });
    expect(res.statusCode).toBe(403);
  });
});


