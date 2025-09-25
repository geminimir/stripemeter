import { FastifyRequest, FastifyReply } from 'fastify';
import { db } from '@stripemeter/database';
import { apiKeys } from '@stripemeter/database';
import { eq, and } from 'drizzle-orm';
import { createHmac } from 'crypto';
import { authFailTotal, crossTenantBlockTotal, scopeDenyTotal } from './metrics';

export type TenantContext = {
  organisationId: string;
  projectId?: string;
  apiKeyId: string;
  apiKeyPrefix: string;
  scopes: string[];
};

declare module 'fastify' {
  interface FastifyRequest {
    tenant?: TenantContext;
  }
}

export async function verifyApiKey(request: FastifyRequest, reply: FastifyReply) {
  const header = request.headers['authorization'] || request.headers['x-api-key'];
  const apiKeyRaw = Array.isArray(header) ? header[0] : header;
  if (!apiKeyRaw) {
    try { authFailTotal.labels('missing_api_key').inc(); } catch {}
    return reply.status(401).send({ error: 'Missing API key' });
  }

  const apiKey = apiKeyRaw.toString().startsWith('Bearer ')
    ? apiKeyRaw.toString().slice(7)
    : apiKeyRaw.toString();

  const dotIndex = apiKey.indexOf('.');
  if (dotIndex < 1) {
    try { authFailTotal.labels('invalid_format').inc(); } catch {}
    return reply.status(401).send({ error: 'Invalid API key format' });
  }
  const prefix = apiKey.substring(0, dotIndex);
  const lastFour = apiKey.substring(apiKey.length - 4);

  // Lookup candidate keys by prefix and last4 to reduce hash checks
  const candidates = await db.select()
    .from(apiKeys)
    .where(and(eq(apiKeys.prefix, prefix), eq(apiKeys.lastFour, lastFour)))
    .limit(10);

  const computed = createHmac('sha256', getKeyDerivationSalt())
    .update(apiKey)
    .digest('base64url');

  const match = candidates.find(k => k.secretHash === computed && k.active && !k.revokedAt && (!k.expiresAt || new Date(k.expiresAt) > new Date()));
  if (!match) {
    try { authFailTotal.labels('invalid_key').inc(); } catch {}
    return reply.status(401).send({ error: 'Invalid API key' });
  }

  request.tenant = {
    organisationId: match.organisationId,
    projectId: match.projectId ?? undefined,
    apiKeyId: match.id,
    apiKeyPrefix: match.prefix,
    scopes: (match.scopes || '').split(',').filter(Boolean),
  };
}

function getKeyDerivationSalt(): string {
  return process.env.API_KEY_SALT || 'dev-api-key-salt';
}

export function getTenantId(request: FastifyRequest): string {
  const testBypass = process.env.NODE_ENV === 'test' && process.env.FORCE_AUTH !== '1';
  if (process.env.BYPASS_AUTH === '1' || testBypass) {
    return '00000000-0000-0000-0000-000000000000';
  }
  const orgId = request.tenant?.organisationId;
  if (!orgId) throw new Error('tenant_not_resolved');
  return orgId;
}

export function requireTenantMatch(request: FastifyRequest, reply: FastifyReply, providedTenantId?: string): boolean {
  const url = request.routerPath || request.url;
  const method = request.method.toUpperCase();
  const testBypass = process.env.NODE_ENV === 'test' && process.env.FORCE_AUTH !== '1';
  if (process.env.BYPASS_AUTH === '1' || testBypass) return true;
  const orgId = request.tenant?.organisationId;
  if (!orgId) {
    try { authFailTotal.labels('no_tenant_context').inc(); } catch {}
    reply.status(401).send({ error: 'unauthenticated' });
    return false;
  }
  if (!providedTenantId) {
    return true;
  }
  if (providedTenantId !== orgId) {
    try { crossTenantBlockTotal.labels(method, String(url)).inc(); } catch {}
    reply.status(403).send({ error: 'forbidden' });
    return false;
  }
  return true;
}

export function requireScopes(scopes: string[]) {
  return async function preHandler(request: FastifyRequest, reply: FastifyReply) {
    const testBypass = process.env.NODE_ENV === 'test' && process.env.FORCE_AUTH !== '1';
    if (process.env.BYPASS_AUTH === '1' || testBypass) return;
    const url = request.routerPath || request.url;
    const method = request.method.toUpperCase();
    const have = new Set(request.tenant?.scopes || []);
    for (const s of scopes) {
      if (!have.has(s)) {
        try { scopeDenyTotal.labels(method, String(url), s).inc(); } catch {}
        return reply.status(403).send({ error: 'forbidden' });
      }
    }
  };
}


