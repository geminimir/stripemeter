/**
 * Prometheus metrics utilities for the API
 */

import type { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { Counter, Histogram, Registry, collectDefaultMetrics } from 'prom-client';

// Dedicated registry so we control what is exposed
export const registry = new Registry();

// Default Node.js process metrics
collectDefaultMetrics({ register: registry });

// HTTP metrics
export const httpRequestsTotal = new Counter({
  name: 'http_requests_total',
  help: 'Total number of HTTP requests',
  registers: [registry],
  labelNames: ['method', 'route', 'status_code'] as const,
});

export const httpRequestDurationSeconds = new Histogram({
  name: 'http_request_duration_seconds',
  help: 'HTTP request duration in seconds',
  registers: [registry],
  labelNames: ['method', 'route'] as const,
  buckets: [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
});

// Ingestion metrics
export const eventsIngestedTotal = new Counter({
  name: 'events_ingested_total',
  help: 'Total number of usage events ingested (accepted)',
  registers: [registry],
  labelNames: ['meter'] as const,
});

export const ingestLatencyMs = new Histogram({
  name: 'ingest_latency_ms',
  help: 'Latency in milliseconds from HTTP accept to events persisted',
  registers: [registry],
  // Milliseconds buckets: 5ms → 2.5s
  buckets: [5, 10, 25, 50, 100, 250, 500, 1000, 2500],
});

// AuthN/AuthZ metrics
export const authFailTotal = new Counter({
  name: 'auth_fail_total',
  help: 'Total number of authentication failures',
  registers: [registry],
  labelNames: ['reason'] as const,
});

export const crossTenantBlockTotal = new Counter({
  name: 'cross_tenant_block_total',
  help: 'Total number of cross-tenant access blocks',
  registers: [registry],
  labelNames: ['method', 'route'] as const,
});

export const scopeDenyTotal = new Counter({
  name: 'scope_deny_total',
  help: 'Total number of scope denials',
  registers: [registry],
  labelNames: ['method', 'route', 'scope'] as const,
});

type MetricsRequest = FastifyRequest & { __metricsStartHr?: bigint };

export function registerHttpMetricsHooks(server: FastifyInstance): void {
  server.addHook('onRequest', async (request: MetricsRequest) => {
    request.__metricsStartHr = process.hrtime.bigint();
  });

  server.addHook('onResponse', async (request: MetricsRequest, reply: FastifyReply) => {
    try {
      const start = request.__metricsStartHr;
      const method = request.method.toUpperCase();
      // Prefer routerPath if available; fallback to raw URL path
      const route = (request as any).routerPath || (request.routeOptions && request.routeOptions.url) || request.url;
      const statusCode = String(reply.statusCode);

      if (start) {
        const diffNs = Number(process.hrtime.bigint() - start);
        const durationSeconds = diffNs / 1e9;
        httpRequestDurationSeconds
          .labels(method, route)
          .observe(durationSeconds);
      }

      httpRequestsTotal
        .labels(method, route, statusCode)
        .inc();
    } catch (_err) {
      // Never fail the request due to metrics
    }
  });
}

export async function renderMetrics(): Promise<string> {
  return await registry.metrics();
}


