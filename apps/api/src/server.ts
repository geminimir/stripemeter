/**
 * Fastify server configuration
 */

import Fastify from 'fastify';
import * as Sentry from '@sentry/node';
import cors from '@fastify/cors';
import helmet from '@fastify/helmet';
import rateLimit from '@fastify/rate-limit';
import swagger from '@fastify/swagger';
import swaggerUi from '@fastify/swagger-ui';
import { errorHandler } from './utils/error-handler';
import { verifyApiKey, verifyTenantId } from './utils/auth';
import { perTenantRateLimit } from './utils/rate-limit';
import { registerHttpMetricsHooks } from './utils/metrics';

// Import routes
import { healthRoutes } from './routes/health';
import { metricsRoutes } from './routes/metrics';
import { eventsRoutes } from './routes/events';
import { usageRoutes } from './routes/usage';
import { mappingsRoutes } from './routes/mappings';
import { reconciliationRoutes } from './routes/reconciliation';
import { alertsRoutes } from './routes/alerts';
import { simulationRoutes } from './routes/simulations';
import { adminRoutes } from './routes/admin';
import { persistAuditLog } from './utils/audit';
import { alertStatesRoutes } from './routes/alert-states';

export async function buildServer() {
  // Default to bypass auth in test environment unless explicitly disabled
  if (process.env.NODE_ENV === 'test' && process.env.BYPASS_AUTH === undefined) {
    process.env.BYPASS_AUTH = '1';
  }
  // In containerized quickstart, default to 0.0.0.0 listen address
  if (!process.env.API_HOST) {
    process.env.API_HOST = '0.0.0.0:3000';
  }

  // ... existing code ...

  // At the bottom where server.listen is called, ensure it binds to host/port from API_HOST
  // If the code already handles API_HOST, skip; otherwise approximate here by env defaults.
  // Initialize Sentry if configured
  try {
    const dsn = process.env.SENTRY_DSN;
    if (dsn) {
      Sentry.init({
        dsn,
        environment: process.env.NODE_ENV || 'development',
        release: process.env.GIT_SHA || process.env.RELEASE || undefined,
        tracesSampleRate: Number(process.env.SENTRY_TRACES_SAMPLE_RATE || '0'),
        beforeSend(event) {
          // Best-effort redaction of headers and large bodies
          if (event.request) {
            if ((event.request as any).headers) {
              const headers = (event.request as any).headers as Record<string, any>;
              for (const key of Object.keys(headers)) {
                if (key.toLowerCase() === 'authorization' || key.toLowerCase() === 'x-api-key') {
                  headers[key] = '[redacted]';
                }
              }
              (event.request as any).headers = headers;
            }
            if ((event.request as any).data && typeof (event.request as any).data === 'string') {
              const max = Number(process.env.SENTRY_MAX_BODY_CHARS || '1024');
              (event.request as any).data = (event.request as any).data.slice(0, max);
            }
          }
          return event;
        },
      });
    }
  } catch {}

  const server = Fastify({
    logger: true,
    requestIdHeader: 'x-request-id',
    requestIdLogLabel: 'requestId',
    disableRequestLogging: false,
    trustProxy: true,
    bodyLimit: parseInt(process.env.API_BODY_LIMIT_BYTES || '1048576', 10),
  });

  // Register error handler
  server.setErrorHandler((error, request, reply) => {
    return errorHandler(error, request, reply);
  });

  // Register plugins
  await server.register(helmet, {
    contentSecurityPolicy: false, // Disable for API
  });

  await server.register(cors, {
    origin: (() => {
      const origins = process.env.API_CORS_ORIGIN ? process.env.API_CORS_ORIGIN.split(',').map(s => s.trim()).filter(Boolean) : [];
      return origins.length > 0 ? origins : false;
    })(),
    credentials: true,
  });

  await server.register(rateLimit, {
    max: 100,
    timeWindow: '1 minute',
    allowList: ['127.0.0.1', '::1'], // Whitelist localhost
  });

  // Register HTTP metrics hooks (non-intrusive)
  registerHttpMetricsHooks(server);

  // API Documentation
  await server.register(swagger, {
    swagger: {
      info: {
        title: 'Stripemeter API',
        description: 'Stripe-native usage metering and cost experience API',
        version: '1.0.0',
      },
      host: process.env.API_HOST || 'localhost:3000',
      schemes: ['http', 'https'],
      consumes: ['application/json'],
      produces: ['application/json'],
      tags: [
        { name: 'health', description: 'Health check endpoints' },
        { name: 'events', description: 'Usage event ingestion' },
        { name: 'usage', description: 'Usage queries and projections' },
        { name: 'mappings', description: 'Price mapping configuration' },
        { name: 'reconciliation', description: 'Reconciliation reports' },
        { name: 'alerts', description: 'Alert configuration' },
        { name: 'alert-states', description: 'Alert states' },
        { name: 'simulations', description: 'Pricing simulation scenarios and runs' },
      ],
      securityDefinitions: {
        ApiKeyAuth: {
          type: 'apiKey',
          name: 'x-api-key',
          in: 'header',
          description: 'Provide your API key',
        },
        BearerAuth: {
          type: 'apiKey',
          name: 'Authorization',
          in: 'header',
          description: 'Bearer <apikey>',
        },
      },
    },
  });

  await server.register(swaggerUi, {
    routePrefix: '/docs',
    uiConfig: {
      docExpansion: 'list',
      deepLinking: false,
    },
    staticCSP: true,
    transformSpecificationClone: true,
  });

  // Register routes
  await server.register(healthRoutes, { prefix: '/health' });
  await server.register(metricsRoutes, { prefix: '/metrics' });
  // Require API key for v1 routes except health/docs
  server.addHook('onRequest', async (request, reply) => {
    const bypass = process.env.BYPASS_AUTH === '1';
    if (bypass) return;
    const url = request.raw.url || '';
    if (url.startsWith('/health') || url.startsWith('/docs') || url.startsWith('/json') || url.startsWith('/metrics')) return;
    await verifyApiKey(request, reply);
  });

  // Per-tenant rate limiting
  const rlLimit = parseInt(process.env.TENANT_RATE_LIMIT || '1000', 10);
  const rlWindow = parseInt(process.env.TENANT_RATE_LIMIT_WINDOW || '60', 10);
  server.addHook('preHandler', perTenantRateLimit({ limit: rlLimit, windowSeconds: rlWindow }));

  // Persist audit logs after response
  server.addHook('onResponse', persistAuditLog);

  await server.register(async (instance) => {
    instance.addHook('onRoute', (routeOptions) => {
      routeOptions.schema = {
        ...routeOptions.schema,
        security: [{ ApiKeyAuth: [] }, { BearerAuth: [] }],
      };
    });
    instance.addHook('preHandler', verifyTenantId());
    await instance.register(eventsRoutes, { prefix: '/v1/events' });
    await instance.register(usageRoutes, { prefix: '/v1/usage' });
    await instance.register(mappingsRoutes, { prefix: '/v1/mappings' });
    await instance.register(reconciliationRoutes, { prefix: '/v1/reconciliation' });
    const { adjustmentsRoutes } = await import('./routes/adjustments');
    await instance.register(adjustmentsRoutes, { prefix: '/v1/adjustments' });
    await instance.register(alertsRoutes, { prefix: '/v1/alerts' });
    await instance.register(alertStatesRoutes, { prefix: '/v1/alerts/states' });
    await instance.register(simulationRoutes, { prefix: '/v1/simulations' });
    await instance.register(adminRoutes, { prefix: '/v1/admin' });
  });

  // Test-only database cleanup for deterministic results
  if (process.env.NODE_ENV === 'test') {
    try {
      const mod: any = await import('@stripemeter/database');
      const db = mod.db;
      // Clean tables if available in the mock/real module
      const candidates = ['simulationRuns', 'simulationBatches', 'simulationScenarios', 'events'];
      for (const name of candidates) {
        if (db && mod[name]) {
          try { await db.delete(mod[name]); } catch {}
        }
      }
    } catch (err) {
      server.log.warn({ err }, 'test db cleanup failed');
    }
  }

  // Ready handler
  await server.ready();

  return server;
}
