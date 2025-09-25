/**
 * Fastify server configuration
 */

import Fastify from 'fastify';
import cors from '@fastify/cors';
import helmet from '@fastify/helmet';
import rateLimit from '@fastify/rate-limit';
import swagger from '@fastify/swagger';
import swaggerUi from '@fastify/swagger-ui';
import { errorHandler } from './utils/error-handler';
import { verifyApiKey } from './utils/auth';
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

export async function buildServer() {
  const server = Fastify({
    logger: true,
    requestIdHeader: 'x-request-id',
    requestIdLogLabel: 'requestId',
    disableRequestLogging: false,
    trustProxy: true,
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
    origin: process.env.API_CORS_ORIGIN?.split(',') || true,
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
      securityDefinitions: {
        ApiKeyAuth: {
          type: 'apiKey',
          name: 'X-API-Key',
          in: 'header',
          description: 'Provide API key as X-API-Key header or Authorization: Bearer <apiKey>',
        },
        BearerApiKey: {
          type: 'apiKey',
          name: 'Authorization',
          in: 'header',
          description: 'Authorization: Bearer <apiKey>'
        }
      },
      security: [{ ApiKeyAuth: [] }],
      tags: [
        { name: 'health', description: 'Health check endpoints' },
        { name: 'events', description: 'Usage event ingestion' },
        { name: 'usage', description: 'Usage queries and projections' },
        { name: 'mappings', description: 'Price mapping configuration' },
        { name: 'reconciliation', description: 'Reconciliation reports' },
        { name: 'alerts', description: 'Alert configuration' },
        { name: 'simulations', description: 'Pricing simulation scenarios and runs' },
      ],
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

  // Expose raw OpenAPI JSON at /json for tests and tooling
  server.get('/json', async (_request, reply) => {
    // @ts-ignore - fastify-swagger decorates instance with swagger()
    const spec = server.swagger();
    reply.send(spec);
  });

  // Register routes
  await server.register(healthRoutes, { prefix: '/health' });
  await server.register(metricsRoutes, { prefix: '/metrics' });
  // Require API key for v1 routes except health/docs (skip when BYPASS_AUTH=1)
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

  await server.register(eventsRoutes, { prefix: '/v1/events' });
  await server.register(usageRoutes, { prefix: '/v1/usage' });
  await server.register(mappingsRoutes, { prefix: '/v1/mappings' });
  await server.register(reconciliationRoutes, { prefix: '/v1/reconciliation' });
  await server.register(alertsRoutes, { prefix: '/v1/alerts' });
  await server.register(simulationRoutes, { prefix: '/v1/simulations' });
  await server.register(adminRoutes, { prefix: '/v1/admin' });

  // Test-only database cleanup for deterministic results
  if (process.env.NODE_ENV === 'test' && process.env.USE_DB_IN_TEST === '1') {
    try {
      const { db, events, simulationRuns, simulationScenarios, simulationBatches } = await import('@stripemeter/database');
      // Order matters due to FKs
      await db.delete(simulationRuns);
      await db.delete(simulationBatches);
      await db.delete(simulationScenarios);
      await db.delete(events);
    } catch (err) {
      server.log.warn({ err }, 'test db cleanup failed');
    }
  }

  // Ready handler
  await server.ready();

  return server;
}
