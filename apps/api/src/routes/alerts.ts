/**
 * Alert configuration routes
 */

import { FastifyPluginAsync } from 'fastify';
import type { AlertConfig } from '@stripemeter/core';
import { getTenantId, requireTenantMatch, requireScopes } from '../utils/auth';

export const alertsRoutes: FastifyPluginAsync = async (server) => {
  /**
   * GET /v1/alerts
   * List all alert configurations for a tenant
   */
  server.get<{
    Querystring: {
      tenantId: string;
      customerRef?: string;
      enabled?: boolean;
    };
    Reply: AlertConfig[];
  }>('/', {
    schema: {
      description: 'List all alert configurations for a tenant',
      tags: ['alerts'],
      querystring: {
        type: 'object',
        required: ['tenantId'],
        properties: {
          tenantId: { type: 'string', format: 'uuid' },
          customerRef: { type: 'string' },
          enabled: { type: 'boolean' },
        },
      },
      response: {
        200: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              id: { type: 'string' },
              tenantId: { type: 'string' },
              customerRef: { type: 'string' },
              metric: { type: 'string' },
              type: { type: 'string', enum: ['threshold', 'spike', 'budget'] },
              threshold: { type: 'number' },
              action: { type: 'string', enum: ['email', 'webhook', 'slack', 'hard_cap', 'soft_cap'] },
              config: { type: 'object' },
              enabled: { type: 'boolean' },
            },
          },
        },
      },
    },
    preHandler: [requireScopes(['alerts:read'])],
  }, async (request, reply) => {
    if (!requireTenantMatch(request, reply, request.query.tenantId)) return;
    // TODO: Implement alert retrieval from database
    reply.send([]);
  });

  /**
   * POST /v1/alerts
   * Create a new alert configuration
   */
  server.post<{
    Body: Omit<AlertConfig, 'id'>;
    Reply: AlertConfig;
  }>('/', {
    schema: {
      description: 'Create a new alert configuration',
      tags: ['alerts'],
      body: {
        type: 'object',
        required: ['tenantId', 'type', 'threshold', 'action'],
        properties: {
          tenantId: { type: 'string', format: 'uuid' },
          customerRef: { type: 'string' },
          metric: { type: 'string' },
          type: { type: 'string', enum: ['threshold', 'spike', 'budget'] },
          threshold: { type: 'number' },
          action: { type: 'string', enum: ['email', 'webhook', 'slack', 'hard_cap', 'soft_cap'] },
          config: { type: 'object' },
          enabled: { type: 'boolean' },
        },
      },
      response: {
        201: {
          type: 'object',
          properties: {
            id: { type: 'string' },
            tenantId: { type: 'string' },
            customerRef: { type: 'string' },
            metric: { type: 'string' },
            type: { type: 'string' },
            threshold: { type: 'number' },
            action: { type: 'string' },
            config: { type: 'object' },
            enabled: { type: 'boolean' },
          },
        },
      },
    },
    preHandler: [requireScopes(['alerts:write'])],
  }, async (request, reply) => {
    if (!requireTenantMatch(request, reply, (request.body as any).tenantId)) return;
    const tenantId = getTenantId(request);
    // TODO: Implement alert creation
    const alertConfig: AlertConfig = {
      id: `alert_${Date.now()}`,
      ...request.body,
      tenantId,
    };
    reply.status(201).send(alertConfig);
  });

  /**
   * PUT /v1/alerts/:id
   * Update an alert configuration
   */
  server.put<{
    Params: { id: string };
    Body: Partial<AlertConfig>;
    Reply: AlertConfig;
  }>('/:id', {
    schema: {
      description: 'Update an alert configuration',
      tags: ['alerts'],
      params: {
        type: 'object',
        required: ['id'],
        properties: {
          id: { type: 'string' },
        },
      },
      body: {
        type: 'object',
        properties: {
          threshold: { type: 'number' },
          action: { type: 'string', enum: ['email', 'webhook', 'slack', 'hard_cap', 'soft_cap'] },
          config: { type: 'object' },
          enabled: { type: 'boolean' },
        },
      },
    },
    preHandler: [requireScopes(['alerts:write'])],
  }, async (_request, reply) => {
    // TODO: Implement alert update
    reply.status(501).send({ 
      error: 'Not Implemented',
      message: 'Alert update endpoint is under development' 
    } as any);
  });

  /**
   * DELETE /v1/alerts/:id
   * Delete an alert configuration
   */
  server.delete<{
    Params: { id: string };
  }>('/:id', {
    schema: {
      description: 'Delete an alert configuration',
      tags: ['alerts'],
      params: {
        type: 'object',
        required: ['id'],
        properties: {
          id: { type: 'string' },
        },
      },
      response: {
        204: {
          type: 'null',
        },
      },
    },
    preHandler: [requireScopes(['alerts:write'])],
  }, async (_request, reply) => {
    // TODO: Implement alert deletion
    reply.status(204).send();
  });

  /**
   * GET /v1/alerts/history
   * Get alert trigger history
   */
  server.get<{
    Querystring: {
      tenantId: string;
      customerRef?: string;
      status?: 'triggered' | 'acknowledged' | 'resolved';
      from?: string;
      to?: string;
    };
  }>('/history', {
    schema: {
      description: 'Get alert trigger history',
      tags: ['alerts'],
      querystring: {
        type: 'object',
        required: ['tenantId'],
        properties: {
          tenantId: { type: 'string', format: 'uuid' },
          customerRef: { type: 'string' },
          status: { type: 'string', enum: ['triggered', 'acknowledged', 'resolved'] },
          from: { type: 'string', format: 'date-time' },
          to: { type: 'string', format: 'date-time' },
        },
      },
      response: {
        200: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              id: { type: 'string' },
              alertConfigId: { type: 'string' },
              tenantId: { type: 'string' },
              customerRef: { type: 'string' },
              metric: { type: 'string' },
              value: { type: 'number' },
              threshold: { type: 'number' },
              action: { type: 'string' },
              status: { type: 'string' },
              triggeredAt: { type: 'string' },
              acknowledgedAt: { type: 'string' },
              resolvedAt: { type: 'string' },
            },
          },
        },
      },
    },
    preHandler: [requireScopes(['alerts:read'])],
  }, async (request, reply) => {
    if (!requireTenantMatch(request, reply, request.query.tenantId)) return;
    // TODO: Implement alert history retrieval
    reply.send([]);
  });
};
