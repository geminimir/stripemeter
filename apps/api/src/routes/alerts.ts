/**
 * Alert configuration routes
 */

import { FastifyPluginAsync } from "fastify";
import type { AlertConfig } from "@stripemeter/core";
import { requireScopes } from "../utils/auth";
import { SCOPES } from "../constants/scopes";
import {
  alertConfigs,
  db,
  AlertConfig as AlertConfigModel,
  alertHistory,
  alertEvents,
} from "@stripemeter/database";
import { and, eq, gte, sql } from "drizzle-orm";

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
  }>(
    "/",
    {
      schema: {
        description: "List all alert configurations for a tenant",
        tags: ["alerts"],
        querystring: {
          type: "object",
          required: ["tenantId"],
          properties: {
            tenantId: { type: "string", format: "uuid" },
            customerRef: { type: "string" },
            enabled: { type: "boolean" },
          },
        },
        response: {
          200: {
            type: "array",
            items: {
              type: "object",
              properties: {
                id: { type: "string" },
                tenantId: { type: "string" },
                customerRef: { type: "string" },
                metric: { type: "string" },
                type: {
                  type: "string",
                  enum: ["threshold", "spike", "budget"],
                },
                threshold: { type: "number" },
                action: {
                  type: "string",
                  enum: ["email", "webhook", "slack", "hard_cap", "soft_cap"],
                },
                config: { type: "object" },
                enabled: { type: "boolean" },
              },
            },
          },
        },
      },
      preHandler: requireScopes(SCOPES.PROJECT_READ, SCOPES.ALERTS_READ),
    },
    async (_request, reply) => {
      const params = [eq(alertConfigs.tenantId, _request.query.tenantId)];

      if (_request.query.customerRef !== undefined) {
        params.push(eq(alertConfigs.customerRef, _request.query.customerRef));
      }

      if (_request.query.enabled !== undefined) {
        params.push(eq(alertConfigs.enabled, _request.query.enabled));
      }

      const configs = await db
        .select()
        .from(alertConfigs)
        .where(and(...params));

      const res = configs.map((alertConfig) => ({
        id: alertConfig.id,
        tenantId: alertConfig.tenantId,
        customerRef: alertConfig.customerRef || "",
        metric: alertConfig.metric || "",
        type: alertConfig.type,
        threshold: Number(alertConfig.threshold),
        action: alertConfig.action,
        config: alertConfig.config || {},
        enabled: alertConfig.enabled,
      }));
      reply.status(200).send(res);
    }
  );

  /**
   * POST /v1/alerts
   * Create a new alert configuration
   */
  server.post<{
    Body: Omit<AlertConfig, "id">;
    Reply: AlertConfig;
  }>(
    "/",
    {
      schema: {
        description: "Create a new alert configuration",
        tags: ["alerts"],
        body: {
          type: "object",
          required: ["tenantId", "type", "threshold", "action"],
          properties: {
            tenantId: { type: "string", format: "uuid" },
            customerRef: { type: "string" },
            metric: { type: "string" },
            type: { type: "string", enum: ["threshold", "spike", "budget"] },
            threshold: { type: "number" },
            action: {
              type: "string",
              enum: ["email", "webhook", "slack", "hard_cap", "soft_cap"],
            },
            config: { type: "object" },
            enabled: { type: "boolean" },
          },
        },
        response: {
          201: {
            type: "object",
            properties: {
              id: { type: "string" },
              tenantId: { type: "string" },
              customerRef: { type: "string" },
              metric: { type: "string" },
              type: { type: "string" },
              threshold: { type: "number" },
              action: { type: "string" },
              config: { type: "object" },
              enabled: { type: "boolean" },
            },
          },
        },
      },
      preHandler: requireScopes(SCOPES.PROJECT_WRITE, SCOPES.ALERTS_WRITE),
    },
    async (request, reply) => {
      const alertConfig: AlertConfig = {
        id: crypto.randomUUID(),
        ...request.body,
      };
      const now = new Date();
      const toInsert: AlertConfigModel = {
        id: alertConfig.id,
        tenantId: alertConfig.tenantId,
        customerRef: alertConfig.customerRef || null,
        metric: alertConfig.metric || null,
        type: alertConfig.type,
        threshold: alertConfig.threshold.toString(),
        action: alertConfig.action,
        config: alertConfig.config,
        enabled: alertConfig.enabled,
        createdAt: now,
        updatedAt: now,
      };
      await db.insert(alertConfigs).values(toInsert);
      reply.status(201).send(alertConfig);
    }
  );

  /**
   * PUT /v1/alerts/:id
   * Update an alert configuration
   */
  server.put<{
    Params: { id: string };
    Body: Partial<AlertConfig>;
    Reply: AlertConfig | null;
  }>(
    "/:id",
    {
      schema: {
        description: "Update an alert configuration",
        tags: ["alerts"],
        params: {
          type: "object",
          required: ["id"],
          properties: {
            id: { type: "string" },
          },
        },
        body: {
          type: "object",
          properties: {
            threshold: { type: "number" },
            action: {
              type: "string",
              enum: ["email", "webhook", "slack", "hard_cap", "soft_cap"],
            },
            config: { type: "object" },
            enabled: { type: "boolean" },
          },
        },
      },
      preHandler: requireScopes(SCOPES.PROJECT_WRITE, SCOPES.ALERTS_WRITE),
    },
    async (_request, reply) => {
      const toUpdate: Partial<AlertConfigModel> = {};

      if (_request.body.threshold) {
        toUpdate.threshold = _request.body.threshold.toString();
      }
      if (_request.body.action) {
        toUpdate.action = _request.body.action;
      }
      if (_request.body.config) {
        toUpdate.config = _request.body.config;
      }
      if (_request.body.enabled) {
        toUpdate.enabled = _request.body.enabled;
      }

      const updateRes = await db
        .update(alertConfigs)
        .set(toUpdate)
        .where(eq(alertConfigs.id, _request.params.id))
        .returning();
      const updatedAlertConfig =
        updateRes && updateRes.length ? updateRes[0] : null;
      const res: AlertConfig | null = updatedAlertConfig
        ? {
            ...updatedAlertConfig,
            id: updatedAlertConfig.id || "",
            customerRef: updatedAlertConfig.customerRef || "",
            metric: updatedAlertConfig.metric || "",
            config: updatedAlertConfig.config || {},
            threshold: Number(updatedAlertConfig?.threshold),
          }
        : null;
      reply.status(200).send(res);
    }
  );

  /**
   * DELETE /v1/alerts/:id
   * Delete an alert configuration
   */
  server.delete<{
    Params: { id: string };
  }>(
    "/:id",
    {
      schema: {
        description: "Delete an alert configuration",
        tags: ["alerts"],
        params: {
          type: "object",
          required: ["id"],
          properties: {
            id: { type: "string" },
          },
        },
        response: {
          204: {
            type: "null",
          },
        },
      },
      preHandler: requireScopes(SCOPES.PROJECT_WRITE, SCOPES.ALERTS_WRITE),
    },
    async (_request, reply) => {
      await db
        .delete(alertConfigs)
        .where(eq(alertConfigs.id, _request.params.id));
      reply.status(204).send();
    }
  );

  /**
   * GET /v1/alerts/history
   * Get alert trigger history
   */
  server.get<{
    Querystring: {
      tenantId: string;
      customerRef?: string;
      status?: "triggered" | "acknowledged" | "resolved";
      from?: string;
      to?: string;
    };
  }>(
    "/history",
    {
      schema: {
        description: "Get alert trigger history",
        tags: ["alerts"],
        querystring: {
          type: "object",
          required: ["tenantId"],
          properties: {
            tenantId: { type: "string", format: "uuid" },
            customerRef: { type: "string" },
            status: {
              type: "string",
              enum: ["triggered", "acknowledged", "resolved"],
            },
            from: { type: "string", format: "date-time" },
            to: { type: "string", format: "date-time" },
          },
        },
        response: {
          200: {
            type: "array",
            items: {
              type: "object",
              properties: {
                id: { type: "string" },
                alertConfigId: { type: "string" },
                tenantId: { type: "string" },
                customerRef: { type: "string" },
                metric: { type: "string" },
                value: { type: "number" },
                threshold: { type: "number" },
                action: { type: "string" },
                status: { type: "string" },
                triggeredAt: { type: "string" },
                acknowledgedAt: { type: "string" },
                resolvedAt: { type: "string" },
              },
            },
          },
        },
      },
      preHandler: requireScopes(SCOPES.PROJECT_READ, SCOPES.ALERTS_READ),
    },
    async (_request, reply) => {
      const params = [eq(alertHistory.tenantId, _request.query.tenantId)];

      if (_request.query.customerRef !== undefined) {
        params.push(eq(alertHistory.customerRef, _request.query.customerRef));
      }
      if (_request.query.status !== undefined) {
        params.push(eq(alertHistory.status, _request.query.status));
      }
      if (_request.query.from !== undefined) {
        params.push(
          gte(alertHistory.triggeredAt, new Date(_request.query.from))
        );
      }
      if (_request.query.to !== undefined) {
        params.push(gte(alertHistory.triggeredAt, new Date(_request.query.to)));
      }

      const res = await db
        .select()
        .from(alertHistory)
        .where(and(...params));
      reply.status(200).send(res);
    }
  );

  /**
   * GET /v1/alerts/events
   * Get alert trigger events
   */
  server.get<{
    Querystring: {
      tenantId: string;
      customerRef?: string;
      status?: "triggered" | "acknowledged" | "resolved";
      from?: string;
      to?: string;
    };
  }>(
    "/events",
    {
      schema: {
        description: "Get alert trigger events",
        tags: ["alerts"],
        querystring: {
          type: "object",
          required: ["tenantId"],
          properties: {
            tenantId: { type: "string", format: "uuid" },
            customerRef: { type: "string" },
            status: {
              type: "string",
              enum: ["triggered", "acknowledged", "resolved"],
            },
            from: { type: "string", format: "date-time" },
            to: { type: "string", format: "date-time" },
          },
        },
        response: {
          200: {
            type: "array",
            items: {
              type: "object",
              properties: {
                id: { type: "string" },
                alertConfigId: { type: "string" },
                tenantId: { type: "string" },
                customerRef: { type: "string" },
                metric: { type: "string" },
                value: { type: "number" },
                threshold: { type: "number" },
                action: { type: "string" },
                status: { type: "string" },
                triggeredAt: { type: "string" },
                acknowledgedAt: { type: "string" },
                resolvedAt: { type: "string" },
              },
            },
          },
        },
      },
      preHandler: requireScopes(SCOPES.PROJECT_READ, SCOPES.ALERTS_READ),
    },
    async (_request, reply) => {
      const params = [eq(alertEvents.tenantId, _request.query.tenantId)];

      if (_request.query.customerRef !== undefined) {
        params.push(eq(alertEvents.customerRef, _request.query.customerRef));
      }
      if (_request.query.status !== undefined) {
        params.push(eq(alertEvents.status, _request.query.status));
      }
      if (_request.query.from !== undefined) {
        params.push(
          gte(alertEvents.triggeredAt, new Date(_request.query.from))
        );
      }
      if (_request.query.to !== undefined) {
        params.push(gte(alertEvents.triggeredAt, new Date(_request.query.to)));
      }

      const res = await db
        .select()
        .from(alertEvents)
        .where(and(...params))
        .orderBy(sql`${alertEvents.triggeredAt} DESC`);
      reply.status(200).send(res);
    }
  );
};
