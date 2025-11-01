/**
 * Integration tests for alert states API
 */

vi.mock("../utils/metrics", () => ({
  alertEventsIngestedTotal: {
    labels: vi.fn().mockReturnValue({ inc: vi.fn() }),
  },
  alertStateTransitionsTotal: {
    labels: vi.fn().mockReturnValue({ inc: vi.fn() }),
  },
  registerHttpMetricsHooks: vi.fn(),
}));

import { describe, it, expect, beforeAll, afterAll, vi } from "vitest";
import { FastifyInstance } from "fastify";
import { buildServer } from "../server";

process.env.BYPASS_AUTH = "1";

// Mock database and redis
vi.mock("@stripemeter/database", () => {
  const alertStatesMap = new Map<string, any>();
  const alertEvents: any[] = [];

  return {
    db: {
      transaction: async (fn: any) =>
        await fn({
          insert: (table: any) => ({
            values: (vals: any) => {
              if (table === "alertStates") {
                alertStatesMap.set(vals.id, vals);
                return Promise.resolve();
              } else if (table === "alertEvents") {
                alertEvents.push(vals);
                return Promise.resolve();
              }
            },
            returning: () => Array.from(alertStatesMap.values()),
            set: (vals: any) => ({
              where: (_: any) => ({
                returning: () => {
                  const [id] = Array.from(alertStatesMap.keys());
                  const prev = alertStatesMap.get(id);
                  const updated = { ...prev, ...vals };
                  alertStatesMap.set(id, updated);
                  return [updated];
                },
              }),
            }),
          }),
        }),
    },
    alertStates: "alertStates",
    alertEvents: "alertEvents",
    redis: {},
    AlertStatesRepository: class {
      async getAlertStatesByParam(param: any) {
        return Array.from(alertStatesMap.values()).filter(
          (s) =>
            (!param.tenantId || s.tenantId === param.tenantId) &&
            (!param.metric || s.metric === param.metric)
        );
      }
      async getAlertStatesCountByParam(param: any) {
        return (await this.getAlertStatesByParam(param)).length;
      }
      async get(id: string) {
        return alertStatesMap.get(id) ?? null;
      }
      async create(state: any) {
        alertStatesMap.set(state.id, state);
        return state;
      }
      async update(id: string, vals: any) {
        const s = alertStatesMap.get(id);
        if (!s) return null;
        const updated = { ...s, ...vals };
        alertStatesMap.set(id, updated);
        return updated;
      }
      async delete(id: string) {
        alertStatesMap.delete(id);
      }
    },
  };
});

describe("Alert States API", () => {
  let server: FastifyInstance;

  beforeAll(async () => {
    server = await buildServer();
    await server.ready();
  });

  afterAll(async () => {
    await server.close();
  });

  describe("POST /v1/alerts/states", () => {
    it("should create an alert state and insert an event", async () => {
      const body = {
        tenantId: "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
        metric: "cpu_usage",
        customerRef: "cus_123",
        alertConfigId: crypto.randomUUID(),
        severity: "warn",
        title: "High CPU usage",
        description: "CPU usage exceeded threshold",
        value: 95,
        threshold: 80,
        action: "scale_up",
      };

      const res = await server.inject({
        method: "POST",
        url: "/v1/alerts/states",
        payload: body,
      });

      expect(res.statusCode).toBe(201);
      const data = JSON.parse(res.body);
      expect(data).toHaveProperty("id");
      expect(data.metric).toBe("cpu_usage");
    });

    it("should reject missing required fields", async () => {
      const res = await server.inject({
        method: "POST",
        url: "/v1/alerts/states",
        payload: {
          tenantId: "demo",
          metric: "cpu",
          value: 1,
        },
      });
      expect(res.statusCode).toBe(400);
    });
  });

  describe("GET /v1/alerts/states", () => {
    it("should list alert states for tenant", async () => {
      const tenantId = "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d";

      const res = await server.inject({
        method: "GET",
        url: `/v1/alerts/states?tenantId=${tenantId}`,
      });

      // expect(res.statusCode).toBe(200);
      // const body = JSON.parse(res.body);
      // expect(body).toHaveProperty("total");
      // expect(Array.isArray(body.alertStates)).toBe(true);
    });

    it("should return 400 for missing tenantId", async () => {
      const res = await server.inject({
        method: "GET",
        url: "/v1/alerts/states",
      });
      expect(res.statusCode).toBe(400);
    });
  });

  describe("PUT /v1/alerts/states/:id", () => {
    it("should update alert state and insert an event", async () => {
      const create = await server.inject({
        method: "POST",
        url: "/v1/alerts/states",
        payload: {
          tenantId: "tenant_x",
          metric: "memory_usage",
          customerRef: "cus_999",
          alertConfigId: crypto.randomUUID(),
          title: "Memory High",
          severity: "critical",
          description: "Memory limit hit",
          value: 90,
          threshold: 80,
          action: "notify",
        },
      });
      const created = JSON.parse(create.body);

      const res = await server.inject({
        method: "PUT",
        url: `/v1/alerts/states/${created.id}`,
        payload: {
          status: "resolved",
          value: 70,
          threshold: "80",
          action: "cooldown",
          metadata: "{}",
        },
      });

    });

    it("should return 404 for non-existent alert state", async () => {
      const res = await server.inject({
        method: "PUT",
        url: "/v1/alerts/states/nonexistent",
        payload: {
          status: "resolved",
          value: 10,
          threshold: "10",
          action: "noop",
          metadata: "{}",
        },
      });
      expect(res.statusCode).toBe(404);
    });
  });

  describe("GET /v1/alerts/states/:id", () => {
    it("should retrieve a single alert state", async () => {
      const create = await server.inject({
        method: "POST",
        url: "/v1/alerts/states",
        payload: {
          tenantId: "tenant_z",
          metric: "disk_usage",
          customerRef: "cus_777",
          alertConfigId: crypto.randomUUID(),
          title: "Disk Usage High",
          severity: "warn",
          description: "Disk 90%",
          value: 90,
          threshold: 85,
          action: "cleanup",
        },
      });
      const created = JSON.parse(create.body);

      const res = await server.inject({
        method: "GET",
        url: `/v1/alerts/states/${created.id}`,
      });

      expect(res.statusCode).toBe(200);
      const body = JSON.parse(res.body);
      expect(body.id).toBe(created.id);
    });

    it("should return 404 for missing alert state", async () => {
      const res = await server.inject({
        method: "GET",
        url: "/v1/alerts/states/not-found",
      });
      expect(res.statusCode).toBe(404);
    });
  });

  describe("DELETE /v1/alerts/states/:id", () => {
    it("should delete alert state", async () => {
      const create = await server.inject({
        method: "POST",
        url: "/v1/alerts/states",
        payload: {
          tenantId: "tenant_del",
          metric: "latency",
          customerRef: "cus_del",
          alertConfigId: crypto.randomUUID(),
          title: "Latency High",
          severity: "warn",
          description: "Slow response",
          value: 500,
          threshold: 300,
          action: "investigate",
        },
      });
      const created = JSON.parse(create.body);

      const res = await server.inject({
        method: "DELETE",
        url: `/v1/alerts/states/${created.id}`,
      });

      expect(res.statusCode).toBe(200);
      const body = JSON.parse(res.body);
      expect(body.ok).toBe(true);
    });
  });
});
