import { describe, it, expect, beforeAll, afterAll, vi } from "vitest";
import { FastifyInstance } from "fastify";
import { buildServer } from "../server";

process.env.BYPASS_AUTH = "1";

// --- Mock Database ---
vi.mock("@stripemeter/database", () => {
  const alertConfigs = new Map<string, any>();
  const alertHistory: any[] = [];
  const alertEvents: any[] = [];

  const db = {
    select: () => ({
      from: (table: any) => ({
        where: (_: any) => {
          if (table === "alertConfigs") return Array.from(alertConfigs.values());
          if (table === "alertHistory") return alertHistory;
          if (table === "alertEvents") return alertEvents;
          return [];
        },
        orderBy: () => alertEvents,
      }),
    }),
    insert: (table: any) => ({
      values: (vals: any) => {
        if (table === "alertConfigs") {
          alertConfigs.set(vals.id, vals);
          return Promise.resolve();
        }
        if (table === "alertHistory") {
          alertHistory.push(vals);
          return Promise.resolve();
        }
        if (table === "alertEvents") {
          alertEvents.push(vals);
          return Promise.resolve();
        }
      },
    }),
    update: (table: any) => ({
      set: (vals: any) => ({
        where: (cond: any) => {
          if (table === "alertConfigs") {
            const id = cond.right?.value || cond?.value;
            const prev = alertConfigs.get(id);
            if (!prev) return { returning: () => [] };
            const updated = { ...prev, ...vals };
            alertConfigs.set(id, updated);
            return { returning: () => [updated] };
          }
          return { returning: () => [] };
        },
      }),
    }),
    delete: (table: any) => ({
      where: (cond: any) => {
        if (table === "alertConfigs") {
          const id = cond.right?.value || cond?.value;
          alertConfigs.delete(id);
        }
        return Promise.resolve();
      },
    }),
  };

  return {
    db,
    alertConfigs: "alertConfigs",
    alertHistory: "alertHistory",
    alertEvents: "alertEvents",
    redis: undefined,
  };
});

// --- Mock Metrics ---
vi.mock("../utils/metrics", () => ({
  alertEventsIngestedTotal: { labels: () => ({ inc: vi.fn() }) },
  alertStateTransitionsTotal: { labels: () => ({ inc: vi.fn() }) },
  registerHttpMetricsHooks: vi.fn(),
}));

describe("Alerts API", () => {
  let server: FastifyInstance;

  beforeAll(async () => {
    server = await buildServer();
    await server.ready();
  });

  afterAll(async () => {
    await server.close();
  });

  describe("POST /v1/alerts", () => {
    it("should create a new alert configuration", async () => {
      const body = {
        tenantId: "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
        type: "threshold",
        threshold: 80,
        action: "email",
        config: { to: "ops@example.com" },
        enabled: true,
      };

      const res = await server.inject({
        method: "POST",
        url: "/v1/alerts",
        payload: body,
      });

      expect(res.statusCode).toBe(201);
      const data = JSON.parse(res.body);
      expect(data.id).toBeDefined();
      expect(data.type).toBe("threshold");
    });

    it("should reject invalid body", async () => {
      const res = await server.inject({
        method: "POST",
        url: "/v1/alerts",
        payload: { type: "threshold" },
      });
      expect(res.statusCode).toBe(400);
    });
  });

  describe("GET /v1/alerts", () => {
    it("should list alert configs for tenant", async () => {
      const tenantId = "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d";
      const res = await server.inject({
        method: "GET",
        url: `/v1/alerts?tenantId=${tenantId}`,
      });
      expect(res.statusCode).toBe(200);
      const data = JSON.parse(res.body);
      expect(Array.isArray(data)).toBe(true);
    });

    it("should return 400 if missing tenantId", async () => {
      const res = await server.inject({ method: "GET", url: "/v1/alerts" });
      expect(res.statusCode).toBe(400);
    });
  });

  describe("PUT /v1/alerts/:id", () => {
    it("should update alert configuration", async () => {
      // create first
      const createRes = await server.inject({
        method: "POST",
        url: "/v1/alerts",
        payload: {
          tenantId: "tenant_x",
          type: "threshold",
          threshold: 50,
          action: "email",
          enabled: true,
        },
      });
      const created = JSON.parse(createRes.body);

      const res = await server.inject({
        method: "PUT",
        url: `/v1/alerts/${created.id}`,
        payload: { threshold: 100, enabled: false },
      });

      expect(res.statusCode).toBe(200);
    });

    it("should return 200 with null if alert config not found", async () => {
      const res = await server.inject({
        method: "PUT",
        url: "/v1/alerts/nonexistent",
        payload: { threshold: 42 },
      });
      expect(res.statusCode).toBe(200);
      expect(res.body).toBe("null");
    });
  });

  describe("DELETE /v1/alerts/:id", () => {
    it("should delete alert configuration", async () => {
      const createRes = await server.inject({
        method: "POST",
        url: "/v1/alerts",
        payload: {
          tenantId: "tenant_del",
          type: "threshold",
          threshold: 90,
          action: "email",
          enabled: true,
        },
      });
      const created = JSON.parse(createRes.body);

      const res = await server.inject({
        method: "DELETE",
        url: `/v1/alerts/${created.id}`,
      });

      expect(res.statusCode).toBe(204);
    });
  });

  describe("GET /v1/alerts/history", () => {
    it("should get alert history for tenant", async () => {
      const res = await server.inject({
        method: "GET",
        url: `/v1/alerts/history?tenantId=9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d`,
      });
      expect(res.statusCode).toBe(200);
      const data = JSON.parse(res.body);
      expect(Array.isArray(data)).toBe(true);
    });
  });

  describe("GET /v1/alerts/events", () => {
    it("should get alert events for tenant", async () => {
      const res = await server.inject({
        method: "GET",
        url: `/v1/alerts/events?tenantId=9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d`,
      });
    });
  });
});
