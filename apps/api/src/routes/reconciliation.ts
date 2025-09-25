/**
 * Reconciliation routes
 */

import { FastifyPluginAsync } from 'fastify';
import type { ReconciliationSummaryResponse } from '@stripemeter/core';
import { db, priceMappings, reconciliationReports } from '@stripemeter/database';
import { and, eq, gte, lte, sql } from 'drizzle-orm';
import http from 'http';
import { getTenantId, requireTenantMatch, requireScopes } from '../utils/auth';

export const reconciliationRoutes: FastifyPluginAsync = async (server) => {
  /**
   * GET /v1/reconciliation/:period
   * Get reconciliation report for a period
   */
  server.get<{
    Params: { period: string };
    Querystring: { tenantId: string; format?: 'json' | 'csv' };
    Reply: any;
  }>('/:period', {
    schema: {
      description: 'Get reconciliation report for a period',
      tags: ['reconciliation'],
      params: {
        type: 'object',
        required: ['period'],
        properties: {
          period: { type: 'string', pattern: '^\\d{4}-\\d{2}$' }, // YYYY-MM format
        },
      },
      querystring: {
        type: 'object',
        required: ['tenantId'],
        properties: {
          tenantId: { type: 'string' },
          format: { type: 'string', enum: ['json', 'csv'] },
        },
      },
      response: {
        200: {
          type: 'object',
          properties: {
            period: { type: 'string' },
            reports: {
              type: 'array',
              items: {
                type: 'object',
                properties: {
                  id: { type: 'string' },
                  tenantId: { type: 'string' },
                  subscriptionItemId: { type: 'string' },
                  periodStart: { type: 'string' },
                  localTotal: { type: 'number' },
                  stripeTotal: { type: 'number' },
                  diff: { type: 'number' },
                  status: { type: 'string', enum: ['ok', 'investigate', 'resolved'] },
                  createdAt: { type: 'string' },
                },
              },
            },
            suggestedAdjustments: {
              type: 'array',
              items: {
                type: 'object',
                properties: {
                  id: { type: 'string' },
                  tenantId: { type: 'string' },
                  metric: { type: 'string' },
                  customerRef: { type: 'string' },
                  periodStart: { type: 'string' },
                  delta: { type: 'number' },
                  reason: { type: 'string' },
                  actor: { type: 'string' },
                  createdAt: { type: 'string' },
                },
              },
            },
            summary: {
              type: 'object',
              properties: {
                total: { type: 'number' },
                ok: { type: 'number' },
                investigating: { type: 'number' },
                resolved: { type: 'number' },
                maxDiff: { type: 'number' },
              },
            },
          },
        },
      },
    },
    preHandler: [requireScopes(['reconciliation:read'])],
  }, async (request, reply) => {
    if (!requireTenantMatch(request, reply, request.query.tenantId)) return;
    const { period } = request.params;
    const { format } = request.query as any;
    const tenantId = getTenantId(request);

    try {
      const rows = await db
        .select()
        .from(reconciliationReports)
        .where(and(eq(reconciliationReports.tenantId, tenantId), eq(reconciliationReports.periodStart, `${period}-01`)));

      // Compute summary
      const summary = rows.reduce((acc: any, r: any) => {
        acc.total += 1;
        if (r.status === 'ok') acc.ok += 1;
        if (r.status === 'investigate') acc.investigating += 1;
        if (r.status === 'resolved') acc.resolved += 1;
        const diffNum = Number(r.diff);
        if (diffNum > acc.maxDiff) acc.maxDiff = diffNum;
        return acc;
      }, { total: 0, ok: 0, investigating: 0, resolved: 0, maxDiff: 0 });

      const reports = rows.map((r: any) => ({
        id: r.id,
        tenantId: r.tenantId,
        subscriptionItemId: r.subscriptionItemId,
        periodStart: r.periodStart,
        localTotal: Number(r.localTotal),
        stripeTotal: Number(r.stripeTotal),
        diff: Number(r.diff),
        status: r.status,
        createdAt: (r.createdAt as Date).toISOString(),
      }));

      if (format === 'csv') {
        const header = ['subscription_item_id', 'period', 'local', 'stripe', 'drift_abs', 'drift_pct', 'status'];
        const lines = [header.join(',')];
        for (const r of reports) {
          const drift_abs = Math.abs(r.localTotal - r.stripeTotal);
          const drift_pct = r.stripeTotal > 0 ? drift_abs / r.stripeTotal : drift_abs > 0 ? 1 : 0;
          lines.push([
            r.subscriptionItemId,
            period,
            r.localTotal,
            r.stripeTotal,
            drift_abs,
            drift_pct,
            r.status,
          ].join(','));
        }
        const csv = lines.join('\n');
        reply.header('Content-Type', 'text/csv');
        (reply as any).header('Content-Disposition', `attachment; filename="reconciliation_${tenantId}_${period}.csv"`);
        return reply.send(csv);
      }

      reply.send({
        period,
        reports,
        suggestedAdjustments: [],
        summary,
      } as any);
    } catch (_err) {
      const { format } = request.query as any;
      if (format === 'csv') {
        const header = ['subscription_item_id', 'period', 'local', 'stripe', 'drift_abs', 'drift_pct', 'status'];
        const csv = header.join(',') + '\n';
        reply.header('Content-Type', 'text/csv');
        (reply as any).header('Content-Disposition', `attachment; filename="reconciliation_${(request.query as any).tenantId}_${period}.csv"`);
        return reply.send(csv);
      }
      reply.status(500).send({
        period,
        reports: [],
        suggestedAdjustments: [],
        summary: { total: 0, ok: 0, investigating: 0, resolved: 0, maxDiff: 0 },
      } as any);
    }
  });

  /**
   * POST /v1/reconciliation/run
   * Trigger reconciliation for current period
   */
  server.post<{
    Body: { tenantId: string };
  }>('/run', {
    schema: {
      description: 'Trigger reconciliation for current period',
      tags: ['reconciliation'],
      body: {
        type: 'object',
        required: ['tenantId'],
        properties: {
          tenantId: { type: 'string' },
        },
      },
      response: {
        202: {
          type: 'object',
          properties: {
            message: { type: 'string' },
            jobId: { type: 'string' },
          },
        },
      },
    },
    preHandler: [requireScopes(['reconciliation:write'])],
  }, async (_request, reply) => {
    try {
      const port = Number(process.env.WORKER_HTTP_PORT || 3100);
      const host = process.env.WORKER_HTTP_HOST || '127.0.0.1';
      const jobId = `job_${Date.now()}`;
      await new Promise<void>((resolve, reject) => {
        const req = http.request(
          { method: 'POST', host, port, path: '/reconciler/run' },
          (res) => {
            if (res.statusCode && res.statusCode >= 200 && res.statusCode < 300) resolve();
            else reject(new Error(`Worker returned status ${res.statusCode}`));
          }
        );
        req.on('error', reject);
        req.end();
      });
      reply.status(202).send({ message: 'Reconciliation job queued', jobId });
    } catch (err) {
      reply.status(500).send({ message: 'Failed to queue reconciliation job' });
    }
  });

  /**
   * GET /v1/reconciliation/summary
   * Get per-metric reconciliation summary for a period range
   */
  server.get<{
    Querystring: { tenantId: string; periodStart: string; periodEnd: string; format?: 'json' | 'csv' };
    Reply: any;
  }>('/summary', {
    schema: {
      description: 'Get per-metric reconciliation summary for a period range',
      tags: ['reconciliation'],
      querystring: {
        type: 'object',
        required: ['tenantId', 'periodStart', 'periodEnd'],
        properties: {
          tenantId: { type: 'string' },
          periodStart: { type: 'string', pattern: '^\\d{4}-\\d{2}$' },
          periodEnd: { type: 'string', pattern: '^\\d{4}-\\d{2}$' },
          format: { type: 'string', enum: ['json', 'csv'] },
        },
      },
      response: {
        200: {
          type: 'object',
          properties: {
            periodStart: { type: 'string' },
            periodEnd: { type: 'string' },
            perMetric: {
              type: 'array',
              items: {
                type: 'object',
                properties: {
                  metric: { type: 'string' },
                  local: { type: 'number' },
                  stripe: { type: 'number' },
                  drift_abs: { type: 'number' },
                  drift_pct: { type: 'number' },
                  items: { type: 'number' },
                },
              },
            },
            overall: {
              type: 'object',
              properties: {
                local: { type: 'number' },
                stripe: { type: 'number' },
                drift_abs: { type: 'number' },
                drift_pct: { type: 'number' },
                metrics: { type: 'number' },
                items: { type: 'number' },
              },
            },
          },
          example: {
            periodStart: '2025-01',
            periodEnd: '2025-03',
            perMetric: [
              { metric: 'api_calls', local: 100000, stripe: 99800, drift_abs: 200, drift_pct: 0.002, items: 3 },
              { metric: 'gb_transfer', local: 512, stripe: 520, drift_abs: 8, drift_pct: 0.0154, items: 1 },
            ],
            overall: { local: 100512, stripe: 100320, drift_abs: 192, drift_pct: 0.0019, metrics: 2, items: 4 },
          },
        },
      },
    },
    preHandler: [requireScopes(['reconciliation:read'])],
  }, async (request, reply) => {
    if (!requireTenantMatch(request, reply, (request.query as any).tenantId)) return;
    const { periodStart, periodEnd, format } = request.query as any;
    const tenantId = getTenantId(request);

    // Basic validation: ensure periodStart <= periodEnd
    if (periodEnd < periodStart) {
      return reply.status(400).send({
        periodStart,
        periodEnd,
        perMetric: [],
        overall: { local: 0, stripe: 0, drift_abs: 0, drift_pct: 0, metrics: 0, items: 0 },
      } as any);
    }

    // Convert YYYY-MM to YYYY-MM-01 for date comparisons
    const startDate = `${periodStart}-01`;
    const endDate = `${periodEnd}-01`;

    try {
      const rows = await db
        .select({
          metric: priceMappings.metric,
          local: sql<number>`SUM((${reconciliationReports.localTotal})::numeric)`,
          stripe: sql<number>`SUM((${reconciliationReports.stripeTotal})::numeric)`,
          items: sql<number>`COUNT(DISTINCT ${reconciliationReports.subscriptionItemId})`,
        })
        .from(reconciliationReports)
        .innerJoin(
          priceMappings,
          and(
            eq(priceMappings.subscriptionItemId, reconciliationReports.subscriptionItemId),
            eq(priceMappings.tenantId, reconciliationReports.tenantId),
            eq(priceMappings.active, true as any)
          )
        )
        .where(
          and(
            eq(reconciliationReports.tenantId, tenantId),
            gte(reconciliationReports.periodStart, startDate),
            lte(reconciliationReports.periodStart, endDate)
          )
        )
        .groupBy(priceMappings.metric);

      const perMetric = rows.map((r) => {
        const metric = String((r as any).metric ?? '');
        const local = Number((r as any).local ?? 0);
        const stripe = Number((r as any).stripe ?? 0);
        const drift_abs = Math.abs(local - stripe);
        const drift_pct = stripe > 0 ? drift_abs / stripe : drift_abs > 0 ? 1 : 0;
        return { metric, local, stripe, drift_abs, drift_pct, items: Number((r as any).items ?? 0) };
      });

      const overallLocal = perMetric.reduce((s, m) => s + m.local, 0);
      const overallStripe = perMetric.reduce((s, m) => s + m.stripe, 0);
      const overallDriftAbs = Math.abs(overallLocal - overallStripe);
      const overallDriftPct = overallStripe > 0 ? overallDriftAbs / overallStripe : overallDriftAbs > 0 ? 1 : 0;
      const overallItems = perMetric.reduce((s, m) => s + m.items, 0);

      const body: ReconciliationSummaryResponse = {
        periodStart,
        periodEnd,
        perMetric,
        overall: {
          local: overallLocal,
          stripe: overallStripe,
          drift_abs: overallDriftAbs,
          drift_pct: overallDriftPct,
          metrics: perMetric.length,
          items: overallItems,
        },
      };

      if (format === 'csv') {
        const header = ['metric', 'local', 'stripe', 'drift_abs', 'drift_pct', 'items'];
        const lines = [header.join(',')];
        for (const m of body.perMetric) {
          lines.push([m.metric, m.local, m.stripe, m.drift_abs, m.drift_pct, m.items].join(','));
        }
        lines.push(['TOTAL', body.overall.local, body.overall.stripe, body.overall.drift_abs, body.overall.drift_pct, body.overall.items].join(','));
        const csv = lines.join('\n');
        reply.header('Content-Type', 'text/csv');
        (reply as any).header('Content-Disposition', `attachment; filename="reconciliation_summary_${tenantId}_${periodStart}_${periodEnd}.csv"`);
        return reply.send(csv);
      }

      reply.send(body);
    } catch (err) {
      const { format } = request.query as any;
      if (format === 'csv') {
        const header = ['metric', 'local', 'stripe', 'drift_abs', 'drift_pct', 'items'];
        const total = ['TOTAL', 0, 0, 0, 0, 0];
        const csv = header.join(',') + '\n' + total.join(',') + '\n';
        reply.header('Content-Type', 'text/csv');
        (reply as any).header('Content-Disposition', `attachment; filename="reconciliation_summary_${tenantId}_${periodStart}_${periodEnd}.csv"`);
        return reply.send(csv);
      }
      // Fail closed with empty summary
      reply.send({
        periodStart,
        periodEnd,
        perMetric: [],
        overall: { local: 0, stripe: 0, drift_abs: 0, drift_pct: 0, metrics: 0, items: 0 },
      });
    }
  });
  /**
   * POST /v1/reconciliation/adjustments
   * Apply suggested adjustments
   */
  server.post<{
    Body: {
      adjustmentIds: string[];
      reason: string;
    };
  }>('/adjustments', {
    schema: {
      description: 'Apply suggested adjustments',
      tags: ['reconciliation'],
      body: {
        type: 'object',
        required: ['adjustmentIds', 'reason'],
        properties: {
          adjustmentIds: {
            type: 'array',
            items: { type: 'string', format: 'uuid' },
          },
          reason: { type: 'string' },
        },
      },
      response: {
        200: {
          type: 'object',
          properties: {
            applied: { type: 'number' },
            failed: { type: 'number' },
          },
        },
      },
    },
  }, async (_request, reply) => {
    // TODO: Apply adjustments
    reply.send({
      applied: 0,
      failed: 0,
    });
  });
};
