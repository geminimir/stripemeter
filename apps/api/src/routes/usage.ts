/**
 * Usage query and projection routes
 */

import { FastifyPluginAsync } from 'fastify';
import type { UsageResponse, ProjectionResponse, GetUsageHistoryResponse } from '@stripemeter/core';
import { getCurrentPeriod, getUsageHistoryQuerySchema } from '@stripemeter/core';
import { db, counters, priceMappings, redis, events } from '@stripemeter/database';
import { InvoiceSimulator, type UsageLineItem, type PriceConfig } from '@stripemeter/pricing-lib';
import { and, eq, gte, lte, sql } from 'drizzle-orm';
import { GetUsageHistoryQueryInput } from '@stripemeter/core';
import { getTenantId, requireTenantMatch, requireScopes } from '../utils/auth';

export const usageRoutes: FastifyPluginAsync = async (server) => {
  /**
   * GET /v1/usage/current
   * Get current period usage for a customer
   */
  server.get<{
    Querystring: {
      tenantId: string;
      customerRef: string;
    };
    Reply: UsageResponse;
  }>('/current', {
    schema: {
      description: 'Get current period usage for a customer',
      tags: ['usage'],
      querystring: {
        type: 'object',
        required: ['tenantId', 'customerRef'],
        properties: {
          tenantId: { type: 'string' },
          customerRef: { type: 'string' },
        },
      },
      response: {
        200: {
          type: 'object',
          properties: {
            customerRef: { type: 'string' },
            period: {
              type: 'object',
              properties: {
                start: { type: 'string' },
                end: { type: 'string' },
              },
            },
            metrics: {
              type: 'array',
              items: {
                type: 'object',
                properties: {
                  name: { type: 'string' },
                  current: { type: 'number' },
                  limit: { type: 'number' },
                  unit: { type: 'string' },
                },
              },
            },
            alerts: {
              type: 'array',
              items: {
                type: 'object',
                properties: {
                  type: { type: 'string' },
                  message: { type: 'string' },
                  severity: { type: 'string', enum: ['info', 'warning', 'critical'] },
                },
              },
            },
          },
        },
      },
    },
    preHandler: [requireScopes(['usage:read'])],
  }, async (request, reply) => {
    if (!requireTenantMatch(request, reply, request.query.tenantId)) return;
    const tenantId = getTenantId(request);
    const { customerRef } = request.query;

    const { start, end } = getCurrentPeriod('monthly');

    try {
      // Try Redis cache first
      const cacheKey = `usage:${tenantId}:${customerRef}:${start}`;
      const cached = await redis.get(cacheKey).catch(() => null);
      if (cached) {
        const metrics = JSON.parse(cached) as UsageResponse['metrics'];
        return reply.send({ customerRef, period: { start, end }, metrics, alerts: [] });
      }

      // Fetch counters for current period from DB
      const rows = await db
        .select({
          metric: counters.metric,
          aggSum: counters.aggSum,
          aggMax: counters.aggMax,
          aggLast: counters.aggLast,
        })
        .from(counters)
        .where(
          and(
            eq(counters.tenantId, tenantId),
            eq(counters.customerRef, customerRef),
            eq(counters.periodStart, start)
          )
        );

      const metrics: UsageResponse['metrics'] = rows.map((r) => ({
        name: r.metric,
        current: Number(r.aggSum ?? 0),
        unit: 'unit',
      }));

      // Cache metrics briefly
      await redis.set(cacheKey, JSON.stringify(metrics), 'EX', 30).catch(() => { });

      reply.send({ customerRef, period: { start, end }, metrics, alerts: [] });
    } catch (_err) {
      // Graceful fallback if DB unavailable
      reply.send({ customerRef, period: { start, end }, metrics: [], alerts: [] });
    }
  });

  /**
   * POST /v1/usage/projection
   * Get cost projection for a customer
   */
  server.post<{
    Body: {
      tenantId: string;
      customerRef: string;
      periodStart?: string;
      periodEnd?: string;
    };
    Reply: ProjectionResponse;
  }>('/projection', {
    schema: {
      description: 'Get cost projection for a customer',
      tags: ['usage'],
      body: {
        type: 'object',
        required: ['tenantId', 'customerRef'],
        properties: {
          tenantId: { type: 'string' },
          customerRef: { type: 'string' },
          periodStart: { type: 'string', format: 'date' },
          periodEnd: { type: 'string', format: 'date' },
        },
      },
      response: {
        200: {
          type: 'object',
          properties: {
            customerRef: { type: 'string' },
            periodStart: { type: 'string' },
            periodEnd: { type: 'string' },
            lineItems: {
              type: 'array',
              items: {
                type: 'object',
                properties: {
                  metric: { type: 'string' },
                  quantity: { type: 'number' },
                  unitPrice: { type: 'number' },
                  total: { type: 'number' },
                },
              },
            },
            subtotal: { type: 'number' },
            credits: { type: 'number' },
            total: { type: 'number' },
            currency: { type: 'string' },
            freshness: {
              type: 'object',
              properties: {
                lastUpdate: { type: 'string' },
                staleness: { type: 'number' },
              },
            },
          },
        },
      },
    },
    preHandler: [requireScopes(['usage:read'])],
  }, async (request, reply) => {
    if (!requireTenantMatch(request, reply, request.body.tenantId)) return;
    const tenantId = getTenantId(request);
    const { customerRef, periodStart, periodEnd } = request.body;

    const period = periodStart && periodEnd ? { start: periodStart, end: periodEnd } : getCurrentPeriod('monthly');

    try {
      // Fetch active price mappings to know which metrics to include and how to aggregate
      const mappings = await db
        .select({
          metric: priceMappings.metric,
          aggregation: priceMappings.aggregation,
          currency: priceMappings.currency,
        })
        .from(priceMappings)
        .where(and(eq(priceMappings.tenantId, tenantId), eq(priceMappings.active, true as any)));

      // Fetch counters for the period start
      const counterRows = await db
        .select({
          metric: counters.metric,
          aggSum: counters.aggSum,
          aggMax: counters.aggMax,
          aggLast: counters.aggLast,
          updatedAt: counters.updatedAt,
        })
        .from(counters)
        .where(
          and(
            eq(counters.tenantId, tenantId),
            eq(counters.customerRef, customerRef),
            eq(counters.periodStart, period.start)
          )
        );

      // Map metric -> quantity according to aggregation type
      const quantities = new Map<string, number>();
      for (const row of counterRows) {
        const mapping = mappings.find((m) => m.metric === row.metric);
        if (!mapping) continue;
        let q = 0;
        if (mapping.aggregation === 'sum') q = Number(row.aggSum ?? 0);
        else if (mapping.aggregation === 'max') q = Number(row.aggMax ?? 0);
        else if (mapping.aggregation === 'last') q = Number(row.aggLast ?? 0);
        quantities.set(row.metric, q);
      }

      // Build invoice via pricing-lib (flat model placeholders until pricing is wired)
      const currency = mappings.find((m) => m.currency)?.currency || 'USD';
      const defaultPriceConfig: PriceConfig = { model: 'flat', currency, unitPrice: 0 };
      const usageItems: UsageLineItem[] = Array.from(quantities.entries()).map(([metric, quantity]) => ({
        metric,
        quantity,
        priceConfig: defaultPriceConfig,
      }));
      const simulator = new InvoiceSimulator();
      const invoice = simulator.simulate({
        customerId: customerRef,
        periodStart: period.start,
        periodEnd: period.end,
        usageItems,
      });
      const lineItems = invoice.lineItems.map((li) => ({
        metric: li.metric,
        quantity: li.quantity,
        unitPrice: li.unitPrice,
        total: li.subtotal,
      }));

      const lastUpdate =
        counterRows.reduce<string | null>((acc, r) => {
          const ts = (r.updatedAt as unknown as string | null);
          if (!ts) return acc;
          return !acc || ts > acc ? ts : acc;
        }, null) || new Date().toISOString();

      reply.send({
        customerRef,
        periodStart: period.start,
        periodEnd: period.end,
        lineItems,
        subtotal: invoice.subtotal,
        credits: invoice.credits,
        total: invoice.total,
        currency,
        freshness: { lastUpdate, staleness: 0 },
      });
    } catch (_err) {
      reply.send({
        customerRef,
        periodStart: period.start,
        periodEnd: period.end,
        lineItems: [],
        subtotal: 0,
        credits: 0,
        total: 0,
        currency: 'USD',
        freshness: { lastUpdate: new Date().toISOString(), staleness: 0 },
      });
    }
  });


  /**
   * POST /v1/usage/history
   * Get usage history in time buckets for a customer
   */
  server.get<{
    Querystring: GetUsageHistoryQueryInput;
    Reply: GetUsageHistoryResponse;
  }>('/history', {
    schema: {
      description: 'Get usage history in time buckets for a customer',
      tags: ['usage'],
      querystring: {
        type: 'object',
        required: ['tenantId', 'customerRef', 'metric', 'periodStart', 'periodEnd', 'step'],
        properties: {
          tenantId: { type: 'string' },
          customerRef: { type: 'string' },
          metric: { type: 'string' },
          periodStart: { type: 'string', format: 'date-time' },
          periodEnd: { type: 'string', format: 'date-time' },
          step: { type: 'string', enum: ['day', 'month'], default: 'month' },
        },
      },
      response: {
        200: {
          type: 'object',
          properties: {
            usage: {
              type: 'array',
              items: {
                type: 'object',
                properties: {
                  ts: { type: 'string' },
                  value: { type: 'number' },
                }
              },
            },
          },
        },
      },
    },
    preHandler: [requireScopes(['usage:read'])],
  }, async (request, reply) => {
    if (!requireTenantMatch(request, reply, request.query.tenantId)) return;
    const validationResult = getUsageHistoryQuerySchema.safeParse(request.query);
    if (!validationResult.success) {
      return reply.status(400).send({
        usage: [],
        errors: validationResult.error.errors.map((err: any, index: number) => ({
          index,
          error: err.message,
        })),
      });
    }

    const tenantId = getTenantId(request);
    const { metric, customerRef, periodStart, periodEnd, step } = request.query;

    const cacheKey = `usage-history:${JSON.stringify({ tenantId, metric, customerRef, periodStart, periodEnd, step })}`;
    const cached = await redis.get(cacheKey).catch(() => null);
    if (cached) {
      const usage = JSON.parse(cached) as GetUsageHistoryResponse['usage'];
      return reply.send({ usage });
    }

    const periodStartDate = new Date(periodStart);
    const periodEndDate = new Date(periodEnd);

    try {
      // Fetch active price mappings to know which metrics to include and how to aggregate
      const mappings = await db
        .select({
          metric: priceMappings.metric,
          aggregation: priceMappings.aggregation,
          currency: priceMappings.currency,
        })
        .from(priceMappings)
        .where(
          and(
            eq(priceMappings.tenantId, tenantId),
            eq(priceMappings.active, true as any),
          ),
        );
      // Calculate aggregations from events
      const aggregations = await db
        .select({
          bucket: sql<Date>`date_trunc('${sql.raw(step)}', ${events.ts})`.as('bucket'),
          sum: sql<string>`COALESCE(SUM(${events.quantity}), 0)::numeric`,
          max: sql<string>`COALESCE(MAX(${events.quantity}), 0)::numeric`,
          last: sql<string>`(
            SELECT ${events.quantity} 
            FROM ${events} 
            WHERE ${events.tenantId} = ${tenantId}
              AND ${events.metric} = ${metric}
              AND ${events.customerRef} = ${customerRef}
              AND ${events.ts} >= ${periodStartDate}
              AND ${events.ts} <= ${periodEndDate}
            ORDER BY ${events.ts} DESC
            LIMIT 1
          )::numeric`,
          maxTs: sql<Date>`MAX(${events.ts})`,
        })
        .from(events)
        .where(
          and(
            eq(events.tenantId, tenantId),
            eq(events.metric, metric),
            eq(events.customerRef, customerRef),
            gte(events.ts, periodStartDate),
            lte(events.ts, periodEndDate),
          )
        )
        .groupBy(sql`bucket`)
        .orderBy(sql`bucket ASC`);

      // Map metric -> value according to aggregation type
      const mapping = mappings.find(m => m.metric === metric);

      const usage: GetUsageHistoryResponse['usage'] = aggregations.map(agg => {
        const usage = {
          ts: agg.bucket.toISOString(),
          value: Number(agg.sum ?? 0),
        };

        if (mapping) {
          switch (mapping.aggregation) {
            case 'max':
              usage.value = Number(agg.max ?? 0);
              break;
            case 'last':
              usage.value = Number(agg.last ?? 0);
              break;
          }
        }

        return usage;
      });

      // cache response
      await redis.set(cacheKey, JSON.stringify(usage), 'EX', 30).catch(() => {});

      reply.status(200).send({
        usage,
      });
    } catch (_err) {
      // Graceful fallback in tests or when DB is unavailable
      reply.status(200).send({
        usage: [],
      });
    }
  });
};
