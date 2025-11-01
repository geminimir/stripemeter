/**
 * Zod validation schemas for API requests
 */

import { z } from 'zod';

// ISO 8601 date-time validation
const isoDateTimeSchema = z.string().refine(
  (val) => !isNaN(Date.parse(val)),
  { message: 'Invalid ISO 8601 date-time format' }
);

// TenantId: relax from UUID to string for DX; still allow UUIDs but don't require
const tenantIdSchema = (() => {
  const strict = process.env.STRICT_TENANT_ID_UUID === 'true';
  return strict ? z.string().uuid() : z.string().min(1);
})();

// Usage event schema
export const usageEventSchema = z.object({
  tenantId: tenantIdSchema,
  metric: z.string().min(1).max(100),
  customerRef: z.string().min(1).max(255),
  resourceId: z.string().max(255).optional(),
  quantity: z.number().positive().finite(),
  ts: isoDateTimeSchema,
  meta: z.record(z.any()).optional(),
  idempotencyKey: z.string().max(255).optional(),
  source: z.enum(['sdk', 'http', 'etl', 'import', 'system']).optional(),
});

export const getUsageHistoryQuerySchema = z.object({
  tenantId: tenantIdSchema,
  customerRef: z.string().min(1).max(255),
  metric: z.string().min(1).max(100),
  periodStart: isoDateTimeSchema,
  periodEnd: isoDateTimeSchema,
  step: z.enum(['day', 'month']).optional().default('month'),
});

// Batch event ingestion schema
export const ingestEventRequestSchema = z.object({
  events: z.array(usageEventSchema).min(1).max(parseInt(process.env.MAX_INGEST_BATCH || '1000', 10)),
});

// Get Event List Query schema
export const getEventsQuerySchema = z.object({
  tenantId: tenantIdSchema,
  metric: z.string().min(1).max(100).optional(),
  customerRef: z.string().min(1).max(255).optional(),
  source: z.enum(['sdk', 'http', 'etl', 'import', 'system']).optional(),
  limit: z.number().int().min(1).max(100).optional().default(25),
  offset: z.number().int().min(0).optional().default(0),
  sort: z.enum(['metric', 'customerRef', 'source', 'ts']).optional().default('ts'),
  sortDir: z.enum(['asc', 'desc']).optional().default('desc'),
  startTime: isoDateTimeSchema.optional(),
  endTime: isoDateTimeSchema.optional(),
});

// Adjustment schema
export const adjustmentRequestSchema = z.object({
  tenantId: tenantIdSchema,
  metric: z.string().min(1).max(100),
  customerRef: z.string().min(1).max(255),
  periodStart: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  delta: z.number().finite().refine((n) => Math.abs(n) <= parseFloat(process.env.MAX_ADJUSTMENT_DELTA || '1000000'),
    { message: 'Adjustment delta out of bounds' }),
  reason: z.enum(['backfill', 'correction', 'promo', 'credit', 'manual']),
});

// Backfill request schema
export const backfillRequestSchema = z.object({
  tenantId: tenantIdSchema,
  metric: z.string().min(1).max(100),
  customerRef: z.string().min(1).max(255).optional(),
  periodStart: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  periodEnd: z.string().regex(/^\d{4}-\d{2}-\d{2}$/).optional(),
  events: z.array(usageEventSchema).max(parseInt(process.env.MAX_BACKFILL_EVENTS || '5000', 10)).optional(),
  csvData: z.string().optional(),
  sourceUrl: z.string().url().optional(),
  reason: z.string().min(1).max(500),
}).refine(
  (data) => data.events || data.csvData || data.sourceUrl,
  { message: 'Either events, csvData, or sourceUrl must be provided' }
);

// Projection request schema
export const projectionRequestSchema = z.object({
  tenantId: tenantIdSchema,
  customerRef: z.string().min(1).max(255),
  periodStart: z.string().regex(/^\d{4}-\d{2}-\d{2}$/).optional(),
  periodEnd: z.string().regex(/^\d{4}-\d{2}-\d{2}$/).optional(),
});

// Price mapping schema
export const priceMappingSchema = z.object({
  tenantId: tenantIdSchema,
  metric: z.string().min(1).max(100),
  aggregation: z.enum(['sum', 'max', 'last']),
  stripeAccount: z.string().startsWith('acct_'),
  priceId: z.string().startsWith('price_'),
  subscriptionItemId: z.string().startsWith('si_').optional(),
  currency: z.string().length(3).optional(),
  active: z.boolean().default(true),
  shadow: z.boolean().default(false).optional(),
  shadowStripeAccount: z.string().startsWith('acct_').optional(),
  shadowPriceId: z.string().startsWith('price_').optional(),
  shadowSubscriptionItemId: z.string().startsWith('si_').optional(),
});

// Alert configuration schema
export const alertConfigSchema = z.object({
  tenantId: tenantIdSchema,
  customerRef: z.string().min(1).max(255).optional(),
  metric: z.string().min(1).max(100).optional(),
  type: z.enum(['threshold', 'spike', 'budget']),
  threshold: z.number().positive().finite(),
  action: z.enum(['email', 'webhook', 'slack', 'hard_cap', 'soft_cap']),
  config: z.record(z.any()),
  enabled: z.boolean().default(true),
});

// Get Event List Query schema
export const getAlertStatesQuerySchema = z.object({
  tenantId: tenantIdSchema,
  customerRef: z.string().min(1).max(255).optional(),
  metric: z.string().min(1).max(100).optional(),
  status: z.enum(['triggered', 'acknowledged', 'resolved']).optional(),
  severity: z.enum(['info', 'warn', 'critical']).optional(),
  title: z.string().min(1).optional(),
  limit: z.number().int().min(1).max(100).optional().default(25),
  offset: z.number().int().min(0).optional().default(0),
  sort: z.enum(['customerRef', 'metric', 'createdAt']).optional().default('createdAt'),
  sortDir: z.enum(['asc', 'desc']).optional().default('desc'),
  startTime: isoDateTimeSchema.optional(),
  endTime: isoDateTimeSchema.optional(),
});


// Type exports
export type UsageEventInput = z.infer<typeof usageEventSchema>;
export type GetUsageHistoryQueryInput = z.infer<typeof getUsageHistoryQuerySchema>;
export type IngestEventRequestInput = z.infer<typeof ingestEventRequestSchema>;
export type GetEventsQueryInput = z.infer<typeof getEventsQuerySchema>;
export type AdjustmentRequestInput = z.infer<typeof adjustmentRequestSchema>;
export type BackfillRequestInput = z.infer<typeof backfillRequestSchema>;
export type ProjectionRequestInput = z.infer<typeof projectionRequestSchema>;
export type PriceMappingInput = z.infer<typeof priceMappingSchema>;
export type AlertConfigInput = z.infer<typeof alertConfigSchema>;
export type GetAlertStatesQueryInput = z.infer<typeof getAlertStatesQuerySchema>;
