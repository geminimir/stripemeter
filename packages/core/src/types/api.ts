/**
 * API request/response types
 */

import { UsageEvent, Adjustment, ReconciliationReport } from './base';

// Request types
export interface IngestEventRequest {
  events: UsageEvent[];
}

export interface GetEventsQueryRequest {
  tenantId: string;
  metric?: string;
  customerRef?: string;
  source?: 'sdk' | 'http' | 'etl' | 'import' | 'system';
  limit?: number;
  offset?: number;
  sort?: 'metric' | 'customerRef' | 'source' | 'ts';
  sortDir?: 'asc' | 'desc';
  startTime?: string;
  endTime?: string;
}

export interface BackfillRequest {
  tenantId: string;
  metric: string;
  customerRef?: string;
  periodStart: string;
  periodEnd?: string;
  events?: UsageEvent[];
  csvData?: string;
  reason: string;
}

export interface AdjustmentRequest {
  tenantId: string;
  metric: string;
  customerRef: string;
  periodStart: string;
  delta: number;
  reason: string;
}

export interface ProjectionRequest {
  tenantId: string;
  customerRef: string;
  periodStart?: string;
  periodEnd?: string;
}

// Response types
export interface IngestEventResponse {
  accepted: number;
  duplicates: number;
  requestId: string;
  results?: Array<{
    idempotencyKey: string;
    status: 'accepted' | 'duplicate' | 'error';
    error?: string;
  }>;
  errors?: Array<{
    index: number;
    error: string;
  }>;
}

export interface GetEventsResponse {
  total: number;
  events: Array<{
    id: string;
    tenantId: string;
    metric: string;
    customerRef: string;
    resourceId?: string;
    quantity: number;
    timestamp: string;
    meta?: string;
    source: string;
  }>;
  errors?: Array<{
    index: number;
    error: string;
  }>;
}

export interface ProjectionResponse {
  customerRef: string;
  periodStart: string;
  periodEnd: string;
  lineItems: Array<{
    metric: string;
    quantity: number;
    unitPrice: number;
    total: number;
  }>;
  subtotal: number;
  credits: number;
  total: number;
  currency: string;
  freshness: {
    lastUpdate: string;
    staleness: number; // seconds
  };
}

export interface UsageResponse {
  customerRef: string;
  period: {
    start: string;
    end: string;
  };
  metrics: Array<{
    name: string;
    current: number;
    limit?: number;
    unit: string;
  }>;
  alerts: Array<{
    type: string;
    message: string;
    severity: 'info' | 'warning' | 'critical';
  }>;
}



export interface GetUsageHistoryResponse {
  usage: Array<{
    ts: string;
    value: number;
  }>;
  errors?: Array<{
    index: number;
    error: string;
  }>;
}

export interface ReconciliationResponse {
  period: string;
  reports: ReconciliationReport[];
  suggestedAdjustments: Adjustment[];
  summary: {
    total: number;
    ok: number;
    investigating: number;
    resolved: number;
    maxDiff: number;
  };
}

export interface HealthResponse {
  status: 'healthy' | 'degraded' | 'unhealthy';
  version: string;
  checks: {
    database: boolean;
    redis: boolean;
    stripe: boolean;
    workers: boolean;
  };
  metrics?: {
    eventsPerSecond: number;
    writerLag: number;
    reconciliationDiff: number;
  };
}

// Reconciliation summary types
export interface ReconciliationSummaryMetric {
  metric: string;
  local: number;
  stripe: number;
  drift_abs: number;
  drift_pct: number;
  items: number;
}

export interface ReconciliationSummaryResponse {
  periodStart: string; // YYYY-MM
  periodEnd: string;   // YYYY-MM
  perMetric: ReconciliationSummaryMetric[];
  overall: {
    local: number;
    stripe: number;
    drift_abs: number;
    drift_pct: number;
    metrics: number;
    items: number;
  };
}

export interface GetAlertStatesResponse {
  total: number;
  alertStates: Array<{
    id: string;
    tenantId: string;
    customerRef: string;
    metric: string;
    alertConfigId: string;
    status: string;
    severity: number;
    title: string;
    description: string;
    createdAt: string;
    updatedAt: string;
  }>;
  errors?: Array<{
    index: number;
    error: string;
  }>;
}
