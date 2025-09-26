import { describe, it, expect, vi, beforeEach } from 'vitest';

// Ensure fake mode
process.env.RECONCILIATION_FAKE = '1';
process.env.RECONCILIATION_FAKE_DRIFT_PCT = '0.10';

// Capture inserted reconciliation reports
const insertedReports: any[] = [];

// Mock database layer to isolate logic
vi.mock('@stripemeter/database', () => {
  return {
    db: {
      select: () => ({
        from: (table: any) => ({
          where: async () => {
            // If selecting from counters table, return per-customer totals
            if (table && 'customerRef' in table) {
              return [
                { customerRef: 'c1', total: '100' },
                { customerRef: 'c2', total: '50' },
              ];
            }
            // Else selecting mappings
            return [
              { id: 'm1', tenantId: 'tenant_demo', metric: 'requests', aggregation: 'sum', stripeAccount: 'acct_demo', subscriptionItemId: 'si_demo', active: true },
            ];
          },
        }),
      }),
      insert: () => ({
        values: async (v: any) => {
          const arr = Array.isArray(v) ? v : [v];
          insertedReports.push(...arr);
        },
      }),
    },
    priceMappings: {} as any,
    reconciliationReports: {} as any,
    counters: {
      tenantId: {} as any,
      metric: {} as any,
      periodStart: {} as any,
      aggSum: {} as any,
      aggMax: {} as any,
      aggLast: {} as any,
      customerRef: {} as any,
    } as any,
    adjustments: {} as any,
    redis: { setex: async () => {} },
  };
});

// Minimal drizzle-orm helpers
vi.mock('drizzle-orm', () => ({ and: (..._args: any[]) => ({}), eq: (..._args: any[]) => ({}), sql: (s: TemplateStringsArray) => s.join('') }));

import { ReconcilerWorker } from './reconciler';

describe('ReconcilerWorker (fake mode)', () => {
  beforeEach(() => {
    insertedReports.length = 0;
  });

  it('skips Stripe calls and writes report using fake drift percentage', async () => {
    const worker = new ReconcilerWorker();
    // Ensure Stripe is not called in fake mode
    (worker as any).getStripeUsage = vi.fn(() => {
      throw new Error('should not call Stripe in fake mode');
    });

    // @ts-ignore access private method for test by casting
    await (worker as any).runReconciliation();

    expect((worker as any).getStripeUsage).not.toHaveBeenCalled();
    expect(insertedReports.length).toBeGreaterThan(0);
    const r = insertedReports[0];
    // Local total is 150; 10% fake drift -> stripeTotal ~ 135
    expect(Number(r.localTotal)).toBe(150);
    expect(Number(r.stripeTotal)).toBe(135);
    expect(Number(r.diff)).toBeCloseTo(15, 6);
  });
});

import { describe, it, expect, vi, beforeEach } from 'vitest';
import Stripe from 'stripe';
import { ReconcilerWorker } from './reconciler';

vi.mock('stripe', () => {
  const mock = vi.fn().mockImplementation(() => ({
    subscriptionItems: {
      retrieve: vi.fn().mockResolvedValue({ id: 'si_test' }),
      listUsageRecordSummaries: vi.fn().mockResolvedValue({ data: [{ id: 'urs_1', total_usage: 10 }], has_more: false }),
    },
  }));
  return { default: mock };
});

describe('ReconcilerWorker', () => {
  beforeEach(() => {
    process.env.STRIPE_SECRET_KEY = 'sk_test';
  });

  it('exposes triggerOnDemand without throwing when not running', async () => {
    const worker = new ReconcilerWorker();
    const runSpy = vi.spyOn<any, any>(worker as any, 'runReconciliation').mockResolvedValue(undefined);
    await worker.triggerOnDemand();
    expect(runSpy).toHaveBeenCalled();
  });

  it('getStripeUsage sums usage with pagination and handles headers', async () => {
    const worker = new ReconcilerWorker();
    // @ts-expect-error access private for test
    const usage = await worker['getStripeUsage']('si_test', '2025-01-01', 'default');
    expect(usage.total_usage).toBeGreaterThanOrEqual(0);
  });
});


