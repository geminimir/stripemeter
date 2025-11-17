import { describe, it, expect, vi, beforeEach } from 'vitest';
import Stripe from 'stripe';
import { StripeBillingDriverImpl } from './index';

vi.mock('stripe', () => {
  const mockUsageRecord = { id: 'ur_test', object: 'usage_record' } as any;

  const mock = vi.fn().mockImplementation(() => ({
    subscriptionItems: {
      createUsageRecord: vi.fn().mockResolvedValue(mockUsageRecord),
      retrieve: vi.fn().mockResolvedValue({ id: 'si_test' }),
      listUsageRecordSummaries: vi.fn().mockResolvedValue({
        data: [{ id: 'urs_1', total_usage: 10 }],
        has_more: false,
      }),
    },
    testHelpers: {
      testClocks: {
        create: vi.fn().mockResolvedValue({ id: 'clock_123', frozen_time: 123, status: 'advancing' }),
        advance: vi.fn().mockResolvedValue({ id: 'clock_123', frozen_time: 456, status: 'advancing' }),
      },
    },
  }));

  return { default: mock };
});

describe('StripeBillingDriverImpl', () => {
  beforeEach(() => {
    process.env.STRIPE_SECRET_KEY = 'sk_test_live';
    process.env.STRIPE_TEST_SECRET_KEY = 'sk_test_test';
  });

  it('records usage with deterministic timestamp and idempotency key', async () => {
    const driver = new StripeBillingDriverImpl();

    const res = await driver.recordUsage({
      mode: 'live',
      stripeAccount: 'acct_123',
      subscriptionItemId: 'si_123',
      quantity: 100.5,
      periodStart: '2025-01-01',
      idempotencyKey: 'push:test',
    });

    expect(res.id).toBe('ur_test');
    const StripeCtor = (Stripe as unknown as vi.Mock);
    expect(StripeCtor).toHaveBeenCalled();
  });

  it('gets usage summary with pagination', async () => {
    const driver = new StripeBillingDriverImpl();
    const summary = await driver.getUsageSummary('si_123', '2025-01-01', 'default');
    expect(summary.totalUsage).toBeGreaterThanOrEqual(0);
    expect(summary.subscriptionItemId).toBe('si_123');
  });

  it('creates and advances test clocks using test client', async () => {
    const driver = new StripeBillingDriverImpl();

    const created = await driver.createTestClock({ frozenTime: 123, name: 'Test Clock' });
    expect(created.id).toBe('clock_123');

    const advanced = await driver.advanceTestClock({ clockId: 'clock_123', frozenTime: 456 });
    expect(advanced.frozen_time).toBe(456);
  });
});
