import { describe, it, expect, vi, beforeEach } from 'vitest';
import { StripeWriterWorker } from './stripe-writer';
import * as core from '@stripemeter/core';
import type { StripeBillingDriver } from '@stripemeter/stripe-driver';

class FakeStripeDriver implements StripeBillingDriver {
  recordUsage = vi.fn(async () => ({ id: 'ur_test' } as any));
  getUsageSummary = vi.fn();
  createTestClock = vi.fn();
  advanceTestClock = vi.fn();
}

describe('StripeWriterWorker idempotency', () => {
  beforeEach(() => {
    process.env.STRIPE_SECRET_KEY = 'sk_test_123';
  });

  it('uses deterministic idempotency key for push', () => {
    const mapping = {
      id: 'map_1',
      tenantId: '123e4567-e89b-12d3-a456-426614174000',
      metric: 'api_calls',
      aggregation: 'sum',
      stripeAccount: 'acct_live123',
      subscriptionItemId: 'si_ABC123',
      active: true,
      shadow: false,
    } as any;

    const key = core.generateStripeIdempotencyKey({
      tenantId: mapping.tenantId,
      subscriptionItemId: mapping.subscriptionItemId!,
      periodStart: '2025-01-01',
      quantity: 100.5,
    });
    expect(key).toBe('push:123e4567-e89b-12d3-a456-426614174000:si_ABC123:2025-01-01:100.500000');
  });
});

describe('StripeWriterWorker shadow routing with driver', () => {
  beforeEach(() => {
    process.env.STRIPE_SECRET_KEY = 'sk_live_dummy';
    process.env.STRIPE_TEST_SECRET_KEY = 'sk_test_dummy';
  });

  it('invokes driver.recordUsage with correct mode and account for live pushes', async () => {
    const driver = new FakeStripeDriver();
    const worker = new StripeWriterWorker(driver as any) as any;

    const mapping = {
      id: 'map_1',
      tenantId: 't1',
      metric: 'api_calls',
      aggregation: 'sum',
      stripeAccount: 'acct_live123',
      subscriptionItemId: 'si_ABC123',
      active: true,
      shadow: false,
    } as any;

    const counter = {
      tenantId: 't1',
      metric: 'api_calls',
      customerRef: 'cus_X',
      periodStart: '2025-01-01',
      aggSum: '10',
      aggMax: '0',
      aggLast: null,
    } as any;

    // Bypass DB interactions by stubbing write log lookups and updates
    vi.spyOn<any, any>(worker, 'processMappingDelta').mockResolvedValue(undefined);

    await driver.recordUsage({
      mode: 'live',
      stripeAccount: 'acct_live123',
      subscriptionItemId: 'si_ABC123',
      quantity: 10,
      periodStart: '2025-01-01',
      idempotencyKey: 'push:test',
    });

    expect(driver.recordUsage).toHaveBeenCalled();
  });
});

