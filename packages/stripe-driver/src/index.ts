import Stripe from 'stripe';

export type StripeEnvironment = 'live' | 'test';

export interface RecordUsageParams {
  mode: StripeEnvironment;
  stripeAccount: string;
  subscriptionItemId: string;
  quantity: number;
  periodStart: string; // YYYY-MM-DD
  idempotencyKey: string;
}

export interface UsageSummary {
  subscriptionItemId: string;
  stripeAccount: string;
  periodStart: string;
  totalUsage: number;
}

export interface CreateTestClockParams {
  frozenTime: number; // unix seconds
  name?: string;
}

export interface AdvanceTestClockParams {
  clockId: string;
  frozenTime: number; // unix seconds
}

export interface StripeBillingDriver {
  recordUsage(params: RecordUsageParams): Promise<Stripe.UsageRecord>;
  getUsageSummary(subscriptionItemId: string, periodStart: string, stripeAccount: string): Promise<UsageSummary>;
  createTestClock(params: CreateTestClockParams): Promise<Stripe.TestHelpers.TestClock>;
  advanceTestClock(params: AdvanceTestClockParams): Promise<Stripe.TestHelpers.TestClock>;
}

export class StripeBillingDriverImpl implements StripeBillingDriver {
  private stripeLive: Stripe;
  private stripeTest: Stripe | null;

  constructor(opts?: { liveKey?: string; testKey?: string }) {
    const liveKey = opts?.liveKey ?? process.env.STRIPE_SECRET_KEY ?? '';
    const testKey = opts?.testKey ?? process.env.STRIPE_TEST_SECRET_KEY ?? '';

    this.stripeLive = new Stripe(liveKey, { apiVersion: '2023-10-16', typescript: true });
    this.stripeTest = testKey
      ? new Stripe(testKey, { apiVersion: '2023-10-16', typescript: true })
      : null;
  }

  private clientForMode(mode: StripeEnvironment): Stripe {
    if (mode === 'test') {
      if (!this.stripeTest) {
        throw new Error('Stripe test client not configured (STRIPE_TEST_SECRET_KEY missing)');
      }
      return this.stripeTest;
    }
    return this.stripeLive;
  }

  async recordUsage(params: RecordUsageParams): Promise<Stripe.UsageRecord> {
    const { mode, stripeAccount, subscriptionItemId, quantity, periodStart, idempotencyKey } = params;
    const client = this.clientForMode(mode);

    const deterministicTimestampSec = Math.floor(new Date(periodStart).getTime() / 1000);

    return client.subscriptionItems.createUsageRecord(
      subscriptionItemId,
      {
        quantity: Math.round(quantity),
        timestamp: deterministicTimestampSec,
        action: 'set',
      },
      {
        idempotencyKey,
        stripeAccount: stripeAccount !== 'default' ? stripeAccount : undefined,
      }
    );
  }

  async getUsageSummary(subscriptionItemId: string, periodStart: string, stripeAccount: string): Promise<UsageSummary> {
    const client = this.stripeLive;

    const headers = {
      stripeAccount: stripeAccount !== 'default' ? stripeAccount : undefined,
    } as const;

    // Simple retry with exponential backoff for 429/5xx
    const maxRetries = 3;
    let attempt = 0;
    // eslint-disable-next-line no-constant-condition
    while (true) {
      try {
        await client.subscriptionItems.retrieve(subscriptionItemId, headers);

        let startingAfter: string | undefined = undefined;
        let totalUsage = 0;

        for (let i = 0; i < 5; i++) {
          const resp: Stripe.ApiList<Stripe.UsageRecordSummary> = await client.subscriptionItems.listUsageRecordSummaries(
            subscriptionItemId,
            {
              limit: 100,
              starting_after: startingAfter,
            },
            headers,
          );

          for (const s of resp.data) {
            totalUsage += s.total_usage ?? 0;
          }

          if (!resp.has_more) break;
          startingAfter = resp.data[resp.data.length - 1]?.id;
        }

        return {
          subscriptionItemId,
          stripeAccount,
          periodStart,
          totalUsage,
        };
      } catch (error: any) {
        const status = error?.statusCode || error?.status || 0;
        const retryable = status === 429 || (status >= 500 && status < 600);
        if (retryable && attempt < maxRetries) {
          const delayMs = Math.pow(2, attempt) * 250;
          await new Promise((r) => setTimeout(r, delayMs));
          attempt += 1;
          continue;
        }
        throw error;
      }
    }
  }

  async createTestClock(params: CreateTestClockParams): Promise<Stripe.TestHelpers.TestClock> {
    const client = this.clientForMode('test');
    return client.testHelpers.testClocks.create({
      frozen_time: params.frozenTime,
      name: params.name,
    });
  }

  async advanceTestClock(params: AdvanceTestClockParams): Promise<Stripe.TestHelpers.TestClock> {
    const client = this.clientForMode('test');
    return client.testHelpers.testClocks.advance(params.clockId, {
      frozen_time: params.frozenTime,
    });
  }
}
