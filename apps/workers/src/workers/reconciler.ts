/**
 * Reconciler Worker - Compares local usage with Stripe and identifies discrepancies
 */

import { db, redis, priceMappings, reconciliationReports, counters, adjustments } from '@stripemeter/database';
import { eq, and, sql } from 'drizzle-orm';
import { logger } from '../utils/logger';
import { reconRunsTotal, reconDurationSeconds, reconciliationDiffAbs, reconciliationDiffPct, workerRunningGauge } from '../utils/metrics';
import { getCurrentPeriod, RECONCILIATION_EPSILON } from '@stripemeter/core';
import { StripeBillingDriver, StripeBillingDriverImpl } from '@stripemeter/stripe-driver';

export class ReconcilerWorker {
  private driver: StripeBillingDriver;
  private intervalId: NodeJS.Timeout | null = null;
  private isRunning = false;

  constructor(driver?: StripeBillingDriver) {
    this.driver = driver ?? new StripeBillingDriverImpl();
  }

  async start() {
    const intervalMs = parseInt(process.env.RECONCILIATION_INTERVAL_MS || '3600000', 10); // Default 1 hour
    
    // Run immediately on start
    this.runReconciliation();
    
    // Then run on interval
    this.intervalId = setInterval(() => {
      if (!this.isRunning) {
        this.runReconciliation();
      }
    }, intervalMs);

    logger.info(`Reconciler started (interval: ${intervalMs}ms)`);
  }

  async triggerOnDemand() {
    if (this.isRunning) {
      logger.info('Reconciliation already running; on-demand trigger ignored');
      return;
    }
    this.runReconciliation();
  }

  async stop() {
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
    
    // Wait for current run to complete
    while (this.isRunning) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    logger.info('Reconciler stopped');
  }

  private async runReconciliation() {
    if (this.isRunning) {
      logger.debug('Reconciliation already in progress, skipping');
      return;
    }

    this.isRunning = true;
    const startTime = Date.now();
    workerRunningGauge.labels('reconciler').inc();
    
    try {
      logger.info('Starting reconciliation run');
      reconRunsTotal.inc();
      
      // Get current period
      const { start: periodStart } = getCurrentPeriod();
      
      // Get all active price mappings with subscription items
      const mappings = await db
        .select()
        .from(priceMappings)
        .where(
          and(
            eq(priceMappings.active, true),
            sql`${priceMappings.subscriptionItemId} IS NOT NULL`
          )
        );

      logger.info(`Reconciling ${mappings.length} subscription items`);

      let totalReports = 0;
      let investigatingCount = 0;

      for (const mapping of mappings) {
        try {
          const reports = await this.reconcileSubscriptionItem(mapping, periodStart);
          totalReports += reports.length;
          investigatingCount += reports.filter(r => r.status === 'investigate').length;
        } catch (error) {
          logger.error(`Failed to reconcile mapping ${mapping.id}:`, error);
        }
      }

      const duration = Date.now() - startTime;
      logger.info(`Reconciliation completed in ${duration}ms: ${totalReports} reports, ${investigatingCount} investigating`);
      reconDurationSeconds.observe(duration / 1000);

      // Store metrics in Redis
      await redis.setex(
        'reconciliation:last_run',
        86400, // 24 hour TTL
        JSON.stringify({
          timestamp: new Date().toISOString(),
          duration,
          totalReports,
          investigatingCount,
          periodStart,
        })
      );

    } catch (error) {
      logger.error('Reconciliation run failed:', error);
    } finally {
      this.isRunning = false;
      workerRunningGauge.labels('reconciler').dec();
    }
  }

  private async reconcileSubscriptionItem(
    mapping: typeof priceMappings.$inferSelect,
    periodStart: string
  ): Promise<Array<typeof reconciliationReports.$inferInsert>> {
    const { tenantId, metric, aggregation, stripeAccount, subscriptionItemId } = mapping;
    
    if (!subscriptionItemId) {
      return [];
    }

    const reports: Array<typeof reconciliationReports.$inferInsert> = [];

    try {
      // Get local totals from counters
      const countersList = await db
        .select({
          customerRef: counters.customerRef,
          total: sql<string>`
            CASE 
              WHEN ${aggregation} = 'sum' THEN ${counters.aggSum}
              WHEN ${aggregation} = 'max' THEN ${counters.aggMax}
              WHEN ${aggregation} = 'last' THEN COALESCE(${counters.aggLast}, '0')
              ELSE ${counters.aggSum}
            END
          `,
        })
        .from(counters)
        .where(
          and(
            eq(counters.tenantId, tenantId),
            eq(counters.metric, metric),
            eq(counters.periodStart, periodStart)
          )
        );

      // Calculate local total across all customers
      const localTotal = countersList.reduce((sum: number, c: any) => sum + parseFloat(c.total), 0);

      // Get Stripe reported usage
      const summary = await this.driver.getUsageSummary(subscriptionItemId, periodStart, stripeAccount);
      const stripeTotal = summary.totalUsage;

      // Calculate difference
      const diff = Math.abs(localTotal - stripeTotal);
      const diffPercentage = stripeTotal > 0 ? diff / stripeTotal : diff > 0 ? 1 : 0;

      // Determine status based on epsilon tolerance
      let status: 'ok' | 'investigate' | 'resolved';
      if (diffPercentage <= RECONCILIATION_EPSILON) {
        status = 'ok';
      } else {
        status = 'investigate';
        logger.warn(`Reconciliation diff exceeds epsilon for ${subscriptionItemId}: local=${localTotal}, stripe=${stripeTotal}, diff=${diff} (${(diffPercentage * 100).toFixed(2)}%)`);
      }

      // Create reconciliation report
      const report: typeof reconciliationReports.$inferInsert = {
        tenantId,
        subscriptionItemId,
        periodStart,
        localTotal: localTotal.toString(),
        stripeTotal: stripeTotal.toString(),
        diff: diff.toString(),
        status,
        createdAt: new Date(),
      };

      await db.insert(reconciliationReports).values(report);
      reports.push(report);

      // If investigating, create suggested adjustments (pending)
      if (status === 'investigate') {
        await this.createSuggestedAdjustments(
          tenantId,
          metric,
          countersList,
          localTotal,
          stripeTotal,
          periodStart
        );
      }

      // Update metrics in Redis and Prometheus
      const metricsKey = `reconciliation:metrics:${tenantId}:${subscriptionItemId}:${periodStart}`;
      await redis.setex(
        metricsKey,
        86400, // 24 hour TTL
        JSON.stringify({
          localTotal,
          stripeTotal,
          diff,
          diffPercentage,
          status,
          timestamp: new Date().toISOString(),
        })
      );

      // Prometheus gauges (label by tenant, subscription_item, period)
      reconciliationDiffAbs.labels(tenantId, subscriptionItemId, periodStart).set(diff);
      reconciliationDiffPct.labels(tenantId, subscriptionItemId, periodStart).set(diffPercentage);

    } catch (error) {
      logger.error(`Failed to reconcile ${subscriptionItemId}:`, error);
    }

    return reports;
  }

  private async createSuggestedAdjustments(
    tenantId: string,
    metric: string,
    counters: Array<{ customerRef: string; total: string }>,
    localTotal: number,
    stripeTotal: number,
    periodStart: string
  ) {
    const diff = stripeTotal - localTotal;
    
    // If Stripe has more usage than local, we might be missing events
    if (diff > 0) {
      // Distribute the difference proportionally across customers
      for (const counter of counters) {
        const customerTotal = parseFloat(counter.total);
        const proportion = localTotal > 0 ? customerTotal / localTotal : 1 / counters.length;
        const adjustmentAmount = diff * proportion;

        if (adjustmentAmount > 0.01) { // Only create adjustment if significant
          await db.insert(adjustments).values({
            tenantId,
            metric,
            customerRef: counter.customerRef,
            periodStart,
            delta: adjustmentAmount.toString(),
            reason: 'correction' as const,
            actor: 'system:reconciliation',
            status: 'pending' as const,
            createdAt: new Date(),
          });

          logger.info(`Created suggested adjustment for ${counter.customerRef}: +${adjustmentAmount.toFixed(2)}`);
        }
      }
    } else if (diff < 0) {
      // If local has more usage than Stripe, we might need to push more
      logger.warn(`Local usage exceeds Stripe for ${metric}: might need manual intervention`);
      
      // Store alert in Redis for admin review
      const alertKey = `reconciliation:alert:${tenantId}:${metric}:${periodStart}`;
      await redis.setex(
        alertKey,
        86400 * 7, // 7 day TTL
        JSON.stringify({
          type: 'local_exceeds_stripe',
          localTotal,
          stripeTotal,
          diff: Math.abs(diff),
          timestamp: new Date().toISOString(),
          message: 'Local usage exceeds Stripe reported usage. Manual review required.',
        })
      );
    }
  }
}
