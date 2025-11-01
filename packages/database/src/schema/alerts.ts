/**
 * Alert configurations table schema - Defines alert rules and actions
 */

import { pgTable, uuid, text, numeric, jsonb, boolean, timestamp, index } from 'drizzle-orm/pg-core';

export const alertConfigs = pgTable('alert_configs', {
  id: uuid('id').primaryKey().defaultRandom(),
  tenantId: uuid('tenant_id').notNull(),
  customerRef: text('customer_ref'),
  metric: text('metric'),
  type: text('type', {
    enum: ['threshold', 'spike', 'budget']
  }).notNull(),
  threshold: numeric('threshold', { precision: 20, scale: 6 }).notNull(),
  action: text('action', {
    enum: ['email', 'webhook', 'slack', 'hard_cap', 'soft_cap']
  }).notNull(),
  config: jsonb('config').notNull().default({}),
  enabled: boolean('enabled').notNull().default(true),
  createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
}, (table) => {
  return {
    // Index for tenant queries
    tenantIdx: index('idx_alert_configs_tenant')
      .on(table.tenantId, table.enabled),

    // Index for customer-specific alerts
    customerIdx: index('idx_alert_configs_customer')
      .on(table.tenantId, table.customerRef, table.enabled),

    // Index for metric-specific alerts
    metricIdx: index('idx_alert_configs_metric')
      .on(table.tenantId, table.metric, table.enabled),
  };
});

// Alert history for tracking triggered alerts
export const alertHistory = pgTable('alert_history', {
  id: uuid('id').primaryKey().defaultRandom(),
  alertConfigId: uuid('alert_config_id').notNull().references(() => alertConfigs.id),
  tenantId: uuid('tenant_id').notNull(),
  customerRef: text('customer_ref'),
  metric: text('metric'),
  value: numeric('value', { precision: 20, scale: 6 }).notNull(),
  threshold: numeric('threshold', { precision: 20, scale: 6 }).notNull(),
  action: text('action').notNull(),
  status: text('status', {
    enum: ['triggered', 'acknowledged', 'resolved']
  }).notNull().default('triggered'),
  metadata: jsonb('metadata').notNull().default({}),
  triggeredAt: timestamp('triggered_at', { withTimezone: true }).notNull().defaultNow(),
  acknowledgedAt: timestamp('acknowledged_at', { withTimezone: true }),
  resolvedAt: timestamp('resolved_at', { withTimezone: true }),
}, (table) => {
  return {
    // Index for tenant queries
    tenantIdx: index('idx_alert_history_tenant')
      .on(table.tenantId, table.triggeredAt),

    // Index for status filtering
    statusIdx: index('idx_alert_history_status')
      .on(table.status, table.tenantId),
  };
});


export const alertStates = pgTable('alert_states', {
  id: uuid('id').primaryKey().defaultRandom(),
  tenantId: uuid('tenant_id').notNull(),
  metric: text('metric'),
  customerRef: text('customer_ref'),
  alertConfigId: uuid('alert_config_id'),
  status: text('status', {
    enum: ['triggered', 'acknowledged', 'resolved', 'snoozed']
  }).notNull().default('triggered'),
  severity: text('severity', { 
    enum: ['info', 'warn', 'critical'] 
  }).notNull().default('info'),
  title: text('title'),
  description: text('description'),
  createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
  updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
}, (table) => {
  return {
    // Index for tenant queries
    tenantIdx: index('idx_alerts_tenant')
      .on(table.tenantId),

    // Index for customer-specific alerts
    customerIdx: index('idx_alerts_customer')
      .on(table.tenantId, table.customerRef),

    // Index for metric-specific alerts
    metricIdx: index('idx_alerts_metric')
      .on(table.tenantId, table.metric),
  };
});

// Alert events for tracking triggered alerts
export const alertEvents = pgTable('alert_events', {
  id: uuid('id').primaryKey().defaultRandom(),
  alertConfigId: uuid('alert_config_id'),
  tenantId: uuid('tenant_id').notNull(),
  customerRef: text('customer_ref'),
  metric: text('metric'),
  value: numeric('value', { precision: 20, scale: 6 }).notNull(),
  threshold: numeric('threshold', { precision: 20, scale: 6 }).notNull(),
  action: text('action').notNull(),
  status: text('status', {
    enum: ['triggered', 'acknowledged', 'resolved', 'snoozed']
  }).notNull().default('triggered'),
  metadata: jsonb('metadata').notNull().default({}),
  triggeredAt: timestamp('triggered_at', { withTimezone: true }).notNull().defaultNow(),
  acknowledgedAt: timestamp('acknowledged_at', { withTimezone: true }),
  resolvedAt: timestamp('resolved_at', { withTimezone: true }),
}, (table) => {
  return {
    // Index for tenant queries
    tenantIdx: index('idx_alert_events_tenant')
      .on(table.tenantId, table.triggeredAt),

    // Index for status filtering
    statusIdx: index('idx_alert_events_status')
      .on(table.status, table.tenantId),
  };
});

// Type inference
export type AlertConfig = typeof alertConfigs.$inferSelect;
export type NewAlertConfig = typeof alertConfigs.$inferInsert;
export type AlertHistory = typeof alertHistory.$inferSelect;
export type NewAlertHistory = typeof alertHistory.$inferInsert;
export type AlertState = typeof alertStates.$inferSelect;
export type NewAlertState = typeof alertStates.$inferInsert;
export type AlertEvent = typeof alertEvents.$inferSelect;
export type NewAlertEvent = typeof alertEvents.$inferInsert;
