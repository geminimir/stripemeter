CREATE TABLE IF NOT EXISTS "alert_states" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"tenant_id" uuid NOT NULL,
	"metric" text,
	"customer_ref" text,
	"alert_config_id" uuid,
	"status" text DEFAULT 'triggered' NOT NULL,
	"severity" text DEFAULT 'info' NOT NULL,
	"title" text,
	"description" text,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL
);
CREATE TABLE IF NOT EXISTS "alert_events" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"alert_config_id" uuid,
	"tenant_id" uuid NOT NULL,
	"customer_ref" text,
	"metric" text,
	"value" numeric(20, 6) NOT NULL,
	"threshold" numeric(20, 6) NOT NULL,
	"action" text NOT NULL,
	"status" text DEFAULT 'triggered' NOT NULL,
	"metadata" jsonb DEFAULT '{}'::jsonb NOT NULL,
	"triggered_at" timestamp with time zone DEFAULT now() NOT NULL,
	"acknowledged_at" timestamp with time zone,
	"resolved_at" timestamp with time zone
);
CREATE INDEX IF NOT EXISTS "idx_alert_events_tenant" ON "alert_events" ("tenant_id","triggered_at");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "idx_alert_events_status" ON "alert_events" ("status","tenant_id");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "idx_alerts_tenant" ON "alert_states" ("tenant_id");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "idx_alerts_customer" ON "alert_states" ("tenant_id","customer_ref");--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "idx_alerts_metric" ON "alert_states" ("tenant_id","metric");--> statement-breakpoint
