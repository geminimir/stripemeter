import {
  and,
  asc,
  desc,
  eq,
  gte,
  ilike,
  lte,
  sql as sqlTag,
} from "drizzle-orm";
import { AlertState, alertStates } from "../schema";
import { db } from "../client";

export class AlertStatesRepository {
  /**
   * Get alert states
   */
  async getAlertStatesByParam(params: {
    tenantId: string;
    customerRef?: string;
    metric?: string;
    status?: "triggered" | "acknowledged" | "resolved";
    severity?: "info" | "warn" | "critical";
    title?: string;
    limit?: number;
    offset?: number;
    sort?: "metric" | "customerRef" | "createdAt";
    sortDir?: "asc" | "desc";
    startTime?: Date;
    endTime?: Date;
  }): Promise<AlertState[]> {
    const {
      tenantId,
      metric,
      customerRef,
      status,
      severity,
      title,
      limit,
      offset,
      sort,
      sortDir,
      startTime,
      endTime,
    } = params;

    const filters = [eq(alertStates.tenantId, tenantId)];

    if (metric !== undefined) {
      filters.push(eq(alertStates.metric, metric));
    }

    if (customerRef !== undefined) {
      filters.push(eq(alertStates.customerRef, customerRef));
    }

    if (status !== undefined) {
      filters.push(eq(alertStates.status, status));
    }

    if (severity !== undefined) {
      filters.push(eq(alertStates.severity, severity));
    }

    if (title !== undefined) {
      filters.push(ilike(alertStates.title, `%${title}%`));
    }

    if (startTime !== undefined) {
      filters.push(gte(alertStates.createdAt, startTime));
    }

    if (endTime !== undefined) {
      filters.push(lte(alertStates.updatedAt, endTime));
    }

    const orderByClause =
      sortDir?.toUpperCase() === "ASC"
        ? asc(alertStates[sort || "createdAt"])
        : desc(alertStates[sort || "createdAt"]);

    const query = db
      .select()
      .from(alertStates)
      .where(and(...filters))
      .orderBy(orderByClause)
      .limit(limit || 25)
      .offset(offset || 0);

    const res = await query;

    return res;
  }

  /**
   * Get alert states count
   */
  async getAlertStatesCountByParam(params: {
    tenantId: string;
    customerRef?: string;
    metric?: string;
    status?: "triggered" | "acknowledged" | "resolved";
    severity?: "info" | "warn" | "critical";
    title?: string;
    startTime?: Date;
    endTime?: Date;
  }): Promise<number> {
    const {
      tenantId,
      metric,
      customerRef,
      status,
      severity,
      title,
      startTime,
      endTime,
    } = params;

    const filters = [eq(alertStates.tenantId, tenantId)];

    if (metric !== undefined) {
      filters.push(eq(alertStates.metric, metric));
    }

    if (customerRef !== undefined) {
      filters.push(eq(alertStates.customerRef, customerRef));
    }

    if (status !== undefined) {
      filters.push(eq(alertStates.status, status));
    }

    if (severity !== undefined) {
      filters.push(eq(alertStates.severity, severity));
    }

    if (title !== undefined) {
      filters.push(ilike(alertStates.title, `%${title}%`));
    }

    if (startTime !== undefined) {
      filters.push(gte(alertStates.createdAt, startTime));
    }

    if (endTime !== undefined) {
      filters.push(lte(alertStates.updatedAt, endTime));
    }

    const query = db
      .select({ count: sqlTag`COUNT(id)` })
      .from(alertStates)
      .where(and(...filters));

    const [res] = await query;

    return res ? Number(res.count) : 0;
  }

  async get(id: string): Promise<AlertState | null> {
    const res = await db
      .select()
      .from(alertStates)
      .where(eq(alertStates.id, id));
    return res && res.length ? res[0] : null;
  }

  async create(alertState: AlertState): Promise<void> {
    await db.insert(alertStates).values(alertState);
  }

  async update(id: string, toUpdate: Partial<AlertState>): Promise<AlertState | null> {
    const res = await db
      .update(alertStates)
      .set({ ...toUpdate, updatedAt: new Date() })
      .where(eq(alertStates.id, id))
      .returning();

    return res && res.length ? res[0] : null;
  }

  async delete(id: string): Promise<void> {
    await db.delete(alertStates).where(eq(alertStates.id, id));
  }
}
