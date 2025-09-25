/**
 * Simulation API routes for managing and running pricing scenarios
 */

import { FastifyPluginAsync } from 'fastify';
import { db, simulationScenarios, simulationRuns, simulationBatches } from '@stripemeter/database';
import { eq, and, desc, asc, sql } from 'drizzle-orm';
import { InvoiceSimulator } from '@stripemeter/pricing-lib';
import { Queue } from 'bullmq';
import { redis } from '@stripemeter/database';
import { z } from 'zod';

// Request/Response schemas
const CreateScenarioSchema = z.object({
  name: z.string().min(1).max(255),
  description: z.string().optional(),
  version: z.string().optional(),
  tags: z.array(z.string()).optional(),
  model: z.any(), // PriceConfig schema from pricing-lib
  inputs: z.object({
    customerId: z.string(),
    periodStart: z.string(),
    periodEnd: z.string(),
    usageItems: z.array(z.any()),
    commitments: z.array(z.any()).optional(),
    credits: z.array(z.any()).optional(),
    taxRate: z.number().optional(),
  }),
  expected: z.any().optional(),
  tolerances: z.object({
    absolute: z.number().optional(),
    relative: z.number().optional(),
  }).optional(),
});

const UpdateScenarioSchema = CreateScenarioSchema.partial();

const RunScenarioSchema = z.object({
  scenarioId: z.string().uuid().optional(),
  scenario: CreateScenarioSchema.optional(),
  name: z.string().optional(),
  description: z.string().optional(),
  metadata: z.record(z.any()).optional(),
});

const BatchRunSchema = z.object({
  name: z.string().min(1),
  description: z.string().optional(),
  scenarioIds: z.array(z.string().uuid()).min(1),
});

// Create simulation queue (use noop queue in tests if redis is unavailable)
const createQueue = () => {
  const isTest = process.env.NODE_ENV === 'test';
  if (isTest && !process.env.USE_REDIS_IN_TEST) {
    return {
      add: async () => ({}),
      addBulk: async () => ({}),
    } as unknown as Queue;
  }
  return new Queue('simulations', {
    connection: redis,
    defaultJobOptions: {
      removeOnComplete: 100,
      removeOnFail: 1000,
      attempts: 3,
      backoff: {
        type: 'exponential',
        delay: 2000,
      },
    },
  });
};
const simulationQueue = createQueue();

export const simulationRoutes: FastifyPluginAsync = async (server) => {
  // Get tenant ID from request context (would be set by auth middleware)
  const getTenantId = (request: any): string => {
    // TODO: Get from auth context
    return request.headers['x-tenant-id'] || '9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d';
  };

  /**
   * GET /v1/simulations/scenarios
   * List all scenarios for a tenant
   */
  server.get('/scenarios', {
    schema: {
      description: 'List simulation scenarios',
      tags: ['simulations'],
      querystring: {
        type: 'object',
        properties: {
          active: { type: 'boolean' },
          tag: { type: 'string' },
          limit: { type: 'integer', minimum: 1, maximum: 100, default: 20 },
          offset: { type: 'integer', minimum: 0, default: 0 },
          sortBy: { type: 'string', enum: ['name', 'createdAt', 'updatedAt'], default: 'createdAt' },
          sortOrder: { type: 'string', enum: ['asc', 'desc'], default: 'desc' },
        },
      },
      response: {
        200: {
          type: 'object',
          properties: {
            scenarios: {
              type: 'array',
              items: {
                type: 'object',
                properties: {
                  id: { type: 'string' },
                  name: { type: 'string' },
                  description: { type: 'string' },
                  version: { type: 'string' },
                  tags: { type: 'array', items: { type: 'string' } },
                  active: { type: 'boolean' },
                  createdAt: { type: 'string' },
                  updatedAt: { type: 'string' },
                },
              },
            },
            total: { type: 'integer' },
            limit: { type: 'integer' },
            offset: { type: 'integer' },
          },
        },
      },
    },
  }, async (request, reply) => {
    const tenantId = getTenantId(request);
    const { active, tag, limit = 20, offset = 0, sortBy = 'createdAt', sortOrder = 'desc' } = request.query as any;

    // Build query conditions
    const conditions = [eq(simulationScenarios.tenantId, tenantId)];
    if (active !== undefined) {
      conditions.push(eq(simulationScenarios.active, active));
    }
    if (tag) {
      conditions.push(sql`${simulationScenarios.tags}::jsonb @> ${JSON.stringify([tag])}::jsonb`);
    }

    // Get total count
    const [{ count }] = await db
      .select({ count: sql<number>`count(*)` })
      .from(simulationScenarios)
      .where(and(...conditions));

    // Get scenarios
    const getSortColumn = (field: string) => {
      switch (field) {
        case 'name': return simulationScenarios.name;
        case 'createdAt': return simulationScenarios.createdAt;
        case 'updatedAt': return simulationScenarios.updatedAt;
        default: return simulationScenarios.createdAt;
      }
    };
    
    const orderBy = sortOrder === 'desc' 
      ? desc(getSortColumn(sortBy))
      : asc(getSortColumn(sortBy));

    const scenarios = await db
      .select({
        id: simulationScenarios.id,
        name: simulationScenarios.name,
        description: simulationScenarios.description,
        version: simulationScenarios.version,
        tags: simulationScenarios.tags,
        active: simulationScenarios.active,
        createdAt: simulationScenarios.createdAt,
        updatedAt: simulationScenarios.updatedAt,
      })
      .from(simulationScenarios)
      .where(and(...conditions))
      .orderBy(orderBy)
      .limit(limit)
      .offset(offset);

    reply.send({
      scenarios,
      total: Number(count),
      limit,
      offset,
    });
  });

  /**
   * GET /v1/simulations/scenarios/:id
   * Get a specific scenario
   */
  server.get<{ Params: { id: string } }>('/scenarios/:id', {
    schema: {
      description: 'Get a simulation scenario by ID',
      tags: ['simulations'],
      params: {
        type: 'object',
        required: ['id'],
        properties: {
          id: { type: 'string', format: 'uuid' },
        },
      },
      response: {
        200: {
          type: 'object',
        },
        404: {
          type: 'object',
          properties: {
            error: { type: 'string' },
          },
        },
      },
    },
  }, async (request, reply) => {
    const tenantId = getTenantId(request);
    const { id } = request.params;

    const [scenario] = await db
      .select()
      .from(simulationScenarios)
      .where(and(
        eq(simulationScenarios.id, id),
        eq(simulationScenarios.tenantId, tenantId)
      ))
      .limit(1);

    if (!scenario) {
      return reply.status(404).send({ error: 'Scenario not found' });
    }

    reply.send(scenario);
  });

  /**
   * POST /v1/simulations/scenarios
   * Create a new scenario
   */
  server.post('/scenarios', {
    schema: {
      description: 'Create a new simulation scenario',
      tags: ['simulations'],
      body: {
        type: 'object',
        required: ['name', 'model', 'inputs'],
      },
      response: {
        201: {
          type: 'object',
          properties: {
            id: { type: 'string' },
            name: { type: 'string' },
            createdAt: { type: 'string' },
          },
        },
      },
    },
  }, async (request, reply) => {
    const tenantId = getTenantId(request);
    const validationResult = CreateScenarioSchema.safeParse(request.body);

    if (!validationResult.success) {
      return reply.status(400).send({
        error: 'Invalid request body',
        details: validationResult.error.errors,
      });
    }

    const data = validationResult.data;

    // Validate scenario by running it
    try {
      const simulator = new InvoiceSimulator();
      simulator.simulate(data.inputs);
    } catch (error) {
      return reply.status(400).send({
        error: 'Invalid scenario configuration',
        details: error instanceof Error ? error.message : 'Unknown error',
      });
    }

    const [scenario] = await db
      .insert(simulationScenarios)
      .values({
        tenantId,
        name: data.name,
        description: data.description,
        version: data.version || '1',
        tags: data.tags || [],
        model: data.model,
        inputs: data.inputs,
        expected: data.expected,
        tolerances: data.tolerances,
        createdBy: request.headers['x-user-id'] as string,
        updatedBy: request.headers['x-user-id'] as string,
      })
      .returning({
        id: simulationScenarios.id,
        name: simulationScenarios.name,
        createdAt: simulationScenarios.createdAt,
      });

    reply.status(201).send(scenario);
  });

  /**
   * PUT /v1/simulations/scenarios/:id
   * Update a scenario
   */
  server.put<{ Params: { id: string } }>('/scenarios/:id', {
    schema: {
      description: 'Update a simulation scenario',
      tags: ['simulations'],
      params: {
        type: 'object',
        required: ['id'],
        properties: {
          id: { type: 'string', format: 'uuid' },
        },
      },
      response: {
        200: {
          type: 'object',
          properties: {
            id: { type: 'string' },
            name: { type: 'string' },
            updatedAt: { type: 'string' },
          },
        },
      },
    },
  }, async (request, reply) => {
    const tenantId = getTenantId(request);
    const { id } = request.params;
    const validationResult = UpdateScenarioSchema.safeParse(request.body);

    if (!validationResult.success) {
      return reply.status(400).send({
        error: 'Invalid request body',
        details: validationResult.error.errors,
      });
    }

    const updates = validationResult.data;

    // If inputs are being updated, validate them
    if (updates.inputs) {
      try {
        const simulator = new InvoiceSimulator();
        simulator.simulate(updates.inputs);
      } catch (error) {
        return reply.status(400).send({
          error: 'Invalid scenario configuration',
          details: error instanceof Error ? error.message : 'Unknown error',
        });
      }
    }

    const [updated] = await db
      .update(simulationScenarios)
      .set({
        ...updates,
        updatedAt: new Date(),
        updatedBy: request.headers['x-user-id'] as string,
      })
      .where(and(
        eq(simulationScenarios.id, id),
        eq(simulationScenarios.tenantId, tenantId)
      ))
      .returning({
        id: simulationScenarios.id,
        name: simulationScenarios.name,
        updatedAt: simulationScenarios.updatedAt,
      });

    if (!updated) {
      return reply.status(404).send({ error: 'Scenario not found' });
    }

    reply.send(updated);
  });

  /**
   * DELETE /v1/simulations/scenarios/:id
   * Delete a scenario (soft delete by marking inactive)
   */
  server.delete<{ Params: { id: string } }>('/scenarios/:id', {
    schema: {
      description: 'Delete a simulation scenario',
      tags: ['simulations'],
      params: {
        type: 'object',
        required: ['id'],
        properties: {
          id: { type: 'string', format: 'uuid' },
        },
      },
      response: {
        204: {
          type: 'null',
        },
      },
    },
  }, async (request, reply) => {
    const tenantId = getTenantId(request);
    const { id } = request.params;

    await db
      .update(simulationScenarios)
      .set({
        active: false,
        updatedAt: new Date(),
        updatedBy: request.headers['x-user-id'] as string,
      })
      .where(and(
        eq(simulationScenarios.id, id),
        eq(simulationScenarios.tenantId, tenantId)
      ));

    reply.status(204).send();
  });

  /**
   * POST /v1/simulations/runs
   * Run a simulation scenario
   */
  server.post('/runs', {
    schema: {
      description: 'Run a simulation scenario',
      tags: ['simulations'],
      body: {
        type: 'object',
      },
      response: {
        202: {
          type: 'object',
          properties: {
            runId: { type: 'string' },
            status: { type: 'string' },
            message: { type: 'string' },
          },
        },
      },
    },
  }, async (request, reply) => {
    const tenantId = getTenantId(request);
    const validationResult = RunScenarioSchema.safeParse(request.body);

    if (!validationResult.success) {
      return reply.status(400).send({
        error: 'Invalid request body',
        details: validationResult.error.errors,
      });
    }

    const { scenarioId, scenario: inlineScenario, name, description, metadata } = validationResult.data;

    // Get scenario configuration
    let scenarioData;
    if (scenarioId) {
      const [dbScenario] = await db
        .select()
        .from(simulationScenarios)
        .where(and(
          eq(simulationScenarios.id, scenarioId),
          eq(simulationScenarios.tenantId, tenantId)
        ))
        .limit(1);

      if (!dbScenario) {
        return reply.status(404).send({ error: 'Scenario not found' });
      }
      scenarioData = dbScenario;
    } else if (inlineScenario) {
      scenarioData = inlineScenario;
    } else {
      return reply.status(400).send({ error: 'Either scenarioId or scenario must be provided' });
    }

    // Create run record
    const [run] = await db
      .insert(simulationRuns)
      .values({
        tenantId,
        scenarioId: scenarioId || null,
        name: name || scenarioData.name,
        description: description || scenarioData.description,
        runType: 'manual',
        scenarioSnapshot: scenarioData,
        status: 'pending',
        triggeredBy: request.headers['x-user-id'] as string,
        metadata,
      })
      .returning({
        id: simulationRuns.id,
        status: simulationRuns.status,
      });

    // Queue the simulation job
    await simulationQueue.add('run-simulation', {
      runId: run.id,
      tenantId,
      scenario: scenarioData,
    }, {
      jobId: run.id,
    });

    reply.status(202).send({
      runId: run.id,
      status: run.status,
      message: 'Simulation queued for execution',
    });
  });

  /**
   * GET /v1/simulations/runs/:id
   * Get simulation run status and results
   */
  server.get<{ Params: { id: string } }>('/runs/:id', {
    schema: {
      description: 'Get simulation run status and results',
      tags: ['simulations'],
      params: {
        type: 'object',
        required: ['id'],
        properties: {
          id: { type: 'string', format: 'uuid' },
        },
      },
      response: {
        200: {
          type: 'object',
        },
        404: {
          type: 'object',
          properties: {
            error: { type: 'string' },
          },
        },
      },
    },
  }, async (request, reply) => {
    const tenantId = getTenantId(request);
    const { id } = request.params;

    const [run] = await db
      .select()
      .from(simulationRuns)
      .where(and(
        eq(simulationRuns.id, id),
        eq(simulationRuns.tenantId, tenantId)
      ))
      .limit(1);

    if (!run) {
      return reply.status(404).send({ error: 'Run not found' });
    }

    reply.send(run);
  });

  /**
   * GET /v1/simulations/runs
   * List simulation runs
   */
  server.get('/runs', {
    schema: {
      description: 'List simulation runs',
      tags: ['simulations'],
      querystring: {
        type: 'object',
        properties: {
          scenarioId: { type: 'string', format: 'uuid' },
          status: { type: 'string', enum: ['pending', 'running', 'completed', 'failed', 'cancelled'] },
          runType: { type: 'string', enum: ['manual', 'scheduled', 'ci'] },
          limit: { type: 'integer', minimum: 1, maximum: 100, default: 20 },
          offset: { type: 'integer', minimum: 0, default: 0 },
        },
      },
      response: {
        200: {
          type: 'object',
          properties: {
            runs: {
              type: 'array',
            },
            total: { type: 'integer' },
            limit: { type: 'integer' },
            offset: { type: 'integer' },
          },
        },
      },
    },
  }, async (request, reply) => {
    const tenantId = getTenantId(request);
    const { scenarioId, status, runType, limit = 20, offset = 0 } = request.query as any;

    // Build query conditions
    const conditions = [eq(simulationRuns.tenantId, tenantId)];
    if (scenarioId) {
      conditions.push(eq(simulationRuns.scenarioId, scenarioId));
    }
    if (status) {
      conditions.push(eq(simulationRuns.status, status));
    }
    if (runType) {
      conditions.push(eq(simulationRuns.runType, runType));
    }

    // Get total count
    const [{ count }] = await db
      .select({ count: sql<number>`count(*)` })
      .from(simulationRuns)
      .where(and(...conditions));

    // Get runs
    const runs = await db
      .select()
      .from(simulationRuns)
      .where(and(...conditions))
      .orderBy(desc(simulationRuns.createdAt))
      .limit(limit)
      .offset(offset);

    reply.send({
      runs,
      total: Number(count),
      limit,
      offset,
    });
  });

  /**
   * POST /v1/simulations/batch
   * Run multiple scenarios in batch
   */
  server.post('/batch', {
    schema: {
      description: 'Run multiple simulation scenarios in batch',
      tags: ['simulations'],
      body: {
        type: 'object',
        required: ['name', 'scenarioIds'],
      },
      response: {
        202: {
          type: 'object',
          properties: {
            batchId: { type: 'string' },
            totalRuns: { type: 'integer' },
            message: { type: 'string' },
          },
        },
      },
    },
  }, async (request, reply) => {
    const tenantId = getTenantId(request);
    const validationResult = BatchRunSchema.safeParse(request.body);

    if (!validationResult.success) {
      return reply.status(400).send({
        error: 'Invalid request body',
        details: validationResult.error.errors,
      });
    }

    const { name, description, scenarioIds } = validationResult.data;

    // Verify all scenarios exist and belong to tenant
    const scenarios = await db
      .select()
      .from(simulationScenarios)
      .where(and(
        eq(simulationScenarios.tenantId, tenantId),
        sql`${simulationScenarios.id} = ANY(${scenarioIds})`
      ));

    if (scenarios.length !== scenarioIds.length) {
      return reply.status(400).send({
        error: 'Some scenarios not found or not accessible',
      });
    }

    // Create batch record
    const [batch] = await db
      .insert(simulationBatches)
      .values({
        tenantId,
        name,
        description,
        scenarioIds,
        totalRuns: scenarioIds.length,
        status: 'pending',
        createdBy: request.headers['x-user-id'] as string,
      })
      .returning({
        id: simulationBatches.id,
        totalRuns: simulationBatches.totalRuns,
      });

    // Create individual runs and queue them
    const runJobs = [];
    for (const scenario of scenarios) {
      const [run] = await db
        .insert(simulationRuns)
        .values({
          tenantId,
          scenarioId: scenario.id,
          name: `${batch.id}-${scenario.name}`,
          description: `Part of batch: ${name}`,
          runType: 'manual',
          scenarioSnapshot: scenario,
          status: 'pending',
          triggeredBy: request.headers['x-user-id'] as string,
          metadata: { batchId: batch.id },
        })
        .returning({
          id: simulationRuns.id,
        });

      runJobs.push({
        name: 'run-simulation',
        data: {
          runId: run.id,
          tenantId,
          scenario,
          batchId: batch.id,
        },
        opts: {
          jobId: run.id,
        },
      });
    }

    // Queue all jobs
    await simulationQueue.addBulk(runJobs);

    // Update batch status to running
    await db
      .update(simulationBatches)
      .set({
        status: 'running',
        startedAt: new Date(),
      })
      .where(eq(simulationBatches.id, batch.id));

    reply.status(202).send({
      batchId: batch.id,
      totalRuns: batch.totalRuns,
      message: `Batch simulation with ${batch.totalRuns} runs queued for execution`,
    });
  });

  /**
   * GET /v1/simulations/batch/:id
   * Get batch run status
   */
  server.get<{ Params: { id: string } }>('/batch/:id', {
    schema: {
      description: 'Get batch simulation status',
      tags: ['simulations'],
      params: {
        type: 'object',
        required: ['id'],
        properties: {
          id: { type: 'string', format: 'uuid' },
        },
      },
      response: {
        200: {
          type: 'object',
        },
        404: {
          type: 'object',
          properties: {
            error: { type: 'string' },
          },
        },
      },
    },
  }, async (request, reply) => {
    const tenantId = getTenantId(request);
    const { id } = request.params;

    const [batch] = await db
      .select()
      .from(simulationBatches)
      .where(and(
        eq(simulationBatches.id, id),
        eq(simulationBatches.tenantId, tenantId)
      ))
      .limit(1);

    if (!batch) {
      return reply.status(404).send({ error: 'Batch not found' });
    }

    // Get run statuses
    const runs = await db
      .select({
        id: simulationRuns.id,
        status: simulationRuns.status,
        scenarioId: simulationRuns.scenarioId,
        passed: simulationRuns.passed,
        error: simulationRuns.error,
      })
      .from(simulationRuns)
      .where(and(
        eq(simulationRuns.tenantId, tenantId),
        sql`${simulationRuns.metadata}->>'batchId' = ${id}`
      ));

    reply.send({
      ...batch,
      runs,
    });
  });
};

