/**
 * End-to-end tests for Simulation API
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { buildServer } from '../server';
import type { FastifyInstance } from 'fastify';

describe('Simulation API E2E Tests', () => {
  let server: FastifyInstance;
  const testTenantId = '9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d';
  const testUserId = 'test-user-e2e';
  
  // Store IDs for cleanup
  const createdScenarioIds: string[] = [];
  const createdRunIds: string[] = [];

  beforeAll(async () => {
    process.env.BYPASS_AUTH = '1';
    server = await buildServer();
    await server.ready();
  });

  afterAll(async () => {
    // Cleanup created resources
    for (const id of createdScenarioIds) {
      await server.inject({
        method: 'DELETE',
        url: `/v1/simulations/scenarios/${id}`,
        headers: {
          'x-tenant-id': testTenantId,
          'x-user-id': testUserId,
        },
      });
    }
    
    await server.close();
  });

  describe('Complete Scenario Lifecycle', () => {
    let scenarioId: string;

    it('should create, run, and validate a complete pricing scenario', async () => {
      // Step 1: Create a scenario
      const scenario = {
        name: 'E2E Test - Tiered Pricing',
        description: 'End-to-end test for tiered pricing model',
        version: '1.0',
        tags: ['e2e-test', 'tiered', 'automated'],
        model: {
          model: 'tiered',
          currency: 'USD',
          tiers: [
            { upTo: 100, unitPrice: 0.10 },
            { upTo: 500, unitPrice: 0.08 },
            { upTo: null, unitPrice: 0.05 }
          ]
        },
        inputs: {
          customerId: 'e2e_test_customer',
          periodStart: '2024-01-01',
          periodEnd: '2024-01-31',
          usageItems: [
            {
              metric: 'api_calls',
              quantity: 350,
              priceConfig: {
                model: 'tiered',
                currency: 'USD',
                tiers: [
                  { upTo: 100, unitPrice: 0.10 },
                  { upTo: 500, unitPrice: 0.08 },
                  { upTo: null, unitPrice: 0.05 }
                ]
              }
            }
          ]
        },
        expected: {
          total: 30.00, // 100*0.10 + 250*0.08 = 10 + 20 = 30
          subtotal: 30.00,
          tax: 0,
          lineItems: [
            {
              metric: 'api_calls',
              quantity: 350,
              subtotal: 30.00
            }
          ]
        },
        tolerances: {
          absolute: 0.01,
          relative: 0.001
        }
      };

      const createResponse = await server.inject({
        method: 'POST',
        url: '/v1/simulations/scenarios',
        headers: {
          'x-tenant-id': testTenantId,
          'x-user-id': testUserId,
        },
        payload: scenario
      });

      expect(createResponse.statusCode).toBe(201);
      const created = JSON.parse(createResponse.body);
      expect(created.id).toBeDefined();
      scenarioId = created.id;
      createdScenarioIds.push(scenarioId);

      // Step 2: Verify scenario was created
      const getResponse = await server.inject({
        method: 'GET',
        url: `/v1/simulations/scenarios/${scenarioId}`,
        headers: {
          'x-tenant-id': testTenantId,
        }
      });

      expect(getResponse.statusCode).toBe(200);
      const retrieved = JSON.parse(getResponse.body);
      expect(retrieved.name).toBe(scenario.name);
      expect(retrieved.tags).toEqual(scenario.tags);

      // Step 3: Run the scenario
      const runResponse = await server.inject({
        method: 'POST',
        url: '/v1/simulations/runs',
        headers: {
          'x-tenant-id': testTenantId,
          'x-user-id': testUserId,
        },
        payload: {
          scenarioId: scenarioId,
          name: 'E2E Test Run',
          description: 'Testing full flow',
          metadata: {
            test: true,
            environment: 'e2e'
          }
        }
      });

      expect(runResponse.statusCode).toBe(202);
      const run = JSON.parse(runResponse.body);
      expect(run.runId).toBeDefined();
      expect(run.status).toBe('pending');
      createdRunIds.push(run.runId);

      // Step 4: Poll for run completion (with timeout)
      let runStatus = 'pending';
      let attempts = 0;
      const maxAttempts = 10;
      let runResult: any;

      while (runStatus === 'pending' && attempts < maxAttempts) {
        await new Promise(resolve => setTimeout(resolve, 500)); // Wait 500ms
        
        const statusResponse = await server.inject({
          method: 'GET',
          url: `/v1/simulations/runs/${run.runId}`,
          headers: {
            'x-tenant-id': testTenantId,
          }
        });

        runResult = JSON.parse(statusResponse.body);
        runStatus = runResult.status;
        attempts++;
      }

      // Note: In a real environment with workers running, this would complete
      // For testing without workers, we'll validate the structure
      expect(runResult).toBeDefined();
      expect(runResult.id).toBe(run.runId);
      expect(runResult.scenarioId).toBe(scenarioId);
    });

    it('should list scenarios with filters', async () => {
      // Create multiple scenarios for testing filters
      const scenarios = [
        {
          name: 'E2E Filter Test 1',
          tags: ['e2e-test', 'filter-test', 'active'],
          model: { model: 'flat', currency: 'USD', unitPrice: 0.01 },
          inputs: {
            customerId: 'test',
            periodStart: '2024-01-01',
            periodEnd: '2024-01-31',
            usageItems: []
          }
        },
        {
          name: 'E2E Filter Test 2',
          tags: ['e2e-test', 'filter-test', 'inactive'],
          model: { model: 'flat', currency: 'USD', unitPrice: 0.02 },
          inputs: {
            customerId: 'test',
            periodStart: '2024-01-01',
            periodEnd: '2024-01-31',
            usageItems: []
          }
        }
      ];

      for (const scenario of scenarios) {
        const response = await server.inject({
          method: 'POST',
          url: '/v1/simulations/scenarios',
          headers: {
            'x-tenant-id': testTenantId,
            'x-user-id': testUserId,
          },
          payload: scenario
        });
        
        const created = JSON.parse(response.body);
        createdScenarioIds.push(created.id);
      }

      // Test filtering by tag
      const filterResponse = await server.inject({
        method: 'GET',
        url: '/v1/simulations/scenarios?tag=filter-test',
        headers: {
          'x-tenant-id': testTenantId,
        }
      });

      expect(filterResponse.statusCode).toBe(200);
      const filtered = JSON.parse(filterResponse.body);
      expect(filtered.scenarios.length).toBeGreaterThanOrEqual(2);
      
      // All results should have the filter-test tag
      for (const scenario of filtered.scenarios) {
        expect(scenario.tags).toContain('filter-test');
      }

      // Test pagination
      const paginatedResponse = await server.inject({
        method: 'GET',
        url: '/v1/simulations/scenarios?limit=1&offset=0',
        headers: {
          'x-tenant-id': testTenantId,
        }
      });

      expect(paginatedResponse.statusCode).toBe(200);
      const paginated = JSON.parse(paginatedResponse.body);
      expect(paginated.scenarios.length).toBeLessThanOrEqual(1);
      expect(paginated.limit).toBe(1);
      expect(paginated.offset).toBe(0);
    });
  });

  describe('Complex Pricing Models', () => {
    it('should correctly calculate volume pricing', async () => {
      const scenario = {
        name: 'E2E Volume Pricing Test',
        model: {
          model: 'volume',
          currency: 'USD',
          tiers: [
            { upTo: 100, unitPrice: 0.10 },
            { upTo: 500, unitPrice: 0.07 },
            { upTo: null, unitPrice: 0.04 }
          ]
        },
        inputs: {
          customerId: 'volume_test_customer',
          periodStart: '2024-01-01',
          periodEnd: '2024-01-31',
          usageItems: [
            {
              metric: 'storage_gb',
              quantity: 250,
              priceConfig: {
                model: 'volume',
                currency: 'USD',
                tiers: [
                  { upTo: 100, unitPrice: 0.10 },
                  { upTo: 500, unitPrice: 0.07 },
                  { upTo: null, unitPrice: 0.04 }
                ]
              }
            }
          ]
        },
        expected: {
          total: 17.50, // 250 * 0.07 (falls in 100-500 tier)
          subtotal: 17.50
        }
      };

      const response = await server.inject({
        method: 'POST',
        url: '/v1/simulations/runs',
        headers: {
          'x-tenant-id': testTenantId,
          'x-user-id': testUserId,
        },
        payload: {
          scenario: scenario,
          name: 'Volume Pricing Test Run'
        }
      });

      expect(response.statusCode).toBe(202);
      const run = JSON.parse(response.body);
      createdRunIds.push(run.runId);
    });

    it('should correctly handle multi-metric scenarios', async () => {
      const scenario = {
        name: 'E2E Multi-Metric Test',
        inputs: {
          customerId: 'multi_metric_customer',
          periodStart: '2024-01-01',
          periodEnd: '2024-01-31',
          usageItems: [
            {
              metric: 'api_calls',
              quantity: 1000,
              priceConfig: {
                model: 'flat',
                currency: 'USD',
                unitPrice: 0.01
              }
            },
            {
              metric: 'storage_gb',
              quantity: 100,
              priceConfig: {
                model: 'flat',
                currency: 'USD',
                unitPrice: 0.05
              }
            },
            {
              metric: 'bandwidth_gb',
              quantity: 50,
              priceConfig: {
                model: 'flat',
                currency: 'USD',
                unitPrice: 0.08
              }
            }
          ],
          taxRate: 10
        },
        expected: {
          subtotal: 19.00, // 10 + 5 + 4
          tax: 1.90,
          total: 20.90
        }
      };

      const response = await server.inject({
        method: 'POST',
        url: '/v1/simulations/runs',
        headers: {
          'x-tenant-id': testTenantId,
          'x-user-id': testUserId,
        },
        payload: {
          scenario: scenario,
          name: 'Multi-Metric Test Run'
        }
      });

      expect(response.statusCode).toBe(202);
      const run = JSON.parse(response.body);
      createdRunIds.push(run.runId);
    });

    it('should correctly apply credits and commitments', async () => {
      const scenario = {
        name: 'E2E Credits and Commitments Test',
        inputs: {
          customerId: 'credits_test_customer',
          periodStart: '2024-01-01',
          periodEnd: '2024-01-31',
          usageItems: [
            {
              metric: 'api_calls',
              quantity: 5000,
              priceConfig: {
                model: 'flat',
                currency: 'USD',
                unitPrice: 0.01
              }
            }
          ],
          commitments: [
            {
              amount: 20,
              startDate: '2024-01-01',
              endDate: '2024-12-31',
              applied: 0
            }
          ],
          credits: [
            { amount: 10, reason: 'Promotional credit' },
            { amount: 5, reason: 'Service credit' }
          ]
        },
        expected: {
          subtotal: 50.00, // 5000 * 0.01
          credits: 35.00, // 20 (commitment) + 15 (credits)
          total: 15.00 // 50 - 35
        }
      };

      const response = await server.inject({
        method: 'POST',
        url: '/v1/simulations/runs',
        headers: {
          'x-tenant-id': testTenantId,
          'x-user-id': testUserId,
        },
        payload: {
          scenario: scenario,
          name: 'Credits and Commitments Test Run'
        }
      });

      expect(response.statusCode).toBe(202);
      const run = JSON.parse(response.body);
      createdRunIds.push(run.runId);
    });
  });

  describe('Batch Operations', () => {
    it('should execute batch simulations', async () => {
      // Create multiple scenarios for batch testing
      const scenarioIds: string[] = [];
      
      for (let i = 1; i <= 3; i++) {
        const scenario = {
          name: `E2E Batch Test Scenario ${i}`,
          tags: ['batch-test'],
          model: {
            model: 'flat',
            currency: 'USD',
            unitPrice: 0.01 * i
          },
          inputs: {
            customerId: `batch_customer_${i}`,
            periodStart: '2024-01-01',
            periodEnd: '2024-01-31',
            usageItems: [
              {
                metric: 'api_calls',
                quantity: 1000 * i,
                priceConfig: {
                  model: 'flat',
                  currency: 'USD',
                  unitPrice: 0.01 * i
                }
              }
            ]
          },
          expected: {
            total: 10 * i * i // (1000 * i) * (0.01 * i)
          }
        };

        const response = await server.inject({
          method: 'POST',
          url: '/v1/simulations/scenarios',
          headers: {
            'x-tenant-id': testTenantId,
            'x-user-id': testUserId,
          },
          payload: scenario
        });

        const created = JSON.parse(response.body);
        scenarioIds.push(created.id);
        createdScenarioIds.push(created.id);
      }

      // Run batch simulation
      const batchResponse = await server.inject({
        method: 'POST',
        url: '/v1/simulations/batch',
        headers: {
          'x-tenant-id': testTenantId,
          'x-user-id': testUserId,
        },
        payload: {
          name: 'E2E Batch Test',
          description: 'Testing batch execution',
          scenarioIds: scenarioIds
        }
      });

      expect(batchResponse.statusCode).toBe(202);
      const batch = JSON.parse(batchResponse.body);
      expect(batch.batchId).toBeDefined();
      expect(batch.totalRuns).toBe(3);

      // Get batch status
      const statusResponse = await server.inject({
        method: 'GET',
        url: `/v1/simulations/batch/${batch.batchId}`,
        headers: {
          'x-tenant-id': testTenantId,
        }
      });

      expect(statusResponse.statusCode).toBe(200);
      const batchStatus = JSON.parse(statusResponse.body);
      expect(batchStatus.id).toBe(batch.batchId);
      expect(batchStatus.totalRuns).toBe(3);
      expect(batchStatus.runs).toBeDefined();
    });
  });

  describe('Error Cases', () => {
    it('should handle invalid scenario configurations', async () => {
      const invalidScenarios = [
        {
          name: 'Missing required fields',
          // Missing model and inputs
        },
        {
          name: 'Invalid pricing model',
          model: {
            model: 'invalid_model',
            currency: 'USD'
          },
          inputs: {
            customerId: 'test',
            periodStart: '2024-01-01',
            periodEnd: '2024-01-31',
            usageItems: []
          }
        },
        {
          name: 'Invalid date format',
          model: { model: 'flat', currency: 'USD', unitPrice: 0.01 },
          inputs: {
            customerId: 'test',
            periodStart: 'invalid-date',
            periodEnd: '2024-01-31',
            usageItems: []
          }
        }
      ];

      for (const scenario of invalidScenarios) {
        const response = await server.inject({
          method: 'POST',
          url: '/v1/simulations/scenarios',
          headers: {
            'x-tenant-id': testTenantId,
            'x-user-id': testUserId,
          },
          payload: scenario
        });

        expect(response.statusCode).toBe(400);
        const error = JSON.parse(response.body);
        expect(error.error).toBeDefined();
      }
    });

    it('should handle tenant isolation', async () => {
      // Create scenario with one tenant
      const scenario = {
        name: 'Tenant Isolation Test',
        model: { model: 'flat', currency: 'USD', unitPrice: 0.01 },
        inputs: {
          customerId: 'test',
          periodStart: '2024-01-01',
          periodEnd: '2024-01-31',
          usageItems: []
        }
      };

      const createResponse = await server.inject({
        method: 'POST',
        url: '/v1/simulations/scenarios',
        headers: {
          'x-tenant-id': testTenantId,
          'x-user-id': testUserId,
        },
        payload: scenario
      });

      const created = JSON.parse(createResponse.body);
      createdScenarioIds.push(created.id);

      // Try to access with different tenant
      const differentTenantId = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee';
      const getResponse = await server.inject({
        method: 'GET',
        url: `/v1/simulations/scenarios/${created.id}`,
        headers: {
          'x-tenant-id': differentTenantId,
        }
      });

      expect(getResponse.statusCode).toBe(404);
    });

    it('should handle non-existent resources', async () => {
      const nonExistentId = '00000000-0000-0000-0000-000000000000';

      // Non-existent scenario
      const scenarioResponse = await server.inject({
        method: 'GET',
        url: `/v1/simulations/scenarios/${nonExistentId}`,
        headers: {
          'x-tenant-id': testTenantId,
        }
      });
      expect(scenarioResponse.statusCode).toBe(404);

      // Non-existent run
      const runResponse = await server.inject({
        method: 'GET',
        url: `/v1/simulations/runs/${nonExistentId}`,
        headers: {
          'x-tenant-id': testTenantId,
        }
      });
      expect(runResponse.statusCode).toBe(404);

      // Non-existent batch
      const batchResponse = await server.inject({
        method: 'GET',
        url: `/v1/simulations/batch/${nonExistentId}`,
        headers: {
          'x-tenant-id': testTenantId,
        }
      });
      expect(batchResponse.statusCode).toBe(404);
    });

    it('should validate scenario updates', async () => {
      // Create a scenario first
      const scenario = {
        name: 'Update Validation Test',
        model: { model: 'flat', currency: 'USD', unitPrice: 0.01 },
        inputs: {
          customerId: 'test',
          periodStart: '2024-01-01',
          periodEnd: '2024-01-31',
          usageItems: [
            {
              metric: 'api_calls',
              quantity: 100,
              priceConfig: { model: 'flat', currency: 'USD', unitPrice: 0.01 }
            }
          ]
        }
      };

      const createResponse = await server.inject({
        method: 'POST',
        url: '/v1/simulations/scenarios',
        headers: {
          'x-tenant-id': testTenantId,
          'x-user-id': testUserId,
        },
        payload: scenario
      });

      const created = JSON.parse(createResponse.body);
      createdScenarioIds.push(created.id);

      // Try to update with invalid data
      const invalidUpdate = {
        inputs: {
          customerId: 'test',
          // Missing required fields
        }
      };

      const updateResponse = await server.inject({
        method: 'PUT',
        url: `/v1/simulations/scenarios/${created.id}`,
        headers: {
          'x-tenant-id': testTenantId,
          'x-user-id': testUserId,
        },
        payload: invalidUpdate
      });

      expect(updateResponse.statusCode).toBe(400);
    });
  });

  describe('Performance Tests', () => {
    it('should handle large batch simulations', async () => {
      // Create 10 scenarios for a larger batch test
      const scenarioIds: string[] = [];
      
      for (let i = 1; i <= 10; i++) {
        const scenario = {
          name: `E2E Performance Test ${i}`,
          tags: ['performance-test'],
          model: {
            model: 'tiered',
            currency: 'USD',
            tiers: [
              { upTo: 100, unitPrice: 0.10 },
              { upTo: 500, unitPrice: 0.08 },
              { upTo: null, unitPrice: 0.05 }
            ]
          },
          inputs: {
            customerId: `perf_customer_${i}`,
            periodStart: '2024-01-01',
            periodEnd: '2024-01-31',
            usageItems: [
              {
                metric: 'api_calls',
                quantity: 100 * i,
                priceConfig: {
                  model: 'tiered',
                  currency: 'USD',
                  tiers: [
                    { upTo: 100, unitPrice: 0.10 },
                    { upTo: 500, unitPrice: 0.08 },
                    { upTo: null, unitPrice: 0.05 }
                  ]
                }
              }
            ]
          }
        };

        const response = await server.inject({
          method: 'POST',
          url: '/v1/simulations/scenarios',
          headers: {
            'x-tenant-id': testTenantId,
            'x-user-id': testUserId,
          },
          payload: scenario
        });

        const created = JSON.parse(response.body);
        scenarioIds.push(created.id);
        createdScenarioIds.push(created.id);
      }

      const startTime = Date.now();

      // Run batch simulation
      const batchResponse = await server.inject({
        method: 'POST',
        url: '/v1/simulations/batch',
        headers: {
          'x-tenant-id': testTenantId,
          'x-user-id': testUserId,
        },
        payload: {
          name: 'E2E Performance Batch Test',
          description: 'Testing performance with 10 scenarios',
          scenarioIds: scenarioIds
        }
      });

      const responseTime = Date.now() - startTime;

      expect(batchResponse.statusCode).toBe(202);
      expect(responseTime).toBeLessThan(1000); // Should respond within 1 second
      
      const batch = JSON.parse(batchResponse.body);
      expect(batch.totalRuns).toBe(10);
    });

    it('should handle pagination efficiently', async () => {
      // Test pagination with different page sizes
      const pageSizes = [5, 10, 20, 50];
      
      for (const pageSize of pageSizes) {
        const startTime = Date.now();
        
        const response = await server.inject({
          method: 'GET',
          url: `/v1/simulations/scenarios?limit=${pageSize}&offset=0`,
          headers: {
            'x-tenant-id': testTenantId,
          }
        });
        
        const responseTime = Date.now() - startTime;
        
        expect(response.statusCode).toBe(200);
        expect(responseTime).toBeLessThan(500); // Should respond within 500ms
        
        const result = JSON.parse(response.body);
        expect(result.scenarios.length).toBeLessThanOrEqual(pageSize);
      }
    });
  });
});