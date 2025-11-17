import { describe, it, expect, vi } from 'vitest';
import { ReconcilerWorker } from './reconciler';

describe('ReconcilerWorker', () => {
  it('exposes triggerOnDemand without throwing when not running', async () => {
    const worker = new ReconcilerWorker();
    const runSpy = vi.spyOn<any, any>(worker as any, 'runReconciliation').mockResolvedValue(undefined);
    await worker.triggerOnDemand();
    expect(runSpy).toHaveBeenCalled();
  });
});

