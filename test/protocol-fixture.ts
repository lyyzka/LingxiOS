import { MemoryModelBudgetStore } from '../src/control-plane/memory-store.js'
import { MemoryStepStore } from '../src/control-plane/steps.js'
import type { HostPort } from '../src/host/port.js'
import type { ModelCallObservation } from '../src/model/execution.js'
import { candidateHash } from '../src/outcome/verification.js'

/** Explicit durable test port; production workers cannot omit these operations. */
export function durableProtocol(observe?: (value: ModelCallObservation) => void): Pick<HostPort,
  'reserveModelCall' | 'recordModelUsage' | 'saveStep' | 'recoverStep' | 'recoverCell' | 'verifyCandidate' | 'waitWork'> {
  const budgets = new MemoryModelBudgetStore(), steps = new MemoryStepStore()
  return {
    waitWork: async () => {},
    reserveModelCall: (work, id, limits) => budgets.reserve(String(work.meta?.['rootWorkId'] ?? work.id), id, limits),
    async recordModelUsage(work, id, usage, observation) {
      await budgets.record(String(work.meta?.['rootWorkId'] ?? work.id), id, usage.inputTokens, usage.outputTokens, usage.costMicros)
      if (observation) observe?.(observation)
    },
    saveStep: (work, step) => steps.save({ workId: work.id, fence: work.fence, leaseTokenHash: '' }, step),
    async recoverStep(work, id) {
      const step = (await steps.list(work.id)).find(step => step.id === id)
      return step?.output === undefined ? null : { output: step.output, artifacts: step.artifacts }
    },
    recoverCell: async () => null,
    verifyCandidate: async (_work, candidate) => ({ requestVersion: candidate.requestVersion, candidateHash: candidateHash(candidate), records: [] }),
  }
}
