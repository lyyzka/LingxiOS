export { summarizeCalibration } from './calibration.js'
export { reviewAnswer } from './review.js'
import type { createLingxiOS, RequestInput } from '../app/index.js'
import { isDeepStrictEqual } from 'node:util'

/** Expected values come from the original-input test case, not the agent's task plan. */
export interface ResourceExpectation {
  id: string
  resource: string
  expected: unknown
}
export interface ResourceObservation {
  resource: string
  requestVersion: number
  value: unknown
}

/** Deterministic resource checks only; this does not score answer quality or citation support. */
export function gradeResources(requestVersion: number, expectations: readonly ResourceExpectation[], observations: readonly ResourceObservation[]) {
  if (!Number.isSafeInteger(requestVersion) || requestVersion < 1) throw new Error('invalid request version')
  if (!Array.isArray(expectations) || !Array.isArray(observations)) throw new Error('resource checks must be arrays')
  if (expectations.length > 1000 || observations.length > 1000) throw new Error('too many resource checks')
  const ids = new Set<string>()
  return expectations.map(expectation => {
    if (!expectation || typeof expectation.id !== 'string' || !expectation.id.trim()
      || typeof expectation.resource !== 'string' || !expectation.resource.trim()
      || !Object.hasOwn(expectation, 'expected') || expectation.expected === undefined
      || ids.has(expectation.id)) throw new Error('invalid or duplicate resource check')
    ids.add(expectation.id)
    const matches = observations.filter(item => item && item.resource === expectation.resource && item.requestVersion === requestVersion)
    if (matches.length !== 1) return { checkId: expectation.id, status: 'not_observed' as const, reason: matches.length ? 'ambiguous resource observations' : 'no observation for this request version' }
    if (!Object.hasOwn(matches[0]!, 'value') || matches[0]!.value === undefined) return { checkId: expectation.id, status: 'not_observed' as const, reason: 'resource value was not observed' }
    return { checkId: expectation.id, status: isDeepStrictEqual(matches[0]!.value, expectation.expected) ? 'pass' as const : 'fail' as const }
  })
}

/** Run only against an isolated evaluation app/queue. This executes real work, not trace replay. */
export async function executeRequest(app: Awaited<ReturnType<typeof createLingxiOS>>, input: RequestInput) {
  const work = await app.enqueue(input)
  if (work.deduplicated) throw new Error('evaluation request already exists; use a new request identity for a fresh execution')
  const identity = { runId: work.id, tenantId: input.tenantId, agentId: input.agentId, sessionId: input.sessionId }
  const workDequeued = await app.runNext()
  const [message, outcome, externalDelivery] = await Promise.all([app.readMessage(identity), app.readOutcome(identity), app.readDelivery(identity)])
  return { mode: 'runtime_execution' as const, identity, workDequeued, message, outcome, externalDelivery,
    delivery: message ? 'observed' as const : 'not_observed' as const,
    semanticQuality: 'not_assessed' as const }
}
