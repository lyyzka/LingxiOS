import { isDeepStrictEqual } from 'node:util'
import { appendResourceCheck } from '../context/resource-checks.js'
import type { RequestSnapshot } from '../context/request.js'
import type { HostPort } from '../host/port.js'
import { actionKeyOf, type WorkItem } from '../protocol/types.js'

/** Fresh, authorized observations for the explicit checks recorded for this revision. */
export async function refreshResourceChecks(host: HostPort, work: WorkItem, request: RequestSnapshot, hop: number,
  signal: AbortSignal): Promise<{ checked: number; gaps: string[] }> {
  const requestVersion = request.revisions.length + 1
  const checks: Array<{ action: string; args: Record<string, unknown>; expected: Record<string, unknown> }> = []
  for (const record of request.resourceChecks ?? []) {
    const value = record.result.value as Record<string, unknown> | undefined
    if (value?.['requestVersion'] !== requestVersion) continue
    if (typeof value['action'] !== 'string' || !value['args'] || typeof value['args'] !== 'object'
      || Array.isArray(value['args']) || !value['expected'] || typeof value['expected'] !== 'object' || Array.isArray(value['expected'])) {
      return { checked: 0, gaps: ['A recorded resource check cannot be refreshed'] }
    }
    const check = { action: value['action'], args: value['args'] as Record<string, unknown>, expected: value['expected'] as Record<string, unknown> }
    if (!checks.some(previous => isDeepStrictEqual(previous, check))) checks.push(check)
  }
  if ((request.resourceChecks?.length ?? 0) + checks.length > 64) {
    return { checked: 0, gaps: ['Resource observation capacity is insufficient for fresh candidate checks'] }
  }
  const gaps: string[] = []
  for (const [callIndex, args] of checks.entries()) {
    signal.throwIfAborted()
    const identity = { runId: work.id, cellId: `resource-review:${work.fence}:${hop}`, callIndex }
    const action = { ...identity, idempotencyKey: actionKeyOf(identity), action: 'task.check_resource', args }
    const result = await host.executeAction(work, action)
    request.resourceChecks = appendResourceCheck(request.resourceChecks ?? [], action.idempotencyKey, result, requestVersion)
    const value = result.value as Record<string, unknown> | undefined
    if (!result.ok || result.executionState === 'unknown' || value?.['status'] !== 'pass') {
      gaps.push(`Resource check ${args.action} did not confirm the expected fields at candidate review`)
    }
  }
  return { checked: checks.length, gaps }
}
