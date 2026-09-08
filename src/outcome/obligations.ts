import { isDeepStrictEqual } from 'node:util'
import { requestSnapshot } from '../app/jobs.js'
import type { SqlQueryable } from '../control-plane/pg-store.js'
import type { KernelArtifact, WorkItem } from '../protocol/types.js'
import type { ToolDefinition } from '../tools/catalog.js'
import { candidateActions } from './action-check.js'
import type { Candidate, VerificationRecord } from './verification.js'

/** Trusted product intent. A model's checklist cannot create, relax or satisfy these obligations. */
export type DeliveryObligation = { id: string; description?: string } & (
  | { kind: 'external-delivery'; action: string; args: Record<string, unknown>; expected?: unknown }
  | { kind: 'artifact'; path: string }
  | { kind: 'answer-content' | 'resource-postcondition'; checker: string }
  | { kind: 'delegation'; taskId: string })

export function snapshotObligations(value: unknown): DeliveryObligation[] {
  if (!Array.isArray(value) || value.length > 32 || Buffer.byteLength(JSON.stringify(value)) > 64_000) throw new Error('invalid delivery obligations')
  const ids = new Set<string>()
  for (const item of value) {
    if (!item || typeof item !== 'object' || Array.isArray(item) || typeof item.id !== 'string' || !/^[A-Za-z0-9._-]{1,128}$/.test(item.id)
      || ids.has(item.id) || item.description !== undefined && (typeof item.description !== 'string' || item.description.length > 2000)) throw new Error('invalid delivery obligation')
    ids.add(item.id)
    let fields: string[]
    switch (item.kind) {
      case 'external-delivery':
        fields = ['action', 'args', 'expected']
        if (typeof item.action !== 'string' || !/^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$/.test(item.action) || item.action.startsWith('task.')
          || !item.args || typeof item.args !== 'object' || Array.isArray(item.args)) throw new Error('invalid action obligation')
        break
      case 'artifact':
        fields = ['path']
        if (typeof item.path !== 'string' || !item.path || item.path.length > 4096) throw new Error('invalid artifact obligation')
        break
      case 'answer-content':
      case 'resource-postcondition':
        fields = ['checker']
        if (typeof item.checker !== 'string' || !/^product:[A-Za-z0-9._:-]{1,128}$/.test(item.checker)) throw new Error('obligation requires a native product checker')
        break
      case 'delegation':
        fields = ['taskId']
        if (typeof item.taskId !== 'string' || !item.taskId || item.taskId.length > 1000) throw new Error('invalid delegation obligation')
        break
      default: throw new Error('unknown delivery obligation kind')
    }
    if (Object.keys(item).some(key => !['id', 'description', 'kind', ...fields].includes(key))) throw new Error('unknown obligation field')
  }
  return structuredClone(value) as DeliveryObligation[]
}

export async function inspectObligations(database: SqlQueryable, work: Omit<WorkItem, 'leaseToken'>, candidate: Candidate,
  tools: readonly ToolDefinition[], checks: readonly VerificationRecord[]): Promise<VerificationRecord[]> {
  const request = await requestSnapshot(database, work.id, candidate.requestVersion)
  const obligations = snapshotObligations(request.obligations ?? [])
  if (!obligations.length) return []
  const actions = obligations.some(item => item.kind === 'external-delivery') ? await candidateActions(database, work.id, candidate.requestVersion, tools) : []
  const records: VerificationRecord[] = []
  for (const obligation of obligations) {
    let passed = false
    switch (obligation.kind) {
      case 'external-delivery':
        passed = actions.length <= 1024 && actions.some(item => item.action.action === obligation.action && isDeepStrictEqual(item.action.args, obligation.args)
          && item.result?.ok && !item.result.approval && !item.result.directive && item.result.executionState !== 'unknown'
          && (obligation.expected === undefined || isDeepStrictEqual(obligation.expected, item.result.value)))
        break
      case 'artifact':
        passed = candidate.artifacts.some((artifact: KernelArtifact) => artifact.path === obligation.path)
          && checks.some(check => check.checker === `artifact:${obligation.path}` && check.status === 'passed')
        break
      case 'answer-content':
      case 'resource-postcondition':
        passed = checks.some(check => check.checker === obligation.checker && check.status === 'passed')
        break
      case 'delegation': {
        const child = await database.query(`SELECT 1 FROM lingxios.agent_work_items child JOIN lingxios.agent_results result ON result.id=child.result_id
          WHERE child.id=$1 AND child.meta->>'parentWorkId'=$2 AND (child.meta->>'parentRequestVersion')::integer=$3
            AND child.tenant_id=$4 AND child.principal_id IS NOT DISTINCT FROM $5 AND child.status='succeeded'
            AND child.cancel_requested_at IS NULL AND result.request_version=jsonb_array_length(child.steer_inputs)+1
            AND result.message->'envelope'->'goalOutcome'->>'status'='satisfied' AND length(result.message->>'body')>0`,
        [obligation.taskId, work.id, candidate.requestVersion, work.tenantId, work.principalId ?? null])
        passed = child.rows.length === 1
        break
      }
    }
    records.push({ checker: `obligation:${obligation.id}`, status: passed ? 'passed' : 'inconclusive',
      evidence: { kind: obligation.kind, ...(passed ? {} : { reason: 'Required delivery has no matching current authoritative evidence' }) } })
  }
  return records
}
