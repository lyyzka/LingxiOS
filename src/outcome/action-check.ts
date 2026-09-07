import type { SqlQueryable } from '../control-plane/pg-store.js'
import type { ActionExecutor } from '../control-plane/stores.js'
import type { HostAction, HostActionResult, WorkItem } from '../protocol/types.js'
import type { ToolDefinition } from '../tools/catalog.js'
import type { VerificationRecord } from './verification.js'

export async function candidateActions(database: SqlQueryable, workId: string, requestVersion: number,
  tools: readonly ToolDefinition[]) {
  const { rows } = await database.query(`SELECT intent.idempotency_key,intent.intent->'action' AS action,
      COALESCE(resolved.result,receipt.result) AS result
    FROM lingxios.agent_action_intents intent
    LEFT JOIN lingxios.agent_action_ledger receipt USING(idempotency_key)
    LEFT JOIN LATERAL (SELECT resolution->'result' AS result FROM lingxios.agent_action_resolutions
      WHERE idempotency_key=intent.idempotency_key ORDER BY resolution_seq DESC LIMIT 1) resolved ON TRUE
    WHERE intent.intent->>'workId'=$1 AND (intent.intent->>'requestVersion')::integer=$2
      AND intent.intent->'action'->>'action' NOT LIKE 'task.%'
      AND NOT (intent.intent->'action'->>'action'=ANY($3::text[]))
    ORDER BY intent.recorded_at,intent.idempotency_key LIMIT 1025`,
  [workId,requestVersion,tools.filter(tool => tool.effect === 'read').map(tool => tool.action)])
  return rows.map(row => ({ key: String(row['idempotency_key']), action: row['action'] as HostAction,
    result: row['result'] as HostActionResult | null }))
}

export async function inspectActions(database: SqlQueryable, work: Omit<WorkItem, 'leaseToken'>,
  requestVersion: number, tools: readonly ToolDefinition[], executor?: ActionExecutor): Promise<VerificationRecord[]> {
  const actions = await candidateActions(database, work.id, requestVersion, tools)
  if (actions.length > 1024) return [{ checker: 'actions', status: 'inconclusive', evidence: { reason: 'More than 1024 write actions require verification' } }]
  const records: VerificationRecord[] = []
  for (const { key, action, result } of actions) {
    const checker = `action:${key}`
    if (!result || result.executionState === 'unknown' || result.approval) {
      records.push({ checker, status: 'inconclusive', evidence: { action: action.action, reason: 'Effect requires reconciliation or approval' } })
    } else if (!result.ok) {
      records.push({ checker, status: 'passed', evidence: { action: action.action, scope: 'confirmed_failed_action_without_effect' } })
    } else if (!executor?.verifyResult) {
      records.push({ checker, status: 'inconclusive', evidence: { action: action.action, reason: 'Authoritative business readback is unavailable' } })
    } else {
      try { records.push({ checker, ...await executor.verifyResult(work, action, result.value) }) }
      catch { records.push({ checker, status: 'inconclusive', evidence: { action: action.action, reason: 'Current authorized resource readback failed' } }) }
    }
  }
  for (let index = 0; index < records.length; index++) {
    const record = records[index]!, resource = record.evidence['resource']
    if (record.status === 'passed' || typeof resource !== 'string') continue
    const missing = record.evidence['mismatches'] as string[] | undefined
    if (!missing?.length) continue
    const replacement = records.slice(index + 1).find(later => later.status === 'passed' && later.evidence['resource'] === resource
      && Array.isArray(later.evidence['fields']) && (later.evidence['fields'].includes('deleted')
        || missing.every(field => (later.evidence['fields'] as unknown[]).includes(field))))
    if (replacement) {
      record.status = 'passed'
      record.evidence = { resource, scope: 'successful_receipt_with_superseded_postconditions', supersededBy: replacement.checker }
    }
  }
  return records
}
