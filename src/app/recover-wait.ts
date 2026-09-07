import type { ControlPlaneService } from '../control-plane/service.js'
import type { SqlPool } from '../control-plane/pg-store.js'
import { sessionKeyOf, type HostActionResult, type WorkItem } from '../protocol/types.js'
import { RUN_SEQUENCE_SPAN } from '../protocol/constants.js'
import { boundedToolOutput } from '../runtime/tool.js'
import { isDeepStrictEqual } from 'node:util'
import type { GoalOutcome } from '../protocol/outcome.js'

/** Durable terminal receipts recover a wait without rerunning Python or any business action. */
export async function recoverWait(database: SqlPool, service: ControlPlaneService, work: WorkItem): Promise<boolean> {
  const session = await service.getSession(work, sessionKeyOf(work))
  if (!session?.request || session.request.workId !== work.id) return false
  const current = await service.heartbeat(work)
  if (!current.ok || current.cancelRequested || !isDeepStrictEqual(current.steer ?? [], session.request.revisions)) return false
  const requestVersion = session.request.revisions.length + 1
  const calls = session.history.filter(item => 'type' in item && item.type === 'function_call')
  const call = calls.at(-1)
  if (!call || !('type' in call) || call.type !== 'function_call') return false
  const { rows } = await database.query(`SELECT intent.intent->'action' AS action,COALESCE(resolved.result,receipt.result) AS result
    FROM lingxios.agent_action_intents intent LEFT JOIN lingxios.agent_action_ledger receipt USING(idempotency_key)
    LEFT JOIN LATERAL (SELECT resolution->'result' AS result FROM lingxios.agent_action_resolutions
      WHERE idempotency_key=intent.idempotency_key ORDER BY resolution_seq DESC LIMIT 1) resolved ON TRUE
    WHERE intent.intent->>'workId'=$1 AND intent.intent->>'tenantId'=$2 AND intent.intent->>'agentId'=$3
      AND intent.intent->>'sessionId'=$4 AND intent.intent->>'principalId' IS NOT DISTINCT FROM $5
      AND intent.intent->>'threadId' IS NOT DISTINCT FROM $6
      AND intent.intent->'action'->>'cellId'=$7 AND (intent.intent->>'requestVersion')::integer=$8
    ORDER BY (intent.intent->'action'->>'callIndex')::integer LIMIT 101`,
  [work.id, work.tenantId, work.agentId, work.sessionId, work.principalId ?? null, work.threadId ?? null, call.stepId ?? call.callId, requestVersion])
  if (!rows.length) return false
  if (rows.length > 100 || rows.some(row => !row['result'])) {
    const goalOutcome: GoalOutcome = { status: 'blocked', verification: 'inconclusive', requestVersion,
      gaps: [rows.length > 100 ? 'Action recovery exceeds the bounded receipt limit' : 'An action intent has no receipt; reconciliation is required'] }
    await service.recordEvent(work, { runId: work.id, seq: (work.fence - 1) * RUN_SEQUENCE_SPAN + 1,
      kind: 'run.completed', stage: 'completed', visibility: 'user', data: { recovered: true, goalOutcome } })
    await service.complete(work, { status: 'completed', goalOutcome })
    return true
  }
  const receipts = rows.map(row => ({ action: row['action'] as { action: string; args: Record<string, unknown>; callIndex: number; idempotencyKey: string }, result: row['result'] as HostActionResult }))
  if (receipts.some((receipt, index) => receipt.action.callIndex !== index)) return false
  const last = receipts.at(-1)!
  const question = last.action.args['question']
  let goalOutcome: GoalOutcome
  if (!last.result.ok && last.result.executionState !== 'unknown' && last.result.approval?.status === 'PENDING'
    && typeof last.result.approval.id === 'string' && last.result.approval.id.trim()) {
    goalOutcome = { status: 'awaiting_approval', approvalId: last.result.approval.id, verification: 'not_run', requestVersion }
  } else if (last.action.action === 'task.ask' && last.result.ok && last.result.directive?.type === 'defer'
    && last.result.directive.reason === 'user' && typeof question === 'string' && question.trim() && question.length <= 4000
    && last.result.directive.data?.['question'] === question) {
    goalOutcome = { status: 'awaiting_input', verification: 'not_run', requestVersion, question }
  } else return false
  const callIndex = session.history.lastIndexOf(call)
  if (!session.history.slice(callIndex + 1).some(item => 'type' in item && item.type === 'function_call_output' && item.callId === call.callId)) {
    session.history.push({ type: 'function_call_output', callId: call.callId, output: boundedToolOutput({
      recovered: true, localExecutionOutput: 'not_recovered',
      ...(goalOutcome.status === 'awaiting_approval' ? { approvalPending: goalOutcome.approvalId } : {}),
      receipts: receipts.map(receipt => ({ action: receipt.action.action, idempotencyKey: receipt.action.idempotencyKey, result: receipt.result })),
    }) })
    await service.saveSession(work, session)
  }
  await service.recordEvent(work, { runId: work.id, seq: (work.fence - 1) * RUN_SEQUENCE_SPAN + 1,
    kind: goalOutcome.status === 'awaiting_approval' ? 'approval.pending' : 'goal.waiting', stage: 'completed', visibility: 'user',
    data: { recovered: true, goalOutcome, ...(goalOutcome.status === 'awaiting_approval' ? { approvalId: goalOutcome.approvalId, cellId: call.callId } : {}) } })
  await service.complete(work, { status: 'completed', goalOutcome })
  return true
}
