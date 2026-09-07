import { createHash } from 'node:crypto'
import { isDeepStrictEqual } from 'node:util'
import { withTransaction, type SqlPool } from './pg-store.js'
import type { ActionContext, ToolDefinition } from '../tools/definition.js'
import { NoEffectError } from '../tools/definition.js'
import type { HostActionResult } from '../protocol/types.js'
import { sessionKeyOf, type HostAction, type WorkItem } from '../protocol/types.js'
import type { ControlPlaneService } from './service.js'
import { boundedToolOutput } from '../runtime/tool.js'

export interface ApprovalIdentity {
  approvalId: string
  tenantId: string
  agentId: string
  sessionId: string
  principalId: string
  threadId?: string
}
export interface ApprovalDecision extends ApprovalIdentity { approved: boolean }
export type ApprovalLookup = Pick<ApprovalIdentity, 'approvalId' | 'tenantId' | 'principalId'>
export interface ApprovalSnapshot extends ApprovalIdentity {
  runId: string
  requestVersion: number
  actionKey: string
  action: string
  args: Record<string, unknown>
  preview: Record<string, unknown>
  decision: boolean | null
  createdAt: string
  decidedAt: string | null
  result: HostActionResult | null
}

/** Called under the action/work locks. The preview must include all mutable resource versions. */
export async function approvalGate(tool: ToolDefinition, context: ActionContext, input: Record<string, unknown>): Promise<HostActionResult | null> {
  if (!tool.approval) return null
  const id = createHash('sha256').update(context.action.idempotencyKey).digest('hex')
  const preview = await tool.preview!(context, input)
  if (Buffer.byteLength(JSON.stringify(preview)) > 64_000) throw new NoEffectError('approval preview exceeds 64 KB')
  await context.database.query(`INSERT INTO lingxios.agent_approvals(id,action_key,preview)
    VALUES($1,$2,$3::jsonb) ON CONFLICT DO NOTHING`, [id,context.action.idempotencyKey,JSON.stringify(preview)])
  const { rows } = await context.database.query('SELECT preview,decision FROM lingxios.agent_approvals WHERE id=$1 FOR UPDATE', [id])
  const approval = rows[0]!
  if (approval['decision'] === null) return { ok: false, executionState: 'awaiting_approval', approval: { id, status: 'PENDING' }, value: { preview } }
  if (approval['decision'] === false) return { ok: false, executionState: 'no_effect', code: 'approval_rejected', error: 'The original principal rejected this action' }
  if (!isDeepStrictEqual(approval['preview'], JSON.parse(JSON.stringify(preview)))) throw new NoEffectError('approved resource changed; request a new preview', 'approval_stale')
  context.approvedPreview = structuredClone(approval['preview'] as Record<string, unknown>)
  return null
}

const scope = `work.tenant_id=$2 AND work.agent_id=$3 AND work.session_id=$4 AND work.principal_id=$5
  AND work.thread_id IS NOT DISTINCT FROM $6`
const identityParams = (input: ApprovalIdentity) => [input.approvalId,input.tenantId,input.agentId,input.sessionId,input.principalId,input.threadId ?? null]

export async function readApproval(pool: SqlPool, input: ApprovalLookup): Promise<ApprovalSnapshot | null> {
  if (!input.principalId?.trim()) throw new Error('authenticated principal is required')
  const { rows } = await pool.query(`SELECT approval.*,work.agent_id,work.session_id,work.thread_id,
      intent.intent->'action' AS action,work.id AS run_id,COALESCE(resolved.result,receipt.result) AS result,
      (intent.intent->>'requestVersion')::integer AS request_version
    FROM lingxios.agent_approvals approval JOIN lingxios.agent_action_intents intent ON intent.idempotency_key=approval.action_key
    JOIN lingxios.agent_work_items work ON work.id=intent.intent->>'workId'
    LEFT JOIN lingxios.agent_action_ledger receipt ON receipt.idempotency_key=approval.action_key
    LEFT JOIN LATERAL (SELECT resolution->'result' AS result FROM lingxios.agent_action_resolutions
      WHERE idempotency_key=approval.action_key ORDER BY resolution_seq DESC LIMIT 1) resolved ON TRUE
    WHERE approval.id=$1 AND work.tenant_id=$2 AND work.principal_id=$3`, [input.approvalId,input.tenantId,input.principalId])
  const row = rows[0]
  if (!row) return null
  const action = row['action'] as HostAction
  return { ...input, runId: String(row['run_id']), agentId: String(row['agent_id']), sessionId: String(row['session_id']),
    ...(row['thread_id'] ? { threadId: String(row['thread_id']) } : {}), requestVersion: Number(row['request_version']),
    actionKey: String(row['action_key']), action: action.action, args: action.args, preview: row['preview'] as Record<string, unknown>,
    decision: row['decision'] as boolean | null, createdAt: new Date(String(row['created_at'])).toISOString(),
    decidedAt: row['decided_at'] ? new Date(String(row['decided_at'])).toISOString() : null, result: row['result'] as HostActionResult | null }
}

/** Trusted ingress supplies the authenticated original principal; execution reauthorizes under a real worker lease. */
export async function decideApproval(pool: SqlPool, input: ApprovalDecision) {
  if (typeof input.approved !== 'boolean' || !input.principalId?.trim()) throw new Error('approval decision and authenticated principal are required')
  const decision = await withTransaction(pool, async db => {
    const { rows } = await db.query(`SELECT approval.decision,work.id,work.cancel_requested_at,
        jsonb_array_length(work.steer_inputs)+1 AS current_version,(intent.intent->>'requestVersion')::integer AS approved_version
      FROM lingxios.agent_approvals approval JOIN lingxios.agent_action_intents intent ON intent.idempotency_key=approval.action_key
      JOIN lingxios.agent_work_items work ON work.id=intent.intent->>'workId'
      WHERE approval.id=$1 AND ${scope} FOR UPDATE OF work,approval`, identityParams(input))
    const row = rows[0]
    if (!row) throw new Error('approval is outside this principal and session')
    if (row['cancel_requested_at'] || row['current_version'] !== row['approved_version']) throw new Error('approval belongs to a cancelled or revised request')
    if (row['decision'] !== null && row['decision'] !== input.approved) throw new Error('approval already has a different decision')
    await db.query(`UPDATE lingxios.agent_approvals SET decision=$2,decided_at=NOW(),decided_by=$3 WHERE id=$1 AND decision IS NULL`,
      [input.approvalId,input.approved,input.principalId])
    return { approvalId: input.approvalId, approved: input.approved, runId: String(row['id']) }
  })
  await resumeDecidedApprovals(pool)
  return decision
}

/** Also runs after a worker parks: a decision can arrive between its receipt and waiting transition. */
export async function resumeDecidedApprovals(pool: SqlPool) {
  await pool.query(`UPDATE lingxios.agent_work_items work SET status='queued',lane='approval',available_at=NOW(),
      finished_at=NULL,goal_outcome=NULL,error=NULL,updated_at=NOW()
    FROM lingxios.agent_approvals approval JOIN lingxios.agent_action_intents intent ON intent.idempotency_key=approval.action_key
    WHERE work.id=intent.intent->>'workId' AND work.status='waiting' AND work.cancel_requested_at IS NULL
      AND work.goal_outcome->>'approvalId'=approval.id AND approval.decision IS NOT NULL
      AND jsonb_array_length(work.steer_inputs)+1=(intent.intent->>'requestVersion')::integer`)
}

export async function executeDecidedApprovals(pool: SqlPool, service: ControlPlaneService, work: WorkItem) {
  const session = await service.getSession(work, sessionKeyOf(work))
  if (session?.request?.workId !== work.id) return
  const { rows } = await pool.query(`SELECT intent.intent->'action' AS action
    FROM lingxios.agent_approvals approval JOIN lingxios.agent_action_intents intent ON intent.idempotency_key=approval.action_key
    WHERE intent.intent->>'workId'=$1 AND (intent.intent->>'requestVersion')::integer=$2
      AND approval.decision IS NOT NULL ORDER BY approval.created_at LIMIT 65`, [work.id,session.request.revisions.length+1])
  if (rows.length > 64) throw new Error('approval recovery exceeds 64 actions')
  let changed = false
  for (const row of rows) {
    const action = row['action'] as HostAction
    await service.executeAction(work, action)
    const call = session.history.find(item => 'type' in item && item.type === 'function_call' && (item.stepId ?? item.callId) === action.cellId)
    if (!call || !('type' in call) || call.type !== 'function_call') continue
    const receipts = await service.recoverCell(work, action.cellId)
    const output = boundedToolOutput({ recovered: true, localExecutionOutput: 'not_recovered', receipts,
      artifacts: receipts?.flatMap(receipt => receipt.result.ok ? receipt.result.artifacts ?? [] : []) ?? [] })
    const old = session.history.find(item => 'type' in item && item.type === 'function_call_output' && item.callId === call.callId)
    if (old && 'type' in old && old.type === 'function_call_output') { changed ||= old.output !== output; old.output = output }
    else { session.history.push({ type: 'function_call_output', callId: call.callId, output }); changed = true }
  }
  if (changed) await service.saveSession(work, session)
}
