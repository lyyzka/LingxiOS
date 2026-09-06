import { isDeepStrictEqual } from 'node:util'
import { withTransaction, type SqlPool } from '../../control-plane/pg-store.js'
import type { ActionIntent } from '../../control-plane/stores.js'
import type { HostAction, WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices } from './service-contracts.js'
import { nativeWork } from './actions.js'
import { inspectApproval, persistApproval, resumeApproved } from './approvals.js'

type Services = Pick<LingxiLoopServices, 'presentations' | 'permissionService'>

export async function preparePresentationApproval(services: Services, work: Omit<WorkItem, 'leaseToken'>,
  action: HostAction, approverId?: string) {
  const api = services.presentations
  if (!api || action.action !== 'presentations.approve_outline'
    || Object.keys(action.args).some(key => !['presentationId', 'expectedRevision'].includes(key))) throw new Error('unsupported presentation approval')
  if (!work.principalId) throw new Error('persisted human authorization principal is required')
  const presentationId = action.args['presentationId']
  if (typeof presentationId !== 'string' || !presentationId.trim()) throw new Error('presentationId is required')
  for (const userId of new Set([work.principalId, ...(approverId ? [approverId] : [])])) {
    await services.permissionService.assertCan({ actorUserId: userId, companyId: work.tenantId,
      action: 'knowledge:write', resource: { type: 'conversation', id: work.sessionId } })
  }
  const input = api.approvePresentationOutlineRequestSchema.parse({ expectedRevision: action.args['expectedRevision'], idempotencyKey: action.idempotencyKey })
  const current = await api.getPresentationForAgent(nativeWork(work), presentationId) as Record<string, unknown>
  if (current['id'] !== presentationId || current['outlineRevision'] !== input.expectedRevision
    || current['status'] !== 'awaitingOutlineApproval' || !current['outline']) throw new Error('presentation outline changed or is not awaiting approval')
  const preview = { presentationId, expectedRevision: input.expectedRevision, title: current['title'], outline: current['outline'] }
  if (JSON.stringify(preview).length > 128_000) throw new Error('presentation outline is too large to approve safely')
  return { preview, execute: () => api.approvePresentationOutlineForAgent(nativeWork(work), presentationId, input) }
}

export async function requestPresentationApproval(database: SqlPool, services: Services,
  work: Omit<WorkItem, 'leaseToken'>, action: HostAction) {
  const { preview } = await preparePresentationApproval(services, work, action)
  return withTransaction(database, db => persistApproval(db, work, action, {
    summary: 'Approve the reviewed presentation outline', scope: { conversationId: work.sessionId, presentationId: preview.presentationId }, preview,
  }))
}

export async function approvePresentation(database: SqlPool, services: Services,
  input: { companyId: string; userId: string; approvalId: string }) {
  const reviewed = await inspectApproval(database, services, input)
  if (reviewed.action.action !== 'presentations.approve_outline') throw new Error('unsupported presentation approval')
  if (reviewed.status === 'EXECUTED') return resumeApproved(database, input, reviewed)
  if (reviewed.status !== 'PENDING') throw new Error('presentation approval is no longer pending')
  const pending = await database.query(`SELECT intent.intent FROM approvals approval
    JOIN lingxios.agent_work_items work ON work.id=approval.work_id AND work.tenant_id=approval.company_id
    JOIN lingxios.agent_action_intents intent ON intent.idempotency_key=approval.idempotency_key
    WHERE approval.id=$1 AND approval.company_id=$2 AND approval.status='PENDING' AND approval.expires_at>NOW()
      AND approval.idempotency_key=$3 AND approval.args=$4::jsonb AND approval.preview=$5::jsonb
      AND work.status='completed' AND work.cancel_requested_at IS NULL
      AND work.goal_outcome->>'status'='awaiting_approval' AND work.goal_outcome->>'approvalId'=$1
      AND (work.goal_outcome->>'requestVersion')::integer=$6 AND jsonb_array_length(work.steer_inputs)+1=$6`,
  [input.approvalId, input.companyId, reviewed.action.idempotencyKey, JSON.stringify(reviewed.action.args), JSON.stringify(reviewed.preview), reviewed.requestVersion])
  const intent = pending.rows[0]?.['intent'] as ActionIntent | undefined
  if (!intent) throw new Error('presentation approval expired or changed before execution')
  const work = { id: intent.workId, fence: 0, homeEpoch: 0, tenantId: intent.tenantId, agentId: intent.agentId,
    sessionId: intent.sessionId, triggerRef: '', kind: 'resume' as const, lane: 'approval' as const,
    ...(intent.principalId ? { principalId: intent.principalId } : {}) }
  const prepared = await preparePresentationApproval(services, work, reviewed.action, input.userId)
  if (!isDeepStrictEqual(prepared.preview, reviewed.preview)) throw new Error('presentation approval preview is stale')
  const value = await prepared.execute()
  await withTransaction(database, async db => {
    const updated = await db.query(`UPDATE approvals SET status='EXECUTED',resolved_at=NOW(),resolved_by=$2,executed_at=NOW(),result=$3::jsonb,error=NULL
      WHERE id=$1 AND company_id=$4 AND status='PENDING' RETURNING id`, [input.approvalId, input.userId, JSON.stringify(value), input.companyId])
    if (updated.rows.length !== 1) throw new Error('presentation approval changed while executing')
    const receipt = await db.query(`UPDATE lingxios.agent_action_ledger SET result=$2::jsonb WHERE idempotency_key=$1 AND result->'approval'->>'id'=$3 RETURNING idempotency_key`,
      [reviewed.action.idempotencyKey, JSON.stringify({ ok: true, value }), input.approvalId])
    if (receipt.rows.length !== 1) throw new Error('presentation approval receipt is missing')
  })
  return resumeApproved(database, input, reviewed)
}
