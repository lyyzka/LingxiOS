import { isDeepStrictEqual } from 'node:util'
import { withTransaction, type SqlPool, type SqlQueryable } from '../../control-plane/pg-store.js'
import type { ActionIntent } from '../../control-plane/stores.js'
import type { HostAction, WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices } from './service-contracts.js'
import { documentApplication, documentScope, queueDocumentEvents, type DocumentWork } from './document-content.js'
import { inspectApproval, persistApproval, resumeApproved } from './approvals.js'

type Services = Pick<LingxiLoopServices, 'documents' | 'permissionService'>

async function prepareDeletion(db: SqlQueryable, services: Services, work: DocumentWork, action: HostAction, approverId?: string) {
  if (!services.documents?.writes?.content || action.action !== 'documents.delete'
    || Object.keys(action.args).some(key => !['documentId', 'expectedRevision'].includes(key))) throw new Error('document.delete requires native content bindings, documentId and expectedRevision')
  const { scope, documentId, current } = await documentScope(db, services, work, action, approverId)
  const preview = { projectId: scope.projectId, documentId, title: current!['title'], createdBy: current!['created_by'], revision: current!['revision'] }
  return { preview, apply: async () => {
    const { application, events } = documentApplication(db, services, work)
    const result = await application.deleteForAgent(scope, documentId)
    const remaining = await db.query('SELECT id FROM documents WHERE company_id=$1 AND project_id=$2 AND id=$3', [scope.companyId, scope.projectId, documentId])
    if (!result.ok || remaining.rows.length || !events.some(event => event.type === 'doc.changed' && event.kind === 'document.deleted' && event.documentId === documentId)) throw new Error('document deletion postcondition was not observed')
    await queueDocumentEvents(db, action, events)
    return { documentId, deleted: true, notification: 'queued' }
  } }
}

export async function requestDocumentApproval(database: SqlPool, services: Services, work: Omit<WorkItem, 'leaseToken'>, action: HostAction) {
  return withTransaction(database, async db => {
    await db.query("SET LOCAL lock_timeout='5s'")
    await db.query("SET LOCAL statement_timeout='15s'")
    const { preview } = await prepareDeletion(db, services, work, action)
    return persistApproval(db, work, action, { summary: 'Delete the reviewed document', scope: { projectId: preview.projectId, conversationId: work.sessionId }, preview })
  })
}

export async function approveDocument(database: SqlPool, services: Services, input: { companyId: string; userId: string; approvalId: string }) {
  const reviewed = await inspectApproval(database, services, input)
  if (reviewed.action.action !== 'documents.delete') throw new Error('unsupported document approval')
  if (reviewed.status === 'EXECUTED') return resumeApproved(database, input, reviewed)
  if (reviewed.status !== 'PENDING') throw new Error('document approval is no longer pending')
  await withTransaction(database, async db => {
    await db.query("SET LOCAL lock_timeout='5s'")
    await db.query("SET LOCAL statement_timeout='15s'")
    const pending = await db.query(`SELECT intent.intent FROM approvals approval
      JOIN lingxios.agent_work_items work ON work.id=approval.work_id AND work.tenant_id=approval.company_id
      JOIN lingxios.agent_action_intents intent ON intent.idempotency_key=approval.idempotency_key
      JOIN lingxios.agent_os_sessions session ON session.tenant_id=work.tenant_id AND session.agent_id=work.agent_id
        AND session.session_id=work.session_id AND session.thread_id IS NOT DISTINCT FROM work.thread_id
      WHERE approval.id=$1 AND approval.company_id=$2 AND approval.status='PENDING' AND approval.expires_at>NOW()
        AND approval.idempotency_key=$3 AND approval.args=$4::jsonb AND approval.preview=$5::jsonb
        AND approval.action='documents.delete' AND work.status='completed' AND work.cancel_requested_at IS NULL
        AND work.goal_outcome->>'status'='awaiting_approval' AND work.goal_outcome->>'approvalId'=$1
        AND (work.goal_outcome->>'requestVersion')::integer=$6 AND jsonb_array_length(work.steer_inputs)+1=$6
        AND session.request_snapshot->>'workId'=work.id AND session.request_snapshot->'revisions'=work.steer_inputs
      FOR UPDATE OF approval,work,session`, [input.approvalId, input.companyId, reviewed.action.idempotencyKey,
      JSON.stringify(reviewed.action.args), JSON.stringify(reviewed.preview), reviewed.requestVersion])
    const intent = pending.rows[0]?.['intent'] as ActionIntent | undefined
    if (!intent) throw new Error('document approval expired or changed before execution')
    const api = services.documents?.writes
    if (!api) throw new Error('native document write bindings are required')
    await api.createPermissionService(db, { lockDependencies: true }).assertCan({ actorUserId: input.userId, companyId: input.companyId,
      action: 'agent_approval:resolve', resource: { type: 'approval', id: input.approvalId } })
    const work = { id: intent.workId, tenantId: intent.tenantId, agentId: intent.agentId, sessionId: intent.sessionId,
      ...(intent.principalId ? { principalId: intent.principalId } : {}) }
    const prepared = await prepareDeletion(db, services, work, reviewed.action, input.userId)
    if (!isDeepStrictEqual(prepared.preview, reviewed.preview)) throw new Error('document approval preview is stale')
    const value = await prepared.apply()
    await db.query("UPDATE approvals SET status='EXECUTED',resolved_at=NOW(),resolved_by=$2,executed_at=NOW(),result=$3::jsonb,error=NULL WHERE id=$1", [input.approvalId, input.userId, JSON.stringify(value)])
    const receipt = await db.query(`UPDATE lingxios.agent_action_ledger SET result=$2::jsonb
      WHERE idempotency_key=$1 AND result->'approval'->>'id'=$3 RETURNING idempotency_key`,
    [reviewed.action.idempotencyKey, JSON.stringify({ ok: true, value }), input.approvalId])
    if (receipt.rows.length !== 1) throw new Error('document approval receipt is missing')
  })
  return resumeApproved(database, input, reviewed)
}
