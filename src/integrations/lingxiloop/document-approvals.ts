import { isDeepStrictEqual } from 'node:util'
import { withTransaction, type SqlPool, type SqlQueryable } from '../../control-plane/pg-store.js'
import type { ActionIntent } from '../../control-plane/stores.js'
import type { HostAction, WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices } from './service-contracts.js'
import { documentApplication, documentScope, queueDocumentEvents, type DocumentWork } from './document-content.js'
import { lockPendingApproval, recordExecutedApproval, inspectApproval, persistApproval, resumeApproved } from './approvals.js'

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
    const intent = await lockPendingApproval(db, input, reviewed)
    const api = services.documents?.writes
    if (!api) throw new Error('native document write bindings are required')
    await api.createPermissionService(db, { lockDependencies: true }).assertCan({ actorUserId: input.userId, companyId: input.companyId,
      action: 'agent_approval:resolve', resource: { type: 'approval', id: input.approvalId } })
    const work = { id: intent.workId, tenantId: intent.tenantId, agentId: intent.agentId, sessionId: intent.sessionId,
      ...(intent.principalId ? { principalId: intent.principalId } : {}) }
    const prepared = await prepareDeletion(db, services, work, reviewed.action, input.userId)
    if (!isDeepStrictEqual(prepared.preview, reviewed.preview)) throw new Error('document approval preview is stale')
    const value = await prepared.apply()
    await recordExecutedApproval(db, input, reviewed, value, 'PENDING')
  })
  return resumeApproved(database, input, reviewed)
}
