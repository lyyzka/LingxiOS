import type { SqlQueryable } from '../../control-plane/pg-store.js'
import type { HostAction, WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices } from './service-contracts.js'

export const DOCUMENT_METHODS = { list: [], read: ['documentId'] } as const

export async function executeDocument(db: SqlQueryable, services: Pick<LingxiLoopServices, 'documents' | 'permissionService'>,
  work: Omit<WorkItem, 'leaseToken'>, action: HostAction) {
  const api = services.documents
  const method = action.action.slice('documents.'.length)
  if (!api || !action.action.startsWith('documents.') || !Object.hasOwn(DOCUMENT_METHODS, method)) throw new Error('unsupported document action')
  const allowed: readonly string[] = DOCUMENT_METHODS[method as keyof typeof DOCUMENT_METHODS]
  if (Object.keys(action.args).some(key => !allowed.includes(key))) throw new Error('unknown document argument')
  if (!work.principalId) throw new Error('document access requires the original human principal')
  const documentId = action.args['documentId']
  if (method === 'read' && (typeof documentId !== 'string' || !documentId.trim() || documentId.length > 2000)) throw new Error('documentId is required')
  await services.permissionService.assertCan({ actorUserId: work.principalId, companyId: work.tenantId,
    action: 'conversation:read', resource: { type: 'conversation', id: work.sessionId } })
  const { rows } = await db.query('SELECT project_id FROM conversations WHERE company_id=$1 AND id=$2', [work.tenantId, work.sessionId])
  const projectId = rows[0]?.['project_id']
  if (typeof projectId !== 'string' || !projectId) throw new Error('document project scope is unavailable')
  const authorization = { actorUserId: work.principalId, companyId: work.tenantId, projectId,
    action: 'document:read' as const, resource: method === 'read'
      ? { type: 'document' as const, id: documentId as string } : { type: 'project' as const, id: projectId } }
  await services.permissionService.assertCan(authorization)
  const scope = { companyId: work.tenantId, projectId, userId: work.principalId }
  if (method === 'list') {
    const documents = await api.listAgentDocuments(scope)
    await services.permissionService.assertCan(authorization)
    return { documents: documents.slice(0, 100), truncated: documents.length > 100 }
  }
  const document = await api.readAgentDocument(scope, documentId as string)
  // Native Agent readers scope their lookup but do not authorize the human.
  await services.permissionService.assertCan(authorization)
  if (document.id !== documentId || typeof document.body !== 'string') throw new Error('invalid document read result')
  return { ...document, body: document.body.slice(0, 64_000), bodyTruncated: document.body.length > 64_000 }
}
