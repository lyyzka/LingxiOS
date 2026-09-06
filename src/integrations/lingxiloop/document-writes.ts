import { withTransaction, type SqlPool } from '../../control-plane/pg-store.js'
import type { HostAction, WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices } from './service-contracts.js'

export async function renameDocument(database: SqlPool, services: Pick<LingxiLoopServices, 'documents'>,
  work: Omit<WorkItem, 'leaseToken'>, action: HostAction) {
  const api = services.documents?.writes
  if (!api || !work.principalId || action.action !== 'documents.rename') throw new Error('document write bindings and original human principal are required')
  if (Object.keys(action.args).some(key => !['documentId', 'title', 'expectedTitle'].includes(key))) throw new Error('unknown document rename argument')
  const { documentId, expectedTitle } = action.args
  if (typeof documentId !== 'string' || !documentId.trim() || documentId.length > 2000) throw new Error('documentId is required')
  if (typeof expectedTitle !== 'string' || expectedTitle.length > 2000) throw new Error('expectedTitle is required')
  const { title } = api.renameDocumentRequestSchema.parse({ title: action.args['title'] })
  const projectId = await withTransaction(database, async client => {
    await client.query("SET LOCAL statement_timeout='10s'")
    const permissions = api.createPermissionService(client, { lockDependencies: true })
    await permissions.assertCan({ actorUserId: work.principalId!, companyId: work.tenantId,
      action: 'conversation:read', resource: { type: 'conversation', id: work.sessionId } })
    const room = await client.query('SELECT project_id FROM conversations WHERE id=$1 AND company_id=$2 FOR SHARE', [work.sessionId, work.tenantId])
    const projectId = room.rows[0]?.['project_id']
    if (typeof projectId !== 'string' || !projectId) throw new Error('document project scope is unavailable')
    const current = await client.query('SELECT title,conversation_id FROM documents WHERE id=$1 AND company_id=$2 AND project_id=$3 FOR UPDATE', [documentId, work.tenantId, projectId])
    if (current.rows.length !== 1) throw new Error('document is outside this project')
    // Native document authorization locks the document, but not its joined conversation membership.
    const conversationId = current.rows[0]!['conversation_id']
    if (typeof conversationId === 'string') {
      await client.query('SELECT id FROM conversations WHERE id=$1 AND company_id=$2 AND project_id=$3 FOR SHARE', [conversationId, work.tenantId, projectId])
    }
    await permissions.assertCan({ actorUserId: work.principalId!, companyId: work.tenantId, projectId,
      action: 'document:write', resource: { type: 'document', id: documentId } })
    if (current.rows[0]!['title'] !== expectedTitle) throw new Error('document title changed; read it again before renaming')
    if (!await api.renameDocument(client, work.tenantId, projectId, documentId, title)) throw new Error('document rename did not update a row')
    return projectId
  })
  let notification: 'published' | 'unconfirmed' = 'published'
  try {
    await api.publish(api.CH_DOCS, { type: 'doc.changed', kind: 'document.updated', companyId: work.tenantId,
      workspaceId: projectId, documentId, actorId: work.agentId })
  } catch { notification = 'unconfirmed' }
  return { documentId, title, notification }
}
