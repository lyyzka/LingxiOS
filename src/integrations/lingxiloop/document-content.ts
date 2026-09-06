import { createHash } from 'node:crypto'
import { withTransaction, type SqlPool, type SqlQueryable } from '../../control-plane/pg-store.js'
import type { HostAction, WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices, NativeDocumentChanged, NativeDocumentEdit, NativeDocumentUpdate, NativeQueryable } from './service-contracts.js'
import { flushEventOutbox } from './event-outbox.js'

type Services = Pick<LingxiLoopServices, 'documents'>
export type DocumentWork = Pick<WorkItem, 'id' | 'tenantId' | 'principalId' | 'agentId' | 'sessionId'>

function fields(value: unknown, allowed: readonly string[]): asserts value is Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).some(key => !allowed.includes(key))) throw new Error('unknown document argument')
}
function text(value: unknown, name: string, max = 64_000, empty = false): asserts value is string {
  if (typeof value !== 'string' || (!empty && !value.trim()) || value.length > max) throw new Error(`invalid document ${name}`)
}

export function documentOperations(value: unknown): NativeDocumentEdit[] {
  if (!Array.isArray(value) || !value.length || value.length > 32 || JSON.stringify(value).length > 64_000) throw new Error('operations must contain 1-32 edits within 64000 characters')
  for (const op of value) {
    fields(op, ['kind', 'text', 'find', 'replace', 'at', 'anchorText', 'src', 'alt', 'placement', 'match'])
    switch (op['kind']) {
      case 'append': fields(op, ['kind', 'text']); text(op['text'], 'text'); break
      case 'replace': fields(op, ['kind', 'find', 'replace']); text(op['find'], 'find'); text(op['replace'], 'replacement', 64_000, true); break
      case 'insertParagraph':
        fields(op, ['kind', 'at', 'text']); text(op['text'], 'text')
        if (!['start', 'end'].includes(String(op['at']))) throw new Error('paragraph position must be start or end')
        break
      case 'replaceBlock': fields(op, ['kind', 'anchorText', 'text']); text(op['anchorText'], 'anchor'); text(op['text'], 'text', 64_000, true); break
      case 'image': {
        fields(op, ['kind', 'src', 'alt', 'placement']); text(op['src'], 'image URL', 8000)
        const url = new URL(op['src'])
        if (url.protocol !== 'https:' || url.username || url.password) throw new Error('image source must be an HTTPS URL without credentials')
        if (op['alt'] !== null) text(op['alt'], 'image alt', 2000, true)
        const placement = op['placement']; fields(placement, ['mode', 'anchorText'])
        if (['start', 'end'].includes(String(placement['mode']))) fields(placement, ['mode'])
        else if (['replace', 'after', 'before'].includes(String(placement['mode']))) text(placement['anchorText'], 'image anchor')
        else throw new Error('invalid image placement')
        break
      }
      case 'imageDelete': {
        fields(op, ['kind', 'match']); const match = op['match']; fields(match, ['by', 'src', 'substring', 'alt'])
        const key = match['by'] === 'src' ? 'src' : match['by'] === 'src-contains' ? 'substring' : match['by'] === 'alt' ? 'alt' : undefined
        if (!key) throw new Error('invalid image deletion match')
        fields(match, ['by', key]); text(match[key], 'image match', 8000)
        break
      }
      default: throw new Error('unsupported document edit kind')
    }
  }
  return value as NativeDocumentEdit[]
}

/** Locks both the original human's authority and the native agent attribution for the mutation. */
export async function documentScope(db: SqlQueryable, services: Services, work: DocumentWork,
  action: HostAction, approverId?: string) {
  const api = services.documents?.writes
  if (!api || !work.principalId) throw new Error('document write bindings and original human principal are required')
  const agent = await db.query(`SELECT p.id FROM participants p JOIN im_channel_bindings b ON b.company_id=p.company_id AND b.channel_id=$3
    WHERE p.company_id=$1 AND p.id=$2 AND p.kind='agent' AND p.departed_at IS NULL
      AND p.capabilities @> '["documents"]'::jsonb AND b.profile->'members' ? p.id
      AND NOT EXISTS(SELECT 1 FROM learning_project_teacher_agents teacher WHERE teacher.company_id=p.company_id AND teacher.agent_id=p.id)
    FOR SHARE OF p,b`, [work.tenantId, work.agentId, work.sessionId])
  if (agent.rows.length !== 1) throw new Error('document agent capability or membership was revoked')
  const permissions = api.createPermissionService(db, { lockDependencies: true })
  const humans = new Set([work.principalId, ...(approverId ? [approverId] : [])])
  for (const actorUserId of humans) await permissions.assertCan({ actorUserId, companyId: work.tenantId,
    action: 'conversation:read', resource: { type: 'conversation', id: work.sessionId } })
  const room = await db.query('SELECT project_id FROM conversations WHERE company_id=$1 AND id=$2 FOR SHARE', [work.tenantId, work.sessionId])
  const projectId = room.rows[0]?.['project_id']; text(projectId, 'project scope', 2000)
  const create = action.action === 'documents.create'
  const documentId = create ? 'doc_' + createHash('sha256').update(action.idempotencyKey).digest('hex') : action.args['documentId']
  text(documentId, 'id', 2000)
  let current: Record<string, unknown> | undefined
  if (!create) {
    if (action.action !== 'documents.read') text(action.args['expectedRevision'], 'expectedRevision', 200)
    const result = await db.query('SELECT title,created_by,conversation_id,updated_at::text AS revision FROM documents WHERE company_id=$1 AND project_id=$2 AND id=$3 FOR UPDATE', [work.tenantId, projectId, documentId])
    current = result.rows[0]
    if (!current || (action.action !== 'documents.read' && current['revision'] !== action.args['expectedRevision'])) throw new Error('document changed or is outside this project; read it again')
    if (typeof current['conversation_id'] === 'string') await db.query('SELECT id FROM conversations WHERE company_id=$1 AND project_id=$2 AND id=$3 FOR SHARE', [work.tenantId, projectId, current['conversation_id']])
  }
  for (const actorUserId of humans) await permissions.assertCan({ actorUserId, companyId: work.tenantId, projectId,
    action: action.action === 'documents.delete' ? 'document:delete' : action.action === 'documents.read' ? 'document:read' : 'document:write',
    resource: { type: create ? 'project' : 'document', id: create ? projectId : documentId } })
  if (action.action === 'documents.delete' && current?.['created_by'] !== work.agentId) throw new Error('only the creating agent can delete this document')
  return { scope: { companyId: work.tenantId, projectId, userId: work.agentId }, documentId, current }
}

/** A fresh native room uses this transaction only; no process-global collaboration cache is mutated. */
export function documentApplication(db: SqlQueryable, services: Services, work: DocumentWork) {
  const api = services.documents?.writes?.content
  if (!api) throw new Error('native document content bindings are required')
  const events: Array<NativeDocumentChanged | NativeDocumentUpdate> = []
  const collaboration = api.createDocumentCollaborationApplication({
    transaction: callback => callback(db as NativeQueryable), instanceId: `lingxios:${work.id}`,
    bus: { publish: async event => { if (event.type === 'doc.update') events.push(event) }, subscribe: async () => { throw new Error('transactional document editor cannot subscribe') } },
    imageStorage: { normalizeKey: value => api.normalizeStorageKey(value ?? ''), keyFromPublicUrl: value => api.storageKeyFromPublicUrl(value ?? ''),
      signedUrlExpiresSoon: api.signedUrlExpiresSoon, publicUrl: key => api.storage.publicUrl(key) },
  })
  const application = new api.DocumentsApplication(db as NativeQueryable, { publish: async event => { events.push(event) } },
    { readText: collaboration.readDocumentText, applyEdit: collaboration.applyAgentEdit })
  return { application, events }
}

export async function queueDocumentEvents(db: SqlQueryable, action: HostAction, events: Array<NativeDocumentChanged | NativeDocumentUpdate>) {
  if (events.length > 100) throw new Error('document mutation emitted too many events')
  for (const event of events) await db.query('INSERT INTO lingxios.agent_document_outbox(id,event) VALUES($1,$2::jsonb) ON CONFLICT DO NOTHING',
    [`${action.idempotencyKey}:${createHash('sha256').update(JSON.stringify(event)).digest('hex')}`, JSON.stringify(event)])
}

export async function flushDocumentEvents(db: SqlQueryable, services: Services) {
  const api = services.documents?.writes
  if (api) await flushEventOutbox(db, 'agent_document_outbox', async raw => {
    const event = raw as NativeDocumentChanged | NativeDocumentUpdate
    if (event.type === 'doc.update') {
      if (!api.content) throw new Error('document collaboration publisher is unavailable')
      await api.content.publish(api.content.CH_DOC_UPDATE, event)
    } else await api.publish(api.CH_DOCS, event)
  })
}

export async function executeDocumentContent(database: SqlPool, services: Services, work: Omit<WorkItem, 'leaseToken'>, action: HostAction) {
  const create = action.action === 'documents.create'
  const read = action.action === 'documents.read'
  if (!create && !read && action.action !== 'documents.edit') throw new Error('unsupported document content action')
  fields(action.args, create ? ['title', 'body'] : read ? ['documentId'] : ['documentId', 'expectedRevision', 'operations'])
  if (create) { text(action.args['title'], 'title', 200); text(action.args['body'], 'body', 64_000, true) }
  const operations = create || read ? undefined : documentOperations(action.args['operations'])
  return withTransaction(database, async db => {
    await db.query("SET LOCAL lock_timeout='5s'")
    await db.query("SET LOCAL statement_timeout='15s'")
    if (!read) {
      const live = await db.query(`SELECT work.id FROM lingxios.agent_work_items work
      JOIN lingxios.agent_action_intents intent ON intent.idempotency_key=$3
      WHERE work.id=$1 AND work.fence=$2 AND work.status='leased' AND work.lease_expires_at>NOW()
        AND work.cancel_requested_at IS NULL AND work.tenant_id=$4 AND work.principal_id=$5
        AND intent.intent->>'workId'=work.id AND intent.intent->'action'->'args'=$6::jsonb
        AND intent.intent->'action'->>'action'=$7
        AND (intent.intent->>'requestVersion')::integer=jsonb_array_length(work.steer_inputs)+1 FOR UPDATE OF work`,
    [work.id, work.fence, action.idempotencyKey, work.tenantId, work.principalId, JSON.stringify(action.args), action.action])
      if (live.rows.length !== 1) throw new Error('document mutation requires the current live action intent')
    }
    const { scope, documentId } = await documentScope(db, services, work, action)
    const { application, events } = documentApplication(db, services, work)
    const result = create ? await application.createForAgent(scope, { id: documentId, title: action.args['title'] as string, body: action.args['body'] as string })
      : read ? await application.readForAgent(scope, documentId) : await application.editForAgent(scope, documentId, operations!)
    if (create && (!('document' in result) || result.document.id !== documentId || result.document.createdBy !== work.agentId || result.replayed)) throw new Error('unexpected document creation result')
    const current = await db.query('SELECT updated_at::text AS revision FROM documents WHERE id=$1 AND company_id=$2 AND project_id=$3', [documentId, work.tenantId, scope.projectId])
    if (current.rows.length !== 1) throw new Error('document mutation postcondition is unavailable')
    await queueDocumentEvents(db, action, events)
    const value = read && 'body' in result ? { ...result, body: result.body.slice(0, 64_000), bodyTruncated: result.body.length > 64_000, revision: current.rows[0]!['revision'] }
      : { documentId, result, revision: current.rows[0]!['revision'], notification: 'queued' as const }
    if (!read) {
      const receipt = await db.query('INSERT INTO lingxios.agent_action_ledger(idempotency_key,result) VALUES($1,$2::jsonb) ON CONFLICT DO NOTHING RETURNING idempotency_key', [action.idempotencyKey, JSON.stringify({ ok: true, value })])
      if (receipt.rows.length !== 1) throw new Error('document mutation receipt already exists')
    }
    return value
  })
}
