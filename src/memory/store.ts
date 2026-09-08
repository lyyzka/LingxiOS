import type { SqlQueryable } from '../control-plane/pg-store.js'
import { memoryWriteBody, type MemoryWritePolicy } from './policy.js'
import { currentMemoryScopes, forgetMemoryScope, lockMemoryScopes } from './forget.js'
import { MEMORY_BODY_BYTES, excerpt, memoryDigest, memoryQuery, memorySearchText, pageLimit, validateMemoryPath } from './text.js'
import type { MemoryApplyResult, MemoryChange, MemoryContent, MemoryDocument, MemoryEntry, MemoryIdentity, MemoryListQuery,
  MemoryPage, MemoryScope, MemorySearchResult, MemorySource, MemoryVersion } from './types.js'
import { authorizedScopes, sameScope, sourceIdentity, type MemoryOptions } from './access.js'
export type { MemoryScope, MemorySnapshot } from './types.js'
export type MemoryScopeType = string

export function validateScope(scope: MemoryScope) {
  if (!scope || ![scope.tenantId,scope.scopeType,scope.scopeId].every(value => typeof value === 'string' && value.trim() && value.length <= 1000)) throw new Error('invalid memory scope')
}

const iso = (value: unknown) => value == null ? null : new Date(value as string | Date).toISOString()
export function memoryDocument(row: Record<string, unknown>): MemoryDocument {
  return { tenantId: String(row['tenant_id']), scopeType: String(row['scope_type']), scopeId: String(row['scope_id']),
    id: String(row['id']), path: String(row['path']), title: String(row['title']), description: String(row['description']),
    body: String(row['body']), layer: row['layer'] as MemoryDocument['layer'], kind: String(row['kind']),
    origin: row['origin'] as MemoryDocument['origin'], locked: row['pinned'] === true, version: Number(row['version']),
    status: row['status'] as MemoryDocument['status'], sources: row['source_refs'] as MemorySource[],
    validUntil: iso(row['valid_until']), updatedAt: iso(row['updated_at'])! }
}
export function memoryEntry(document: MemoryDocument): MemoryEntry {
  const { body: _body, sources: _sources, ...entry } = document
  return entry
}
export const scopeParams = (scope: MemoryScope) => [scope.tenantId, scope.scopeType, scope.scopeId]

/** Database accessors are internal: the service resolves current permissions before calling them. */
export async function readMemory(database: SqlQueryable, scope: MemoryScope, id: string, version?: number): Promise<MemoryDocument | null> {
  validateScope(scope)
  if (typeof id !== 'string' || !id || id.length > 1000) throw new Error('invalid memory identity')
  const row = (await database.query(`SELECT * FROM lingxios.agent_memories
    WHERE tenant_id=$1 AND scope_type=$2 AND scope_id=$3 AND id=$4 AND origin<>'evolved'`, [...scopeParams(scope),id])).rows[0]
  if (version!==undefined && (!Number.isSafeInteger(version) || version<1)) throw new Error('invalid memory version')
  if (!row) return null
  if (version===undefined || Number(row['version'])===version) return memoryDocument(row)
  const previous=(await database.query('SELECT snapshot FROM lingxios.agent_memory_versions WHERE tenant_id=$1 AND memory_id=$2 AND version=$3',[scope.tenantId,id,version])).rows[0]
  return previous ? memoryDocument(previous['snapshot'] as Record<string,unknown>) : null
}

export async function listMemories(database: SqlQueryable, scope: MemoryScope, query: MemoryListQuery = {}): Promise<MemoryPage<MemoryDocument>> {
  validateScope(scope)
  const limit = pageLimit(query.limit)
  if (query.prefix !== undefined && (typeof query.prefix !== 'string' || query.prefix.length > 512)
    || query.cursor !== undefined && (typeof query.cursor !== 'string' || query.cursor.length > 512)
    || query.layer !== undefined && !['core','reference'].includes(query.layer)
    || query.includeInactive !== undefined && typeof query.includeInactive !== 'boolean') throw new Error('invalid memory listing')
  const { rows } = await database.query(`SELECT * FROM lingxios.agent_memories
    WHERE tenant_id=$1 AND scope_type=$2 AND scope_id=$3 AND origin<>'evolved'
      AND ($4::boolean OR (status='active' AND (valid_until IS NULL OR valid_until>NOW())))
      AND starts_with(path,$5) AND ($6::text IS NULL OR layer=$6) AND path>$7
    ORDER BY path LIMIT $8`, [...scopeParams(scope),query.includeInactive ?? false,query.prefix ?? '',query.layer ?? null,query.cursor ?? '',limit+1])
  const items = rows.slice(0,limit).map(memoryDocument)
  return { items, nextCursor: rows.length > limit ? items.at(-1)!.path : null }
}

export async function searchMemories(database: SqlQueryable, scope: MemoryScope, query: string, limit = 12, cursor?: string): Promise<MemorySearchResult> {
  validateScope(scope); pageLimit(limit)
  const terms = memoryQuery(query)
  if (!query.trim()) {
    const page = await listMemories(database,scope,{ limit, ...cursor === undefined ? {} : { cursor } })
    return { ...page, items: page.items.map(doc => ({ ...memoryEntry(doc), excerpt: excerpt(doc.body,1600), score: 0 })), retrieval: 'browse' }
  }
  const offset = cursor === undefined ? 0 : Number(cursor)
  if (!Number.isSafeInteger(offset) || offset < 0 || offset > 10_000 || cursor !== undefined && String(offset) !== cursor) throw new Error('invalid memory search cursor')
  if (!terms) return { items: [], nextCursor: null, retrieval: 'keyword' }
  const { rows } = await database.query(`SELECT *,ts_rank_cd(search_vector,to_tsquery('simple',$4)) AS score
    FROM lingxios.agent_memories WHERE tenant_id=$1 AND scope_type=$2 AND scope_id=$3
      AND origin<>'evolved' AND status='active' AND (valid_until IS NULL OR valid_until>NOW())
      AND search_vector @@ to_tsquery('simple',$4)
    ORDER BY score DESC,path LIMIT $5 OFFSET $6`, [...scopeParams(scope),terms,limit+1,offset])
  return { items: rows.slice(0,limit).map(row => ({ ...memoryEntry(memoryDocument(row)), excerpt: excerpt(String(row['body']),1600), score: Number(row['score']) })),
    nextCursor: rows.length > limit && offset+limit <= 10_000 ? String(offset+limit) : null, retrieval: 'keyword' }
}

export async function memoryVersions(database: SqlQueryable, scope: MemoryScope, id: string, limit = 32, cursor?: string): Promise<MemoryPage<MemoryVersion>> {
  pageLimit(limit)
  const current = await readMemory(database,scope,id)
  if (!current) return { items: [], nextCursor: null }
  const before = cursor === undefined ? current.version + 1 : Number(cursor)
  if (!Number.isSafeInteger(before) || before < 1) throw new Error('invalid memory version cursor')
  const rows = (await database.query(`SELECT version,snapshot,replaced_at FROM lingxios.agent_memory_versions
    WHERE tenant_id=$1 AND memory_id=$2 AND version<$3 ORDER BY version DESC LIMIT $4`, [scope.tenantId,id,before,limit+1])).rows
  const versions: MemoryVersion[] = rows.map(row => ({ version: Number(row['version']), snapshot: memoryDocument(row['snapshot'] as Record<string, unknown>), replacedAt: iso(row['replaced_at']) }))
  if (current.version < before) versions.unshift({ version: current.version, snapshot: current, replacedAt: null })
  const items = versions.slice(0,limit)
  return { items, nextCursor: versions.length > limit ? String(items.at(-1)!.version) : null }
}

function record(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('invalid memory change')
  return value as Record<string,unknown>
}
function keys(value: Record<string, unknown>, allowed: string[]) {
  if (Object.keys(value).some(key => !allowed.includes(key))) throw new Error('unknown memory field')
}
function versioned(value: Record<string, unknown>) {
  if (typeof value['id'] !== 'string' || !value['id'] || value['id'].length > 1000
    || !Number.isSafeInteger(value['expectedVersion']) || Number(value['expectedVersion']) < 1) throw new Error('memory change requires identity and expectedVersion')
}
export function validateMemoryContent(value: unknown): asserts value is MemoryContent {
  const content = record(value)
  keys(content,['path','title','description','body','layer','kind','locked','validUntil'])
  validateMemoryPath(content['path'])
  for (const [field,limit] of [['title',200],['description',500]] as const) {
    if (typeof content[field] !== 'string' || !content[field].trim() || content[field].length > limit) throw new Error(`invalid memory ${field}`)
  }
  if (typeof content['body'] !== 'string' || !content['body'].trim() || Buffer.byteLength(content['body']) > MEMORY_BODY_BYTES
    || !['core','reference'].includes(String(content['layer']))
    || content['locked'] !== undefined && typeof content['locked'] !== 'boolean'
    || content['kind'] !== undefined && (typeof content['kind'] !== 'string' || !/^[a-z_]{1,32}$/.test(content['kind'])
      || ['experience','skill','strategy'].includes(content['kind']))) throw new Error('invalid memory content')
  const until = content['validUntil']
  if (until !== undefined && until !== null && (typeof until !== 'string'
    || !/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,3})?(?:Z|[+-]\d\d:\d\d)$/.test(until)
    || !Number.isFinite(Date.parse(until)) || Date.parse(until) <= Date.now())) throw new Error('memory expiry must be a future ISO timestamp')
}

export function parseDocumentChanges(value: unknown): MemoryChange[] {
  if (!Array.isArray(value) || value.length > 12) throw new Error('memory requires at most 12 changes')
  const ids = new Set<string>()
  for (const raw of value) {
    const change = record(raw), action = change['action']
    if (!['create','update','move','expire','merge','delete'].includes(String(action))) throw new Error('invalid memory action')
    keys(change,['action',...action === 'create' ? ['content'] : ['id','expectedVersion',
      ...action === 'update' ? ['content'] : action === 'merge' ? ['content','from'] : action === 'move' ? ['path'] : []]])
    if (action !== 'create') {
      versioned(change)
      if (ids.has(String(change['id']))) throw new Error('memory batch must reference each identity once')
      ids.add(String(change['id']))
    }
    if (['create','update','merge'].includes(String(action))) validateMemoryContent(change['content'])
    if (action === 'move') validateMemoryPath(change['path'])
    if (action === 'merge') {
      const from = change['from']
      if (!Array.isArray(from) || from.length < 1 || from.length > 12) throw new Error('merge requires 1-12 versioned sources')
      for (const source of from) {
        const item = record(source); keys(item,['id','expectedVersion']); versioned(item)
        if (ids.has(String(item['id']))) throw new Error('memory batch must reference each identity once')
        ids.add(String(item['id']))
      }
    }
  }
  return value as MemoryChange[]
}

export interface MemoryWriteContext {
  identity: MemoryIdentity
  actionId: string
  epoch: number
  sources: MemorySource[]
  /** Set only by authenticated host ingress or an independent worker review, never tool arguments. */
  explicit: boolean
  policy?: MemoryWritePolicy
  resolveScopes?: MemoryOptions['resolveScopes']
}

export async function applyMemoryChanges(database: SqlQueryable, scope: MemoryScope, input: MemoryChange[], context: MemoryWriteContext): Promise<MemoryApplyResult> {
  validateScope(scope)
  const changes = parseDocumentChanges(input)
  const { identity } = context
  if (scope.tenantId !== identity.tenantId || !identity.principalId || !context.actionId || context.actionId.length > 2000
    || !context.sources.length || context.sources.length > 64 || context.sources.some(source => !source.sourceRef || !source.authorId)) throw new Error('memory write requires provenance')
  const [epoch] = await lockMemoryScopes(database,[scope])
  if (epoch!.epoch !== context.epoch) throw new Error('memory scope was forgotten')
  for (const source of context.sources) if (source.workId && !(await currentMemoryScopes(database,[epoch!],source.workId)).length) throw new Error('memory source predates forgetting')
  const fingerprint = memoryDigest({ identity,scope,changes,sources: context.sources.map(({ observedAt: _time,...source }) => source),explicit: context.explicit })
  const previous = (await database.query(`SELECT fingerprint,result FROM lingxios.agent_memory_commands
    WHERE tenant_id=$1 AND scope_type=$2 AND scope_id=$3 AND epoch=$4 AND action_id=$5`, [...scopeParams(scope),context.epoch,context.actionId])).rows[0]
  if (previous) {
    if (previous['fingerprint'] !== fingerprint) throw new Error('memory idempotency key was reused')
    return previous['result'] as unknown as MemoryApplyResult
  }
  const result: MemoryApplyResult = { documents: [], deleted: [] }
  const get = async (id: string, version: number) => {
    const doc = await readMemory(database,scope,id)
    if (!doc || doc.version !== version) throw new Error('memory is unavailable or stale')
    if (!context.explicit && (doc.origin === 'explicit' || doc.locked)) throw new Error('memory is protected; a current explicit user request is required')
    if (context.resolveScopes) for (const source of doc.sources) {
      if (!source.workId) continue
      const row=(await database.query('SELECT * FROM lingxios.agent_work_items WHERE id=$1 AND tenant_id=$2',[source.workId,scope.tenantId])).rows[0]
      if (!row || !(await authorizedScopes({resolveScopes:context.resolveScopes},sourceIdentity(row),database)).some(item=>sameScope(item,scope))) throw new Error('memory source scope was revoked')
    }
    return doc
  }
  for (const [index,change] of changes.entries()) {
    const current = change.action === 'create' ? undefined : await get(change.id,change.expectedVersion)
    const sources = new Map<string,MemorySource>()
    for (const source of [...current?.sources ?? [],...context.sources]) sources.set(memoryDigest(source),source)
    if (sources.size > 64) throw new Error('memory provenance limit reached; split the document')
    if (change.action === 'delete') {
      if (!context.explicit) throw new Error('deletion requires an explicit user request')
      await database.query('DELETE FROM lingxios.agent_memories WHERE tenant_id=$1 AND id=$2',[scope.tenantId,change.id])
      result.deleted.push(change.id)
      continue
    }
    if (change.action === 'expire') {
      const row = (await database.query(`UPDATE lingxios.agent_memories SET status='expired',version=version+1,updated_at=NOW(),source_refs=$3::jsonb
        WHERE tenant_id=$1 AND id=$2 RETURNING *`,[scope.tenantId,change.id,JSON.stringify([...sources.values()])])).rows[0]!
      result.documents.push(memoryDocument(row)); continue
    }
    const content: MemoryContent = change.action === 'move' ? { ...contentOf(current!),path: change.path } : change.content
    if (!context.explicit && content.locked) throw new Error('automatic memory cannot lock documents')
    if (change.action === 'merge') {
      for (const item of change.from) {
        const donor = await get(item.id,item.expectedVersion)
        for (const source of donor.sources) sources.set(memoryDigest(source),source)
        const donorSources=[...new Map([...donor.sources,...context.sources].map(source=>[memoryDigest(source),source])).values()]
        if (donorSources.length>64) throw new Error('memory provenance limit reached; split the document')
        await database.query(`UPDATE lingxios.agent_memories SET status='retired',version=version+1,updated_at=NOW(),source_refs=$3::jsonb
          WHERE tenant_id=$1 AND id=$2`,[scope.tenantId,item.id,JSON.stringify(donorSources)])
      }
    }
    if (sources.size > 64) throw new Error('memory provenance limit reached; split the document')
    if (current && (current.status === 'expired' || current.validUntil && Date.parse(current.validUntil) <= Date.now())) {
      const expiredAt = current.status === 'expired' ? current.updatedAt : current.validUntil!
      if (!content.validUntil || !context.sources.some(source => source.observedAt && Date.parse(source.observedAt)>Date.parse(expiredAt))) throw new Error('expired memory requires new evidence and a future expiry')
    }
    const origin = context.explicit ? 'explicit' : 'synthesized'
    const body = await memoryWriteBody({ scope,principalId: identity.principalId,sourceWorkId: identity.workId ?? context.actionId,
      origin,kind: content.kind ?? current?.kind ?? 'observation',body: content.body },context.policy)
    // Metadata enters context and search too, so it crosses the same content boundary as the body.
    const title = await memoryWriteBody({ scope,principalId: identity.principalId,sourceWorkId: identity.workId ?? context.actionId,
      origin,kind: 'title',body: content.title },context.policy)
    const description = await memoryWriteBody({ scope,principalId: identity.principalId,sourceWorkId: identity.workId ?? context.actionId,
      origin,kind: 'description',body: content.description },context.policy)
    const path = await memoryWriteBody({ scope,principalId: identity.principalId,sourceWorkId: identity.workId ?? context.actionId,
      origin,kind:'path',body:content.path },context.policy)
    validateMemoryPath(path)
    if (title.length > 200 || description.length > 500) throw new Error('invalid memory metadata after content policy')
    const id = current?.id ?? `mem-${memoryDigest([scope,context.epoch,context.actionId,index])}`
    const values = [scope.tenantId,id,scope.scopeType,scope.scopeId,path,title,description,body,content.layer,
      content.kind ?? current?.kind ?? 'observation',origin,content.locked ?? current?.locked ?? false,JSON.stringify([...sources.values()]),
      content.validUntil ?? null,memorySearchText(`${path} ${title} ${description} ${body}`)]
    const saved = current ? await database.query(`UPDATE lingxios.agent_memories SET path=$5,title=$6,description=$7,body=$8,layer=$9,kind=$10,
        origin=$11,pinned=$12,source_refs=$13::jsonb,valid_until=$14::timestamptz,search_text=$15,status='active',version=version+1,updated_at=NOW()
        WHERE tenant_id=$1 AND id=$2 AND scope_type=$3 AND scope_id=$4 RETURNING *`,values)
      : await database.query(`INSERT INTO lingxios.agent_memories
        (tenant_id,id,scope_type,scope_id,path,title,description,body,layer,kind,origin,pinned,source_refs,valid_until,search_text)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14::timestamptz,$15) RETURNING *`,values)
    result.documents.push(memoryDocument(saved.rows[0]!))
  }
  const savedEpoch = result.deleted.length ? (await forgetMemoryScope(database,scope,false)).epoch : context.epoch
  await database.query(`INSERT INTO lingxios.agent_memory_commands(tenant_id,scope_type,scope_id,epoch,action_id,fingerprint,result)
    VALUES($1,$2,$3,$4,$5,$6,$7::jsonb)`,[...scopeParams(scope),savedEpoch,context.actionId,fingerprint,JSON.stringify(result)])
  return result
}

export function contentOf(doc: MemoryDocument): MemoryContent {
  return { path: doc.path,title: doc.title,description: doc.description,body: doc.body,layer: doc.layer,kind: doc.kind,locked: doc.locked,validUntil: doc.validUntil }
}

export async function restoreMemory(database: SqlQueryable, scope: MemoryScope, id: string, expectedVersion: number, version: number, context: MemoryWriteContext) {
  if (!Number.isSafeInteger(version) || version < 1) throw new Error('invalid memory restore version')
  const current = await readMemory(database,scope,id)
  if (!current) throw new Error('memory is unavailable or stale')
  const row = (await database.query(`SELECT snapshot FROM lingxios.agent_memory_versions WHERE tenant_id=$1 AND memory_id=$2 AND version=$3`,[scope.tenantId,id,version])).rows[0]
  const target = current.version === version ? current : row ? memoryDocument(row['snapshot'] as Record<string,unknown>) : null
  if (!target || target.status !== 'active') throw new Error('memory version is unavailable or inactive')
  validateMemoryContent(contentOf(target))
  return applyMemoryChanges(database,scope,[{ action: 'update',id,expectedVersion,content: contentOf(target) }],context)
}

// Trusted native integrations use the same document writer; the public API is control.memory.
export type MemoryMutation = { method: 'note'; body: string; kind?: string; validUntil?: string }
  | { method: 'verify'; id: string; expectedVersion: number; validUntil?: string }
  | { method: 'pin'; id: string; expectedVersion: number; pinned: boolean }
  | { method: 'delete'; id: string; expectedVersion: number }
export async function writeMemory(database: SqlQueryable, scope: MemoryScope, mutation: MemoryMutation,
  provenance: { actionId: string; workId: string; request: import('../context/request.js').RequestSnapshot }, policy?: MemoryWritePolicy) {
  const request = provenance.request
  if (request.workId !== provenance.workId || request.tenantId !== scope.tenantId || !request.authorId) throw new Error('memory write requires request provenance')
  const [epoch] = await lockMemoryScopes(database,[scope])
  const identity = { tenantId: scope.tenantId,agentId: 'native',principalId: request.authorId,sessionId: request.sessionId,workId: provenance.workId }
  const context: MemoryWriteContext = { identity,actionId: provenance.actionId,epoch: epoch!.epoch,explicit: true,
    sources: [{ workId: provenance.workId,sourceRef: request.sourceRef,authorId: request.authorId,requestVersion: request.revisions.length+1,
      actionId: provenance.actionId,observedAt: new Date().toISOString() }],...policy ? { policy } : {} }
  let change: MemoryChange
  if (mutation.method === 'note') change = { action: 'create',content: { path: `notes/${memoryDigest(provenance.actionId)}.md`,
    title: 'Saved note',description: 'Explicitly saved memory',body: mutation.body,layer: 'reference',...mutation.kind ? { kind: mutation.kind } : {},
    ...mutation.validUntil ? { validUntil: mutation.validUntil } : {} } }
  else if (mutation.method === 'delete') change = { action: 'delete',id: mutation.id,expectedVersion: mutation.expectedVersion }
  else {
    const current = await readMemory(database,scope,mutation.id)
    if (!current) throw new Error('memory is unavailable')
    change = { action: 'update',id: mutation.id,expectedVersion: mutation.expectedVersion,content: { ...contentOf(current),
      ...mutation.method === 'pin' ? { locked: mutation.pinned } : mutation.validUntil ? { validUntil: mutation.validUntil } : {} } }
  }
  const result = await applyMemoryChanges(database,scope,[change],context)
  return mutation.method === 'delete' ? { id: mutation.id,deleted: true } : result.documents[0]!
}

/** Internal compatibility for the separately evaluated evolution path. */
export async function recallMemories(database: SqlQueryable, scope: MemoryScope, query: string, limit: number) {
  const result = await searchMemories(database,scope,query,limit)
  return Promise.all(result.items.map(item => readMemory(database,scope,(item as MemoryEntry).id)))
}
