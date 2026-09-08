import type { SqlQueryable } from '../control-plane/pg-store.js'
import { createHash } from 'node:crypto'
import { memoryWriteBody, type MemoryWritePolicy } from './policy.js'
import { currentMemoryScopes, forgetMemoryScope, lockMemoryScopes } from './forget.js'

export type MemoryScopeType = string
export interface MemoryScope { tenantId: string; scopeType: MemoryScopeType; scopeId: string }
export interface MemorySnapshot {
  id: string
  status: 'available' | 'unavailable'
  items: Array<Record<string, unknown>>
  omitted: number
}

/** Keep complete records, sharing the context budget across authorized scopes. */
export function snapshotMemories(groups: Array<{ scope: MemoryScope; items: Array<Record<string, unknown>> }>): MemorySnapshot {
  const items: Array<Record<string, unknown>> = []
  let remaining = 11_500 // Reserve space for snapshot metadata and array separators.
  let omitted = 0
  for (let index = 0; index < 12; index++) {
    for (const group of groups) {
      const row = group.items[index]
      if (!row) continue
      const item = { ...row, scopeType: group.scope.scopeType, scopeId: group.scope.scopeId }
      const bytes = Buffer.byteLength(JSON.stringify(item))
      if (bytes > remaining) { omitted++; continue }
      remaining -= bytes
      items.push(item)
    }
  }
  return { id: `memory:${createHash('sha256').update(JSON.stringify({ items, omitted })).digest('hex')}`, status: 'available', items, omitted }
}

/** Scope must already have been resolved and authorized by the product. */
export async function recallMemories(database: SqlQueryable, scope: MemoryScope, query: string, limit: number) {
  validateScope(scope)
  if (query.length > 2000 || !Number.isSafeInteger(limit) || limit < 1 || limit > 12) throw new Error('invalid memory query or limit')
  // Deterministic list/fallback path; configured semantic recall lives in semantic.ts.
  const { rows } = await database.query(`SELECT id,body,kind,origin,pinned,version,source_refs,valid_until,updated_at
    FROM lingxios.agent_memories WHERE tenant_id=$1 AND scope_type=$2 AND scope_id=$3
      AND status='active' AND origin<>'evolved' AND (valid_until IS NULL OR valid_until>NOW())
      AND ($4::text='' OR strpos(lower(body),lower($4))>0)
    ORDER BY pinned DESC,updated_at DESC,id LIMIT $5`, [scope.tenantId, scope.scopeType, scope.scopeId, query, limit])
  return rows
}

export function validateScope(scope: MemoryScope) {
  if (![scope.tenantId,scope.scopeType,scope.scopeId].every(value => typeof value === 'string' && value.trim() && value.length <= 1000)) throw new Error('invalid memory scope')
}

export async function readMemory(database: SqlQueryable, scope: MemoryScope, id: string) {
  validateScope(scope)
  return (await database.query(`SELECT id,body,kind,origin,pinned,version,source_refs,valid_until,updated_at,status
    FROM lingxios.agent_memories WHERE tenant_id=$1 AND scope_type=$2 AND scope_id=$3 AND id=$4`,
  [scope.tenantId,scope.scopeType,scope.scopeId,id])).rows[0] ?? null
}

export type MemoryMutation = { method: 'note'; body: string; kind?: string; validUntil?: string }
  | { method: 'verify'; id: string; expectedVersion: number; validUntil?: string }
  | { method: 'pin'; id: string; expectedVersion: number; pinned: boolean }
  | { method: 'delete'; id: string; expectedVersion: number }

/** Use the action's database transaction so its receipt and this mutation commit together. */
export async function writeMemory(database: SqlQueryable, scope: MemoryScope, mutation: MemoryMutation,
  provenance: { actionId: string; workId: string; request: import('../context/request.js').RequestSnapshot }, policy?: MemoryWritePolicy) {
  validateScope(scope)
  const { request } = provenance
  if (request.workId !== provenance.workId || request.tenantId !== scope.tenantId || !provenance.actionId || !request.authorId) throw new Error('memory write requires request provenance')
  if (!['note','verify','pin','delete'].includes(mutation.method)) throw new Error('invalid memory mutation')
  const epochs = await lockMemoryScopes(database, [scope])
  if (mutation.method !== 'delete' && !(await currentMemoryScopes(database, epochs, provenance.workId)).length) throw new Error('memory source predates forgetting')
  const validUntil = 'validUntil' in mutation ? mutation.validUntil : undefined
  if (validUntil !== undefined && (!/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,3})?(?:Z|[+-]\d\d:\d\d)$/.test(validUntil)
    || !Number.isFinite(Date.parse(validUntil)) || Date.parse(validUntil) <= Date.now())) throw new Error('memory expiry must be a future ISO timestamp')
  const source = JSON.stringify([{ workId: provenance.workId, requestVersion: request.revisions.length + 1,
    sourceRef: request.sourceRef, authorId: request.authorId, actionId: provenance.actionId,
    inputSha256: createHash('sha256').update(JSON.stringify({ originalText: request.originalText,
      inheritedRevisions: request.inheritedRevisions, revisions: request.revisions, attachments: request.attachments })).digest('hex') }])
  if (mutation.method === 'note') {
    if (!mutation.body.trim() || mutation.body.length > 2000 || !/^[a-z_]{1,32}$/.test(mutation.kind ?? 'observation')) throw new Error('invalid memory body or kind')
    const body = await memoryWriteBody({ scope, principalId: request.authorId, sourceWorkId: provenance.workId,
      origin: 'explicit', kind: mutation.kind ?? 'observation', body: mutation.body }, policy)
    const id = `mem-${createHash('sha256').update(provenance.actionId).digest('hex')}`
    const saved = await database.query(`INSERT INTO lingxios.agent_memories
      (tenant_id,id,scope_type,scope_id,body,kind,origin,source_refs,valid_until)
      VALUES($1,$2,$3,$4,$5,$6,'explicit',$7::jsonb,$8::timestamptz) RETURNING id,body,kind,origin,pinned,version,source_refs,valid_until,status`,
    [scope.tenantId,id,scope.scopeType,scope.scopeId,body,mutation.kind ?? 'observation',source,validUntil ?? null])
    return saved.rows[0]!
  }
  if (!mutation.id || !Number.isSafeInteger(mutation.expectedVersion) || mutation.expectedVersion < 1) throw new Error('memory changes require an identity and version')
  const identity = [scope.tenantId,mutation.id,scope.scopeType,scope.scopeId,mutation.expectedVersion]
  if (mutation.method === 'delete') {
    const deleted = await database.query(`DELETE FROM lingxios.agent_memories
      WHERE tenant_id=$1 AND id=$2 AND scope_type=$3 AND scope_id=$4 AND version=$5 AND origin<>'evolved' RETURNING id`, identity)
    if (!deleted.rows.length) throw new Error('memory is unavailable or stale')
    await forgetMemoryScope(database, scope, false)
    return { id: mutation.id, deleted: true }
  }
  if (mutation.method === 'pin' && typeof mutation.pinned !== 'boolean') throw new Error('invalid memory pin')
  const saved = await database.query(`UPDATE lingxios.agent_memories SET version=version+1,updated_at=NOW(),
    pinned=COALESCE($6::boolean,pinned),valid_until=COALESCE($7::timestamptz,valid_until),
    status=CASE WHEN $8::boolean THEN 'active' ELSE status END,source_refs=source_refs||$9::jsonb
    WHERE tenant_id=$1 AND id=$2 AND scope_type=$3 AND scope_id=$4 AND version=$5 AND origin<>'evolved' AND jsonb_array_length(source_refs)<64
      AND (NOT $8::boolean OR (origin='explicit' AND (valid_until IS NULL OR valid_until>NOW() OR $7::timestamptz IS NOT NULL)))
    RETURNING id,body,kind,origin,pinned,version,source_refs,valid_until,status`,
  [...identity,mutation.method === 'pin' ? mutation.pinned : null,validUntil ?? null,mutation.method === 'verify',source])
  if (!saved.rows.length) throw new Error('memory is unavailable, stale, protected, or at its provenance limit')
  return saved.rows[0]!
}
