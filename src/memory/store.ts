import type { SqlQueryable } from '../control-plane/pg-store.js'
import { createHash } from 'node:crypto'

export type MemoryScopeType = 'learner' | 'course' | 'agent_role'
export interface MemoryScope { tenantId: string; scopeType: MemoryScopeType; scopeId: string }
export interface MemorySnapshot {
  id: string
  status: 'available' | 'unavailable'
  items: Array<Record<string, unknown>>
  omitted: number
}

/** Keep complete records, sharing the context budget across the three scopes. */
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

/** Scope must already have been resolved and authorized by the packaged integration. */
export async function recallMemories(database: SqlQueryable, scope: MemoryScope, query: string, limit: number) {
  // Deterministic list/fallback path; configured semantic recall lives in semantic.ts.
  const { rows } = await database.query(`SELECT id,body,kind,origin,pinned,version,source_refs,valid_until,updated_at
    FROM lingxios.agent_memories WHERE tenant_id=$1 AND scope_type=$2 AND scope_id=$3
      AND status='active' AND (valid_until IS NULL OR valid_until>NOW())
      AND ($4::text='' OR strpos(lower(body),lower($4))>0)
    ORDER BY pinned DESC,updated_at DESC,id LIMIT $5`, [scope.tenantId, scope.scopeType, scope.scopeId, query, limit])
  return rows
}
