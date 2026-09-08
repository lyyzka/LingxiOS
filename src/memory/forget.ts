import type { SqlQueryable } from '../control-plane/pg-store.js'
import { validateScope, type MemoryScope } from './store.js'

export interface MemoryEpoch extends MemoryScope { epoch: number; forgottenAt: string | null }

/** The caller holds one transaction through source validation and memory commit. */
export async function lockMemoryScopes(database: SqlQueryable, scopes: readonly MemoryScope[]): Promise<MemoryEpoch[]> {
  if (scopes.length > 12) throw new Error('too many memory scopes')
  const epochs: MemoryEpoch[] = []
  const key = (scope: MemoryScope) => JSON.stringify([scope.tenantId, scope.scopeType, scope.scopeId])
  for (const scope of [...scopes].sort((a, b) => key(a).localeCompare(key(b)))) {
    validateScope(scope)
    const params = [scope.tenantId, scope.scopeType, scope.scopeId]
    await database.query(`INSERT INTO lingxios.agent_memory_scopes(tenant_id,scope_type,scope_id)
      VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, params)
    const { rows } = await database.query(`SELECT epoch,forgotten_at FROM lingxios.agent_memory_scopes
      WHERE tenant_id=$1 AND scope_type=$2 AND scope_id=$3 FOR UPDATE`, params)
    const row = rows[0]!, epoch = Number(row['epoch'])
    if (!Number.isSafeInteger(epoch) || epoch < 0) throw new Error('invalid memory scope epoch')
    epochs.push({ ...scope, epoch, forgottenAt: row['forgotten_at'] == null ? null : new Date(row['forgotten_at'] as string | Date).toISOString() })
  }
  return epochs
}

export function sameMemoryEpochs(current: readonly MemoryEpoch[], recorded: readonly MemoryEpoch[]): boolean {
  return current.every(scope => scope.epoch === (recorded.find(item => item.tenantId === scope.tenantId
    && item.scopeType === scope.scopeType && item.scopeId === scope.scopeId)?.epoch ?? 0))
}

/** A source started before forgetting cannot be captured again with a fresh epoch. */
export async function currentMemoryScopes(database: SqlQueryable, epochs: readonly MemoryEpoch[], sourceWorkId: string) {
  const source = (await database.query('SELECT tenant_id,created_at FROM lingxios.agent_work_items WHERE id=$1', [sourceWorkId])).rows[0]
  if (!source) throw new Error('memory source work is unavailable')
  return epochs.filter(scope => scope.tenantId === source['tenant_id'] && (scope.forgottenAt === null
    || new Date(source['created_at'] as string | Date).getTime() > Date.parse(scope.forgottenAt)))
}

/** Trusted, already-authorized scope; call in the same transaction as the deletion. */
export async function forgetMemoryScope(database: SqlQueryable, scope: MemoryScope, purge = true): Promise<{ epoch: number }> {
  await lockMemoryScopes(database, [scope])
  // ponytail: invalidate all old sources in this scope; use source tombstones if selective resynthesis is needed.
  const updated = await database.query(`UPDATE lingxios.agent_memory_scopes SET epoch=epoch+1,forgotten_at=clock_timestamp()
    WHERE tenant_id=$1 AND scope_type=$2 AND scope_id=$3 RETURNING epoch`, [scope.tenantId, scope.scopeType, scope.scopeId])
  await database.query(`UPDATE lingxios.agent_memory_evidence SET status='superseded',input_text='',assistant_text='',search_text=''
    WHERE tenant_id=$1 AND scopes @> $2::jsonb`, [scope.tenantId, JSON.stringify([scope])])
  await database.query(`UPDATE lingxios.agent_memory_evidence_scopes SET status='superseded'
    WHERE source_run_id IN (SELECT source_run_id FROM lingxios.agent_memory_evidence WHERE tenant_id=$1 AND status='superseded')`, [scope.tenantId])
  for (const table of ['agent_memory_commands','agent_memory_reviews','agent_memory_conflicts']) {
    await database.query(`DELETE FROM lingxios.${table} WHERE tenant_id=$1 AND scope_type=$2 AND scope_id=$3`,[scope.tenantId,scope.scopeType,scope.scopeId])
  }
  await database.query(`UPDATE lingxios.agent_work_items SET cancel_requested_at=clock_timestamp(),
    status=CASE WHEN status='queued' THEN 'cancelled' ELSE status END,meta=meta-'memorySnapshot'
    WHERE tenant_id=$1 AND kind='memory_synthesis' AND meta->>'scopeType'=$2 AND meta->>'scopeId'=$3
      AND status IN ('queued','leased','failed')`,[scope.tenantId,scope.scopeType,scope.scopeId])
  if (purge) await database.query('DELETE FROM lingxios.agent_memories WHERE tenant_id=$1 AND scope_type=$2 AND scope_id=$3',
    [scope.tenantId, scope.scopeType, scope.scopeId])
  return { epoch: Number(updated.rows[0]!['epoch']) }
}
