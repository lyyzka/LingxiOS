import { lockAction, recordActionResult } from '../../control-plane/action-transaction.js'
import { createHash } from 'node:crypto'
import { recallMemories, snapshotMemories, type MemoryScope, type MemoryScopeType } from '../../memory/store.js'
import { sessionKeyOf, type HostAction, type WorkItem } from '../../protocol/types.js'
import { withTransaction, type SqlPool } from '../../control-plane/pg-store.js'
import type { RequestSnapshot } from '../../context/request.js'
import type { LingxiLoopServices } from './service-contracts.js'
import type { SemanticMemory } from '../../memory/semantic.js'

export const MEMORY_METHODS = {
  list: ['scope', 'learnerId', 'limit'], recall: ['scope', 'learnerId', 'query', 'limit'],
  note: ['scope', 'learnerId', 'body', 'kind', 'validUntil'],
  verify: ['scope', 'learnerId', 'id', 'expectedVersion', 'validUntil'],
  pin: ['scope', 'learnerId', 'id', 'expectedVersion', 'pinned'],
  delete: ['scope', 'learnerId', 'id', 'expectedVersion'],
} as const

export async function recallMemoryContext(work: Omit<WorkItem, 'leaseToken'>,
  services: Pick<LingxiLoopServices, 'permissionService'>, database: SqlPool, semantic?: SemanticMemory) {
  if (!work.principalId) throw new Error('missing memory recall principal')
  await services.permissionService.assertCan({ actorUserId: work.principalId, companyId: work.tenantId,
    action: 'agent_memory:read', resource: { type: 'conversation', id: work.sessionId } })
  const { rows } = await database.query(`SELECT 1 FROM participants p JOIN im_channel_bindings b ON b.company_id=p.company_id
    WHERE p.id=$1 AND p.company_id=$2 AND p.kind='human' AND p.departed_at IS NULL
      AND b.channel_id=$3 AND b.profile->'members' ? p.id`, [work.principalId, work.tenantId, work.sessionId])
  const scopes: MemoryScope[] = [
    ...(rows.length ? [{ tenantId: work.tenantId, scopeType: 'learner' as const, scopeId: work.principalId }] : []),
    { tenantId: work.tenantId, scopeType: 'course', scopeId: work.sessionId },
    { tenantId: work.tenantId, scopeType: 'agent_role', scopeId: work.agentId },
  ]
  if (!semantic) return snapshotMemories(await Promise.all(scopes.map(async scope => ({ scope, items: await recallMemories(database, scope, '', 12) }))))
  const saved = await database.query(`SELECT request_snapshot FROM lingxios.agent_request_snapshots
    WHERE session_key=$1 AND work_id=$2`, [sessionKeyOf(work), work.id])
  const request = saved.rows[0]?.['request_snapshot'] as RequestSnapshot | undefined
  const query = (request?.workId === work.id ? `${request.revisions.at(-1)?.text ?? ''}\n${request.originalText}` : String(work.meta?.['text'] ?? '')).slice(0, 2000).replace(/[\uD800-\uDBFF]$/, '')
  const groups = []
  for (const scope of scopes) groups.push({ scope, items: await semantic.recall(work, scope, query, 12) })
  return snapshotMemories(groups)
}

export async function executeMemory(work: Omit<WorkItem, 'leaseToken'>, action: HostAction,
  services: Pick<LingxiLoopServices, 'permissionService'>, database: SqlPool, semantic?: SemanticMemory) {
  const method = action.action.slice('memory.'.length)
  if (!work.principalId || !action.action.startsWith('memory.') || !Object.hasOwn(MEMORY_METHODS, method)) throw new Error('unsupported memory action')
  const allowed: readonly string[] = MEMORY_METHODS[method as keyof typeof MEMORY_METHODS]
  if (Object.keys(action.args).some(key => !allowed.includes(key))) throw new Error('unknown memory argument')
  const scopeType = action.args['scope'] ?? 'course'
  if (scopeType !== 'learner' && scopeType !== 'course' && scopeType !== 'agent_role') throw new Error('invalid memory scope')
  const learnerId = action.args['learnerId']
  if (scopeType !== 'learner' && learnerId !== undefined) throw new Error('learnerId requires learner scope')
  if (scopeType === 'learner' && (typeof learnerId !== 'string' || !learnerId.trim())) throw new Error('learnerId is required')
  const write = method !== 'list' && method !== 'recall'
  await services.permissionService.assertCan({ actorUserId: work.principalId, companyId: work.tenantId,
    action: write ? 'agent_memory:write' : 'agent_memory:read', resource: { type: 'conversation', id: work.sessionId } })
  if (scopeType === 'learner') {
    const { rows } = await database.query(`SELECT 1 FROM participants p JOIN im_channel_bindings b ON b.company_id=p.company_id
      WHERE p.id=$1 AND p.company_id=$2 AND p.kind='human' AND p.departed_at IS NULL
        AND b.channel_id=$3 AND b.profile->'members' ? p.id`, [learnerId, work.tenantId, work.sessionId])
    if (!rows.length) throw new Error('learner is not an active human member of this conversation')
  }
  const scope = { tenantId: work.tenantId, scopeType: scopeType as MemoryScopeType,
    scopeId: scopeType === 'course' ? work.sessionId : scopeType === 'agent_role' ? work.agentId : learnerId as string }
  if (!write) {
    const query = action.args['query'] ?? '', limit = action.args['limit'] ?? 12
    if (typeof query !== 'string' || query.length > 2000 || !Number.isSafeInteger(limit) || Number(limit) < 1 || Number(limit) > 12) throw new Error('invalid memory query or limit')
    return semantic ? semantic.recall(work, scope, query, Number(limit)) : recallMemories(database, scope, query, Number(limit))
  }
  const validUntil = action.args['validUntil']
  if (validUntil !== undefined && (typeof validUntil !== 'string' || validUntil.length > 40
    || !/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,3})?(?:Z|[+-]\d\d:\d\d)$/.test(validUntil)
    || !Number.isFinite(Date.parse(validUntil)) || Date.parse(validUntil) <= Date.now())) throw new Error('validUntil must be a future ISO timestamp')
  return withTransaction(database, async client => {
    await lockAction(client, work, action)
    const { rows } = await client.query(`SELECT snapshot.request_snapshot FROM lingxios.agent_work_items w
      JOIN lingxios.agent_os_sessions s ON s.session_key=$4
      JOIN lingxios.agent_request_snapshots snapshot ON snapshot.work_id=w.id AND snapshot.session_key=s.session_key
      WHERE w.id=$1 AND w.fence=$2 AND w.status='leased' AND w.lease_expires_at>NOW() AND w.cancel_requested_at IS NULL
        AND w.principal_id=$3
        AND jsonb_array_length(snapshot.request_snapshot->'revisions')=jsonb_array_length(w.steer_inputs)
      FOR UPDATE OF w`, [work.id, work.fence, work.principalId, sessionKeyOf(work)])
    const request = rows[0]?.['request_snapshot'] as RequestSnapshot | undefined
    if (!request) throw new Error('memory write requires the current leased request')
    const source = { workId: work.id, requestVersion: request.revisions.length + 1, sourceRef: request.sourceRef,
      authorId: request.authorId, inputSha256: createHash('sha256').update(JSON.stringify({ originalText: request.originalText, revisions: request.revisions, attachments: request.attachments })).digest('hex') }
    if (method === 'note') {
      const body = action.args['body'], kind = action.args['kind'] ?? 'observation'
      if (typeof body !== 'string' || !body.trim() || body.length > 2000
        || typeof kind !== 'string' || !/^[a-z_]{1,32}$/.test(kind)) throw new Error('invalid memory body or kind')
      const id = `mem-${createHash('sha256').update(action.idempotencyKey).digest('hex')}`
      const saved = await client.query(`INSERT INTO lingxios.agent_memories
        (tenant_id,id,scope_type,scope_id,body,kind,origin,source_refs,valid_until)
        VALUES($1,$2,$3,$4,$5,$6,'explicit',$7::jsonb,$8::timestamptz)
        ON CONFLICT(tenant_id,id) DO NOTHING RETURNING id,body,kind,origin,version,source_refs,valid_until`,
      [work.tenantId, id, scopeType, scope.scopeId, body.trim(), kind, JSON.stringify([source]), validUntil ?? null])
      if (!saved.rows.length) throw new Error('memory note already exists; reconcile its original action receipt')
      return recordActionResult(client, action, saved.rows[0])
    }
    const id = action.args['id'], version = action.args['expectedVersion']
    if (typeof id !== 'string' || !id || !Number.isSafeInteger(version) || Number(version) < 1) throw new Error('memory changes require id and expectedVersion')
    if (method === 'delete') {
      const deleted = await client.query(`DELETE FROM lingxios.agent_memories
        WHERE tenant_id=$1 AND id=$2 AND scope_type=$3 AND scope_id=$4 AND version=$5 RETURNING id`,
      [work.tenantId, id, scopeType, scope.scopeId, version])
      if (!deleted.rows.length) throw new Error('memory is unavailable or stale')
      return recordActionResult(client, action, { id, deleted: true })
    }
    if (method === 'pin') {
      const pinned = action.args['pinned']
      if (typeof pinned !== 'boolean') throw new Error('pinned must be a boolean')
      const saved = await client.query(`UPDATE lingxios.agent_memories SET pinned=$6,version=version+1,
        source_refs=source_refs||$7::jsonb,updated_at=NOW()
        WHERE tenant_id=$1 AND id=$2 AND scope_type=$3 AND scope_id=$4 AND version=$5
          AND jsonb_array_length(source_refs)<64
        RETURNING id,body,kind,origin,pinned,version,source_refs,valid_until`,
      [work.tenantId, id, scopeType, scope.scopeId, version, pinned, JSON.stringify([source])])
      if (!saved.rows.length) throw new Error('memory is unavailable, stale, or at its provenance limit')
      return recordActionResult(client, action, saved.rows[0])
    }
    const saved = await client.query(`UPDATE lingxios.agent_memories SET version=version+1,status='active',
      valid_until=COALESCE($6::timestamptz,valid_until),source_refs=source_refs||$7::jsonb,updated_at=NOW()
      WHERE tenant_id=$1 AND id=$2 AND scope_type=$3 AND scope_id=$4 AND version=$5 AND origin='explicit'
        AND jsonb_array_length(source_refs)<64
        AND (valid_until IS NULL OR valid_until>NOW() OR $6::timestamptz IS NOT NULL)
      RETURNING id,body,kind,origin,version,source_refs,valid_until`,
    [work.tenantId, id, scopeType, scope.scopeId, version, validUntil ?? null, JSON.stringify([source])])
    if (!saved.rows.length) throw new Error('memory is unavailable, stale, non-explicit, or at its provenance limit')
    return recordActionResult(client, action, saved.rows[0])
  })
}
