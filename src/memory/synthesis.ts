import { createHash } from 'node:crypto'
import type { SqlQueryable } from '../control-plane/pg-store.js'
import type { WorkItem } from '../protocol/types.js'
import { snapshotMemories, type MemoryScope, type MemoryScopeType } from './store.js'
import { lockMemoryScopes, sameMemoryEpochs, type MemoryEpoch } from './forget.js'
import { memoryWriteBody, type MemoryWritePolicy } from './policy.js'

export interface MemoryChange {
  action: 'create' | 'update' | 'expire'
  scopeType: MemoryScopeType
  sourceRunIds: string[]
  id?: string
  expectedVersion?: number
  body?: string
  kind?: string
  validUntil?: string
}
export interface MemoryBatch {
  evolutionEnabled?: boolean
  evidence: Record<string, unknown>
  currentMemories: Array<Record<string, unknown>>
  omittedMemories: number
  scopes: MemoryScope[]
}

export function parseMemoryChanges(value: unknown): MemoryChange[] {
  if (!Array.isArray(value) || value.length > 12) throw new Error('memory synthesis requires at most 12 changes')
  const ids = new Set<string>()
  for (const item of value) {
    if (!item || typeof item !== 'object' || Array.isArray(item)
      || Object.keys(item).some(key => !['action','scopeType','sourceRunIds','id','expectedVersion','body','kind','validUntil'].includes(key))
      || !['create','update','expire'].includes(item.action) || typeof item.scopeType !== 'string' || !item.scopeType.trim() || item.scopeType.length > 1000
      || !Array.isArray(item.sourceRunIds) || item.sourceRunIds.length !== 1
      || typeof item.sourceRunIds[0] !== 'string' || !item.sourceRunIds[0] || item.sourceRunIds[0].length > 1000
      || ['experience','skill','strategy'].includes(item.kind)) throw new Error('invalid memory synthesis change')
    if (item.action === 'create') {
      if (item.id !== undefined || item.expectedVersion !== undefined) throw new Error('new memory cannot supply an identity')
    } else {
      if (typeof item.id !== 'string' || !item.id || item.id.length > 1000 || !Number.isSafeInteger(item.expectedVersion)
        || item.expectedVersion < 1 || ids.has(item.id)) throw new Error('memory change requires a unique identity and version')
      ids.add(item.id)
    }
    if (item.action !== 'expire' && (typeof item.body !== 'string' || !item.body.trim() || item.body.length > 500)) throw new Error('invalid synthesized memory body')
    if (item.kind !== undefined && (typeof item.kind !== 'string' || !/^[a-z_]{1,32}$/.test(item.kind))) throw new Error('invalid synthesized memory kind')
    if (item.validUntil !== undefined && (typeof item.validUntil !== 'string' || item.validUntil.length > 40
      || !/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,3})?(?:Z|[+-]\d\d:\d\d)$/.test(item.validUntil)
      || !Number.isFinite(Date.parse(item.validUntil)) || Date.parse(item.validUntil) <= Date.now())) throw new Error('invalid synthesized memory expiry')
    if (item.action === 'expire' && (item.body !== undefined || item.kind !== undefined || item.validUntil !== undefined)) throw new Error('expiry cannot replace memory content')
  }
  return value as MemoryChange[]
}

/** Integration must authorize the live principal, conversation and memory permission before calling. */
export async function executeMemorySynthesis(client: SqlQueryable, work: Omit<WorkItem, 'leaseToken'>,
  method: string, args: Record<string, unknown>, authorizedScopes: readonly MemoryScope[], policy?: MemoryWritePolicy): Promise<MemoryBatch | { outcome: string; changeCount: number } | null> {
  if (work.kind !== 'memory_synthesis' || !work.principalId || typeof work.meta?.['sourceRunId'] !== 'string'
    || !['load','apply'].includes(method)) throw new Error('invalid memory synthesis work')
  if (Object.keys(args).some(key => !(method === 'load' ? [] : ['changes','approved','confidence']).includes(key))) throw new Error('invalid memory synthesis arguments')
  const changes = method === 'apply' ? parseMemoryChanges(args['changes']) : []
  const epochs = await lockMemoryScopes(client, authorizedScopes)
  if (method === 'apply' && (typeof args['approved'] !== 'boolean' || typeof args['confidence'] !== 'number'
    || !Number.isFinite(args['confidence']) || args['confidence'] < 0 || args['confidence'] > 1)) throw new Error('invalid memory verification')
    // Source and synthesis leases are checked under locks, so cancellation and steering cannot race the commit.
    const { rows } = await client.query(`SELECT e.*,source.created_at::text AS observed_at,
      source.steer_inputs->-1->>'createdAt' AS revised_at,job.meta->'memorySnapshot' AS memory_snapshot FROM lingxios.agent_work_items job
      JOIN lingxios.agent_work_items source ON source.id=job.meta->>'sourceRunId'
      JOIN lingxios.agent_memory_evidence e ON e.source_run_id=source.id
      WHERE job.id=$1 AND job.fence=$2 AND job.status='leased' AND job.lease_expires_at>NOW() AND job.cancel_requested_at IS NULL
        AND job.kind='memory_synthesis' AND job.tenant_id=$3 AND job.agent_id=$4 AND job.principal_id=$5 AND job.session_id=$6
        AND source.tenant_id=job.tenant_id AND source.agent_id=job.agent_id AND source.principal_id=job.principal_id
        AND source.session_id=job.session_id AND source.thread_id IS NOT DISTINCT FROM job.thread_id
        AND source.status IN ('succeeded','partial') AND source.cancel_requested_at IS NULL
        AND e.tenant_id=job.tenant_id AND e.agent_id=job.agent_id AND e.principal_id=job.principal_id AND e.session_id=job.session_id
        AND e.request_version=jsonb_array_length(source.steer_inputs)+1
      FOR UPDATE OF job,source,e`, [work.id, work.fence, work.tenantId, work.agentId, work.principalId, work.sessionId])
    const evidence = rows[0]
    if (!evidence) throw new Error('memory synthesis lease or source is unavailable')
    const revisedAt = evidence['revised_at']
    if (typeof revisedAt === 'string' && /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?(?:Z|[+-]\d\d:\d\d)$/.test(revisedAt)) {
      const time = Date.parse(revisedAt)
      // Only persisted server revisions preceding the committed evidence can advance observation time.
      if (Number.isFinite(time) && time > Date.parse(String(evidence['observed_at']))
        && time <= new Date(evidence['created_at'] as string | Date).getTime()) evidence['observed_at'] = revisedAt
    }
    delete evidence['revised_at']
    const previousSnapshot = evidence['memory_snapshot'] as { fence?: number; memories?: Array<{ id: string; version: number }>; epochs?: MemoryEpoch[] } | undefined
    delete evidence['memory_snapshot']
    if (evidence['status'] !== 'pending') return method === 'load' ? null : { outcome: String(evidence['status']), changeCount: 0 }
    const recordedScopes = evidence['scopes'] as MemoryScope[]
    const scopes = authorizedScopes.filter(scope => scope.tenantId === work.tenantId
      && recordedScopes.some(recorded => recorded.tenantId === scope.tenantId && recorded.scopeType === scope.scopeType && recorded.scopeId === scope.scopeId))
    if (!scopes.length || scopes.length > 12 || new Set(scopes.map(scope => scope.scopeType)).size !== scopes.length) throw new Error('memory evidence scope is unavailable')
    const currentEpochs = epochs.filter(scope => scopes.some(item => item.scopeType === scope.scopeType && item.scopeId === scope.scopeId))
    if (!sameMemoryEpochs(currentEpochs, evidence['scope_epochs'] as MemoryEpoch[] ?? [])) {
      return method === 'load' ? null : { outcome: 'forgotten', changeCount: 0 }
    }
    const scopeIds = Object.fromEntries(scopes.map(scope => [scope.scopeType,scope.scopeId]))
    if (method === 'load') {
      const groups = []
      for (const scope of scopes) {
        for (const expired of [false, true]) {
          const memories = await client.query(`SELECT id,body,kind,origin,pinned,version,source_refs,valid_until,updated_at,status,
            (status='expired' OR valid_until<=NOW()) IS TRUE AS "needsReverification"
            FROM lingxios.agent_memories WHERE tenant_id=$1 AND scope_type=$2 AND scope_id=$3 AND origin<>'evolved'
              AND ((status='expired' OR valid_until<=NOW()) IS TRUE)=$4
            ORDER BY pinned DESC,updated_at DESC,id LIMIT 12`, [scope.tenantId, scope.scopeType, scope.scopeId, expired])
          groups.push({ scope, items: memories.rows })
        }
      }
      const snapshot = snapshotMemories(groups)
      await client.query(`UPDATE lingxios.agent_work_items SET meta=jsonb_set(meta,'{memorySnapshot}',$2::jsonb) WHERE id=$1`,
      [work.id, JSON.stringify({ fence: work.fence, epochs: currentEpochs, memories: snapshot.items.map(item => ({ id: item['id'], version: item['version'] })) })])
      const excerpt = (value: unknown) => String(value).slice(0, 4000).replace(/[\uD800-\uDBFF]$/, '')
      return { evidence: { ...evidence, input_text: excerpt(evidence['input_text']), assistant_text: excerpt(evidence['assistant_text']),
        input_truncated: evidence['input_truncated'] === true || String(evidence['input_text']).length > 4000,
        assistant_truncated: evidence['assistant_truncated'] === true || String(evidence['assistant_text']).length > 4000 },
        currentMemories: snapshot.items, omittedMemories: snapshot.omitted, scopes }
    }
    if (changes.some(change => change.sourceRunIds[0] !== evidence['source_run_id'])) throw new Error('memory change uses unknown evidence')
    if (changes.some(change => !Object.hasOwn(scopeIds, change.scopeType))) throw new Error('memory change uses an unauthorized scope')
    if (previousSnapshot?.fence !== work.fence || !Array.isArray(previousSnapshot.memories) || !previousSnapshot.epochs
      || !sameMemoryEpochs(currentEpochs, previousSnapshot.epochs)
      || changes.some(change => change.action !== 'create' && !previousSnapshot.memories!.some(item => item.id === change.id && item.version === change.expectedVersion))) {
      throw new Error('memory synthesis requires the loaded snapshot and versions')
    }
    const approved = args['approved'] === true && Number(args['confidence']) >= 0.6
    if (approved) {
      const source = JSON.stringify([{ workId: evidence['source_run_id'], requestVersion: evidence['request_version'],
        sourceRef: evidence['source_ref'], authorId: evidence['principal_id'], inputSha256: evidence['input_sha256'],
        synthesisWorkId: work.id, confidence: args['confidence'] }])
      for (const [index, change] of changes.entries()) {
        const scopeId = scopeIds[change.scopeType]
        const body = change.action === 'expire' ? null : await memoryWriteBody({ scope: { tenantId: work.tenantId, scopeType: change.scopeType, scopeId: scopeId! },
          principalId: work.principalId, sourceWorkId: String(evidence['source_run_id']), origin: 'synthesized', kind: change.kind ?? 'observation', body: change.body! }, policy)
        if (change.action === 'create') {
          const id = `mem-${createHash('sha256').update(JSON.stringify([work.id,index])).digest('hex')}`
          await client.query(`INSERT INTO lingxios.agent_memories(tenant_id,id,scope_type,scope_id,body,kind,origin,source_refs,valid_until)
            VALUES($1,$2,$3,$4,$5,$6,'synthesized',$7::jsonb,$8::timestamptz)`,
          [work.tenantId, id, change.scopeType, scopeId, body, change.kind ?? 'observation', source, change.validUntil ?? null])
        } else {
          const updated = await client.query(`UPDATE lingxios.agent_memories SET version=version+1,updated_at=NOW(),
            body=COALESCE($6,body),kind=COALESCE($7,kind),valid_until=COALESCE($8::timestamptz,valid_until),
            status=CASE WHEN $9::boolean THEN 'expired' ELSE 'active' END,source_refs=source_refs||$10::jsonb
            WHERE tenant_id=$1 AND id=$2 AND scope_type=$3 AND scope_id=$4 AND version=$5
              AND origin='synthesized' AND NOT pinned AND jsonb_array_length(source_refs)<64
              AND (($9::boolean AND status='active') OR (NOT $9::boolean AND (
                (status='active' AND (valid_until IS NULL OR valid_until>NOW()))
                OR ($8::timestamptz>NOW() AND $11::timestamptz>CASE WHEN status='expired' THEN updated_at ELSE valid_until END)))) RETURNING id`,
          [work.tenantId, change.id, change.scopeType, scopeId, change.expectedVersion, body,
            change.kind ?? null, change.validUntil ?? null, change.action === 'expire', source, evidence['observed_at']])
          if (updated.rows.length !== 1) throw new Error('memory is stale, protected, or unavailable')
        }
      }
    }
    await client.query('UPDATE lingxios.agent_memory_evidence SET status=$2 WHERE source_run_id=$1', [evidence['source_run_id'], approved ? 'processed' : 'rejected'])
    return { outcome: approved ? 'committed' : 'rejected', changeCount: approved ? changes.length : 0 }
}
