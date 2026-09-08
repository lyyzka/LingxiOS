import type { SqlQueryable } from '../control-plane/pg-store.js'
import type { WorkItem } from '../protocol/types.js'
import { applyMemoryChanges, memoryDocument, parseDocumentChanges, scopeParams } from './store.js'
import { lockMemoryScopes } from './forget.js'
import { authorizedScopes, sameScope, sourceIdentity, type MemoryOptions } from './access.js'
import { excerpt, memoryDigest, memoryQuery } from './text.js'
import { memoryWriteBody } from './policy.js'
import type { MemoryChange, MemoryDocument, MemoryIdentity, MemoryScope, MemorySource } from './types.js'

export interface SynthesisChange { change: MemoryChange; sourceRunIds: string[] }
export interface MemoryConflict { sourceRunIds: string[]; memoryIds: string[]; reason: string }
export interface MemoryEvidence {
  sourceRunId: string
  sourceRef: string
  requestVersion: number
  inputSha256: string
  userText: string
  assistantText: string
  truncated: boolean
  observedAt: string
}
export interface MemoryBatch {
  scope: MemoryScope
  epoch: number
  evidence: MemoryEvidence[]
  currentMemories: MemoryDocument[]
  omittedMemories: number
  evolutionEnabled: boolean
}

export function parseMemoryChanges(value: unknown): SynthesisChange[] {
  if (!Array.isArray(value) || value.length>12) throw new Error('memory synthesis requires at most 12 changes')
  for (const item of value) {
    if (!item || typeof item !== 'object' || Object.keys(item).some(key => !['change','sourceRunIds'].includes(key))
      || !Array.isArray(item.sourceRunIds) || !item.sourceRunIds.length || item.sourceRunIds.length>20
      || item.sourceRunIds.some((id: unknown) => typeof id !== 'string' || !id || id.length>1000)
      || new Set(item.sourceRunIds).size !== item.sourceRunIds.length) throw new Error('invalid synthesis sources')
    parseDocumentChanges([item.change])
    if (item.change.action === 'delete') throw new Error('automatic synthesis cannot delete memory')
  }
  parseDocumentChanges(value.map(item => item.change))
  return value as SynthesisChange[]
}
export function parseMemoryConflicts(value: unknown): MemoryConflict[] {
  if (!Array.isArray(value) || value.length>12) throw new Error('too many memory conflicts')
  for (const item of value) {
    if (!item || typeof item !== 'object' || Object.keys(item).some(key => !['sourceRunIds','memoryIds','reason'].includes(key))
      || !Array.isArray(item.sourceRunIds) || !item.sourceRunIds.length || item.sourceRunIds.length>20
      || !Array.isArray(item.memoryIds) || item.memoryIds.length>12
      || [...item.sourceRunIds,...item.memoryIds].some(id => typeof id !== 'string' || !id || id.length>1000)
      || typeof item.reason !== 'string' || !item.reason.trim() || Buffer.byteLength(item.reason)>4096) throw new Error('invalid memory conflict')
  }
  return value as MemoryConflict[]
}

/** All locks live only through load/apply transactions; model calls never hold database locks. */
export async function executeMemorySynthesis(client: SqlQueryable, work: Omit<WorkItem,'leaseToken'>, method: string,
  args: Record<string,unknown>, options: MemoryOptions): Promise<MemoryBatch | { outcome: string; changeCount: number } | null> {
  if (work.kind !== 'memory_synthesis' || !work.principalId || !['load','apply'].includes(method)) throw new Error('invalid memory synthesis work')
  if (Object.keys(args).some(key => !(method === 'load' ? [] : ['changes','conflicts','approved','confidence']).includes(key))) throw new Error('invalid memory synthesis arguments')
  const scope = { tenantId: work.tenantId,scopeType: String(work.meta?.['scopeType'] ?? ''),scopeId: String(work.meta?.['scopeId'] ?? '') }
  const [epoch] = await lockMemoryScopes(client,[scope])
  const job = (await client.query(`SELECT * FROM lingxios.agent_work_items WHERE id=$1 AND fence=$2 AND tenant_id=$3
    AND agent_id=$4 AND principal_id=$5 AND kind='memory_synthesis' AND status='leased' AND lease_expires_at>NOW()
    AND cancel_requested_at IS NULL FOR UPDATE`,[work.id,work.fence,work.tenantId,work.agentId,work.principalId])).rows[0]
  if (!job) throw new Error('memory synthesis lease is unavailable')
  const meta = job['meta'] as Record<string,unknown>
  if (meta['scopeType']!==scope.scopeType || meta['scopeId']!==scope.scopeId || Number(meta['epoch'])!==epoch!.epoch) return method==='load' ? null : { outcome: 'forgotten',changeCount: 0 }
  const sources = (await client.query(`SELECT e.*,w.thread_id,w.created_at AS source_started_at,w.steer_inputs
    FROM lingxios.agent_memory_evidence_scopes s JOIN lingxios.agent_memory_evidence e USING(source_run_id)
    JOIN lingxios.agent_work_items w ON w.id=e.source_run_id
    WHERE s.job_id=$1 AND s.tenant_id=$2 AND s.scope_type=$3 AND s.scope_id=$4 AND s.epoch=$5 AND s.status='pending'
      AND e.tenant_id=$2 AND e.agent_id=$6 AND e.principal_id=$7 AND e.status IN ('pending','processed')
      AND w.status IN ('succeeded','partial') AND w.cancel_requested_at IS NULL AND e.request_version=jsonb_array_length(w.steer_inputs)+1
    ORDER BY e.created_at,e.source_run_id LIMIT 20 FOR UPDATE OF s,e,w`,[work.id,...scopeParams(scope),epoch!.epoch,work.agentId,work.principalId])).rows
  if (!sources.length) return method==='load' ? null : { outcome: 'processed',changeCount: 0 }
  for (const source of sources) {
    if (!(await authorizedScopes(options,sourceIdentity(source),client)).some(item => sameScope(item,scope))) throw new Error('memory source scope was revoked')
  }
  const evidence: MemoryEvidence[] = sources.map(row => {
    const userText = excerpt(String(row['input_text']),1200),assistantText = excerpt(String(row['assistant_text']),600)
    return { sourceRunId: String(row['source_run_id']),sourceRef: String(row['source_ref']),requestVersion: Number(row['request_version']),
      inputSha256: String(row['input_sha256']),userText,assistantText,
      truncated: userText.length<String(row['input_text']).length || assistantText.length<String(row['assistant_text']).length || row['input_truncated']===true || row['assistant_truncated']===true,
      observedAt: new Date(Math.min(new Date(row['created_at'] as string | Date).getTime(),
        Math.max(new Date(row['source_started_at'] as string | Date).getTime(),
          ...(row['steer_inputs'] as Array<{createdAt:string;author?:{kind:string}}>).filter(item=>!item.author || item.author.kind==='human')
            .map(item=>Date.parse(item.createdAt)).filter(Number.isFinite)))).toISOString() }
  })
  if (method==='load') {
    const terms = memoryQuery(evidence.map(item => item.userText).join(' ').slice(0,2000))
    const rows = (await client.query(`SELECT *,COUNT(*) OVER() AS total FROM lingxios.agent_memories
      WHERE tenant_id=$1 AND scope_type=$2 AND scope_id=$3 AND origin<>'evolved'
      ORDER BY layer='core' DESC,ts_rank_cd(search_vector,to_tsquery('simple',$4)) DESC,updated_at DESC,path LIMIT 64`,[...scopeParams(scope),terms])).rows
    const currentMemories: MemoryDocument[] = []
    let remaining = 24_000
    for (const row of rows) {
      const document = memoryDocument(row),bytes = Buffer.byteLength(JSON.stringify(document))
      if (bytes>remaining) continue
      currentMemories.push(document); remaining-=bytes
    }
    await client.query(`UPDATE lingxios.agent_work_items SET meta=jsonb_set(meta,'{memorySnapshot}',$2::jsonb) WHERE id=$1`,
      [work.id,JSON.stringify({ fence: work.fence,epoch: epoch!.epoch,sources: evidence.map(item => ({ id: item.sourceRunId,version: item.requestVersion })),
        memories: currentMemories.map(item => ({ id: item.id,version: item.version })) })])
    return { scope,epoch: epoch!.epoch,evidence,currentMemories,omittedMemories: Number(rows[0]?.['total'] ?? 0)-currentMemories.length,evolutionEnabled: !!options.evolution }
  }
  const changes = parseMemoryChanges(args['changes']),conflicts = parseMemoryConflicts(args['conflicts'] ?? [])
  if (typeof args['approved']!=='boolean' || typeof args['confidence']!=='number' || !Number.isFinite(args['confidence'])
    || args['confidence']<0 || args['confidence']>1) throw new Error('invalid memory verification')
  const snapshot = meta['memorySnapshot'] as { fence?: number; epoch?: number; sources?: Array<{ id: string; version: number }>; memories?: Array<{ id: string; version: number }> } | undefined
  if (snapshot?.fence!==work.fence || snapshot.epoch!==epoch!.epoch || !snapshot.sources || !snapshot.memories
    || memoryDigest(snapshot.sources)!==memoryDigest(evidence.map(item => ({ id: item.sourceRunId,version: item.requestVersion })))) throw new Error('memory synthesis requires the loaded snapshot and versions')
  const evidenceById = new Map(evidence.map(item => [item.sourceRunId,item]))
  const checkSources = (ids: string[]) => { if (ids.some(id => !evidenceById.has(id))) throw new Error('memory change uses unknown evidence') }
  const checkMemory = (id: string,version?: number) => {
    if (!snapshot.memories!.some(item => item.id===id && (version===undefined || item.version===version))) throw new Error('memory change is outside the loaded snapshot')
  }
  for (const item of changes) {
    checkSources(item.sourceRunIds)
    if (item.change.action!=='create') checkMemory(item.change.id,item.change.expectedVersion)
    if (item.change.action==='merge') for (const donor of item.change.from) checkMemory(donor.id,donor.expectedVersion)
  }
  for (const conflict of conflicts) { checkSources(conflict.sourceRunIds); conflict.memoryIds.forEach(id => checkMemory(id)) }
  const approved = args['approved']===true && args['confidence']>=0.6
  if (approved) {
    const identity: MemoryIdentity = sourceIdentity(sources[0]!)
    for (const [index,item] of changes.entries()) {
      const refs: MemorySource[] = item.sourceRunIds.map(id => {
        const source = evidenceById.get(id)!
        return { workId: id,sourceRef: source.sourceRef,authorId: work.principalId!,requestVersion: source.requestVersion,
          inputSha256: source.inputSha256,synthesisWorkId: work.id,observedAt: source.observedAt,confidence: args['confidence'] as number }
      })
      await applyMemoryChanges(client,scope,[item.change],{ identity,actionId: `${work.id}:${index}`,epoch: epoch!.epoch,explicit: false,sources: refs,resolveScopes:options.resolveScopes,
        ...options.writePolicy ? { policy: options.writePolicy } : {} })
    }
    for (const conflict of conflicts) {
      const reason = await memoryWriteBody({ scope,principalId: work.principalId,sourceWorkId: work.id,origin: 'synthesized',kind: 'conflict',body: conflict.reason },options.writePolicy)
      await client.query(`INSERT INTO lingxios.agent_memory_conflicts(id,tenant_id,scope_type,scope_id,source_run_ids,memory_ids,reason)
        VALUES($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7) ON CONFLICT(id) DO NOTHING`,
        [`conflict:${memoryDigest([scope,conflict])}`,...scopeParams(scope),JSON.stringify(conflict.sourceRunIds),JSON.stringify(conflict.memoryIds),reason])
    }
  }
  await client.query(`UPDATE lingxios.agent_memory_evidence_scopes SET status=$2 WHERE job_id=$1 AND status='pending'`,[work.id,approved?'processed':'rejected'])
  await client.query(`UPDATE lingxios.agent_memory_evidence e SET status=CASE WHEN EXISTS(SELECT 1 FROM lingxios.agent_memory_evidence_scopes s
      WHERE s.source_run_id=e.source_run_id AND s.status='processed') THEN 'processed'
      WHEN EXISTS(SELECT 1 FROM lingxios.agent_memory_evidence_scopes s WHERE s.source_run_id=e.source_run_id AND s.status='pending') THEN 'pending' ELSE 'rejected' END
    WHERE e.source_run_id=ANY($1::text[]) AND e.status<>'superseded'`,[evidence.map(item => item.sourceRunId)])
  return { outcome: approved?'committed':'rejected',changeCount: approved?changes.length:0 }
}
