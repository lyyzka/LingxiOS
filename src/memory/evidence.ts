import type { RequestSnapshot } from '../context/request.js'
import { withTransaction, type SqlPool, type SqlQueryable } from '../control-plane/pg-store.js'
import { sessionKeyOf, type AssistantMessage, type WorkItem } from '../protocol/types.js'
import type { MemoryIdentity, MemoryScope } from './types.js'
import { currentMemoryScopes, lockMemoryScopes } from './forget.js'
import { authorizeScope, memorySettings, sourceIdentity, type MemoryOptions } from './access.js'
import { excerpt, memoryDigest, memoryQuery, memorySearchText, pageLimit } from './text.js'
import type { MemoryHistoryHit, MemorySearchResult } from './types.js'
import { MemoryContentRejected, memoryWriteBody, type MemoryWritePolicy } from './policy.js'

/** Privacy hooks run before acquiring work or memory row locks. Nothing unfiltered is queued. */
export async function prepareMemoryEvidence(database: SqlQueryable, work: Omit<WorkItem,'leaseToken'>, message: AssistantMessage, scopes: readonly MemoryScope[], policy?: MemoryWritePolicy, signal?: AbortSignal) {
  if (!scopes.length || !work.principalId) return
  if (scopes.length > 12 || scopes.some(scope => scope.tenantId !== work.tenantId)) throw new Error('invalid memory evidence scopes')
  const request = (await database.query(`SELECT request_snapshot FROM lingxios.agent_request_snapshots WHERE session_key=$1 AND work_id=$2`,
    [sessionKeyOf(work),work.id])).rows[0]?.['request_snapshot'] as RequestSnapshot | undefined
  if (!request || request.workId !== work.id || request.authorId !== work.principalId
    || request.revisions.length+1 !== message.envelope.requestVersion) throw new Error('memory evidence requires the committed request version')
  // Delegated assignments and agent-authored steering are not additional human observations.
  if (request.instructionAuthor || request.parentWorkId) return
  const currentScopes = scopes
  const input = JSON.stringify({ originalText: request.originalText,
    revisions: request.revisions.filter(item => !item.author || item.author.kind === 'human').map(item => ({ text: item.text,createdAt: item.createdAt })) })
  let inputText = excerpt(input,16_000), assistantText = excerpt(message.body,16_000)
  try {
    for (const scope of currentScopes) {
      const boundary = { scope,principalId:work.principalId,sourceWorkId:work.id,origin:'synthesized' as const }
      inputText = await memoryWriteBody({...boundary,kind:'history_user',body:inputText},policy,signal)
      if (assistantText.trim()) assistantText = await memoryWriteBody({...boundary,kind:'history_assistant',body:assistantText},policy,signal)
    }
  } catch (error) {
    if (error instanceof MemoryContentRejected) return
    throw error
  }
  return { inputText, assistantText, inputHash: memoryDigest(input), sourceRef: request.sourceRef,
    inputTruncated: inputText.length < input.length, assistantTruncated: assistantText.length < message.body.length,
    fingerprint: memoryDigest([request, message.body, message.envelope.requestVersion]) }
}

/** Install only prefiltered evidence, with the request and forgetting epochs checked under locks. */
export async function captureMemoryEvidence(database: SqlQueryable, work: Omit<WorkItem,'leaseToken'>, message: AssistantMessage,
  scopes: readonly MemoryScope[], policy?: MemoryWritePolicy, prepared?: Awaited<ReturnType<typeof prepareMemoryEvidence>>) {
  const evidence = prepared ?? await prepareMemoryEvidence(database, work, message, scopes, policy)
  if (!evidence) return
  const request = (await database.query('SELECT request_snapshot FROM lingxios.agent_request_snapshots WHERE work_id=$1', [work.id])).rows[0]?.['request_snapshot']
  if (evidence.fingerprint !== memoryDigest([request, message.body, message.envelope.requestVersion])) throw new Error('memory evidence preparation is stale')
  const epochs = await currentMemoryScopes(database, await lockMemoryScopes(database, scopes), work.id)
  if (!epochs.length) return
  const currentScopes = epochs.map(({ tenantId, scopeType, scopeId }) => ({ tenantId, scopeType, scopeId }))
  const { inputText, assistantText } = evidence
  await database.query(`INSERT INTO lingxios.agent_memory_evidence
    (source_run_id,tenant_id,agent_id,principal_id,session_id,request_version,source_ref,input_sha256,input_text,assistant_text,input_truncated,assistant_truncated,scopes,scope_epochs,search_text)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14::jsonb,$15) ON CONFLICT(source_run_id) DO NOTHING`,
    [work.id,work.tenantId,work.agentId,work.principalId,work.sessionId,message.envelope.requestVersion,evidence.sourceRef,evidence.inputHash,
      inputText,assistantText,evidence.inputTruncated,evidence.assistantTruncated,JSON.stringify(currentScopes),JSON.stringify(epochs),
      memorySearchText(`${inputText} ${assistantText}`)])
  for (const epoch of epochs) await database.query(`INSERT INTO lingxios.agent_memory_evidence_scopes
    (tenant_id,agent_id,principal_id,scope_type,scope_id,epoch,source_run_id)
    SELECT $1,$2,$3,$4,$5,$6,$7 WHERE EXISTS(SELECT 1 FROM lingxios.agent_memory_evidence WHERE source_run_id=$7 AND status='pending')
    ON CONFLICT DO NOTHING`,[work.tenantId,work.agentId,work.principalId,epoch.scopeType,epoch.scopeId,epoch.epoch,work.id])
}

/** Persistent due times are derived from committed evidence; no in-process timers or counters own learning state. */
export async function scheduleMemoryReflection(database: SqlPool, options: MemoryOptions, manual?: { identity: MemoryIdentity; scope: MemoryScope }): Promise<{ jobIds: string[] }> {
  return withTransaction(database,client => scheduleMemoryReflectionInTransaction(client,options,manual))
}
export async function scheduleMemoryReflectionInTransaction(client: SqlQueryable, options: MemoryOptions, manual?: { identity: MemoryIdentity; scope: MemoryScope }): Promise<{ jobIds: string[] }> {
  const { afterInteractions,idleMs } = memorySettings(options)
    if (manual) await authorizeScope(options,manual.identity,client,manual.scope)
    const jobIds: string[] = manual ? (await client.query(`SELECT DISTINCT w.id FROM lingxios.agent_work_items w
      JOIN lingxios.agent_memory_evidence_scopes s ON s.job_id=w.id
      WHERE s.tenant_id=$1 AND s.agent_id=$2 AND s.principal_id=$3 AND s.scope_type=$4 AND s.scope_id=$5 AND s.status='pending'
        AND (w.status IN ('queued','leased') OR (w.status='failed' AND w.attempts<3 AND w.cancel_requested_at IS NULL))`,
      [manual.identity.tenantId,manual.identity.agentId,manual.identity.principalId,manual.scope.scopeType,manual.scope.scopeId])).rows.map(row=>String(row['id'])) : []
    const buckets = (await client.query(`SELECT s.tenant_id,s.agent_id,s.principal_id,s.scope_type,s.scope_id,s.epoch
      FROM lingxios.agent_memory_evidence_scopes s JOIN lingxios.agent_memory_evidence e USING(source_run_id)
      JOIN lingxios.agent_work_items w ON w.id=s.source_run_id
      WHERE s.status='pending' AND s.job_id IS NULL AND e.status IN ('pending','processed')
        AND w.status IN ('succeeded','partial') AND w.cancel_requested_at IS NULL AND e.request_version=jsonb_array_length(w.steer_inputs)+1
        AND ($3::text IS NULL OR (s.tenant_id=$3 AND s.agent_id=$4 AND s.principal_id=$5 AND s.scope_type=$6 AND s.scope_id=$7))
      GROUP BY s.tenant_id,s.agent_id,s.principal_id,s.scope_type,s.scope_id,s.epoch
      HAVING $3::text IS NOT NULL OR COUNT(*) >= $1 OR MAX(s.created_at)+$2::bigint*INTERVAL '1 millisecond'<=NOW()
        OR MIN(s.created_at)<=(SELECT MAX(j.created_at) FROM lingxios.agent_work_items j
          WHERE j.kind='memory_synthesis' AND j.status IN ('succeeded','partial') AND j.tenant_id=s.tenant_id
            AND j.agent_id=s.agent_id AND j.principal_id=s.principal_id AND j.meta->>'scopeType'=s.scope_type
            AND j.meta->>'scopeId'=s.scope_id AND (j.meta->>'epoch')::bigint=s.epoch
            AND jsonb_array_length(j.meta->'sourceRunIds')=20)
      ORDER BY s.tenant_id,s.scope_type,s.scope_id,s.agent_id,s.principal_id,s.epoch LIMIT 32`,[afterInteractions,idleMs,manual?.identity.tenantId ?? null,manual?.identity.agentId ?? null,
        manual?.identity.principalId ?? null,manual?.scope.scopeType ?? null,manual?.scope.scopeId ?? null])).rows
    for (const bucket of buckets.sort((a,b)=>JSON.stringify([a['tenant_id'],a['scope_type'],a['scope_id']]).localeCompare(JSON.stringify([b['tenant_id'],b['scope_type'],b['scope_id']])))) {
      const scope = { tenantId: String(bucket['tenant_id']),scopeType: String(bucket['scope_type']),scopeId: String(bucket['scope_id']) }
      const [epoch] = await lockMemoryScopes(client,[scope])
      if (epoch!.epoch !== Number(bucket['epoch'])) continue
      const reflectionKey = memoryDigest([scope,bucket['agent_id'],bucket['principal_id'],epoch!.epoch])
      const active = (await client.query(`SELECT id FROM lingxios.agent_work_items WHERE kind='memory_synthesis' AND meta->>'reflectionKey'=$1
        AND (status IN ('queued','leased') OR (status='failed' AND attempts<3 AND cancel_requested_at IS NULL)) LIMIT 1`,[reflectionKey])).rows[0]
      if (active) { if (manual) jobIds.push(String(active['id'])); continue }
      const candidates = (await client.query(`SELECT w.*,e.source_run_id FROM lingxios.agent_memory_evidence_scopes s
        JOIN lingxios.agent_work_items w ON w.id=s.source_run_id JOIN lingxios.agent_memory_evidence e ON e.source_run_id=w.id
        WHERE s.tenant_id=$1 AND s.agent_id=$2 AND s.principal_id=$3 AND s.scope_type=$4 AND s.scope_id=$5 AND s.epoch=$6
          AND s.status='pending' AND s.job_id IS NULL AND e.status IN ('pending','processed') AND w.status IN ('succeeded','partial')
          AND w.cancel_requested_at IS NULL AND e.request_version=jsonb_array_length(w.steer_inputs)+1
        ORDER BY s.created_at,s.source_run_id LIMIT 20 FOR UPDATE OF s`,
        [scope.tenantId,bucket['agent_id'],bucket['principal_id'],scope.scopeType,scope.scopeId,epoch!.epoch])).rows
      const sourceRunIds: string[] = []
      for (const source of candidates) {
        const scopes = await options.resolveScopes(sourceIdentity(source),client)
        if (scopes.some(item => item.tenantId===scope.tenantId && item.scopeType===scope.scopeType && item.scopeId===scope.scopeId)) sourceRunIds.push(String(source['id']))
        else await client.query(`UPDATE lingxios.agent_memory_evidence_scopes SET status='superseded'
          WHERE tenant_id=$1 AND scope_type=$2 AND scope_id=$3 AND source_run_id=$4`,[scope.tenantId,scope.scopeType,scope.scopeId,source['id']])
      }
      if (!sourceRunIds.length) continue
      const id = `memory-synthesis:${memoryDigest([reflectionKey,sourceRunIds])}`
      await client.query(`INSERT INTO lingxios.agent_work_items(id,tenant_id,agent_id,principal_id,session_id,kind,lane,trigger_ref,meta)
        VALUES($1,$2,$3,$4,$5,'memory_synthesis','background',$6,$7::jsonb) ON CONFLICT(id) DO NOTHING`,
        [id,scope.tenantId,bucket['agent_id'],bucket['principal_id'],`memory:${reflectionKey}`,sourceRunIds[0],
          JSON.stringify({ reflectionKey,scopeType: scope.scopeType,scopeId: scope.scopeId,epoch: epoch!.epoch,sourceRunId: sourceRunIds[0],sourceRunIds })])
      await client.query(`UPDATE lingxios.agent_memory_evidence_scopes SET job_id=$5
        WHERE tenant_id=$1 AND scope_type=$2 AND scope_id=$3 AND source_run_id=ANY($4::text[])`,[scope.tenantId,scope.scopeType,scope.scopeId,sourceRunIds,id])
      jobIds.push(id)
    }
    return { jobIds: [...new Set(jobIds)] }
}

/** Failed auxiliary work backs off and stops after three attempts, without retrying the user turn. */
export async function retryMemorySynthesis(database: SqlQueryable) {
  await database.query(`WITH exhausted AS (
    SELECT id FROM lingxios.agent_work_items WHERE kind IN ('memory_synthesis','memory_index','memory_evaluation') AND attempts>=3
      AND (status='queued' OR (status='leased' AND lease_expires_at<=NOW())) ORDER BY updated_at LIMIT 32 FOR UPDATE SKIP LOCKED)
    UPDATE lingxios.agent_work_items w SET status='failed',finished_at=NOW(),updated_at=NOW(),error='memory attempts exhausted'
    FROM exhausted WHERE w.id=exhausted.id`)
  await database.query(`WITH retry AS (
    SELECT w.id FROM lingxios.agent_work_items w WHERE w.kind IN ('memory_synthesis','memory_index','memory_evaluation')
      AND w.status='failed' AND w.attempts<3 AND w.cancel_requested_at IS NULL
      AND (w.kind<>'memory_synthesis' OR EXISTS(SELECT 1 FROM lingxios.agent_memory_evidence_scopes s WHERE s.job_id=w.id AND s.status='pending'))
      AND w.updated_at+INTERVAL '30 seconds'*power(2,w.attempts)<=NOW()
    ORDER BY w.updated_at LIMIT 32 FOR UPDATE OF w SKIP LOCKED)
    UPDATE lingxios.agent_work_items w SET status='queued',available_at=NOW(),finished_at=NULL,error=NULL,updated_at=NOW(),meta=meta-'memorySnapshot'
    FROM retry WHERE w.id=retry.id`)
  await database.query(`UPDATE lingxios.agent_memory_evidence_scopes s SET status='rejected'
    FROM lingxios.agent_work_items w WHERE s.job_id=w.id AND s.status='pending'
      AND (w.status='cancelled' OR (w.status='failed' AND w.attempts>=3))`)
}

export async function searchMemoryHistory(database: SqlQueryable, options: MemoryOptions, identity: MemoryIdentity, scope: MemoryScope,
  query: string, limit = 12, cursor?: string): Promise<MemorySearchResult> {
  await authorizeScope(options,identity,database,scope)
  pageLimit(limit)
  const terms = memoryQuery(query)
  if (query.trim() && !terms) return {items:[],nextCursor:null,retrieval:'keyword'}
  let after='',afterRole=-1
  if (cursor!==undefined) {
    if (cursor.length>2000) throw new Error('invalid history cursor')
    const value:unknown=JSON.parse(cursor)
    if (!Array.isArray(value) || value.length!==2 || typeof value[0]!=='string' || value[0].length>1000 || ![0,1].includes(value[1])) throw new Error('invalid history cursor')
    after=value[0]; afterRole=value[1] as number
  }
  const items: MemoryHistoryHit[] = []
  // Bound permission checks as well as output. The cursor advances over inaccessible sources.
  const rows = (await database.query(`SELECT e.*,w.thread_id,r.role FROM lingxios.agent_memory_evidence e
    JOIN lingxios.agent_memory_evidence_scopes s USING(source_run_id)
    JOIN lingxios.agent_memory_scopes p ON p.tenant_id=s.tenant_id AND p.scope_type=s.scope_type AND p.scope_id=s.scope_id AND p.epoch=s.epoch
    JOIN lingxios.agent_work_items w ON w.id=e.source_run_id
    CROSS JOIN (VALUES(0),(1)) r(role)
    WHERE e.tenant_id=$1 AND e.agent_id=$2 AND e.principal_id=$3 AND s.scope_type=$4 AND s.scope_id=$5 AND s.tenant_id=$1
      AND e.status IN ('pending','processed','rejected') AND s.status<>'superseded'
      AND w.status IN ('succeeded','partial') AND w.cancel_requested_at IS NULL AND e.request_version=jsonb_array_length(w.steer_inputs)+1
      AND (e.source_run_id,r.role)>($6,$8::integer) AND ($7::text='' OR e.search_vector @@ to_tsquery('simple',$7))
    ORDER BY e.source_run_id,r.role LIMIT 65`,[identity.tenantId,identity.agentId,identity.principalId,scope.scopeType,scope.scopeId,after,terms,afterRole])).rows
  let scanned = 0
  for (const row of rows.slice(0,64)) {
    if (items.length>=limit) break
    scanned++; after=String(row['source_run_id']); afterRole=Number(row['role'])
    const allowed = await options.resolveScopes(sourceIdentity(row),database)
    if (!allowed.some(item => item.tenantId===scope.tenantId && item.scopeType===scope.scopeType && item.scopeId===scope.scopeId)) continue
    {
      const role = afterRole===0?'user':'assistant'
      const original = String(row[role === 'user' ? 'input_text' : 'assistant_text'])
      const text = excerpt(original,2400)
      items.push({ sourceRunId: after,sessionId: String(row['session_id']),sourceRef: String(row['source_ref']),requestVersion: Number(row['request_version']),
        role,text,truncated: text.length<original.length || row[role === 'user' ? 'input_truncated' : 'assistant_truncated'] === true,
        observedAt: new Date(row['created_at'] as string | Date).toISOString() })
    }
  }
  return { items,nextCursor: rows.length>scanned ? JSON.stringify([after,afterRole]) : null,retrieval: query.trim() ? 'keyword' : 'browse' }
}
