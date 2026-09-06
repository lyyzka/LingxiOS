import { createHash } from 'node:crypto'
import type { RequestSnapshot } from '../context/request.js'
import type { SqlQueryable } from '../control-plane/pg-store.js'
import { sessionKeyOf, type AssistantMessage, type WorkItem } from '../protocol/types.js'

/** Called inside the message commit transaction, never for drafts or progress text. */
export async function captureMemoryEvidence(database: SqlQueryable, work: Omit<WorkItem, 'leaseToken'>, message: AssistantMessage) {
  const { rows } = await database.query('SELECT request_snapshot FROM lingxios.agent_os_sessions WHERE session_key=$1', [sessionKeyOf(work)])
  const request = rows[0]?.['request_snapshot'] as RequestSnapshot | undefined
  if (!request || request.workId !== work.id || request.authorId !== work.principalId
    || request.revisions.length + 1 !== message.envelope.requestVersion) throw new Error('memory evidence requires the committed request version')
  const input = JSON.stringify({ originalText: request.originalText, revisions: request.revisions, attachments: request.attachments })
  const excerpt = (text: string) => text.slice(0, 16_000).replace(/[\uD800-\uDBFF]$/, '')
  const inputText = excerpt(input), assistantText = excerpt(message.body)
  await database.query(`INSERT INTO lingxios.agent_memory_evidence
    (source_run_id,tenant_id,agent_id,principal_id,session_id,request_version,source_ref,input_sha256,input_text,assistant_text,input_truncated,assistant_truncated)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT(source_run_id) DO NOTHING`,
  [work.id, work.tenantId, work.agentId, work.principalId, work.sessionId, message.envelope.requestVersion, request.sourceRef,
    createHash('sha256').update(input).digest('hex'), inputText, assistantText, inputText.length < input.length, assistantText.length < message.body.length])
  // One source per durable job; the current scoped memories provide cross-turn consolidation.
  await database.query(`INSERT INTO lingxios.agent_work_items
    (id,tenant_id,agent_id,principal_id,session_id,thread_id,kind,lane,trigger_ref,meta)
    VALUES($1,$2,$3,$4,$5,$6,'memory_synthesis','background',$7,$8::jsonb) ON CONFLICT(id) DO NOTHING`,
  [`memory-synthesis:${work.id}`, work.tenantId, work.agentId, work.principalId, work.sessionId, work.threadId ?? null,
    work.id, JSON.stringify({ sourceRunId: work.id })])
}

/** Failed auxiliary work backs off and stops after three attempts, without retrying the user turn. */
export async function retryMemorySynthesis(database: SqlQueryable) {
  await database.query(`WITH exhausted AS (
    SELECT id FROM lingxios.agent_work_items WHERE kind IN ('memory_synthesis','memory_index') AND attempts>=3
      AND (status='queued' OR (status='leased' AND lease_expires_at<=NOW())) ORDER BY updated_at LIMIT 12 FOR UPDATE SKIP LOCKED)
    UPDATE lingxios.agent_work_items w SET status='failed',finished_at=NOW(),updated_at=NOW(),error='memory synthesis attempts exhausted'
    FROM exhausted WHERE w.id=exhausted.id`)
  await database.query(`WITH retry AS (
    SELECT w.id FROM lingxios.agent_work_items w LEFT JOIN lingxios.agent_memory_evidence e ON e.source_run_id=w.meta->>'sourceRunId'
    WHERE w.kind IN ('memory_synthesis','memory_index') AND w.status='failed' AND w.attempts<3 AND w.cancel_requested_at IS NULL
      AND (w.kind='memory_index' OR e.status='pending') AND w.updated_at+INTERVAL '30 seconds'*power(2,w.attempts)<=NOW()
    ORDER BY w.updated_at LIMIT 12 FOR UPDATE OF w SKIP LOCKED)
    UPDATE lingxios.agent_work_items w SET status='queued',available_at=NOW(),finished_at=NULL,error=NULL,updated_at=NOW()
    FROM retry WHERE w.id=retry.id`)
  await database.query(`WITH exhausted AS (
    SELECT e.source_run_id FROM lingxios.agent_memory_evidence e JOIN lingxios.agent_work_items w ON w.meta->>'sourceRunId'=e.source_run_id
    WHERE e.status='pending' AND w.kind='memory_synthesis' AND (w.status='cancelled' OR (w.status='failed' AND w.attempts>=3))
    ORDER BY e.created_at LIMIT 12 FOR UPDATE OF e SKIP LOCKED)
    UPDATE lingxios.agent_memory_evidence e SET status='rejected' FROM exhausted WHERE e.source_run_id=exhausted.source_run_id`)
}
