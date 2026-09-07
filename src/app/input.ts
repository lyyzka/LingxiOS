import { withTransaction, type SqlPool } from '../control-plane/pg-store.js'
import { sessionKeyOf, type SteerInput } from '../protocol/types.js'
import { isDeepStrictEqual } from 'node:util'
import { snapshotAttachments, type RequestAttachment } from '../context/attachments.js'
import type { RequestSnapshot } from '../context/request.js'

export interface InputContinuation {
  runId: string
  tenantId: string
  agentId: string
  sessionId: string
  threadId?: string
  principalId: string
  inputId: string
  requestVersion: number
  text: string
  attachments?: RequestAttachment[]
}

/** Trusted server boundary: principalId must come from authenticated ingress. */
export async function continueInput(database: SqlPool, input: InputContinuation) {
  if (!input || !['runId', 'tenantId', 'agentId', 'sessionId', 'principalId', 'inputId', 'text'].every(key => {
    const value = input[key as keyof InputContinuation]
    return typeof value === 'string' && value.trim().length > 0 && value.length <= (key === 'text' ? 32_000 : 1_000)
  }) || !Number.isSafeInteger(input.requestVersion) || input.requestVersion < 1
    || (input.threadId !== undefined && typeof input.threadId !== 'string')) throw new Error('invalid input continuation')
  const attachments = snapshotAttachments(input.attachments ?? [])
  return withTransaction(database, async client => {
    await client.query("SET LOCAL lock_timeout='5s'")
    await client.query("SET LOCAL statement_timeout='15s'")
    // ponytail: serialize rare human continuations with lease acquisition; shard if contention matters.
    await client.query('LOCK TABLE lingxios.agent_os_session_leases IN SHARE ROW EXCLUSIVE MODE')
    const { rows } = await client.query(`SELECT work.status,work.cancel_requested_at,work.goal_outcome,work.steer_inputs,
      request.request_snapshot FROM lingxios.agent_work_items work
      JOIN lingxios.agent_os_sessions session ON session.session_key=$7
        AND session.tenant_id=work.tenant_id AND session.agent_id=work.agent_id
        AND session.session_id=work.session_id AND session.thread_id IS NOT DISTINCT FROM work.thread_id
      JOIN lingxios.agent_request_snapshots request ON request.work_id=work.id AND request.session_key=session.session_key
      WHERE work.id=$1 AND work.tenant_id=$2 AND work.agent_id=$3 AND work.session_id=$4
        AND work.principal_id=$5 AND work.thread_id IS NOT DISTINCT FROM $6
      FOR UPDATE OF work,session,request`,
    [input.runId, input.tenantId, input.agentId, input.sessionId, input.principalId, input.threadId ?? null, sessionKeyOf(input)])
    const row = rows[0]
    if (!row) throw new Error('input continuation does not match the original request identity')
    const revisions = row['steer_inputs'] as SteerInput[]
    const revisionId = JSON.stringify(['input', input.requestVersion, input.inputId])
    const existing = revisions.find(revision => revision.id === revisionId)
    if (existing) {
      if (existing.text !== input.text || !isDeepStrictEqual(existing.attachments ?? [], attachments)) throw new Error('input identity was reused with different text or attachments')
      return { status: 'already_resumed' as const, workId: input.runId }
    }
    const outcome = row['goal_outcome'] as { status?: string; requestVersion?: number } | null
    if (row['status'] !== 'waiting' || row['cancel_requested_at'] !== null || outcome?.status !== 'awaiting_input'
      || outcome.requestVersion !== input.requestVersion || revisions.length + 1 !== input.requestVersion) throw new Error('work is not waiting for this input version')
    const active = await client.query('SELECT 1 FROM lingxios.agent_os_session_leases WHERE session_key=$1 AND expires_at>NOW()', [sessionKeyOf(input)])
    if (active.rows.length) throw new Error('session is active; retry after it pauses')
    if (revisions.length >= 200) throw new Error('request revision limit reached')
    const request = row['request_snapshot'] as RequestSnapshot
    if (request.attachments.length + revisions.reduce((count, revision) => count + (revision.attachments?.length ?? 0), 0) + attachments.length > 20) throw new Error('request attachment limit reached')
    const revision = { id: revisionId, text: input.text, createdAt: new Date().toISOString(), ...(attachments.length ? { attachments } : {}) }
    const updated = await client.query(`UPDATE lingxios.agent_request_snapshots
      SET request_snapshot=jsonb_set(request_snapshot-'contract','{revisions}',$2::jsonb),updated_at=NOW()
      WHERE session_key=$1 AND work_id=$4 AND request_snapshot->'revisions'=$3::jsonb RETURNING session_key`,
    [sessionKeyOf(input), JSON.stringify([...revisions, revision]), JSON.stringify(revisions), input.runId])
    if (updated.rows.length !== 1) throw new Error('request revisions changed before continuation')
    await client.query(`UPDATE lingxios.agent_os_sessions
      SET request_snapshot=(SELECT request_snapshot FROM lingxios.agent_request_snapshots WHERE work_id=$2),
        revision=revision+1,updated_at=NOW()
      WHERE session_key=$1 AND request_snapshot->>'workId'=$2`, [sessionKeyOf(input), input.runId])
    await client.query(`UPDATE lingxios.agent_work_items SET steer_inputs=$2::jsonb,status='queued',available_at=NOW(),
      goal_outcome=NULL,finished_at=NULL,result_text=NULL,error=NULL,updated_at=NOW() WHERE id=$1`, [input.runId, JSON.stringify([...revisions, revision])])
    return { status: 'resumed' as const, workId: input.runId }
  })
}
