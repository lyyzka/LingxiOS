import type { EnqueueWorkInput, EnqueueResult } from '../control-plane/stores.js'
import { randomUUID } from 'node:crypto'
import type { SqlQueryable } from '../control-plane/pg-store.js'
import type { RequestSnapshot } from '../context/request.js'
import type { GoalOutcome } from '../protocol/outcome.js'
import type { WorkItem, WorkLane, WorkStatus } from '../protocol/types.js'
import type { RequestInput, MessageIdentity } from './index.js'
import { executionMode } from '../runtime/execution-policy.js'
import { authorizeConversationWork, digest, participant, requireAudience } from '../collaboration/access.js'
import { authorizeRunRead } from '../collaboration/api.js'

export interface JobInput extends RequestInput {
  kind: string
  lane: WorkLane
  availableAt?: string
  priority?: number
  meta?: Record<string, unknown>
}
export interface ChildInput {
  id: string
  agentId: string
  text: string
  kind?: string
  sessionId?: string
  threadId?: string
  dependsOn?: string[]
  meta?: Record<string, unknown>
}
export interface RunSnapshot {
  id: string
  fence: number
  resultId: string | null
  resultFence: number | null
  status: WorkStatus
  requestVersion: number
  kind: string
  attempts: number
  createdAt: string
  availableAt: string
  heartbeatAt: string | null
  lastProgressAt: string | null
  goalOutcome: GoalOutcome | null
  error: string | null
}
export type RunIdentity = MessageIdentity & Pick<RequestInput, 'principalId' | 'threadId'>
type StoredRunIdentity = Omit<RunIdentity, 'principalId'> & { principalId: string | null }
const scopeSql = 'id=$1 AND tenant_id=$2 AND agent_id=$3 AND session_id=$4 AND principal_id IS NOT DISTINCT FROM $5 AND thread_id IS NOT DISTINCT FROM $6'
const params = (identity: StoredRunIdentity) => [identity.runId,identity.tenantId,identity.agentId,identity.sessionId,identity.principalId,identity.threadId ?? null]
const iso = (value: unknown) => value == null ? null : new Date(value as Date | string).toISOString()

export async function readRun(database: SqlQueryable, identity: RunIdentity): Promise<RunSnapshot | null> {
  await authorizeRunRead(database, identity)
  const { rows } = await database.query(`SELECT *,jsonb_array_length(steer_inputs)+1 AS request_version,
    (SELECT fence FROM lingxios.agent_results WHERE id=work.result_id) AS result_fence
    FROM lingxios.agent_work_items work WHERE ${scopeSql}`, params(identity))
  const row = rows[0]
  return row ? runSnapshot(row) : null
}

export function runSnapshot(row: Record<string, unknown>): RunSnapshot {
  return { id: String(row['id']), fence: Number(row['fence']), resultId: row['result_id'] as string | null,
    resultFence: row['result_fence'] == null ? null : Number(row['result_fence']), status: row['status'] as WorkStatus, kind: String(row['kind']), attempts: Number(row['attempts']),
    requestVersion: Number(row['request_version']), createdAt: iso(row['created_at'])!, availableAt: iso(row['available_at'])!,
    heartbeatAt: iso(row['heartbeat_at']), lastProgressAt: iso(row['last_progress_at']),
    goalOutcome: row['goal_outcome'] as GoalOutcome | null, error: row['error'] as string | null }
}

export type DeliveryState = 'pending' | 'delivered' | 'failed' | 'not_observed'
export interface RunState { run: RunSnapshot; message: import('../protocol/types.js').AssistantMessage | null; delivery: DeliveryState | null }

/** A single MVCC snapshot keeps the response, result identity and delivery status consistent. */
export async function readRunState(database: SqlQueryable, identity: RunIdentity): Promise<RunState | null> {
  const row = await readRunRecord(database, identity)
  return row ? runStateFromRow(row) : null
}

/** Internal shared read for state and streaming policy; both use the very same work/result version. */
export async function readRunRecord(database: SqlQueryable, identity: RunIdentity): Promise<Record<string, unknown> | null> {
  await authorizeRunRead(database, identity)
  const { rows } = await database.query(`SELECT work.*,jsonb_array_length(steer_inputs)+1 AS request_version,
    result.fence AS result_fence,result.message,CASE WHEN work.result_id IS NULL THEN NULL WHEN outbox.result_id IS NULL THEN 'not_observed'
      WHEN outbox.delivered_at IS NOT NULL THEN 'delivered' WHEN outbox.failed_at IS NOT NULL THEN 'failed' ELSE 'pending' END AS delivery
    FROM (SELECT * FROM lingxios.agent_work_items WHERE ${scopeSql}) work
    LEFT JOIN lingxios.agent_results result ON result.id=work.result_id
    LEFT JOIN lingxios.agent_delivery_outbox outbox ON outbox.result_id=work.result_id`, params(identity))
  return rows[0] ?? null
}

export function runStateFromRow(row: Record<string, unknown>): RunState {
  return { run: runSnapshot(row), message: row['message'] as RunState['message'], delivery: row['delivery'] as RunState['delivery'] }
}

export async function requestSnapshot(database: SqlQueryable, workId: string, requestVersion: number | null): Promise<RequestSnapshot> {
  const { rows } = await database.query('SELECT request_snapshot FROM lingxios.agent_request_snapshots WHERE work_id=$1', [workId])
  const request = rows[0]?.['request_snapshot'] as RequestSnapshot | undefined
  if (!request || request.workId !== workId || request.revisions.length + 1 !== requestVersion) throw new Error('current original request snapshot is required')
  return request
}

/** Used inside the parent's action transaction. Identity, lineage and budget root cannot be supplied by the model. */
export async function enqueueChild(database: SqlQueryable, parent: Omit<WorkItem, 'leaseToken'>, version: number | null, input: ChildInput, graphId?: string) {
  const request = await requestSnapshot(database, parent.id, version)
  const policy = await authorizeConversationWork(database, parent, 'execute', true)
  if (policy && parent.conversation) {
    participant(policy, input.agentId, 'execute', 'agent')
    requireAudience(policy, parent.conversation.audience, [input.agentId])
    if (input.threadId !== undefined && input.threadId !== parent.threadId) throw new Error('delegation cannot change its conversation thread')
  }
  const ancestors = await database.query(`WITH RECURSIVE lineage AS (
    SELECT id,meta,ARRAY[id] AS path FROM lingxios.agent_work_items WHERE id=$1 AND tenant_id=$2 AND principal_id=$3
    UNION ALL SELECT ancestor.id,ancestor.meta,child.path||ancestor.id FROM lingxios.agent_work_items ancestor
      JOIN lineage child ON ancestor.id=child.meta->>'parentWorkId' WHERE ancestor.tenant_id=$2 AND ancestor.principal_id=$3
        AND NOT ancestor.id=ANY(child.path) AND cardinality(child.path)<64)
    SELECT work.id,work.status,work.cancel_requested_at FROM lingxios.agent_work_items work JOIN lineage ON lineage.id=work.id
    ORDER BY work.id FOR SHARE OF work`, [parent.id,parent.tenantId,parent.principalId])
  if (ancestors.rows.length >= 64 || !ancestors.rows.some(row => row['id'] === (parent.meta?.['rootWorkId'] ?? parent.id))
    || ancestors.rows.some(row => !['queued','leased','waiting'].includes(String(row['status'])) || row['cancel_requested_at'])) {
    throw new Error('an ancestor task is no longer active')
  }
  if (!parent.principalId || !input.id?.trim() || !input.agentId?.trim() || !input.text?.trim() || input.text.length > 100_000
    || input.dependsOn && (input.dependsOn.length > 64 || input.dependsOn.some(id => !id.trim() || id === input.id))) throw new Error('invalid child request')
  const children = await database.query(`SELECT COUNT(*)::int AS count FROM lingxios.agent_work_items WHERE meta->>'parentWorkId'=$1 AND id<>$2`, [parent.id,input.id])
  if (Number(children.rows[0]?.['count']) >= 64) throw new Error('a parent may create at most 64 child tasks')
  const { harnessHash: _untrustedHarness, mode: _untrustedMode, obligations: _untrustedObligations, codeExecution: childCode,
    deliveryMode: _untrustedDeliveryMode, graphId: _untrustedGraph, ...childMeta } = input.meta ?? {}
  const inheritedCode = parent.meta?.['codeExecution']
  const codeExecution = inheritedCode === 'disabled' || childCode === 'disabled' ? 'disabled'
    : inheritedCode === 'enabled' || childCode === 'enabled' ? 'enabled' : undefined
  const meta = { ...childMeta, ...(parent.meta?.['harnessHash'] ? { harnessHash: parent.meta['harnessHash'] } : {}),
    ...(parent.meta?.['mode'] === undefined ? {} : { mode: executionMode(parent) }), ...(codeExecution ? { codeExecution } : {}), text: input.text, authorName: parent.agentId, attachments: request.attachments,
    parentWorkId: parent.id, rootWorkId: parent.meta?.['rootWorkId'] ?? parent.id, parentRequestVersion: version,
    ...(graphId ? { graphId } : {}), dependsOn: input.dependsOn ?? [], delegation: { parentWorkId: parent.id, rootWorkId: parent.meta?.['rootWorkId'] ?? parent.id,
      parentRequestVersion: version, instructionAuthorId: parent.agentId, parentRequest: request, assignment: input.text } }
  if (input.dependsOn?.length) {
    const dependencies = await database.query(`SELECT id FROM lingxios.agent_work_items WHERE id=ANY($1::text[])
      AND tenant_id=$2 AND principal_id IS NOT DISTINCT FROM $3 AND meta->>'parentWorkId'=$4
      AND meta->'parentRequestVersion'=$5::jsonb AND meta->>'graphId' IS NOT DISTINCT FROM $6`,
    [input.dependsOn, parent.tenantId, parent.principalId ?? null, parent.id, JSON.stringify(version), graphId ?? null])
    if (dependencies.rows.length !== input.dependsOn.length) throw new Error('dependencies must be existing siblings of this graph and request')
  }
  const result = await enqueueWork(database, { id: input.id, tenantId: parent.tenantId, agentId: input.agentId,
    sessionId: parent.conversation || input.sessionId === undefined ? 'delegate-session:' + digest([parent.id, version, input.id]) : input.sessionId,
    ...(input.threadId ?? parent.threadId ? { threadId: input.threadId ?? parent.threadId! } : {}),
    ...(parent.conversation ? { conversation: { ...parent.conversation, internal: true } } : {}),
    kind: input.kind ?? 'turn', lane: 'collaboration', triggerRef: parent.triggerRef, principalId: parent.principalId, meta })
  for (const id of input.dependsOn ?? []) await database.query(`INSERT INTO lingxios.agent_work_dependencies(work_id,dependency_id) VALUES($1,$2) ON CONFLICT DO NOTHING`, [input.id,id])
  return result
}

export async function childIdentity(database: SqlQueryable, parent: Omit<WorkItem, 'leaseToken'>, id: string): Promise<RunIdentity> {
  const { rows } = await database.query(`SELECT agent_id,session_id,thread_id FROM lingxios.agent_work_items
    WHERE id=$1 AND tenant_id=$2 AND principal_id=$3 AND meta->>'parentWorkId'=$4`, [id,parent.tenantId,parent.principalId,parent.id])
  const row = rows[0]
  if (!row || !parent.principalId) throw new Error('child is outside this parent and principal')
  return { runId: id, tenantId: parent.tenantId, principalId: parent.principalId, agentId: String(row['agent_id']),
    sessionId: String(row['session_id']), ...(row['thread_id'] ? { threadId: String(row['thread_id']) } : {}) }
}

export async function reviseRun(database: SqlQueryable, identity: RunIdentity, text: string,
  author: import('../protocol/types.js').SteerInput['author'] = { id: identity.principalId, kind: 'human' }) {
  await authorizeRunRead(database, identity, 'execute')
  if (!text?.trim() || text.length > 8000) throw new Error('revision must contain 1 to 8000 characters')
  if (!author?.id?.trim() || !['human','agent'].includes(author.kind) || author.kind === 'human' && author.id !== identity.principalId) throw new Error('revision author is outside the original principal')
  const { rows } = await database.query(`UPDATE lingxios.agent_work_items SET steer_inputs=steer_inputs||$7::jsonb,
    status=CASE WHEN status='waiting' THEN 'queued' ELSE status END,available_at=NOW(),goal_outcome=NULL,updated_at=NOW()
    WHERE ${scopeSql} AND status IN ('queued','leased','waiting') AND cancel_requested_at IS NULL RETURNING id`,
  [...params(identity),JSON.stringify([{ id: randomUUID(), text, createdAt: new Date().toISOString(), author }])])
  if (rows.length) await cancelDescendants(database, identity.runId)
  return rows.length === 1
}

export async function cancelRun(database: SqlQueryable, identity: StoredRunIdentity) {
  const { rows } = await database.query(`UPDATE lingxios.agent_work_items SET cancel_requested_at=COALESCE(cancel_requested_at,NOW()),
      status=CASE WHEN status='leased' THEN status ELSE 'cancelled' END,
      goal_outcome=CASE WHEN status='leased' THEN goal_outcome ELSE jsonb_build_object('status','blocked','verification','not_run',
        'requestVersion',jsonb_array_length(steer_inputs)+1,'gaps',jsonb_build_array('Cancelled by user')) END,
      finished_at=CASE WHEN status='leased' THEN finished_at ELSE NOW() END,updated_at=NOW()
    WHERE ${scopeSql} AND status IN ('queued','leased','waiting') RETURNING id`, params(identity))
  if (rows.length) await cancelDescendants(database, identity.runId)
  return rows.length === 1
}

/** The caller holds the parent's row lock, so no child can be inserted behind cancellation. */
export async function cancelDescendants(database: SqlQueryable, parentId: string) {
  await database.query(`WITH RECURSIVE descendants AS (
    SELECT child.id,child.tenant_id,child.principal_id,ARRAY[parent.id,child.id] AS path FROM lingxios.agent_work_items parent
      JOIN lingxios.agent_work_items child ON child.meta->>'parentWorkId'=parent.id
        AND child.tenant_id=parent.tenant_id AND child.principal_id IS NOT DISTINCT FROM parent.principal_id WHERE parent.id=$1
    UNION ALL SELECT child.id,child.tenant_id,child.principal_id,parent.path||child.id FROM lingxios.agent_work_items child
      JOIN descendants parent ON child.meta->>'parentWorkId'=parent.id AND child.tenant_id=parent.tenant_id
        AND child.principal_id IS NOT DISTINCT FROM parent.principal_id WHERE NOT child.id=ANY(parent.path) AND cardinality(parent.path)<64
  ) UPDATE lingxios.agent_work_items work SET cancel_requested_at=COALESCE(cancel_requested_at,NOW()),
      status=CASE WHEN status='leased' THEN status ELSE 'cancelled' END,
      finished_at=CASE WHEN status='leased' THEN finished_at ELSE NOW() END,
      goal_outcome=CASE WHEN status='leased' THEN goal_outcome ELSE jsonb_build_object('status','blocked','verification','not_run',
        'requestVersion',jsonb_array_length(steer_inputs)+1,'gaps',jsonb_build_array('Parent request terminated or revised')) END,updated_at=NOW()
    WHERE id IN (SELECT id FROM descendants) AND status IN ('queued','leased','waiting')`, [parentId])
}

export async function enqueueWork(database: SqlQueryable, input: EnqueueWorkInput): Promise<EnqueueResult> {
  for (const field of ['tenantId', 'agentId', 'sessionId', 'kind', 'lane', 'triggerRef'] as const) {
    if (typeof input[field] !== 'string' || !input[field].trim()) throw new Error(`enqueue requires a non-empty ${field}`)
  }
  const id = input.id ?? randomUUID()
  const { rows } = await database.query(
    `INSERT INTO lingxios.agent_work_items
       (id, tenant_id, agent_id, session_id, thread_id, kind, lane, trigger_ref, principal_id, priority, available_at, meta, conversation)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,COALESCE($11::timestamptz, NOW()),$12::jsonb,$13::jsonb)
     ON CONFLICT (id) DO NOTHING
     RETURNING id`,
    [id, input.tenantId, input.agentId, input.sessionId, input.threadId ?? null, input.kind, input.lane,
      input.triggerRef, input.principalId ?? null, input.priority ?? 0, input.availableAt ?? null,
      input.meta ? JSON.stringify(input.meta) : null, input.conversation ? JSON.stringify(input.conversation) : null],
  )
  if (rows.length === 0) {
    const existing = await database.query(
      `SELECT id FROM lingxios.agent_work_items WHERE id=$1 AND tenant_id=$2 AND agent_id=$3
         AND session_id=$4 AND thread_id IS NOT DISTINCT FROM $5 AND kind=$6 AND lane=$7
         AND trigger_ref=$8 AND principal_id IS NOT DISTINCT FROM $9 AND priority=$10
         AND meta IS NOT DISTINCT FROM $11::jsonb AND conversation IS NOT DISTINCT FROM $12::jsonb`,
      [id, input.tenantId, input.agentId, input.sessionId, input.threadId ?? null, input.kind, input.lane,
        input.triggerRef, input.principalId ?? null, input.priority ?? 0,
        input.meta ? JSON.stringify(input.meta) : null, input.conversation ? JSON.stringify(input.conversation) : null],
    )
    if (existing.rows.length !== 1) throw new Error('work identity reused with a different request or principal')
  }
  return { id, deduplicated: rows.length === 0 }
}
