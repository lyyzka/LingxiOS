import { teacherPreview } from './teacher-preview.js'
import { boundedToolOutput } from '../../runtime/tool.js'
import { createHash, randomUUID } from 'node:crypto'
import type { HostAction, HostActionResult, WorkItem } from '../../protocol/types.js'
import { nativeWork } from './actions.js'
import { isDeepStrictEqual } from 'node:util'
import { withTransaction, type SqlPool, type SqlQueryable } from '../../control-plane/pg-store.js'
import type { ActionIntent } from '../../control-plane/stores.js'
import type { RequestSnapshot } from '../../context/request.js'
import type { LingxiLoopServices } from './service-contracts.js'
import { teacherTransaction } from './teacher.js'
import { teacherContext } from './teacher-context.js'
import { RUN_SEQUENCE_SPAN } from '../../protocol/constants.js'
import { applyRoutineApproval, routinePreview, routineScope } from './routines.js'

export const TEACHER_APPROVAL_TARGETS = {
  publish_objective: { table: 'learning_knowledge_units', key: 'objectiveId', status: 'PUBLISHED' },
  archive_objective: { table: 'learning_knowledge_units', key: 'objectiveId', status: 'ARCHIVED' },
  publish_activity: { table: 'learning_activities', key: 'activityId', status: 'PUBLISHED' },
  close_activity: { table: 'learning_activities', key: 'activityId', status: 'CLOSED' },
  review_evaluation: { table: 'learning_evaluations', key: 'evaluationId', status: 'ACCEPTED' },
} as const

export const TEACHER_APPROVAL_METHODS = [...Object.keys(TEACHER_APPROVAL_TARGETS), 'transition_course']

export async function requestRoutineApproval(database: SqlPool, services: Pick<LingxiLoopServices, 'permissionService'>,
  work: Omit<WorkItem, 'leaseToken'>, action: HostAction) {
  return withTransaction(database, async client => {
    const preview = await routinePreview(client, services, work, action)
    return persistApproval(client, work, action, {
      summary: action.action === 'routines.create' ? 'Create a paused routine; activation requires separate approval' : 'Activate the reviewed routine',
      scope: { projectId: preview.projectId, conversationId: work.sessionId }, preview,
    })
  })
}

export async function approveRoutine(database: SqlPool, services: Pick<LingxiLoopServices, 'permissionService'>,
  input: { companyId: string; userId: string; approvalId: string }) {
  const reviewed = await inspectApproval(database, services, input)
  if (!['routines.create', 'routines.activate'].includes(reviewed.action.action)) throw new Error('unsupported routine approval')
  if (reviewed.status === 'EXECUTED') return resumeApproved(database, input, reviewed)
  if (reviewed.status !== 'PENDING') throw new Error('routine approval is no longer pending')
  await withTransaction(database, async client => {
    await client.query("SET LOCAL lock_timeout='5s'")
    await client.query("SET LOCAL statement_timeout='15s'")
    await lockPendingApproval(client, input, reviewed)
    const native = await approvalNativeWork(client, input.companyId, reviewed.action.idempotencyKey)
    const work = { tenantId: native.companyId, agentId: native.agentId, sessionId: native.channelId,
      principalId: native.authorizationUserId!, ...(native.threadRootClientMsgNo !== undefined ? { threadId: native.threadRootClientMsgNo } : {}) }
    await routineScope(client, services, { ...work, principalId: input.userId })
    const preview = await routinePreview(client, services, work, reviewed.action)
    if (!isDeepStrictEqual(preview, reviewed.preview)) throw new Error('routine approval preview is stale')
    const value = await applyRoutineApproval(client, services, work, reviewed.action, preview)
    await recordExecutedApproval(client, input, reviewed, value, 'PENDING')
  })
  return resumeApproved(database, input, reviewed)
}

/** Lock the pending request, intent and approval together before a transactional domain mutation. */
export async function lockPendingApproval(database: SqlQueryable, input: { companyId: string; approvalId: string },
  reviewed: Awaited<ReturnType<typeof inspectApproval>>): Promise<ActionIntent> {
  const pending = await database.query(`SELECT intent.intent FROM approvals approval
    JOIN lingxios.agent_work_items work ON work.id=approval.work_id AND work.tenant_id=approval.company_id
    JOIN lingxios.agent_action_intents intent ON intent.idempotency_key=approval.idempotency_key
    JOIN lingxios.agent_os_sessions session ON session.tenant_id=work.tenant_id AND session.agent_id=work.agent_id
      AND session.session_id=work.session_id AND session.thread_id IS NOT DISTINCT FROM work.thread_id
    WHERE approval.id=$1 AND approval.company_id=$2 AND approval.status='PENDING' AND approval.expires_at>NOW()
      AND approval.idempotency_key=$3 AND approval.args=$4::jsonb AND approval.preview=$5::jsonb AND approval.action=$7
      AND work.status='waiting' AND work.cancel_requested_at IS NULL
      AND work.goal_outcome->>'status'='awaiting_approval' AND work.goal_outcome->>'approvalId'=$1
      AND (work.goal_outcome->>'requestVersion')::integer=$6 AND jsonb_array_length(work.steer_inputs)+1=$6
      AND EXISTS (SELECT 1 FROM lingxios.agent_request_snapshots snapshot WHERE snapshot.work_id=work.id
        AND snapshot.session_key=session.session_key AND snapshot.request_snapshot->'revisions'=work.steer_inputs)
    FOR UPDATE OF approval,work,session,intent`, [input.approvalId,input.companyId,reviewed.action.idempotencyKey,
    JSON.stringify(reviewed.action.args),JSON.stringify(reviewed.preview),reviewed.requestVersion,reviewed.action.action])
  const intent = pending.rows[0]?.['intent'] as ActionIntent | undefined
  if (!intent) throw new Error('approval expired or changed before execution')
  return intent
}

/** Called inside the same transaction as an effect, or after an authoritative external receipt. */
export async function recordExecutedApproval(database: SqlQueryable, input: { companyId: string; userId: string; approvalId: string },
  reviewed: Awaited<ReturnType<typeof inspectApproval>>, value: unknown, expectedStatus: 'PENDING' | 'EXECUTING') {
  const updated = await database.query(`UPDATE approvals SET status='EXECUTED',resolved_at=NOW(),resolved_by=$2,executed_at=NOW(),result=$3::jsonb,error=NULL
    WHERE id=$1 AND company_id=$4 AND status=$5 AND idempotency_key=$6 RETURNING id`,
    [input.approvalId,input.userId,JSON.stringify(value),input.companyId,expectedStatus,reviewed.action.idempotencyKey])
  if (updated.rows.length !== 1) throw new Error('approval changed while executing')
  const receipt = await database.query(`UPDATE lingxios.agent_action_ledger SET result=$2::jsonb
    WHERE idempotency_key=$1 AND result->'approval'->>'id'=$3 RETURNING idempotency_key`,
    [reviewed.action.idempotencyKey,JSON.stringify({ ok: true, value }),input.approvalId])
  if (receipt.rows.length !== 1) throw new Error('approval receipt is missing')
}

/** Human-facing review only; this neither decides nor executes an approval. */
export async function inspectApproval(database: SqlQueryable, services: Pick<LingxiLoopServices, 'permissionService'>,
  input: { companyId: string; userId: string; approvalId: string }) {
  for (const value of [input.companyId, input.userId, input.approvalId]) {
    if (typeof value !== 'string' || !value.trim()) throw new Error('approval identity is required')
  }
  await services.permissionService.assertCan({ actorUserId: input.userId, companyId: input.companyId,
    action: 'agent_approval:resolve', resource: { type: 'approval', id: input.approvalId } })
  const { rows } = await database.query(`SELECT to_jsonb(approval) AS approval, intent.intent,
      snapshot.request_snapshot AS request, work.steer_inputs AS revisions, work.thread_id AS thread_id
    FROM approvals approval
    JOIN lingxios.agent_action_intents intent ON intent.idempotency_key=approval.idempotency_key
    JOIN lingxios.agent_work_items work ON work.id=approval.work_id
      AND work.tenant_id=approval.company_id AND work.agent_id=approval.agent_id AND work.session_id=approval.channel_id
      AND work.principal_id=approval.authorization_user_id
    JOIN lingxios.agent_os_sessions session ON session.tenant_id=work.tenant_id AND session.agent_id=work.agent_id
      AND session.session_id=work.session_id AND session.thread_id IS NOT DISTINCT FROM work.thread_id
    JOIN lingxios.agent_request_snapshots snapshot ON snapshot.work_id=work.id AND snapshot.session_key=session.session_key
    WHERE approval.id=$1 AND approval.company_id=$2 AND approval.source='AGENT_OS'`, [input.approvalId, input.companyId])
  if (rows.length !== 1) throw new Error('approval has no unique namespaced recovery record')
  const row = rows[0]!
  const approval = row['approval'] as Record<string, unknown>
  const intent = row['intent'] as ActionIntent | null
  const request = row['request'] as RequestSnapshot | null
  if (!intent || !request || intent.tenantId !== input.companyId || intent.workId !== approval['work_id']
    || intent.threadId !== row['thread_id'] || intent.action.runId !== intent.workId
    || intent.agentId !== approval['agent_id'] || intent.sessionId !== approval['channel_id']
    || intent.principalId !== approval['authorization_user_id'] || request.authorId !== intent.principalId
    || request.workId !== intent.workId || request.tenantId !== intent.tenantId || request.sessionId !== intent.sessionId
    || intent.requestVersion !== request.revisions.length + 1 || !isDeepStrictEqual(request.revisions, row['revisions'])
    || intent.action.idempotencyKey !== approval['idempotency_key'] || intent.action.action !== approval['action']
    || !isDeepStrictEqual(intent.action.args, approval['args'])) throw new Error('approval does not match the current request and action intent')
  await services.permissionService.assertCan({ actorUserId: input.userId, companyId: input.companyId,
    action: 'conversation:read', resource: { type: 'conversation', id: intent.sessionId } })
  if (intent.action.action.startsWith('teacher.')) await services.permissionService.assertCan({ actorUserId: input.userId, companyId: input.companyId,
    action: 'learning:manage', resource: { type: 'approval', id: input.approvalId } })
  return { approvalId: input.approvalId, status: approval['status'], requestVersion: intent.requestVersion,
    originalInput: request.originalText, revisions: structuredClone(request.revisions), action: structuredClone(intent.action),
    summary: approval['summary'] ?? null, preview: approval['preview'], result: approval['result'] ?? null, expiresAt: approval['expires_at'] }
}

/** Linearization point shared by approvals whose native effect runs outside PostgreSQL. */
export async function claimApprovalExecution(database: SqlQueryable,
  input: { companyId: string; userId: string; approvalId: string },
  reviewed: Awaited<ReturnType<typeof inspectApproval>>) {
  const params = [input.approvalId, input.userId, input.companyId, reviewed.action.idempotencyKey,
    JSON.stringify(reviewed.action.args), JSON.stringify(reviewed.preview), reviewed.requestVersion]
  if (reviewed.status === 'PENDING') {
    const claimed = await database.query(`UPDATE approvals approval
      SET status='EXECUTING',resolved_at=COALESCE(resolved_at,NOW()),resolved_by=COALESCE(resolved_by,$2)
      FROM lingxios.agent_work_items work,lingxios.agent_action_intents intent
      WHERE work.id=approval.work_id AND work.tenant_id=approval.company_id
        AND intent.idempotency_key=approval.idempotency_key
        AND approval.id=$1 AND approval.company_id=$3 AND approval.status='PENDING' AND approval.expires_at>NOW()
        AND approval.idempotency_key=$4 AND approval.args=$5::jsonb AND approval.preview=$6::jsonb
        AND work.status='waiting' AND work.cancel_requested_at IS NULL
        AND work.goal_outcome->>'status'='awaiting_approval' AND work.goal_outcome->>'approvalId'=$1
        AND (work.goal_outcome->>'requestVersion')::integer=$7 AND jsonb_array_length(work.steer_inputs)+1=$7
      RETURNING intent.intent`, params)
    const intent = claimed.rows[0]?.['intent'] as ActionIntent | undefined
    if (!intent) throw new Error('approval expired or changed before execution')
    return { intent, recovering: false }
  }
  if (reviewed.status !== 'EXECUTING') throw new Error('approval is no longer pending or executing')
  const existing = await database.query(`SELECT intent.intent FROM approvals approval
    JOIN lingxios.agent_work_items work ON work.id=approval.work_id AND work.tenant_id=approval.company_id
    JOIN lingxios.agent_action_intents intent ON intent.idempotency_key=approval.idempotency_key
    WHERE approval.id=$1 AND approval.company_id=$3 AND approval.status='EXECUTING'
      AND approval.idempotency_key=$4 AND approval.args=$5::jsonb AND approval.preview=$6::jsonb
      AND work.status='waiting' AND work.cancel_requested_at IS NULL
      AND work.goal_outcome->>'status'='awaiting_approval' AND work.goal_outcome->>'approvalId'=$1
      AND (work.goal_outcome->>'requestVersion')::integer=$7 AND jsonb_array_length(work.steer_inputs)+1=$7`, params)
  const intent = existing.rows[0]?.['intent'] as ActionIntent | undefined
  if (!intent) throw new Error('executing approval no longer matches its durable intent')
  return { intent, recovering: true }
}

/** Creates a domain approval; the business mutation remains unexecuted. */
export async function requestKnowledgeApproval(database: SqlQueryable, services: LingxiLoopServices,
  work: Omit<WorkItem, 'leaseToken'>, action: HostAction): Promise<HostActionResult> {
  const enabledChange = action.action === 'knowledge.set_source_enabled'
  const allowed = enabledChange ? ['sourceId', 'enabled'] : ['sourceId']
  if ((!enabledChange && action.action !== 'knowledge.delete_source')
    || Object.keys(action.args).some(key => !allowed.includes(key))
    || typeof action.args['sourceId'] !== 'string' || !action.args['sourceId'].trim()
    || (enabledChange && typeof action.args['enabled'] !== 'boolean')) throw new Error('invalid knowledge approval arguments')
  const native = nativeWork(work)
  const sourceId = action.args['sourceId']
  await services.permissionService.assertCan({ actorUserId: native.authorizationUserId!, companyId: native.companyId,
    action: 'knowledge:manage', resource: { type: 'knowledge_source', id: sourceId } })
  const sources = await services.knowledge.listKnowledgeSourcesForAgent(native)
  const source = sources.find(item => item && typeof item === 'object' && (item as Record<string, unknown>)['id'] === sourceId) as Record<string, unknown> | undefined
  if (!source) throw new Error('knowledge source is outside the current workspace')
  const preview = { sourceId, sourceStateSha256: createHash('sha256').update(JSON.stringify(source)).digest('hex'), title: typeof source['title'] === 'string' ? source['title'].slice(0, 2000) : sourceId,
    ...(enabledChange ? { enabled: action.args['enabled'] } : {}) }
  return persistApproval(database, work, action, {
    summary: enabledChange ? 'Change knowledge source availability' : 'Delete knowledge source',
    scope: { risk: 'sensitive_or_destructive_action', sourceId }, preview,
  })
}

export async function requestTeacherApproval(database: SqlQueryable, services: Pick<LingxiLoopServices, 'teacher' | 'permissionService'>,
  work: Omit<WorkItem, 'leaseToken'>, action: HostAction): Promise<HostActionResult> {
  if (work.kind === 'teacher_digest') throw new Error('scheduled teacher summaries are read-only')
  const method = action.action.slice('teacher.'.length)
  const fields: Record<string, string[]> = { publish_objective: ['objectiveId'], archive_objective: ['objectiveId'], publish_activity: ['activityId'], close_activity: ['activityId'], transition_course: ['command'], set_teacher_membership: ['userId', 'enabled'], review_evaluation: ['evaluationId', 'decision', 'reason'] }
  if (!action.action.startsWith('teacher.') || !Object.hasOwn(fields, method)
    || Object.keys(action.args).some(key => !fields[method]!.includes(key))) throw new Error('invalid teacher approval action')
  for (const key of fields[method]!) {
    const value = action.args[key]
    if (key === 'enabled') { if (typeof value !== 'boolean') throw new Error('enabled must be boolean') }
    else if (typeof value !== 'string' || !value.trim() || value.length > (key === 'reason' ? 10000 : 2000)) throw new Error('invalid teacher approval argument')
  }
  if (method === 'transition_course' && !['END', 'ENTER_READ_ONLY', 'ARCHIVE'].includes(String(action.args['command']))) throw new Error('invalid course transition')
  if (method === 'review_evaluation' && action.args['decision'] !== 'accept' && action.args['decision'] !== 'reject') throw new Error('invalid evaluation decision')
  const context = await teacherContext(work, services, database)
  if (!services.teacher?.assertTeacherApprovalFresh) throw new Error('native teacher approval services are required')
  if (method === 'set_teacher_membership' && (!services.teacher.requireLearningCourseRole || !services.teacher.setLearningCourseMembershipRecord || !services.teacher.enqueueLearningEffect)) throw new Error('native transactional teacher membership services are required')
  const metadata = await teacherPreview(database, services.teacher, work.tenantId, context, action)
  const entityId = method === 'transition_course' ? context.course.id
    : String(action.args[fields[method]![0]!] ?? '').trim()
  if (!metadata || metadata.requestedBy !== work.principalId || metadata.scope['projectId'] !== context.agent.projectId
    || metadata.scope['courseId'] !== context.course.id || metadata.scope['roomId'] !== work.sessionId
    || metadata.preview['method'] !== method || metadata.preview['entityId'] !== entityId || !isDeepStrictEqual(metadata.preview['args'], action.args)
    || (method === 'set_teacher_membership' ? typeof metadata.preview['currentVersion'] !== 'boolean'
      : typeof metadata.preview['currentVersion'] !== 'string' || !metadata.preview['currentVersion'].trim())
    || typeof metadata.summary !== 'string' || !metadata.summary.trim()) throw new Error('teacher approval preview does not match the scoped request')
  return persistApproval(database, work, action, method === 'transition_course' && action.args['command'] !== 'END'
    ? { ...metadata, summary: `${metadata.summary}. This closes the teacher room and stops continuation of this task; external archive synchronization is separate.` }
    : metadata)
}

export async function persistApproval(database: SqlQueryable, work: Omit<WorkItem, 'leaseToken'>, action: HostAction,
  metadata: { summary: string; scope: Record<string, unknown>; preview: Record<string, unknown> }): Promise<HostActionResult> {
  const { rows } = await database.query(`INSERT INTO approvals
      (id,company_id,agent_id,channel_id,source,work_id,authorization_user_id,idempotency_key,action,args,summary,requested_by,scope,preview,expires_at)
    SELECT $1,$2,$3,$4,'AGENT_OS',$5,$6,$7,$8,$9::jsonb,$10,$6,$11::jsonb,$12::jsonb,NOW()+INTERVAL '1 hour'
    FROM lingxios.agent_action_intents intent
    JOIN lingxios.agent_work_items work ON work.id=$5 AND work.tenant_id=$2 AND work.agent_id=$3 AND work.session_id=$4
    WHERE intent.idempotency_key=$7 AND work.fence=$13 AND work.status='leased'
      AND work.cancel_requested_at IS NULL AND work.lease_expires_at>NOW()
      AND (intent.intent->>'requestVersion')::integer=jsonb_array_length(work.steer_inputs)+1
      AND intent.intent->>'workId'=$5 AND intent.intent->>'tenantId'=$2
      AND intent.intent->>'principalId'=$6 AND intent.intent->'action'->'args'=$9::jsonb
      AND intent.intent->'action'->>'action'=$8
    ON CONFLICT(idempotency_key) DO NOTHING RETURNING id`,
    [randomUUID(), work.tenantId, work.agentId, work.sessionId, work.id, work.principalId, action.idempotencyKey,
      action.action, JSON.stringify(action.args), metadata.summary,
      JSON.stringify(metadata.scope), JSON.stringify(metadata.preview), work.fence])
  if (rows.length !== 1) throw new Error('approval creation requires a new matching durable action intent; reconcile existing approvals')
  return { ok: false, approval: { id: String(rows[0]!['id']), status: 'PENDING' } }
}

export async function rejectApproval(database: SqlPool, services: Pick<LingxiLoopServices, 'permissionService'>,
  input: { companyId: string; userId: string; approvalId: string }) {
  const reviewed = await inspectApproval(database, services, input)
  return withTransaction(database, async client => {
    await client.query("SET LOCAL lock_timeout='5s'")
    await client.query("SET LOCAL statement_timeout='15s'")
    // ponytail: rare approval resumes briefly serialize lease writes; use per-session coordination if contention matters.
    await client.query('LOCK TABLE lingxios.agent_os_session_leases IN SHARE ROW EXCLUSIVE MODE')
    const { rows } = await client.query(`SELECT approval.status, work.id, session.session_key
      FROM approvals approval JOIN lingxios.agent_work_items work ON work.id=approval.work_id
      JOIN lingxios.agent_os_sessions session ON session.tenant_id=work.tenant_id AND session.agent_id=work.agent_id
        AND session.session_id=work.session_id AND session.thread_id IS NOT DISTINCT FROM work.thread_id
      WHERE approval.id=$1 AND approval.company_id=$2 AND work.tenant_id=$2
        AND approval.idempotency_key=$3 AND approval.args=$4::jsonb AND approval.action=$5
        AND EXISTS (SELECT 1 FROM lingxios.agent_request_snapshots snapshot WHERE snapshot.work_id=work.id
          AND snapshot.session_key=session.session_key AND snapshot.request_snapshot->'revisions'=work.steer_inputs)
        AND jsonb_array_length(work.steer_inputs)+1=$6
      FOR UPDATE OF approval,work,session`,
      [input.approvalId, input.companyId, reviewed.action.idempotencyKey, JSON.stringify(reviewed.action.args), reviewed.action.action, reviewed.requestVersion])
    const row = rows[0]
    if (rows.length !== 1 || !row) throw new Error('approval changed before rejection')
    if (row['status'] === 'REJECTED') return { status: 'already_rejected' as const, workId: String(row['id']) }
    if (row['status'] !== 'PENDING') throw new Error('approval is no longer pending')
    const active = await client.query('SELECT work_id FROM lingxios.agent_os_session_leases WHERE session_key=$1 AND expires_at>NOW()', [row['session_key']])
    if (active.rows.length) throw new Error('session is active; retry rejection after it pauses')
    const resumed = await client.query(`UPDATE lingxios.agent_work_items SET status='queued', available_at=NOW(),
        goal_outcome=NULL,finished_at=NULL,result_text=NULL,error=NULL,updated_at=NOW()
      WHERE id=$1 AND status='waiting' AND cancel_requested_at IS NULL
        AND goal_outcome->>'status'='awaiting_approval' AND goal_outcome->>'approvalId'=$2
        AND (goal_outcome->>'requestVersion')::integer=$3 RETURNING id`, [row['id'], input.approvalId, reviewed.requestVersion])
    if (resumed.rows.length !== 1) throw new Error('work is not waiting for this approval')
    const receipt = await client.query(`UPDATE lingxios.agent_action_ledger SET result=$2::jsonb
      WHERE idempotency_key=$1 AND result->'approval'->>'id'=$3 RETURNING idempotency_key`,
      [reviewed.action.idempotencyKey, JSON.stringify({ ok: false, error: 'Human rejected this action; do not execute it' }), input.approvalId])
    if (receipt.rows.length !== 1) throw new Error('pending approval receipt is missing')
    await client.query(`UPDATE approvals SET status='REJECTED',resolved_at=NOW(),resolved_by=$2 WHERE id=$1`, [input.approvalId, input.userId])
    await client.query(`UPDATE lingxios.agent_os_sessions SET history=history || $2::jsonb,revision=revision+1,updated_at=NOW() WHERE session_key=$1`,
      [row['session_key'], JSON.stringify([{ role: 'user', content: `Approval ${input.approvalId} was rejected by the human reviewer. The proposed action was not executed by this approval. Do not repeat it or bypass the rejection; continue with the permitted parts of the original request.` }])])
    return { status: 'resumed' as const, workId: String(row['id']) }
  })
}

export async function approveTeacher(database: SqlPool, services: Pick<LingxiLoopServices, 'teacher' | 'permissionService'>,
  input: { companyId: string; userId: string; approvalId: string }) {
  const reviewed = await inspectApproval(database, services, input)
  const method = reviewed.action.action.slice('teacher.'.length)
  const membership = method === 'set_teacher_membership'
  const transition = method === 'transition_course'
  if (!reviewed.action.action.startsWith('teacher.') || !membership && !TEACHER_APPROVAL_METHODS.includes(method)) throw new Error('unsupported teacher approval execution')
  if (reviewed.status === 'EXECUTED') return resumeApproved(database, input, reviewed)
  if (reviewed.status !== 'PENDING') throw new Error('teacher approval is no longer pending')
  const teacher = services.teacher
  if (!teacher?.assertTeacherApprovalFresh) throw new Error('native teacher approval services are required')
  if (membership && (!teacher.requireLearningCourseRole || !teacher.setLearningCourseMembershipRecord || !teacher.enqueueLearningEffect)) throw new Error('native transactional teacher membership services are required')
  const target = TEACHER_APPROVAL_TARGETS[method as keyof typeof TEACHER_APPROVAL_TARGETS]
  await withTransaction(database, async client => {
    await client.query("SET LOCAL lock_timeout='5s'")
    await client.query("SET LOCAL statement_timeout='15s'")
    await lockPendingApproval(client, input, reviewed)
    const native = await approvalNativeWork(client, input.companyId, reviewed.action.idempotencyKey)
    const locked = transition ? await client.query(`SELECT project.id,course.id AS course_id,course.project_id FROM projects project
      JOIN courses course ON course.company_id=project.company_id AND course.project_id=project.id
      JOIN learning_course_teacher_rooms room ON room.company_id=course.company_id AND room.course_id=course.id
      WHERE project.company_id=$1 AND course.id=$2 AND room.conversation_id=$3 AND room.status='active'
      FOR UPDATE OF project,course,room`, [input.companyId, (reviewed.preview as Record<string, unknown>)['entityId'], native.channelId])
      : membership ? await client.query(`SELECT course.id AS course_id,course.project_id FROM courses course
      JOIN learning_course_teacher_rooms room ON room.company_id=course.company_id AND room.course_id=course.id
      WHERE course.company_id=$1 AND room.conversation_id=$2 FOR UPDATE OF course,room`, [input.companyId, native.channelId])
      : await client.query(`SELECT target.id,course.id AS course_id,course.project_id FROM ${target.table} target
      JOIN courses course ON course.company_id=target.company_id AND course.project_id=target.project_id
      JOIN learning_course_teacher_rooms room ON room.company_id=course.company_id AND room.course_id=course.id
      WHERE target.company_id=$1 AND target.id=$2 AND room.conversation_id=$3
      FOR UPDATE OF target,course,room`, [input.companyId, reviewed.action.args[target.key], native.channelId])
    if (locked.rows.length !== 1) throw new Error('teacher approval target is outside the current course')
    const context = await teacherContext({ tenantId: native.companyId, agentId: native.agentId, sessionId: native.channelId,
      principalId: native.authorizationUserId!, kind: 'resume' }, services, client)
    const metadata = await teacherPreview(client, teacher, native.companyId, context, reviewed.action)
    if (!metadata || metadata.requestedBy !== native.authorizationUserId || metadata.scope['courseId'] !== locked.rows[0]!['course_id']
      || metadata.scope['projectId'] !== locked.rows[0]!['project_id'] || metadata.scope['roomId'] !== native.channelId
      || !isDeepStrictEqual(metadata.preview, reviewed.preview)) throw new Error('teacher approval preview is stale')
    await teacher.assertTeacherApprovalFresh!({ channelId: native.channelId, companyId: input.companyId, action: reviewed.action.action, preview: metadata.preview }, client)
    let value: unknown
    if (membership) {
      const courseId = String(locked.rows[0]!['course_id'])
      await teacher.requireLearningCourseRole!(client, { companyId: input.companyId, courseId, userId: native.authorizationUserId!, role: 'teacher' })
      const outcome = await teacher.setLearningCourseMembershipRecord!(client, { companyId: input.companyId, courseId,
        userId: String(reviewed.action.args['userId']), role: 'teacher', enabled: reviewed.action.args['enabled'] as boolean })
      if (outcome !== 'updated') throw new Error(`teacher membership change rejected: ${outcome}`)
      const observed = await client.query(`SELECT 1 FROM project_memberships WHERE company_id=$1 AND project_id=$2
        AND user_id=$3 AND status='ACTIVE' AND role IN ('OWNER','TEACHER')`, [input.companyId, locked.rows[0]!['project_id'], reviewed.action.args['userId']])
      if (Boolean(observed.rows.length) !== reviewed.action.args['enabled']) throw new Error('teacher membership postcondition was not observed')
      await teacher.enqueueLearningEffect!(client, { companyId: input.companyId, courseId, kind: 'teacher_room.sync' })
      value = { ok: true, enabled: reviewed.action.args['enabled'], channelSync: 'queued' }
    } else {
      const scope = { companyId: input.companyId, courseId: context.course.id, teacherId: context.trigger.teacherId }
      const transaction = teacherTransaction(client), args = reviewed.action.args
      if (method === 'publish_objective' || method === 'archive_objective') {
        await teacher.setLearningObjectiveStatus(client, { ...scope, objectiveId: String(args['objectiveId']).trim(), status: method === 'publish_objective' ? 'PUBLISHED' : 'ARCHIVED' })
        value = { ok: true }
      } else if (method === 'publish_activity') {
        await teacher.publishLearningActivity(transaction, { ...scope, activityId: String(args['activityId']).trim() })
        value = { ok: true }
      } else if (method === 'close_activity') {
        await teacher.closeLearningActivity(client, { ...scope, activityId: String(args['activityId']).trim() })
        value = { ok: true }
      } else if (method === 'review_evaluation') {
        await teacher.reviewLearningEvaluation(client, transaction, teacher.inc, { ...scope, evaluationId: String(args['evaluationId']).trim(), decision: args['decision'] as 'accept' | 'reject', reason: String(args['reason']).trim() })
        value = { ok: true }
      } else if (method === 'transition_course') {
        const lifecycle = new teacher.ProjectLifecycleApplication({ transaction, auditInTransaction: teacher.auditInTransaction, projectLifecycleProjection: teacher.projectLifecycleProjection })
        value = await lifecycle.executeInTransaction(client, { actorUserId: context.trigger.teacherId, companyId: input.companyId, projectId: context.agent.projectId, command: args['command'] as 'END' | 'ENTER_READ_ONLY' | 'ARCHIVE' })
      } else throw new Error('unsupported teacher approval execution')
      if (!value || typeof value !== 'object' || Reflect.get(value, 'ok') !== true) throw new Error('teacher approval action was not acknowledged')
      if (transition) {
        const expectedStatus = { END: 'COURSE_ENDED', ENTER_READ_ONLY: 'READ_ONLY', ARCHIVE: 'ARCHIVED' }[String(reviewed.action.args['command'])]
        const observed = await client.query(`SELECT project.status,room.status AS room_status FROM projects project
          JOIN courses course ON course.company_id=project.company_id AND course.project_id=project.id
          JOIN learning_course_teacher_rooms room ON room.company_id=course.company_id AND room.course_id=course.id
          WHERE project.company_id=$1 AND course.id=$2 AND room.conversation_id=$3`,
          [input.companyId, locked.rows[0]!['course_id'], native.channelId])
        const roomStatus = expectedStatus === 'COURSE_ENDED' ? 'active' : 'closed'
        if (!expectedStatus || observed.rows.length !== 1 || observed.rows[0]!['status'] !== expectedStatus
          || observed.rows[0]!['room_status'] !== roomStatus || Reflect.get(value, 'status') !== expectedStatus
          || typeof Reflect.get(value, 'applied') !== 'boolean') throw new Error('teacher course transition postcondition was not observed')
        value = { ...value, teacherRoomStatus: roomStatus }
      } else {
        const observed = await client.query(`SELECT status FROM ${target.table} WHERE company_id=$1 AND id=$2`, [input.companyId, reviewed.action.args[target.key]])
        const expectedStatus = method === 'review_evaluation' && reviewed.action.args['decision'] === 'reject' ? 'REJECTED' : target.status
        if (observed.rows.length !== 1 || observed.rows[0]!['status'] !== expectedStatus) throw new Error('teacher approval postcondition was not observed')
      }
    }
    await recordExecutedApproval(client, input, reviewed, value, 'PENDING')
  })
  return resumeApproved(database, input, reviewed)
}

export async function approveKnowledge(database: SqlPool, services: LingxiLoopServices,
  input: { companyId: string; userId: string; approvalId: string }) {
  const reviewed = await inspectApproval(database, services, input)
  if (reviewed.action.action !== 'knowledge.set_source_enabled' && reviewed.action.action !== 'knowledge.delete_source') throw new Error('unsupported approval action')
  if (reviewed.status === 'EXECUTED') return resumeApproved(database, input, reviewed)
  if (reviewed.status === 'APPROVED') return { status: 'reconciliation_required' as const }
  if (reviewed.status !== 'PENDING') throw new Error('approval is no longer pending')
  const native = await approvalNativeWork(database, input.companyId, reviewed.action.idempotencyKey)
  const sourceId = reviewed.action.args['sourceId']
  const enabled = reviewed.action.args['enabled']
  if (typeof sourceId !== 'string' || (reviewed.action.action === 'knowledge.set_source_enabled' && typeof enabled !== 'boolean')) throw new Error('invalid approved action arguments')
  await services.permissionService.assertCan({ actorUserId: native.authorizationUserId!, companyId: input.companyId,
    action: 'knowledge:manage', resource: { type: 'knowledge_source', id: sourceId } })
  const sources = await services.knowledge.listKnowledgeSourcesForAgent(native)
  const source = sources.find(item => item && typeof item === 'object' && (item as Record<string, unknown>)['id'] === sourceId)
  const preview = reviewed.preview as Record<string, unknown> | null
  if (!source || !preview || preview['sourceStateSha256'] !== createHash('sha256').update(JSON.stringify(source)).digest('hex')) throw new Error('knowledge source changed; request a fresh approval preview')
  const started = await withTransaction(database, async client => {
    const changed = await client.query(`UPDATE approvals SET status='APPROVED',resolved_at=NOW(),resolved_by=$2
      WHERE id=$1 AND company_id=$3 AND status='PENDING' AND expires_at>NOW()
        AND idempotency_key=$4 AND args=$5::jsonb AND preview=$6::jsonb
        AND EXISTS(SELECT 1 FROM lingxios.agent_work_items work WHERE work.id=approvals.work_id
          AND work.tenant_id=$3 AND work.status='waiting' AND work.cancel_requested_at IS NULL
          AND work.goal_outcome->>'status'='awaiting_approval' AND work.goal_outcome->>'approvalId'=$1
          AND (work.goal_outcome->>'requestVersion')::integer=$7 AND jsonb_array_length(work.steer_inputs)+1=$7)
      RETURNING id`, [input.approvalId, input.userId, input.companyId, reviewed.action.idempotencyKey,
      JSON.stringify(reviewed.action.args), JSON.stringify(reviewed.preview), reviewed.requestVersion])
    if (!changed.rows.length) return false
    const receipt = await client.query(`UPDATE lingxios.agent_action_ledger SET result=$2::jsonb
      WHERE idempotency_key=$1 AND result->'approval'->>'id'=$3 RETURNING idempotency_key`,
      [reviewed.action.idempotencyKey, JSON.stringify({ ok: false, executionState: 'unknown', error: 'Approved action execution started; reconcile before retrying' }), input.approvalId])
    if (receipt.rows.length !== 1) throw new Error('pending approval receipt is missing')
    return true
  })
  if (!started) throw new Error('approval expired or changed before execution')
  let value: unknown
  try {
    if (reviewed.action.action === 'knowledge.set_source_enabled') {
      const result = await services.knowledge.setKnowledgeSourceEnabled(native, sourceId, enabled as boolean)
      if (result.enabled !== enabled) throw new Error('native result does not confirm the approved availability')
      value = result
    } else {
      const result = await services.knowledge.deleteKnowledgeSourceForAgent(native, sourceId)
      if (result.deleted !== true) throw new Error('native result does not confirm deletion')
      value = { ...result, externalAssetCleanup: 'not_verified' }
    }
  } catch {
    return { status: 'reconciliation_required' as const }
  }
  await recordApprovedResult(database, input, reviewed.action.idempotencyKey, value)
  return resumeApproved(database, input, reviewed)
}

export async function resumeApproved(database: SqlPool, input: { companyId: string; userId: string; approvalId: string },
  reviewed: Awaited<ReturnType<typeof inspectApproval>>) {
  return withTransaction(database, async client => {
    await client.query("SET LOCAL lock_timeout='5s'")
    await client.query("SET LOCAL statement_timeout='15s'")
    // ponytail: same brief lease-write lock as rejection; replace with per-session coordination if contention matters.
    await client.query('LOCK TABLE lingxios.agent_os_session_leases IN SHARE ROW EXCLUSIVE MODE')
    const { rows } = await client.query(`SELECT approval.resumed_at,approval.result,work.id,session.session_key
      FROM approvals approval JOIN lingxios.agent_work_items work ON work.id=approval.work_id
      JOIN lingxios.agent_action_intents intent ON intent.idempotency_key=approval.idempotency_key
      JOIN lingxios.agent_action_ledger ledger ON ledger.idempotency_key=approval.idempotency_key
      JOIN lingxios.agent_os_sessions session ON session.tenant_id=work.tenant_id AND session.agent_id=work.agent_id
        AND session.session_id=work.session_id AND session.thread_id IS NOT DISTINCT FROM work.thread_id
      WHERE approval.id=$1 AND approval.company_id=$2 AND approval.source='AGENT_OS' AND work.tenant_id=$2 AND approval.status='EXECUTED'
        AND approval.idempotency_key=$3 AND approval.args=$4::jsonb AND approval.action=$5
        AND intent.intent->>'workId'=work.id AND intent.intent->>'tenantId'=work.tenant_id
        AND intent.intent->>'agentId'=work.agent_id AND intent.intent->>'sessionId'=work.session_id
        AND intent.intent->>'principalId'=work.principal_id
        AND intent.intent->'action'->>'idempotencyKey'=approval.idempotency_key
        AND intent.intent->'action'->>'action'=approval.action AND intent.intent->'action'->'args'=approval.args
        AND (intent.intent->>'requestVersion')::integer=$6
        AND ledger.result=jsonb_build_object('ok',true,'value',approval.result)
        AND EXISTS (SELECT 1 FROM lingxios.agent_request_snapshots snapshot WHERE snapshot.work_id=work.id
          AND snapshot.session_key=session.session_key AND snapshot.request_snapshot->'revisions'=work.steer_inputs
          AND snapshot.request_snapshot->>'tenantId'=work.tenant_id AND snapshot.request_snapshot->>'sessionId'=work.session_id
          AND snapshot.request_snapshot->>'authorId'=work.principal_id)
        AND jsonb_array_length(work.steer_inputs)+1=$6
      FOR UPDATE OF approval,work,session,intent,ledger`,
      [input.approvalId, input.companyId, reviewed.action.idempotencyKey, JSON.stringify(reviewed.action.args), reviewed.action.action, reviewed.requestVersion])
    const row = rows[0]
    if (rows.length !== 1 || !row) throw new Error('approved execution lacks matching durable recovery records')
    const closedCourse = reviewed.action.action === 'teacher.transition_course'
      && (row['result'] as Record<string, unknown> | null)?.['teacherRoomStatus'] === 'closed'
    const unavailable = { status: 'continuation_unavailable' as const, workId: String(row['id']), result: row['result'] }
    if (row['resumed_at']) return closedCourse ? unavailable : { status: 'already_resumed' as const, workId: String(row['id']) }
    const active = await client.query('SELECT work_id FROM lingxios.agent_os_session_leases WHERE session_key=$1 AND expires_at>NOW()', [row['session_key']])
    if (active.rows.length) throw new Error('execution recorded; session is active, retry continuation after it pauses')
    const gap = 'The approved course transition closed the teacher room. Remaining work cannot continue in this room; overall goal acceptance and external archive synchronization are not verified.'
    const outcome = closedCourse ? { status: 'blocked', verification: 'inconclusive', requestVersion: reviewed.requestVersion, gaps: [gap] } : null
    const resumed = await client.query(`UPDATE lingxios.agent_work_items SET status=CASE WHEN $4::jsonb IS NULL THEN 'queued' ELSE 'blocked' END,available_at=NOW(),
        goal_outcome=$4::jsonb,finished_at=CASE WHEN $4::jsonb IS NULL THEN NULL ELSE NOW() END,result_text=$5,error=NULL,updated_at=NOW()
      WHERE id=$1 AND status='waiting' AND cancel_requested_at IS NULL
        AND goal_outcome->>'status'='awaiting_approval' AND goal_outcome->>'approvalId'=$2
        AND (goal_outcome->>'requestVersion')::integer=$3 RETURNING id`, [row['id'], input.approvalId, reviewed.requestVersion, outcome ? JSON.stringify(outcome) : null, closedCourse ? gap : null])
    if (resumed.rows.length !== 1) throw new Error('execution recorded; work is not waiting for this approval')
    if (closedCourse) {
      const event = await client.query(`INSERT INTO lingxios.agent_run_events(run_id,seq,tenant_id,agent_id,kind,stage,visibility,data,recorded_at)
        SELECT work.id,GREATEST(COALESCE(MAX(event.seq),0),(work.fence::bigint-1)*$4)+1,
          work.tenant_id,work.agent_id,'approval.continuation_stopped','completed','user',$3::jsonb,NOW()
        FROM lingxios.agent_work_items work LEFT JOIN lingxios.agent_run_events event ON event.run_id=work.id
        WHERE work.id=$1 AND work.tenant_id=$2
        GROUP BY work.id,work.tenant_id,work.agent_id,work.fence
        HAVING GREATEST(COALESCE(MAX(event.seq),0),(work.fence::bigint-1)*$4)<work.fence::bigint*$4 RETURNING seq`,
        [row['id'], input.companyId, JSON.stringify({ approvalId: input.approvalId, goalOutcome: outcome }), RUN_SEQUENCE_SPAN])
      if (event.rows.length !== 1) throw new Error('execution recorded; no event sequence remains for the stopped continuation')
    }
    await client.query(`UPDATE lingxios.agent_os_sessions SET history=history || $2::jsonb,revision=revision+1,updated_at=NOW() WHERE session_key=$1`,
      [row['session_key'], JSON.stringify([{ role: 'user', content: `Approval ${input.approvalId} was granted. Recorded action result: ${boundedToolOutput(row['result'])}. Do not execute the same action again. ` + (closedCourse ? gap : 'Continue checking the original request and remaining deliverables; this receipt alone does not prove overall completion.') }])])
    await client.query('UPDATE approvals SET resumed_at=NOW() WHERE id=$1', [input.approvalId])
    return closedCourse ? unavailable : { status: 'resumed' as const, workId: String(row['id']) }
  })
}

async function approvalNativeWork(database: SqlQueryable, companyId: string, idempotencyKey: string) {
  const { rows } = await database.query(`SELECT work.*,intent.intent FROM lingxios.agent_work_items work
    JOIN lingxios.agent_action_intents intent ON intent.intent->>'workId'=work.id
    WHERE intent.idempotency_key=$1 AND work.tenant_id=$2`, [idempotencyKey, companyId])
  if (rows.length !== 1) throw new Error('approval action identity is missing')
  const row = rows[0]!
  const intent = row['intent'] as ActionIntent
  const work = { id: intent.workId, tenantId: intent.tenantId, principalId: intent.principalId!, agentId: intent.agentId,
    sessionId: intent.sessionId, ...(intent.threadId !== null ? { threadId: intent.threadId } : {}), kind: 'resume', lane: 'approval' as const,
    triggerRef: String(row['trigger_ref']), fence: Number(row['fence']), homeEpoch: 1 }
  return nativeWork(work)
}

async function recordApprovedResult(database: SqlPool, input: { companyId: string; approvalId: string }, idempotencyKey: string, value: unknown) {
  await withTransaction(database, async client => {
    const saved = await client.query(`UPDATE approvals SET status='EXECUTED',executed_at=NOW(),result=$2::jsonb,error=NULL
      WHERE id=$1 AND company_id=$3 AND idempotency_key=$4 AND status='APPROVED' RETURNING id`,
      [input.approvalId, JSON.stringify(value), input.companyId, idempotencyKey])
    if (saved.rows.length !== 1) {
      const existing = await client.query("SELECT id FROM approvals WHERE id=$1 AND company_id=$2 AND idempotency_key=$3 AND status='EXECUTED'", [input.approvalId, input.companyId, idempotencyKey])
      if (existing.rows.length !== 1) throw new Error('approval changed before execution receipt was recorded')
      return
    }
    const receipt = await client.query('UPDATE lingxios.agent_action_ledger SET result=$2::jsonb WHERE idempotency_key=$1 RETURNING idempotency_key',
      [idempotencyKey, JSON.stringify({ ok: true, value })])
    if (receipt.rows.length !== 1) throw new Error('approval action ledger is missing')
  })
}

/** Observe state only. A matching postcondition does not establish which actor changed it. */
export async function reconcileKnowledgeApproval(database: SqlPool, services: LingxiLoopServices,
  input: { companyId: string; userId: string; approvalId: string }) {
  const reviewed = await inspectApproval(database, services, input)
  if (reviewed.status === 'EXECUTED') return resumeApproved(database, input, reviewed)
  if (reviewed.status !== 'APPROVED') throw new Error('approval has no unknown approved execution')
  if (reviewed.action.action === 'knowledge.delete_source') {
    const native = await approvalNativeWork(database, input.companyId, reviewed.action.idempotencyKey)
    const { rows } = await database.query(`SELECT source.project_id FROM knowledge_sources source
      JOIN approvals approval ON approval.id=$1 AND approval.company_id=source.company_id
        AND approval.args->>'sourceId'=source.id AND approval.status='APPROVED'
      WHERE source.company_id=$2 AND source.deleted_at IS NOT NULL AND source.deleted_at>=approval.resolved_at
        AND (source.visibility_scope='PROJECT' OR (source.visibility_scope='PRIVATE' AND source.owner_user_id=$3))`,
      [input.approvalId, input.companyId, native.authorizationUserId])
    if (rows.length !== 1 || typeof rows[0]!['project_id'] !== 'string') return { status: 'not_observed' as const }
    await services.permissionService.assertCan({ actorUserId: native.authorizationUserId!, companyId: input.companyId,
      action: 'knowledge:read', resource: { type: 'project', id: rows[0]!['project_id'] } })
    await recordApprovedResult(database, input, reviewed.action.idempotencyKey,
      { deleted: true, externalAssetCleanup: 'not_verified', reconciliation: 'postcondition_observed', executionAttribution: 'not_verified' })
    return resumeApproved(database, input, reviewed)
  }
  if (reviewed.action.action !== 'knowledge.set_source_enabled') return { status: 'not_observed' as const }
  const sourceId = reviewed.action.args['sourceId']
  const enabled = reviewed.action.args['enabled']
  if (typeof sourceId !== 'string' || typeof enabled !== 'boolean') throw new Error('invalid approved availability action')
  const native = await approvalNativeWork(database, input.companyId, reviewed.action.idempotencyKey)
  await services.permissionService.assertCan({ actorUserId: native.authorizationUserId!, companyId: input.companyId,
    action: 'knowledge:read', resource: { type: 'knowledge_source', id: sourceId } })
  const sources = (await services.knowledge.listKnowledgeSourcesForAgent(native))
    .filter(item => item && typeof item === 'object' && (item as Record<string, unknown>)['id'] === sourceId) as Record<string, unknown>[]
  if (sources.length !== 1 || sources[0]!['enabled'] !== enabled) return { status: 'not_observed' as const }
  await recordApprovedResult(database, input, reviewed.action.idempotencyKey,
    { enabled, reconciliation: 'postcondition_observed', executionAttribution: 'not_verified' })
  return resumeApproved(database, input, reviewed)
}
