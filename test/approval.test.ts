import { teacherPreview } from '../src/integrations/lingxiloop/teacher-preview.js'
import { teacherContext } from '../src/integrations/lingxiloop/teacher-context.js'
import { teacherReportingFixture } from './teacher-reporting-fixture.js'
import assert from 'node:assert/strict'
import { it } from 'node:test'
import { readFile } from 'node:fs/promises'
import { PGlite } from '@electric-sql/pglite'
import { approveTeacher, inspectApproval, requestTeacherApproval, resumeApproved } from '../src/integrations/lingxiloop/approvals.js'
import type { SqlPool, SqlQueryable } from '../src/control-plane/pg-store.js'
import type { HostAction } from '../src/protocol/types.js'

it('persists scoped teacher approval previews only for a live matching action intent', async () => {
  const db = new PGlite()
  let failResume = false
  const database: SqlPool = { query: async (sql, params) => {
    if (failResume && sql.includes('UPDATE approvals SET resumed_at')) { failResume = false; throw new Error('injected resume failure') }
    const result = await db.query<Record<string, unknown>>(sql, params); return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
  }, connect: async () => ({ query: database.query, release() {} }) }
  const work = { id: 'w', tenantId: 't', principalId: 'u', agentId: 'a', sessionId: 's', kind: 'turn', lane: 'interactive' as const, triggerRef: 'm', fence: 1, homeEpoch: 1 }
  const action: HostAction = { runId: 'w', cellId: 'c', callIndex: 0, idempotencyKey: 'key', action: 'teacher.publish_objective', args: { objectiveId: 'objective' } }
  let foreign = true
  let previewPatch: Record<string, unknown> = {}
  let execution: 'forbidden' | 'fail' | 'noop' | 'success' = 'forbidden'
  let failEnqueue = false
  let leaveRoomOpen = false
  let wrongTransitionResult = false
  const services = { permissionService: { assertCan: async () => {} }, teacher: { ...teacherReportingFixture,
    findTeacherScopeBinding: async () => ({ company_id: 't', agent_id: 'a', project_id: 'p', course_id: 'course', course_title: 'Course', course_status: 'ACTIVE', room_id: 's', room_status: 'active', agent_name: 'Teacher', has_teacher: true }),
    findTeacherTurnCounts: async () => ({ learners: 0, objectives: 0, activities: 0, pending_reviews: 0 }),
    findTeacherObjectiveApprovalTarget: async () => foreign ? undefined : ({ status: 'DRAFT', updatedAt: Object.hasOwn(previewPatch, 'currentVersion') ? previewPatch['currentVersion'] : 'v1', label: null }),
    findTeacherCourseApprovalTarget: async () => ({ status: 'ACTIVE', updatedAt: 'v1', label: null }),
    findTeacherMembershipApprovalTarget: async () => ({ enabled: Boolean(previewPatch['currentVersion']), label: null }),
    findTeacherEvaluationApprovalTarget: async () => ({ status: 'PENDING', label: null }),
    requireLearningCourseRole: async (_client: SqlQueryable, input: { userId: string }) => { assert.equal(input.userId, 'u') },
    setLearningCourseMembershipRecord: async (client: SqlQueryable, input: { userId: string; enabled: boolean }) => {
      assert.equal(input.userId, 'new-teacher')
      await client.query('UPDATE project_memberships SET role=$1', [input.enabled ? 'TEACHER' : 'STUDENT'])
      return 'updated'
    },
    enqueueLearningEffect: async (client: SqlQueryable, input: { kind: string }) => {
      await client.query('INSERT INTO effect_fixture VALUES($1)', [input.kind])
      if (failEnqueue) throw new Error('injected effect failure')
    },
    assertTeacherApprovalFresh: async () => {},
    setLearningObjectiveStatus: async (client: SqlQueryable, input: { teacherId: string; status: string }) => {
      assert.notEqual(execution, 'forbidden'); assert.equal(input.teacherId, 'u')
      if (execution !== 'noop') await client.query("UPDATE learning_knowledge_units SET status=$1 WHERE id='objective'", [input.status])
      if (execution === 'fail') throw new Error('injected native failure')
    },
    reviewLearningEvaluation: async (client: SqlQueryable, _transaction: unknown, _metric: unknown, input: { teacherId: string; decision: string }) => {
      assert.notEqual(execution, 'forbidden'); assert.equal(input.teacherId, 'u')
      if (execution !== 'noop') await client.query('UPDATE learning_evaluations SET status=$1', [input.decision === 'accept' ? 'ACCEPTED' : 'REJECTED'])
      if (execution === 'fail') throw new Error('injected native failure')
    },
    ProjectLifecycleApplication: class {
      async executeInTransaction(client: SqlQueryable, input: { actorUserId: string; command: string }) {
        assert.notEqual(execution, 'forbidden'); assert.equal(input.actorUserId, 'u')
        const status = { END: 'COURSE_ENDED', ENTER_READ_ONLY: 'READ_ONLY', ARCHIVE: 'ARCHIVED' }[input.command]!
        if (execution !== 'noop') {
          await client.query('UPDATE projects SET status=$1', [status])
          if (status !== 'COURSE_ENDED' && !leaveRoomOpen) await client.query("UPDATE learning_course_teacher_rooms SET status='closed'")
        }
        if (execution === 'fail') throw new Error('injected native failure')
        return { ok: true as const, status: wrongTransitionResult ? 'ACTIVE' : status, applied: true }
      }
    },
  } }
  try {
    await db.exec(await readFile(new URL('../../db/schema.sql', import.meta.url), 'utf8'))
    await db.exec(`CREATE TABLE approvals(id text,company_id text,agent_id text,channel_id text,source text,work_id text,authorization_user_id text,idempotency_key text UNIQUE,action text,args jsonb,summary text,requested_by text,scope jsonb,preview jsonb,expires_at timestamptz);
      INSERT INTO lingxios.agent_work_items(id,tenant_id,agent_id,session_id,principal_id,kind,lane,trigger_ref,fence,status,lease_expires_at) VALUES('w','t','a','s','u','turn','interactive','m',1,'leased',NOW()+INTERVAL '1 hour')`)
    await db.query('INSERT INTO lingxios.agent_action_intents(idempotency_key,fingerprint,intent) VALUES($1,$2,$3)', ['key', 'fingerprint', JSON.stringify({ workId: 'w', tenantId: 't', principalId: 'u', agentId: 'a', sessionId: 's', threadId: null, requestVersion: 1, action })])
    await assert.rejects(requestTeacherApproval(database, services, work, action), /outside the current course/)
    foreign = false
    for (const patch of [{ currentVersion: '' }, { currentVersion: null }, { currentVersion: false }]) {
      previewPatch = patch
      await assert.rejects(requestTeacherApproval(database, services, work, action), /scoped request/)
    }
    previewPatch = {}
    await assert.rejects(requestTeacherApproval(database, services, { ...work, fence: 2 }, action), /durable action intent/)
    for (const args of [{ objectiveId: '' }, { objectiveId: 'objective', courseId: 'other' }]) await assert.rejects(requestTeacherApproval(database, services, work, { ...action, args }))
    assert.deepEqual((await db.query('SELECT id FROM approvals')).rows, [])
    const result = await requestTeacherApproval(database, services, work, action)
    assert.equal(result.ok, false)
    assert.ok(result.approval?.id)
    assert.deepEqual((await db.query('SELECT action,args,requested_by,scope,preview FROM approvals')).rows, [{ action: action.action, args: action.args, requested_by: 'u', scope: { projectId: 'p', courseId: 'course', roomId: 's', risk: 'course_management' }, preview: { method: 'publish_objective', args: action.args, entityId: 'objective', entityLabel: null, currentState: 'DRAFT', currentVersion: 'v1' } }])
    await assert.rejects(requestTeacherApproval(database, services, work, action), /reconcile existing/)
    const approvalId = result.approval.id
    await db.exec(`ALTER TABLE approvals ADD COLUMN status text DEFAULT 'PENDING', ADD COLUMN resolved_at timestamptz,
      ADD COLUMN resolved_by text, ADD COLUMN executed_at timestamptz, ADD COLUMN result jsonb, ADD COLUMN error text, ADD COLUMN resumed_at timestamptz;
      CREATE TABLE courses(id text,company_id text,project_id text);
      CREATE TABLE learning_course_teacher_rooms(company_id text,course_id text,conversation_id text);
      CREATE TABLE learning_knowledge_units(id text,company_id text,project_id text,status text);
      INSERT INTO courses VALUES('course','t','p');
      INSERT INTO learning_course_teacher_rooms VALUES('t','course','s');
      INSERT INTO learning_knowledge_units VALUES('objective','t','p','DRAFT');`)
    await db.query("UPDATE lingxios.agent_work_items SET status='completed',goal_outcome=$1", [JSON.stringify({ status: 'awaiting_approval', approvalId, requestVersion: 1 })])
    await db.query('INSERT INTO lingxios.agent_os_sessions(session_key,tenant_id,agent_id,session_id,request_snapshot) VALUES($1,$2,$3,$4,$5)', ['session', 't', 'a', 's', JSON.stringify({ workId: 'w', tenantId: 't', sessionId: 's', authorId: 'u', originalText: 'Publish objective', revisions: [] })])
    await db.query('INSERT INTO lingxios.agent_action_ledger(idempotency_key,result) VALUES($1,$2)', ['key', JSON.stringify(result)])
    const decision = { companyId: 't', userId: 'reviewer', approvalId }
    await db.exec("UPDATE approvals SET expires_at=NOW()-INTERVAL '1 minute'")
    await assert.rejects(approveTeacher(database, services, decision), /expired or changed/)
    await db.exec("UPDATE approvals SET expires_at=NOW()+INTERVAL '1 hour'; UPDATE lingxios.agent_work_items SET cancel_requested_at=NOW()")
    await assert.rejects(approveTeacher(database, services, decision), /expired or changed/)
    await db.exec('UPDATE lingxios.agent_work_items SET cancel_requested_at=NULL')
    previewPatch = { currentVersion: 'v2' }
    await assert.rejects(approveTeacher(database, services, decision), /stale/)
    previewPatch = {}
    for (const mode of ['fail', 'noop'] as const) {
      execution = mode
      await assert.rejects(approveTeacher(database, services, decision), mode === 'fail' ? /injected native/ : /postcondition/)
      assert.deepEqual((await db.query('SELECT status FROM learning_knowledge_units')).rows, [{ status: 'DRAFT' }])
      assert.deepEqual((await db.query('SELECT status FROM approvals')).rows, [{ status: 'PENDING' }])
      assert.deepEqual((await db.query('SELECT result FROM lingxios.agent_action_ledger')).rows, [{ result }])
    }
    execution = 'success'
    await db.exec('DELETE FROM lingxios.agent_action_ledger')
    await assert.rejects(approveTeacher(database, services, decision), /receipt is missing/)
    assert.deepEqual((await db.query('SELECT status FROM learning_knowledge_units')).rows, [{ status: 'DRAFT' }])
    assert.deepEqual((await db.query('SELECT status FROM approvals')).rows, [{ status: 'PENDING' }])
    await db.query('INSERT INTO lingxios.agent_action_ledger(idempotency_key,result) VALUES($1,$2)', ['key', JSON.stringify(result)])
    failResume = true
    await assert.rejects(approveTeacher(database, services, decision), /injected resume failure/)
    assert.deepEqual((await db.query('SELECT status FROM learning_knowledge_units')).rows, [{ status: 'PUBLISHED' }])
    assert.deepEqual((await db.query('SELECT status,resolved_by,result FROM approvals')).rows, [{ status: 'EXECUTED', resolved_by: 'reviewer', result: { ok: true } }])
    assert.deepEqual((await db.query('SELECT status FROM lingxios.agent_work_items')).rows, [{ status: 'completed' }])
    const executed = await inspectApproval(database, services, decision)
    await db.query("UPDATE lingxios.agent_action_intents SET intent=jsonb_set(intent,'{agentId}','\"other-agent\"'::jsonb)")
    await assert.rejects(resumeApproved(database, decision, executed), /matching durable recovery records/)
    await db.query("UPDATE lingxios.agent_action_intents SET intent=jsonb_set(intent,'{agentId}','\"a\"'::jsonb)")
    execution = 'forbidden'
    assert.deepEqual(await approveTeacher(database, services, decision), { status: 'resumed', workId: 'w' })
    assert.deepEqual(await approveTeacher(database, services, decision), { status: 'already_resumed', workId: 'w' })
    await db.exec("CREATE TABLE learning_evaluations(id text,company_id text,project_id text,status text); INSERT INTO learning_evaluations VALUES('evaluation','t','p','PENDING')")
    for (const review of ['accept', 'reject']) {
      action.action = 'teacher.review_evaluation'
      action.args = { evaluationId: 'evaluation', decision: review, reason: 'Reviewed evidence' }
      const metadata = await teacherPreview(database, services.teacher, work.tenantId, await teacherContext(work, services, database), action)
      await db.query("UPDATE approvals SET action=$1,args=$2,preview=$3,status='PENDING',resumed_at=NULL,result=NULL", [action.action, JSON.stringify(action.args), JSON.stringify(metadata.preview)])
      await db.query("UPDATE lingxios.agent_action_intents SET intent=jsonb_set(intent,'{action}',$1)", [JSON.stringify(action)])
      await db.query('UPDATE lingxios.agent_action_ledger SET result=$1', [JSON.stringify(result)])
      await db.query("UPDATE lingxios.agent_work_items SET status='completed',goal_outcome=$1", [JSON.stringify({ status: 'awaiting_approval', approvalId, requestVersion: 1 })])
      await db.exec("UPDATE learning_evaluations SET status='PENDING'")
      execution = 'success'
      assert.deepEqual(await approveTeacher(database, services, decision), { status: 'resumed', workId: 'w' })
      assert.deepEqual((await db.query('SELECT status FROM learning_evaluations')).rows, [{ status: review === 'accept' ? 'ACCEPTED' : 'REJECTED' }])
    }
    await db.exec("CREATE TABLE project_memberships(company_id text,project_id text,user_id text,status text,role text); INSERT INTO project_memberships VALUES('t','p','new-teacher','ACTIVE','STUDENT'); CREATE TABLE effect_fixture(kind text)")
    for (const enabled of [true, false]) {
      action.action = 'teacher.set_teacher_membership'
      action.args = { userId: 'new-teacher', enabled }
      previewPatch = { currentVersion: !enabled }
      const metadata = await teacherPreview(database, services.teacher, work.tenantId, await teacherContext(work, services, database), action)
      await db.query("UPDATE approvals SET action=$1,args=$2,preview=$3,status='PENDING',resumed_at=NULL,result=NULL", [action.action, JSON.stringify(action.args), JSON.stringify(metadata.preview)])
      await db.query("UPDATE lingxios.agent_action_intents SET intent=jsonb_set(intent,'{action}',$1)", [JSON.stringify(action)])
      await db.query('UPDATE lingxios.agent_action_ledger SET result=$1', [JSON.stringify(result)])
      await db.query("UPDATE lingxios.agent_work_items SET status='completed',goal_outcome=$1", [JSON.stringify({ status: 'awaiting_approval', approvalId, requestVersion: 1 })])
      execution = 'forbidden'
      failEnqueue = true
      await assert.rejects(approveTeacher(database, services, decision), /injected effect failure/)
      assert.deepEqual((await db.query('SELECT role FROM project_memberships')).rows, [{ role: enabled ? 'STUDENT' : 'TEACHER' }])
      assert.deepEqual((await db.query('SELECT status FROM approvals')).rows, [{ status: 'PENDING' }])
      failEnqueue = false
      assert.deepEqual(await approveTeacher(database, services, decision), { status: 'resumed', workId: 'w' })
      assert.deepEqual((await db.query('SELECT role FROM project_memberships')).rows, [{ role: enabled ? 'TEACHER' : 'STUDENT' }])
      assert.deepEqual((await db.query('SELECT result FROM approvals')).rows, [{ result: { ok: true, enabled, channelSync: 'queued' } }])
    }
    assert.deepEqual((await db.query('SELECT kind FROM effect_fixture')).rows, [{ kind: 'teacher_room.sync' }, { kind: 'teacher_room.sync' }])
    await db.exec("CREATE TABLE projects(id text,company_id text,status text); INSERT INTO projects VALUES('p','t','ACTIVE'); ALTER TABLE learning_course_teacher_rooms ADD COLUMN status text DEFAULT 'active'")
    for (const command of ['END', 'ENTER_READ_ONLY', 'ARCHIVE']) {
      action.action = 'teacher.transition_course'
      action.args = { command }
      previewPatch = {}
      const metadata = await teacherPreview(database, services.teacher, work.tenantId, await teacherContext(work, services, database), action)
      await db.query("UPDATE approvals SET action=$1,args=$2,preview=$3,status='PENDING',resumed_at=NULL,result=NULL", [action.action, JSON.stringify(action.args), JSON.stringify(metadata.preview)])
      await db.query("UPDATE lingxios.agent_action_intents SET intent=jsonb_set(intent,'{action}',$1)", [JSON.stringify(action)])
      await db.query('UPDATE lingxios.agent_action_ledger SET result=$1', [JSON.stringify(result)])
      await db.query("UPDATE lingxios.agent_work_items SET status='completed',goal_outcome=$1", [JSON.stringify({ status: 'awaiting_approval', approvalId, requestVersion: 1 })])
      await db.exec("UPDATE projects SET status='ACTIVE'; UPDATE learning_course_teacher_rooms SET status='active'")
      const fence = command === 'END' ? 1 : command === 'ENTER_READ_ONLY' ? 2 : 3
      await db.query('UPDATE lingxios.agent_work_items SET fence=$1', [fence])
      for (const mode of ['fail', 'noop'] as const) {
        execution = mode
        await assert.rejects(approveTeacher(database, services, decision), mode === 'fail' ? /injected native/ : /postcondition/)
        assert.deepEqual((await db.query('SELECT status FROM projects')).rows, [{ status: 'ACTIVE' }])
      }
      execution = 'success'
      wrongTransitionResult = true
      await assert.rejects(approveTeacher(database, services, decision), /postcondition/)
      wrongTransitionResult = false
      if (command !== 'END') {
        leaveRoomOpen = true
        await assert.rejects(approveTeacher(database, services, decision), /postcondition/)
        leaveRoomOpen = false
      }
      assert.deepEqual((await db.query('SELECT status FROM approvals')).rows, [{ status: 'PENDING' }])
      const eventsBefore = (await db.query('SELECT seq FROM lingxios.agent_run_events')).rows
      failResume = true
      await assert.rejects(approveTeacher(database, services, decision), /injected resume failure/)
      assert.deepEqual((await db.query('SELECT status FROM approvals')).rows, [{ status: 'EXECUTED' }])
      assert.equal((await db.query<{ outcome: { status: string } }>('SELECT goal_outcome AS outcome FROM lingxios.agent_work_items')).rows[0]!.outcome.status, 'awaiting_approval')
      assert.deepEqual((await db.query('SELECT seq FROM lingxios.agent_run_events')).rows, eventsBefore)
      execution = 'forbidden'
      if (command === 'END') {
        assert.deepEqual(await approveTeacher(database, services, decision), { status: 'resumed', workId: 'w' })
      } else {
        const expected = { status: 'continuation_unavailable', workId: 'w', result: { ok: true, status: command === 'ARCHIVE' ? 'ARCHIVED' : 'READ_ONLY', applied: true, teacherRoomStatus: 'closed' } }
        assert.deepEqual(await approveTeacher(database, services, decision), expected)
        assert.deepEqual(await approveTeacher(database, services, decision), expected)
        const stopped = (await db.query<{ status: string; outcome: { status: string; verification: string; requestVersion: number; gaps: string[] } }>('SELECT status,goal_outcome AS outcome FROM lingxios.agent_work_items')).rows[0]!
        assert.equal(stopped.status, 'completed')
        assert.deepEqual([stopped.outcome.status, stopped.outcome.verification, stopped.outcome.requestVersion], ['blocked', 'inconclusive', 1])
        assert.match(stopped.outcome.gaps[0]!, /closed the teacher room/)
        const events = (await db.query<{ seq: number }>('SELECT seq FROM lingxios.agent_run_events ORDER BY seq')).rows
        assert.equal(events.length, eventsBefore.length + 1)
        assert.equal(Number(events.at(-1)!.seq), (fence - 1) * 100_000 + 1)
      }
    }
  } finally { await db.close() }
})

it('binds human approval review to namespaced intent, identity and current revisions', async () => {
  const db = new PGlite()
  const database: SqlQueryable = { query: async (sql, params) => {
    const result = await db.query<Record<string, unknown>>(sql, params)
    return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
  } }
  let denied = false
  const permissions: string[] = []
  const services = { permissionService: { assertCan: async (input: { action: string }) => {
    permissions.push(input.action)
    if (denied) throw new Error('permission denied')
  } } }
  const request = { version: 1, workId: 'w', tenantId: 't', sessionId: 's', authorId: 'u', sourceRef: 'm', attachments: [], originalText: 'Save the document', revisions: [] }
  const action = { runId: 'w', cellId: 'c', callIndex: 0, action: 'files.save', args: { title: 'Document' }, idempotencyKey: '[\"w\",\"c\",0]' }
  const intent = { workId: 'w', tenantId: 't', principalId: 'u', agentId: 'a', sessionId: 's', threadId: null, requestVersion: 1, action }
  const input = { companyId: 't', userId: 'reviewer', approvalId: 'approval' }
  try {
    await db.exec(await readFile(new URL('../../db/schema.sql', import.meta.url), 'utf8'))
    await db.exec(`CREATE TABLE public.approvals(id text, company_id text, agent_id text, channel_id text, work_id text,
      authorization_user_id text, idempotency_key text, action text, args jsonb, source text, status text, preview jsonb, expires_at text);
      INSERT INTO lingxios.agent_work_items(id,tenant_id,agent_id,session_id,principal_id,kind,lane,trigger_ref)
      VALUES('w','t','a','s','u','turn','interactive','m')`)
    await db.query(`INSERT INTO approvals VALUES('approval','t','a','s','w','u','[\"w\",\"c\",0]','files.save',$1,'AGENT_OS','PENDING','{}',NULL)`, [JSON.stringify(action.args)])
    await db.query("INSERT INTO lingxios.agent_action_intents(idempotency_key,fingerprint,intent) VALUES('[\"w\",\"c\",0]','fingerprint',$1)", [JSON.stringify(intent)])
    await db.query("INSERT INTO lingxios.agent_os_sessions(session_key,tenant_id,agent_id,session_id,request_snapshot) VALUES('[\"t\",\"a\",\"s\",null]','t','a','s',$1)", [JSON.stringify(request)])
    assert.deepEqual(await inspectApproval(database, services, input), { approvalId: 'approval', status: 'PENDING', requestVersion: 1,
      originalInput: request.originalText, revisions: [], action, summary: null, preview: {}, result: null, expiresAt: null })
    assert.deepEqual(permissions, ['agent_approval:resolve', 'conversation:read'])
    denied = true
    await assert.rejects(inspectApproval(database, services, input), /permission denied/)
    denied = false
    await assert.rejects(inspectApproval(database, services, { ...input, companyId: 'other' }), /no unique/)
    await db.query("UPDATE approvals SET args='{}'")
    await assert.rejects(inspectApproval(database, services, input), /does not match/)
    await db.query('UPDATE approvals SET args=$1', [JSON.stringify(action.args)])
    await db.query(`UPDATE lingxios.agent_work_items SET steer_inputs='[{"id":"r","text":"Changed request","createdAt":"now"}]'`)
    await assert.rejects(inspectApproval(database, services, input), /does not match/)
    assert.deepEqual((await db.query('SELECT status FROM approvals')).rows, [{ status: 'PENDING' }])
  } finally { await db.close() }
})
