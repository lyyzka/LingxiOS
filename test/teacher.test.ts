import { teacherReportingFixture } from './teacher-reporting-fixture.js'
import { PGlite } from '@electric-sql/pglite'
import type { SqlPool } from '../src/control-plane/pg-store.js'
import assert from 'node:assert/strict'
import { it } from 'node:test'
import { executeTeacher, teacherTransaction } from '../src/integrations/lingxiloop/teacher.js'
import type { LingxiLoopServices } from '../src/integrations/lingxiloop/service-contracts.js'
import type { HostAction, WorkItem } from '../src/protocol/types.js'

it('builds teacher context from persisted identity and checks current role without message history or legacy routines', async () => {
  const work: WorkItem = { id: 'w', tenantId: 't', principalId: 'human', agentId: 'teacher', sessionId: 'room', triggerRef: 'old-message', kind: 'turn', lane: 'interactive', fence: 1, homeEpoch: 1, leaseToken: 'token' }
  const scope = { company_id: 't', agent_id: 'teacher', project_id: 'project', course_id: 'course', course_title: 'Course', course_status: 'ACTIVE', room_id: 'room', room_status: 'active', agent_name: 'Teacher', has_teacher: true }
  let denied = false
  let missing = false
  const metrics: string[] = []
  let observed: unknown
  let found = true
  let auditFails = false
  let audit: unknown
  const database: SqlPool = { query: async sql => { assert.match(sql, /FROM lingxios\.agent_routines/); return { rows: [], rowCount: 0 } }, connect: async () => { throw new Error('unexpected transaction') } }
  const services: Pick<LingxiLoopServices, 'teacher' | 'permissionService'> = {
    permissionService: { assertCan: async () => {} },
    teacher: { ...teacherReportingFixture, inc: name => { metrics.push(name) },
      findTeacherScopeBinding: async (db, tenant, agent, room) => { assert.equal(db, database); assert.deepEqual([tenant, agent, room], ['t', 'teacher', 'room']); return missing ? undefined : scope },
      loadTeacherOverviewRows: async (_db, scope, days) => { observed = { scope, days }; return { distribution: [], missions: [], activity: [{ learners: 5 }], attention: [], coverage: [] } },
      findTeacherTurnCounts: async () => ({ learners: 5, objectives: 2, activities: 1, pending_reviews: 0 }),
      requireLearningCourseRole: async (_db, input) => { assert.deepEqual(input, { companyId: 't', courseId: 'course', userId: 'human', role: 'teacher' }); if (denied) throw new Error('permission denied') },
      listTeacherLearnerRows: async (_db, scope, attentionOnly) => { observed = { scope, attentionOnly }; return [{ user_id: 'learner', attention_reasons: ['review'] }] },
      findTeacherLearner: async (_db, scope, learnerId) => { observed = { scope, learnerId }; return found ? { display_name: 'Learner' } : undefined },
      loadTeacherLearnerDetailRows: async () => ({ states: [], missions: [], attempts: [] }),
      findTeacherAttemptDetail: async (_db, scope, attemptId) => { observed = { scope, attemptId }; return found ? { id: attemptId, evidence: { answer: '42' } } : undefined },
      auditInTransaction: async (_db, input) => { if (auditFails) throw new Error('audit unavailable'); audit = input },
      listTeacherObjectives: async (_db, scope) => [{ scope, kind: 'objectives' }],
      listTeacherActivities: async (_db, scope) => [{ scope, kind: 'activities' }],
      listTeacherReviews: async (_db, scope) => [{ scope, kind: 'reviews' }],
      listTeacherBindableRooms: async (_db, scope) => [{ scope, kind: 'rooms' }],
    },
  }
  const action = (name: string, args: Record<string, unknown> = {}): HostAction => ({ runId: 'w', cellId: 'c', callIndex: 0, idempotencyKey: 'key', action: 'teacher.' + name, args })
  const context = await executeTeacher(work, action('current'), services, database)
  assert.deepEqual(context, { agent: { id: 'teacher', name: 'Teacher', projectId: 'project' }, course: { id: 'course', projectId: 'project', title: 'Course', status: 'ACTIVE' }, room: { id: 'room', status: 'active' }, trigger: { mode: 'teacher', teacherId: 'human' }, counts: { learners: 5, objectives: 2, activities: 1, pendingReviews: 0 }, digest: { frequency: 'off', timezone: 'Asia/Shanghai', status: 'paused' } })
  const overview = await executeTeacher(work, action('overview', { windowDays: 365 }), services, database) as Record<string, unknown>
  assert.deepEqual(overview['activity'], { learners: 5 })
  assert.equal(overview['windowDays'], 365)
  assert.deepEqual(observed, { scope: { companyId: 't', courseId: 'course', projectId: 'project', teacherUserId: 'human' }, days: 365 })
  const reportingScope = { companyId: 't', courseId: 'course', projectId: 'project', teacherUserId: 'human' }
  for (const kind of ['objectives', 'activities', 'reviews', 'rooms']) {
    assert.deepEqual(await executeTeacher(work, action('list_' + kind), services, database), [{ scope: reportingScope, kind }])
  }
  assert.deepEqual(await executeTeacher(work, action('list_learners', { attentionOnly: true }), services, database), [{ user_id: 'learner', attention_reasons: ['review'], attentionReasons: ['review'] }])
  assert.deepEqual(observed, { scope: reportingScope, attentionOnly: true })
  assert.deepEqual(await executeTeacher(work, action('get_learner', { learnerId: ' learner ' }), services, database), { learner: { id: 'learner', display_name: 'Learner' }, states: [], missions: [], attempts: [] })
  assert.deepEqual(observed, { scope: reportingScope, learnerId: 'learner' })
  assert.deepEqual(await executeTeacher(work, action('get_attempt', { attemptId: 'attempt' }), services, database), { id: 'attempt', evidence: { answer: '42' } })
  assert.deepEqual(audit, { kind: 'teacher_agent_attempt_access', userId: 'human', companyId: 't', detail: { courseId: 'course', attemptId: 'attempt', agentId: 'teacher' } })
  auditFails = true
  await assert.rejects(executeTeacher(work, action('get_attempt', { attemptId: 'attempt' }), services, database), /audit unavailable/)
  found = false
  for (const [method, args] of [['get_attempt', { attemptId: 'foreign' }], ['get_learner', { learnerId: 'foreign' }]] as const) {
    await assert.rejects(executeTeacher(work, action(method, args), services, database), /outside the current course/)
  }
  assert.deepEqual(metrics, ['learning.teacher_agent.summary_generated', 'learning.teacher_agent.learner_drilldown', 'learning.teacher_agent.evidence_accessed'])
  for (const patch of [{ company_id: 'other' }, { agent_id: 'other' }, { room_id: 'other' }, { room_status: 'closed' }, { course_status: 'ARCHIVED' }, { has_teacher: false }]) {
    const original = { ...scope }
    Object.assign(scope, patch)
    await assert.rejects(executeTeacher(work, action('current'), services, database), /teacher scope/)
    Object.assign(scope, original)
  }
  missing = true
  await assert.rejects(executeTeacher(work, action('current'), services, database), /teacher scope/)
  missing = false
  denied = true
  observed = undefined
  await assert.rejects(executeTeacher(work, action('overview'), services, database), /permission denied/)
  assert.equal(observed, undefined)
})

it('commits teacher metadata together and rolls back failed or mismatched native results', async () => {
  await assert.rejects(teacherTransaction({ query: async () => ({ rows: [], rowCount: 0 }) })(db => db.query('SELECT 1')), /complete pg query results/)
  const db = new PGlite()
  const database: SqlPool = { query: async (sql, params) => { const result = await db.query<Record<string, unknown>>(sql, params); return { command: 'TEST', oid: 0, fields: [], rows: result.rows, rowCount: result.affectedRows ?? result.rows.length } }, connect: async () => ({ query: database.query, release() {} }) }
  const work: WorkItem = { id: 'w', tenantId: 'tenant', principalId: 'human', agentId: 'teacher', sessionId: 'room', triggerRef: 'message', kind: 'turn', lane: 'interactive', fence: 1, homeEpoch: 1, leaseToken: 'token' }
  const action: HostAction = { runId: 'w', cellId: 'c', callIndex: 0, idempotencyKey: 'key', action: 'teacher.update_course', args: { title: 'Updated' } }
  let failure: 'write' | 'scope' | 'value' | undefined = 'write'
  let objectiveResult: unknown = [{ id: 'unit', courseId: 'course', title: 'Draft', status: 'DRAFT' }]
  const services: Pick<LingxiLoopServices, 'teacher' | 'permissionService'> = {
    permissionService: { assertCan: async () => {} },
    teacher: { ...teacherReportingFixture, findTeacherScopeBinding: async () => ({ company_id: 'tenant', agent_id: 'teacher', project_id: 'project', course_id: 'course', course_title: 'Course', course_status: 'ACTIVE', room_id: 'room', room_status: 'active', agent_name: 'Teacher', has_teacher: true }),
    findTeacherTurnCounts: async () => ({ learners: 0, objectives: 0, activities: 0, pending_reviews: 0 }),
    requireLearningCourseRole: async () => {},
      setLearningCourseMembership: async (client, transaction, input) => {
        assert.deepEqual(input, { companyId: 'tenant', courseId: 'course', managerId: 'human', userId: 'learner', role: 'learner', enabled: input.enabled })
        await transaction(async db => { await db.query("UPDATE metadata SET name=$1 WHERE id='project'", [input.enabled ? 'member' : 'removed']) })
        if (failure === 'value') throw new Error('membership domain failure')
      },
      bindLearningCourseRoom: async (client, input) => {
        assert.equal(input.managerId, 'human')
        assert.equal(input.courseId, 'course')
        await client.query("UPDATE metadata SET name=$1 WHERE id='project'", [input.enabled ? input.purpose : 'unbound'])
        if (failure === 'value') throw new Error('room binding domain failure')
      },
      createLearningActivity: async (client, transaction, input) => {
        assert.equal(input.actorId, 'human'); assert.equal(input.actorKind, 'teacher')
        assert.equal(input.evaluationMode, 'TEACHER_REQUIRED'); assert.equal(input.targetLevel, 2)
        await transaction(async db => { await db.query("INSERT INTO metadata VALUES('activity','Activity')") })
        return { id: 'activity', courseId: failure === 'scope' ? 'other' : 'course', status: 'DRAFT' }
      },
      createLearningObjectives: async (client, transaction, input) => {
        assert.equal(input.actorId, 'human'); assert.equal(input.actorKind, 'teacher')
        await transaction(async db => { await db.query("INSERT INTO metadata VALUES('unit','Draft')") })
        return objectiveResult
      },
      updateTeacherCourseMetadata: async (client, input) => {
        assert.deepEqual(input, { companyId: 'tenant', courseId: 'course', title: 'Updated' })
        await client.query('UPDATE metadata SET name=$1 WHERE id=$2', [input.title, 'project'])
        if (failure === 'write') throw new Error('participant update failed')
        await client.query('UPDATE metadata SET name=$1 WHERE id=$2', ['Pulse · ' + input.title, 'teacher'])
        return { id: failure === 'scope' ? 'other' : 'project', company_id: 'tenant', name: failure === 'value' ? 'Original' : input.title }
      } },
  }

  try {
    await db.exec("CREATE TABLE metadata(id text PRIMARY KEY,name text); INSERT INTO metadata VALUES('project','Original'),('teacher','Pulse · Original')")
    for (const mode of ['write', 'scope', 'value'] as const) {
      failure = mode
      await assert.rejects(executeTeacher(work, action, services, database), mode === 'write' ? /participant update failed/ : mode === 'scope' ? /scoped project/ : /requested metadata/)
      assert.deepEqual((await db.query('SELECT * FROM metadata ORDER BY id')).rows, [{ id: 'project', name: 'Original' }, { id: 'teacher', name: 'Pulse · Original' }])
    }
    failure = undefined
    assert.deepEqual(await executeTeacher(work, action, services, database), { id: 'project', company_id: 'tenant', name: 'Updated' })
    assert.deepEqual((await db.query('SELECT * FROM metadata ORDER BY id')).rows, [{ id: 'project', name: 'Updated' }, { id: 'teacher', name: 'Pulse · Updated' }])
    const draft = { ...action, action: 'teacher.draft_objectives', args: { objectives: [{ title: 'Draft', successCriteria: 'Explain the idea', targetLevel: 3, prerequisiteIds: ['prior'] }] } }
    for (const result of [[], [null], [{ id: 'unit', courseId: 'other' }], [{ id: '', courseId: 'course' }], [{ id: 'unit', courseId: 'course' }, { id: 'unit', courseId: 'course' }]]) {
      objectiveResult = result
      await assert.rejects(executeTeacher(work, draft, services, database), /scoped objectives/)
      assert.deepEqual((await db.query("SELECT id FROM metadata WHERE id='unit'")).rows, [])
    }
    objectiveResult = [{ id: 'unit', courseId: 'course', status: 'DRAFT' }]
    await assert.rejects(executeTeacher(work, { ...draft, args: { objectives: [...draft.args.objectives, ...draft.args.objectives] } }, services, database), /scoped objectives/)
    assert.deepEqual((await db.query("SELECT id FROM metadata WHERE id='unit'")).rows, [])
    objectiveResult = [{ id: 'prior', courseId: 'course', status: 'PUBLISHED' }, { id: 'unit', courseId: 'course', title: 'Draft', status: 'DRAFT' }]
    assert.deepEqual(await executeTeacher(work, draft, services, database), objectiveResult)
    for (const objectives of [[], Array(101).fill({}), [{ title: 'Draft', success_criteria: 'Wrong alias' }], [{ title: 'Draft', successCriteria: 'Explain', targetLevel: '3' }], [{ title: 'Draft', successCriteria: 'Explain', prerequisiteIds: ['prior', 'prior'] }]]) {
      await assert.rejects(executeTeacher(work, { ...draft, args: { objectives } }, services, database))
    }
    const activity = { ...action, action: 'teacher.draft_activity', args: { title: 'Activity', instructions: 'Compare fractions', type: 'PRACTICE', objectiveIds: ['unit'] } }
    failure = 'scope'
    await assert.rejects(executeTeacher(work, activity, services, database), /scoped draft/)
    assert.deepEqual((await db.query("SELECT id FROM metadata WHERE id='activity'")).rows, [])
    failure = undefined
    assert.deepEqual(await executeTeacher(work, activity, services, database), { id: 'activity', courseId: 'course', status: 'DRAFT' })
    for (const patch of [{ title: '' }, { instructions: 1 }, { type: 'unknown' }, { evaluationMode: 'automatic' }, { targetLevel: '2' }, { rubric: {} }, { rubric: Array(101).fill('x') }, { objectiveIds: ['unit', 'unit'] }, { dueAt: 'tomorrow' }, { courseId: 'other' }]) {
      await assert.rejects(executeTeacher(work, { ...activity, args: { ...activity.args, ...patch } }, services, database))
    }
    for (const args of [{}, { title: '' }, { description: 1 }, { title: 'x'.repeat(2001) }, { courseId: 'other' }]) {
      await assert.rejects(executeTeacher(work, { ...action, args }, services, database))
    }
    const binding = { ...action, action: 'teacher.set_room_binding', args: { conversationId: 'room', enabled: true, purpose: 'lab' } }
    failure = 'value'
    await assert.rejects(executeTeacher(work, binding, services, database), /room binding domain failure/)
    assert.deepEqual((await db.query("SELECT name FROM metadata WHERE id='project'")).rows, [{ name: 'Updated' }])
    failure = undefined
    assert.deepEqual(await executeTeacher(work, binding, services, database), { ok: true, enabled: true })
    assert.deepEqual((await db.query("SELECT name FROM metadata WHERE id='project'")).rows, [{ name: 'lab' }])
    assert.deepEqual(await executeTeacher(work, { ...binding, args: { conversationId: 'room', enabled: false } }, services, database), { ok: true, enabled: false })
    assert.deepEqual((await db.query("SELECT name FROM metadata WHERE id='project'")).rows, [{ name: 'unbound' }])
    for (const args of [{ conversationId: 'room', enabled: true }, { conversationId: 'room', enabled: 'false' }, { conversationId: 'room', enabled: false, purpose: 'lab' }, { conversationId: 'room', enabled: true, purpose: 'study' }, { conversation_id: 'room', enabled: false }]) {
      await assert.rejects(executeTeacher(work, { ...binding, args }, services, database))
    }
    const membership = { ...action, action: 'teacher.set_learner_membership', args: { userId: 'learner', enabled: true } }
    failure = 'value'
    await assert.rejects(executeTeacher(work, membership, services, database), /membership domain failure/)
    assert.deepEqual((await db.query("SELECT name FROM metadata WHERE id='project'")).rows, [{ name: 'unbound' }])
    failure = undefined
    for (const enabled of [true, false]) {
      assert.deepEqual(await executeTeacher(work, { ...membership, args: { userId: 'learner', enabled } }, services, database), { ok: true })
      assert.deepEqual((await db.query("SELECT name FROM metadata WHERE id='project'")).rows, [{ name: enabled ? 'member' : 'removed' }])
    }
    for (const args of [{ userId: 'learner' }, { userId: 'learner', enabled: 'false' }, { userId: '', enabled: true }, { userId: 'learner', enabled: true, role: 'teacher' }]) {
      await assert.rejects(executeTeacher(work, { ...membership, args }, services, database))
    }
  } finally { await db.close() }
})
