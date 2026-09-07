import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { it } from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import type { SqlPool } from '../src/control-plane/pg-store.js'
import type { WorkItem } from '../src/protocol/types.js'
import type { LingxiLoopServices } from '../src/integrations/lingxiloop/service-contracts.js'
import { configureTeacherDigest, getTeacherDigest, scheduleTeacherDigests, assertTeacherDigestWork } from '../src/integrations/lingxiloop/teacher-digest.js'
import { teacherContext } from '../src/integrations/lingxiloop/teacher-context.js'
import { executeTeacher } from '../src/integrations/lingxiloop/teacher.js'
import { requestTeacherApproval } from '../src/integrations/lingxiloop/approvals.js'
import { teacherReportingFixture } from './teacher-reporting-fixture.js'

it('owns digest versions, coalesces missed runs, cancels old work and preserves authorization and atomic enqueue', async () => {
  const db = new PGlite()
  let failRunInsert = false
  let clock: string | undefined
  const database: SqlPool = { query: async (sql, params) => {
    if (clock && sql === 'SELECT NOW() AS now') return { rows: [{ now: clock }], rowCount: 1 }
    if (failRunInsert && sql.startsWith('INSERT INTO lingxios.agent_routine_runs')) throw new Error('run insert failed')
    const result = await db.query<Record<string, unknown>>(sql, params)
    return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
  }, connect: async () => ({ query: database.query, release() {} }) }
  const work: WorkItem = { id: 'request', tenantId: 't', agentId: 'a', sessionId: 'room', principalId: 'teacher', kind: 'turn', lane: 'interactive', triggerRef: 'old-message', fence: 1, homeEpoch: 1, leaseToken: 'token' }
  let denied: 'revoked' | 'transient' | undefined
  let courseId = 'course'
  const services: Pick<LingxiLoopServices, 'teacher' | 'permissionService'> = {
    permissionService: { assertCan: async () => {} },
    teacher: { ...teacherReportingFixture,
      findTeacherScopeBinding: async () => ({ company_id: 't', agent_id: 'a', project_id: 'project', course_id: courseId, course_title: 'Course', course_status: 'ACTIVE', room_id: 'room', room_status: 'active', agent_name: 'Teacher', has_teacher: true }),
      findTeacherTurnCounts: async () => ({ learners: 2, objectives: 3, activities: 1, pending_reviews: 0 }),
      requireLearningCourseRole: async (_db, input) => {
        assert.equal(input.userId, 'teacher')
        if (denied === 'revoked') throw Object.assign(new Error('forbidden'), { name: 'ForbiddenError', status: 403 })
        if (denied === 'transient') throw new Error('database unavailable')
      },
    },
  }
  const due = () => database.query("UPDATE lingxios.agent_routines SET next_run_at=NOW()-INTERVAL '10 days' WHERE status='active'")
  const jobs = async () => (await database.query('SELECT * FROM lingxios.agent_work_items ORDER BY created_at,id')).rows
  try {
    await db.exec(await readFile(new URL('../../db/schema.sql', import.meta.url), 'utf8'))
    const context = await teacherContext(work, services, database)
    assert.deepEqual(await getTeacherDigest(database, work, context), { frequency: 'off', timezone: 'Asia/Shanghai', status: 'paused' })
    for (const args of [{ frequency: 'hourly' }, { frequency: 'daily', localTime: '25:00' }, { frequency: 'weekly', localTime: '09:00' },
      { frequency: 'daily', localTime: '09:00', weekday: 'monday' }, { frequency: 'daily', localTime: '09:00', timezone: 'invalid/zone' }, { frequency: 'daily', localTime: '09:00', timezone: '+08:00' }, { frequency: 'off', localTime: '09:00' }, { frequency: 'off', courseId: 'other' }]) {
      await assert.rejects(configureTeacherDigest(database, services, work, args))
    }
    assert.equal((await database.query('SELECT id FROM lingxios.agent_routines')).rows.length, 0)
    const configured = await configureTeacherDigest(database, services, work, { frequency: 'weekly', localTime: '09:30', timezone: 'America/New_York', weekday: 'sunday' })
    assert.equal(configured['status'], 'active'); assert.equal(configured['version'], 1)
    assert.equal(await scheduleTeacherDigests(database, services), 0)
    await due()
    failRunInsert = true
    await assert.rejects(scheduleTeacherDigests(database, services), /run insert failed/)
    assert.equal((await jobs()).length, 0)
    assert.equal((await database.query('SELECT work_id FROM lingxios.agent_routine_runs')).rows.length, 0)
    failRunInsert = false
    assert.equal(await scheduleTeacherDigests(database, services), 1)
    assert.equal(await scheduleTeacherDigests(database, services), 0)
    const [job] = await jobs()
    assert.ok(job)
    assert.deepEqual([job['tenant_id'], job['agent_id'], job['session_id'], job['principal_id'], job['kind'], job['lane'], job['priority']], ['t', 'a', 'room', 'teacher', 'teacher_digest', 'background', -10])
    const scheduled = { ...work, id: String(job['id']), kind: 'teacher_digest', lane: 'background' as const }
    await assertTeacherDigestWork(database, scheduled, context)
    await assert.rejects(assertTeacherDigestWork(database, { ...scheduled, id: 'forged' }, context), /obsolete/)
    await assert.rejects(assertTeacherDigestWork(database, { ...scheduled, principalId: 'other' }, context), /obsolete/)
    const action = { runId: scheduled.id, cellId: 'c', callIndex: 0, idempotencyKey: 'k', action: 'teacher.get_attempt', args: { attemptId: 'attempt' } }
    await assert.rejects(executeTeacher(scheduled, action, services, database), /read-only/)
    await assert.rejects(configureTeacherDigest(database, services, scheduled, { frequency: 'off' }), /read-only/)
    await assert.rejects(requestTeacherApproval(database, services, scheduled, { ...action, action: 'teacher.publish_objective', args: { objectiveId: 'unit' } }), /read-only/)
    await due()
    assert.equal(await scheduleTeacherDigests(database, services), 0)
    assert.equal((await jobs()).length, 1)
    assert.equal((await database.query('SELECT next_run_at>NOW() AS advanced FROM lingxios.agent_routines')).rows[0]!['advanced'], true)
    await configureTeacherDigest(database, services, work, { frequency: 'daily', localTime: '08:00' })
    assert.equal((await jobs())[0]!['status'], 'cancelled')
    await assert.rejects(assertTeacherDigestWork(database, scheduled, context), /obsolete/)
    await due()
    assert.equal(await scheduleTeacherDigests(database, services), 1)
    const active = (await jobs()).find(row => row['status'] === 'queued')!
    await database.query("UPDATE lingxios.agent_work_items SET status='leased' WHERE id=$1", [active['id']])
    const paused = await configureTeacherDigest(database, services, work, { frequency: 'off' })
    assert.equal(paused['status'], 'paused')
    assert.ok((await database.query('SELECT cancel_requested_at FROM lingxios.agent_work_items WHERE id=$1', [active['id']])).rows[0]!['cancel_requested_at'])
    assert.equal(await scheduleTeacherDigests(database, services), 0)
    await configureTeacherDigest(database, services, work, { frequency: 'daily', localTime: '08:00' })
    await due()
    denied = 'transient'
    await assert.rejects(scheduleTeacherDigests(database, services), /database unavailable/)
    assert.equal((await getTeacherDigest(database, work, context))['status'], 'active')
    denied = 'revoked'
    assert.equal(await scheduleTeacherDigests(database, services), 0)
    assert.equal((await getTeacherDigest(database, work, context))['pauseReason'], 'authorization_revoked')
    await assert.rejects(configureTeacherDigest(database, services, work, { frequency: 'daily', localTime: '08:00' }), /forbidden/)
    denied = undefined
    await configureTeacherDigest(database, services, work, { frequency: 'daily', localTime: '08:00' })
    await due()
    courseId = 'rebound-course'
    assert.equal(await scheduleTeacherDigests(database, services), 0)
    assert.equal((await getTeacherDigest(database, work, context))['pauseReason'], 'scope_changed')
    assert.equal((await jobs()).length, 2)
    courseId = 'course'
    await configureTeacherDigest(database, services, work, { frequency: 'daily', localTime: '08:00' })
    await due()
    assert.equal(await scheduleTeacherDigests(database, services), 1)
    await database.query("UPDATE lingxios.agent_work_items SET status='succeeded' WHERE status='queued'")
    await database.query(`UPDATE lingxios.agent_routines routine SET next_run_at=run.scheduled_at FROM lingxios.agent_routine_runs run
      WHERE run.routine_id=routine.id AND run.routine_version=routine.version`)
    assert.equal(await scheduleTeacherDigests(database, services), 0)
    assert.equal((await jobs()).length, 3)
    await database.query("UPDATE lingxios.agent_routines SET schedule='{}',next_run_at=NOW()-INTERVAL '1 day'")
    assert.equal(await scheduleTeacherDigests(database, services), 0)
    assert.equal((await getTeacherDigest(database, work, context))['pauseReason'], 'invalid_schedule')
    for (const [from, frequency, localTime, expected] of [
      ['2026-03-08T06:00:00Z', 'daily', '02:30', '2026-03-08T07:30:00.000Z'],
      ['2026-11-01T04:00:00Z', 'daily', '01:30', '2026-11-01T06:30:00.000Z'],
      ['2026-11-01T05:45:00Z', 'daily', '01:30', '2026-11-01T06:30:00.000Z'],
      ['2026-03-08T06:00:00Z', 'weekly', '09:00', '2026-03-09T13:00:00.000Z'],
      ['2026-03-09T13:00:00Z', 'daily', '09:00', '2026-03-10T13:00:00.000Z'],
      ['2026-03-09T13:00:00Z', 'weekly', '09:00', '2026-03-16T13:00:00.000Z'],
    ]) {
      clock = from
      const result = await configureTeacherDigest(database, services, work, {
        frequency, localTime, timezone: 'America/New_York', ...(frequency === 'weekly' ? { weekday: 'monday' } : {}),
      })
      assert.equal(result['nextRunAt'], expected, `schedule from ${from}`)
    }
  } finally { await db.close() }
})
