import { createHash } from 'node:crypto'
import { withTransaction, type SqlPool, type SqlQueryable } from '../../control-plane/pg-store.js'
import type { WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices } from './service-contracts.js'
import { teacherContext, TeacherScopeError } from './teacher-context.js'

type Services = Pick<LingxiLoopServices, 'teacher' | 'permissionService'>
type Context = Awaited<ReturnType<typeof teacherContext>>
const weekdays = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
export const TEACHER_DIGEST_METHODS = ['current', 'overview', 'get_digest_schedule'] as const
const instruction = 'Generate the scheduled teacher digest using host.teacher.overview(windowDays=7). Summarize bounded aggregate course facts, current evidence coverage, and observed attention items. Do not infer hidden traits, read raw attempts, perform writes, contact learners, or ask questions. Clearly state unavailable information.'

function instant(value: unknown): Date { return value instanceof Date ? value : new Date(String(value)) }

function schedule(args: Record<string, unknown>) {
  const frequency = args['frequency']
  if (frequency !== 'daily' && frequency !== 'weekly') throw new Error('frequency must be daily, weekly, or off')
  const localTime = args['localTime'], timezone = args['timezone'] ?? 'Asia/Shanghai'
  if (typeof timezone !== 'string' || timezone.length > 100) throw new Error('invalid timezone')
  try {
    if (/^[+-]/.test(new Intl.DateTimeFormat('en-US', { timeZone: timezone }).resolvedOptions().timeZone)) throw new Error('offset')
  } catch { throw new Error('timezone must be a valid IANA timezone') }
  if (typeof localTime !== 'string' || !/^([01]\d|2[0-3]):[0-5]\d$/.test(localTime)) throw new Error('localTime must use HH:mm')
  const weekday = args['weekday']
  if (frequency === 'weekly' ? typeof weekday !== 'string' || !weekdays.includes(weekday) : weekday !== undefined) throw new Error('weekday is required only for weekly digests')
  return { frequency: frequency as 'daily' | 'weekly', localTime, timezone, ...(typeof weekday === 'string' ? { weekday } : {}) }
}

async function nextRun(database: SqlQueryable, value: ReturnType<typeof schedule>, now: Date) {
  // PostgreSQL resolves missing/repeated local times using its timezone rules.
  const { rows } = await database.query(`WITH candidates AS (
    SELECT (($1::timestamptz AT TIME ZONE $2)::date + day + $3::time) AS local_time
    FROM generate_series(0,7) AS day
  ) SELECT MIN(local_time AT TIME ZONE $2) AS next_run_at FROM candidates
    WHERE (local_time AT TIME ZONE $2)>$1::timestamptz
      AND ($4='daily' OR EXTRACT(ISODOW FROM local_time)=$5::int)`,
    [now, value.timezone, value.localTime, value.frequency, value.weekday ? weekdays.indexOf(value.weekday) + 1 : 1])
  const parsed = instant(rows[0]?.['next_run_at'])
  if (!Number.isFinite(parsed.getTime()) || parsed <= now) throw new Error('digest schedule did not return a future instant')
  return parsed.toISOString()
}

export async function cancelRoutineRuns(database: SqlQueryable, id: string) {
  await database.query(`UPDATE lingxios.agent_work_items work SET cancel_requested_at=COALESCE(cancel_requested_at,NOW()),
    status=CASE WHEN status='queued' THEN 'cancelled' ELSE status END,updated_at=NOW()
    FROM lingxios.agent_routine_runs run WHERE run.routine_id=$1 AND run.work_id=work.id
      AND (work.status IN ('queued','leased') OR EXISTS(SELECT 1 FROM lingxios.agent_delivery_outbox outbox WHERE outbox.run_id=work.id AND outbox.delivered_at IS NULL))`, [id])
}

export async function getTeacherDigest(database: SqlQueryable, work: Pick<WorkItem, 'tenantId' | 'agentId' | 'sessionId'>, context: Context): Promise<Record<string, unknown>> {
  const { rows } = await database.query(`SELECT schedule,timezone,status,version,next_run_at,pause_reason FROM lingxios.agent_routines
    WHERE tenant_id=$1 AND agent_id=$2 AND session_id=$3 AND course_id=$4 AND project_id=$5 AND kind='teacher_digest'`,
    [work.tenantId, work.agentId, work.sessionId, context.course.id, context.agent.projectId])
  const row = rows[0]
  if (!row) return { frequency: 'off', timezone: 'Asia/Shanghai', status: 'paused' }
  return { ...(row['schedule'] as Record<string, unknown>), timezone: row['timezone'], status: row['status'], version: row['version'],
    ...(row['status'] === 'active' ? { nextRunAt: instant(row['next_run_at']).toISOString() } : { frequency: 'off' }),
    ...(row['pause_reason'] ? { pauseReason: row['pause_reason'] } : {}) }
}

export async function configureTeacherDigest(database: SqlPool, services: Services, work: Omit<WorkItem, 'leaseToken'>, args: Record<string, unknown>) {
  if (work.kind === 'teacher_digest') throw new Error('scheduled teacher summaries are read-only')
  if (Object.keys(args).some(key => !['frequency', 'localTime', 'timezone', 'weekday'].includes(key))) throw new Error('unknown digest argument')
  const value = args['frequency'] === 'off' ? undefined : schedule(args)
  if (!value && Object.keys(args).some(key => key !== 'frequency')) throw new Error('pausing a digest only accepts frequency=off')
  return withTransaction(database, async client => {
    const context = await teacherContext(work, services, client)
    const id = 'teacher-digest-' + createHash('sha256').update(JSON.stringify([work.tenantId, work.agentId, work.sessionId])).digest('hex')
    if (!value) {
      await client.query(`UPDATE lingxios.agent_routines SET status='paused',next_run_at=NULL,version=version+1,pause_reason='requested',updated_at=NOW()
        WHERE id=$1 AND tenant_id=$2`, [id, work.tenantId])
    } else {
      const now = (await client.query('SELECT NOW() AS now')).rows[0]!['now']
      const next = await nextRun(client, value, instant(now))
      await client.query(`INSERT INTO lingxios.agent_routines(id,tenant_id,agent_id,session_id,principal_id,project_id,course_id,kind,schedule,timezone,status,next_run_at)
        VALUES($1,$2,$3,$4,$5,$6,$7,'teacher_digest',$8::jsonb,$9,'active',$10)
        ON CONFLICT(tenant_id,agent_id,session_id,kind) WHERE kind='teacher_digest' DO UPDATE SET principal_id=EXCLUDED.principal_id,project_id=EXCLUDED.project_id,course_id=EXCLUDED.course_id,
          schedule=EXCLUDED.schedule,timezone=EXCLUDED.timezone,status='active',next_run_at=EXCLUDED.next_run_at,
          version=lingxios.agent_routines.version+1,pause_reason=NULL,updated_at=NOW()`,
        [id, work.tenantId, work.agentId, work.sessionId, work.principalId, context.agent.projectId, context.course.id, JSON.stringify(value), value.timezone, next])
    }
    await cancelRoutineRuns(client, id)
    services.teacher!.inc('learning.teacher_agent.digest_configured', { frequency: value?.frequency ?? 'off' })
    return getTeacherDigest(client, work, context)
  })
}

export async function assertTeacherDigestWork(database: SqlQueryable, work: Omit<WorkItem, 'leaseToken'>, context: Context) {
  const { rows } = await database.query(`SELECT 1 FROM lingxios.agent_routines routine
    JOIN lingxios.agent_routine_runs run ON run.routine_id=routine.id AND run.routine_version=routine.version
    JOIN lingxios.agent_work_items work ON work.id=run.work_id
    WHERE run.work_id=$1 AND routine.status='active' AND routine.kind='teacher_digest'
      AND routine.tenant_id=$2 AND routine.agent_id=$3 AND routine.session_id=$4 AND routine.principal_id=$5
      AND routine.course_id=$6 AND routine.project_id=$7 AND work.kind='teacher_digest'
      AND work.tenant_id=routine.tenant_id AND work.agent_id=routine.agent_id AND work.session_id=routine.session_id
      AND work.principal_id=routine.principal_id AND work.thread_id IS NULL AND work.cancel_requested_at IS NULL`,
    [work.id, work.tenantId, work.agentId, work.sessionId, work.principalId, context.course.id, context.agent.projectId])
  if (rows.length !== 1 || work.threadId !== undefined) throw new Error('scheduled teacher work is cancelled, obsolete, or outside its authorized routine')
}

export async function scheduleTeacherDigests(database: SqlPool, services: Services): Promise<number> {
  if (!services.teacher) return 0
  return withTransaction(database, async client => {
    await client.query("SET LOCAL lock_timeout='2s'")
    await client.query("SET LOCAL statement_timeout='5s'")
    const { rows } = await client.query(`SELECT routine.*,NOW() AS clock,
      EXISTS(SELECT 1 FROM lingxios.agent_routine_runs run JOIN lingxios.agent_work_items work ON work.id=run.work_id
        WHERE run.routine_id=routine.id AND work.cancel_requested_at IS NULL
          AND (work.status IN ('queued','leased') OR EXISTS(SELECT 1 FROM lingxios.agent_delivery_outbox outbox WHERE outbox.run_id=work.id AND outbox.delivered_at IS NULL))) AS pending
      FROM lingxios.agent_routines routine WHERE routine.status='active' AND routine.kind='teacher_digest' AND routine.next_run_at<=NOW()
      ORDER BY routine.next_run_at,routine.id LIMIT 8 FOR UPDATE OF routine SKIP LOCKED`)
    let enqueued = 0
    for (const row of rows) {
      const identity = { tenantId: String(row['tenant_id']), agentId: String(row['agent_id']), sessionId: String(row['session_id']), principalId: String(row['principal_id']), kind: 'teacher_digest' }
      try {
        const context = await teacherContext(identity, services, client)
        if (context.course.id !== row['course_id'] || context.agent.projectId !== row['project_id']) throw new TeacherScopeError('scheduled course binding changed')
      } catch (error) {
        if (!(error instanceof TeacherScopeError) && !(error instanceof Error && error.name === 'ForbiddenError' && [403, 404].includes(Reflect.get(error, 'status')))) throw error
        await client.query("UPDATE lingxios.agent_routines SET status='paused',next_run_at=NULL,version=version+1,pause_reason=$2,updated_at=NOW() WHERE id=$1",
          [row['id'], error instanceof TeacherScopeError ? 'scope_changed' : 'authorization_revoked'])
        await cancelRoutineRuns(client, String(row['id']))
        continue
      }
      const scheduledAt = instant(row['next_run_at']).toISOString()
      const id = 'digest-run-' + createHash('sha256').update(JSON.stringify([row['id'], row['version'], scheduledAt])).digest('hex')
      let value: ReturnType<typeof schedule>
      try { value = schedule({ ...(row['schedule'] as Record<string, unknown>), timezone: row['timezone'] }) }
      catch {
        await client.query("UPDATE lingxios.agent_routines SET status='paused',next_run_at=NULL,version=version+1,pause_reason='invalid_schedule',updated_at=NOW() WHERE id=$1", [row['id']])
        await cancelRoutineRuns(client, String(row['id']))
        continue
      }
      const next = await nextRun(client, value, instant(row['clock']))
      const existing = await client.query('SELECT work_id FROM lingxios.agent_routine_runs WHERE routine_id=$1 AND routine_version=$2 AND scheduled_at=$3', [row['id'], row['version'], scheduledAt])
      if (row['pending'] || existing.rows.length) {
        await client.query('UPDATE lingxios.agent_routines SET next_run_at=$2,updated_at=NOW() WHERE id=$1', [row['id'], next])
        continue
      }
      await client.query(`INSERT INTO lingxios.agent_work_items(id,tenant_id,agent_id,session_id,principal_id,kind,lane,trigger_ref,priority,meta)
        VALUES($1,$2,$3,$4,$5,'teacher_digest','background',$1,-10,$6::jsonb)`,
        [id, identity.tenantId, identity.agentId, identity.sessionId, identity.principalId, JSON.stringify({ text: instruction, authorName: 'Scheduled teacher digest', routineId: row['id'], routineVersion: row['version'], scheduledAt })])
      await client.query('INSERT INTO lingxios.agent_routine_runs(routine_id,routine_version,scheduled_at,work_id) VALUES($1,$2,$3,$4)', [row['id'], row['version'], scheduledAt, id])
      await client.query('UPDATE lingxios.agent_routines SET next_run_at=$2,updated_at=NOW() WHERE id=$1', [row['id'], next])
      enqueued++
    }
    return enqueued
  })
}
