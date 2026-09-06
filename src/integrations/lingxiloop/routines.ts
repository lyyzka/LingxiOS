import { createHash } from 'node:crypto'
import { withTransaction, type SqlPool, type SqlQueryable } from '../../control-plane/pg-store.js'
import type { HostAction, WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices } from './service-contracts.js'
import { cancelRoutineRuns } from './teacher-digest.js'

type Identity = Pick<WorkItem, 'tenantId' | 'agentId' | 'sessionId' | 'principalId' | 'threadId'>
type Services = Pick<LingxiLoopServices, 'permissionService'>
export const ROUTINE_METHODS = { list: [], pause: ['routineId'], create: ['kind', 'title', 'instructions', 'schedule', 'timezone'], activate: ['routineId'] } as const
class RoutineScopeError extends Error {}
function instant(value: unknown): Date { return value instanceof Date ? value : new Date(String(value)) }

function text(value: unknown, max: number): string {
  if (typeof value !== 'string' || !value.trim() || value.length > max) throw new Error('invalid routine text')
  return value.trim()
}

export function routineArguments(action: Pick<HostAction, 'action' | 'args'>) {
  const method = action.action.slice('routines.'.length)
  if (!action.action.startsWith('routines.') || !Object.hasOwn(ROUTINE_METHODS, method)) throw new Error('unsupported routine action')
  const fields: readonly string[] = ROUTINE_METHODS[method as keyof typeof ROUTINE_METHODS]
  if (Object.keys(action.args).some(key => !fields.includes(key))) throw new Error('unknown routine argument')
  if (method === 'pause' || method === 'activate') return { routineId: text(action.args['routineId'], 200) }
  if (method === 'list') return {}
  const kind = text(action.args['kind'], 100)
  if (!/^[a-z][a-z0-9_]*$/.test(kind) || kind.startsWith('teacher_')) throw new Error('reserved or invalid routine kind')
  const timezone = text(action.args['timezone'] ?? 'Asia/Shanghai', 100)
  try {
    if (/^[+-]/.test(new Intl.DateTimeFormat('en-US', { timeZone: timezone }).resolvedOptions().timeZone)) throw new Error('offset')
  } catch { throw new Error('timezone must be a valid IANA timezone') }
  const schedule = action.args['schedule']
  if (!schedule || typeof schedule !== 'object' || Array.isArray(schedule)) throw new Error('routine schedule is required')
  const fieldsInSchedule = Object.keys(schedule)
  const everyMinutes: unknown = Reflect.get(schedule, 'everyMinutes'), time: unknown = Reflect.get(schedule, 'time')
  if (fieldsInSchedule.length !== 1 || (everyMinutes !== undefined
    ? fieldsInSchedule[0] !== 'everyMinutes' || !Number.isSafeInteger(everyMinutes) || Number(everyMinutes) < 5 || Number(everyMinutes) > 525600
    : fieldsInSchedule[0] !== 'time' || typeof time !== 'string' || !/^([01]\d|2[0-3]):[0-5]\d$/.test(time))) throw new Error('schedule requires everyMinutes=5..525600 or time=HH:mm')
  return { kind, title: text(action.args['title'], 2000), instructions: text(action.args['instructions'], 20000), schedule, timezone }
}

export async function routineScope(database: SqlQueryable, services: Services, work: Identity) {
  if (!work.principalId) throw new RoutineScopeError('routine principal is required')
  await services.permissionService.assertCan({ actorUserId: work.principalId, companyId: work.tenantId,
    action: 'agent_run:control', resource: { type: 'conversation', id: work.sessionId } })
  const { rows } = await database.query(`SELECT conversation.project_id FROM conversations conversation
    JOIN im_channel_bindings binding ON binding.company_id=conversation.company_id AND binding.channel_id=conversation.id
    JOIN participants agent ON agent.company_id=conversation.company_id AND agent.id=$3 AND agent.kind='agent' AND agent.departed_at IS NULL
    JOIN participants principal ON principal.company_id=conversation.company_id AND principal.id=$4 AND principal.kind='human' AND principal.departed_at IS NULL
    WHERE conversation.company_id=$1 AND conversation.id=$2
      AND binding.profile->'members' ? agent.id AND binding.profile->'members' ? principal.id
      AND agent.capabilities @> '["routines"]'::jsonb
      AND NOT EXISTS(SELECT 1 FROM learning_project_teacher_agents teacher WHERE teacher.company_id=agent.company_id AND teacher.agent_id=agent.id)`,
    [work.tenantId, work.sessionId, work.agentId, work.principalId])
  if (rows.length !== 1) throw new RoutineScopeError('routine is outside the active agent and human conversation binding')
  return rows[0]!['project_id'] ?? null
}

export async function findRoutine(database: SqlQueryable, work: Identity, id: string, projectId: unknown, lock = false) {
  const { rows } = await database.query(`SELECT * FROM lingxios.agent_routines
    WHERE id=$1 AND tenant_id=$2 AND agent_id=$3 AND session_id=$4 AND thread_id IS NOT DISTINCT FROM $5
      AND project_id IS NOT DISTINCT FROM $6 AND kind<>'teacher_digest'${lock ? ' FOR UPDATE' : ''}`,
    [id, work.tenantId, work.agentId, work.sessionId, work.threadId ?? null, projectId])
  if (rows.length !== 1) throw new RoutineScopeError('routine is outside the current conversation and project')
  return rows[0]!
}

export async function routinePreview(database: SqlQueryable, services: Services, work: Identity, action: HostAction) {
  const args = routineArguments(action), projectId = await routineScope(database, services, work)
  if (action.action === 'routines.create') return { ...args, projectId, principalId: work.principalId!, threadId: work.threadId ?? null }
  if (action.action !== 'routines.activate') throw new Error('routine approval requires create or activate')
  const row = await findRoutine(database, work, args.routineId!, projectId, true)
  routineArguments({ action: 'routines.create', args: { kind: row['kind'], title: row['title'], instructions: row['instructions'], schedule: row['schedule'], timezone: row['timezone'] } })
  return { routineId: row['id'], version: row['version'], kind: row['kind'], title: row['title'], instructions: row['instructions'],
    schedule: row['schedule'], timezone: row['timezone'], projectId, principalId: row['principal_id'], threadId: row['thread_id'], status: row['status'] }
}

export async function nextRoutineRun(database: SqlQueryable, schedule: Record<string, unknown>, timezone: string, now: Date) {
  const { rows } = await database.query(`SELECT CASE WHEN $2::int IS NOT NULL THEN $1::timestamptz + $2::int * INTERVAL '1 minute'
    ELSE (SELECT MIN((($1::timestamptz AT TIME ZONE $3)::date+day+$4::time) AT TIME ZONE $3)
      FROM generate_series(0,2) day WHERE ((($1::timestamptz AT TIME ZONE $3)::date+day+$4::time) AT TIME ZONE $3)>$1::timestamptz) END AS next`,
    [now, schedule['everyMinutes'] ?? null, timezone, schedule['time'] ?? '09:00'])
  const next = instant(rows[0]?.['next'])
  if (!Number.isFinite(next.getTime()) || next <= now) throw new Error('routine schedule did not produce a future instant')
  return next.toISOString()
}

/** Runs only within the approval transaction, after its preview was checked. */
export async function applyRoutineApproval(database: SqlQueryable, services: Services, work: Identity, action: HostAction, preview: Record<string, unknown>) {
  const args = routineArguments(action)
  const id = action.action === 'routines.create' ? 'routine-' + createHash('sha256').update(action.idempotencyKey).digest('hex') : args.routineId!
  if (action.action === 'routines.create') {
    await database.query(`INSERT INTO lingxios.agent_routines(id,tenant_id,agent_id,session_id,principal_id,project_id,thread_id,kind,title,instructions,schedule,timezone,status)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12,'paused')`,
      [id, work.tenantId, work.agentId, work.sessionId, work.principalId, preview['projectId'], work.threadId ?? null,
        args.kind, args.title, args.instructions, JSON.stringify(args.schedule), args.timezone])
  } else if (action.action === 'routines.activate') {
    const creator = { ...work, principalId: String(preview['principalId']) }
    if (await routineScope(database, services, creator) !== preview['projectId']) throw new RoutineScopeError('routine project changed')
    const now = (await database.query('SELECT NOW() AS now')).rows[0]!['now']
    const next = await nextRoutineRun(database, preview['schedule'] as Record<string, unknown>, String(preview['timezone']), instant(now))
    await database.query(`UPDATE lingxios.agent_routines SET status='active',next_run_at=$2,version=version+1,pause_reason=NULL,updated_at=NOW() WHERE id=$1`, [id, next])
    await cancelRoutineRuns(database, id)
  } else throw new Error('unsupported routine approval')
  return findRoutine(database, work, id, preview['projectId'])
}

export async function executeRoutine(database: SqlPool, services: Services, work: Identity, action: HostAction) {
  const args = routineArguments(action)
  if (action.action !== 'routines.list' && action.action !== 'routines.pause') throw new Error('routine action requires approval')
  return withTransaction(database, async client => {
    const projectId = await routineScope(client, services, work)
    if (action.action === 'routines.list') {
      const { rows } = await client.query(`SELECT * FROM lingxios.agent_routines
        WHERE tenant_id=$1 AND agent_id=$2 AND session_id=$3 AND thread_id IS NOT DISTINCT FROM $4
          AND project_id IS NOT DISTINCT FROM $5 AND kind<>'teacher_digest' ORDER BY created_at DESC,id LIMIT 101`,
        [work.tenantId, work.agentId, work.sessionId, work.threadId ?? null, projectId])
      return { routines: rows.slice(0, 100), truncated: rows.length > 100 }
    }
    const row = await findRoutine(client, work, args.routineId!, projectId, true)
    await client.query(`UPDATE lingxios.agent_routines SET status='paused',next_run_at=NULL,version=version+1,pause_reason='requested',updated_at=NOW() WHERE id=$1`, [row['id']])
    await cancelRoutineRuns(client, String(row['id']))
    return findRoutine(client, work, String(row['id']), projectId)
  })
}

function identity(row: Record<string, unknown>): Identity {
  return { tenantId: String(row['tenant_id']), agentId: String(row['agent_id']), sessionId: String(row['session_id']), principalId: String(row['principal_id']),
    ...(row['thread_id'] !== null ? { threadId: String(row['thread_id']) } : {}) }
}

export async function assertRoutineWork(database: SqlQueryable, services: Services, work: Omit<WorkItem, 'leaseToken'>) {
  const projectId = await routineScope(database, services, work)
  const { rows } = await database.query(`SELECT 1 FROM lingxios.agent_routines routine
    JOIN lingxios.agent_routine_runs run ON run.routine_id=routine.id AND run.routine_version=routine.version
    JOIN lingxios.agent_work_items work ON work.id=run.work_id
    WHERE run.work_id=$1 AND routine.status='active' AND routine.kind<>'teacher_digest' AND work.kind='routine'
      AND routine.tenant_id=$2 AND routine.agent_id=$3 AND routine.session_id=$4 AND routine.principal_id=$5
      AND routine.project_id IS NOT DISTINCT FROM $6 AND routine.thread_id IS NOT DISTINCT FROM $7
      AND work.tenant_id=routine.tenant_id AND work.agent_id=routine.agent_id AND work.session_id=routine.session_id
      AND work.principal_id=routine.principal_id AND work.thread_id IS NOT DISTINCT FROM routine.thread_id AND work.cancel_requested_at IS NULL`,
    [work.id, work.tenantId, work.agentId, work.sessionId, work.principalId, projectId, work.threadId ?? null])
  if (rows.length !== 1) throw new RoutineScopeError('scheduled work is cancelled, obsolete, or outside its routine')
}

export async function scheduleRoutines(database: SqlPool, services: Services) {
  return withTransaction(database, async client => {
    await client.query("SET LOCAL lock_timeout='2s'")
    await client.query("SET LOCAL statement_timeout='5s'")
    const { rows } = await client.query(`SELECT routine.*,NOW() AS clock,
      EXISTS(SELECT 1 FROM lingxios.agent_routine_runs run JOIN lingxios.agent_work_items work ON work.id=run.work_id
        WHERE run.routine_id=routine.id AND work.cancel_requested_at IS NULL AND (work.status IN ('queued','leased')
          OR EXISTS(SELECT 1 FROM lingxios.agent_delivery_outbox outbox WHERE outbox.run_id=work.id AND outbox.delivered_at IS NULL))) AS pending
      FROM lingxios.agent_routines routine WHERE status='active' AND kind<>'teacher_digest' AND next_run_at<=NOW()
      ORDER BY next_run_at,id LIMIT 8 FOR UPDATE OF routine SKIP LOCKED`)
    let enqueued = 0
    for (const row of rows) {
      const work = identity(row)
      try {
        if (await routineScope(client, services, work) !== row['project_id']) throw new RoutineScopeError('routine project changed')
      } catch (error) {
        if (!(error instanceof RoutineScopeError) && !(error instanceof Error && error.name === 'ForbiddenError' && [403, 404].includes(Reflect.get(error, 'status')))) throw error
        await client.query("UPDATE lingxios.agent_routines SET status='paused',next_run_at=NULL,version=version+1,pause_reason='authorization_or_scope_changed',updated_at=NOW() WHERE id=$1", [row['id']])
        await cancelRoutineRuns(client, String(row['id']))
        continue
      }
      try {
        routineArguments({ action: 'routines.create', args: {
          kind: row['kind'], title: row['title'], instructions: row['instructions'], schedule: row['schedule'], timezone: row['timezone'],
        } })
      } catch {
        await client.query("UPDATE lingxios.agent_routines SET status='paused',next_run_at=NULL,version=version+1,pause_reason='invalid_schedule',updated_at=NOW() WHERE id=$1", [row['id']])
        await cancelRoutineRuns(client, String(row['id']))
        continue
      }
      const scheduledAt = instant(row['next_run_at']).toISOString()
      const next = await nextRoutineRun(client, row['schedule'] as Record<string, unknown>, String(row['timezone']), instant(row['clock']))
      const existing = await client.query('SELECT work_id FROM lingxios.agent_routine_runs WHERE routine_id=$1 AND routine_version=$2 AND scheduled_at=$3', [row['id'], row['version'], scheduledAt])
      if (!row['pending'] && !existing.rows.length) {
        const id = 'routine-run-' + createHash('sha256').update(JSON.stringify([row['id'], row['version'], scheduledAt])).digest('hex')
        await client.query(`INSERT INTO lingxios.agent_work_items(id,tenant_id,agent_id,session_id,principal_id,thread_id,kind,lane,trigger_ref,meta)
          VALUES($1,$2,$3,$4,$5,$6,'routine','background',$1,$7::jsonb)`,
          [id, work.tenantId, work.agentId, work.sessionId, work.principalId, work.threadId ?? null,
            JSON.stringify({ text: row['instructions'], authorName: row['title'], routineId: row['id'], routineVersion: row['version'], scheduledAt })])
        await client.query('INSERT INTO lingxios.agent_routine_runs(routine_id,routine_version,scheduled_at,work_id) VALUES($1,$2,$3,$4)', [row['id'], row['version'], scheduledAt, id])
        enqueued++
      }
      await client.query('UPDATE lingxios.agent_routines SET next_run_at=$2,updated_at=NOW() WHERE id=$1', [row['id'], next])
    }
    return enqueued
  })
}
