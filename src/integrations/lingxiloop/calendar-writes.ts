import { isDeepStrictEqual } from 'node:util'
import { flushEventOutbox } from './event-outbox.js'
import { withTransaction, type SqlPool, type SqlQueryable } from '../../control-plane/pg-store.js'
import type { HostAction, WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices, NativeCalendarChanged, NativeQueryable } from './service-contracts.js'




export async function updateCalendar(database: SqlPool, services: Pick<LingxiLoopServices, 'calendar'>,
  work: Omit<WorkItem, 'leaseToken'>, action: HostAction) {
  const api = services.calendar?.writes
  if (!api || !work.principalId || action.action !== 'calendar.update') throw new Error('calendar write bindings and original human principal are required')
  if (Object.keys(action.args).some(key => !['eventId', 'expected', 'patch'].includes(key))) throw new Error('unknown calendar update argument')
  const { eventId, expected } = action.args
  if (typeof eventId !== 'string' || !eventId.trim() || eventId.length > 2000) throw new Error('eventId is required')
  if (!expected || typeof expected !== 'object' || Array.isArray(expected) || JSON.stringify(expected).length > 32_000) throw new Error('expected must contain the observed event')
  const patch = api.updateCalendarEventRequestSchema.parse(action.args['patch'])
  return withTransaction(database, async client => {
    await client.query("SET LOCAL lock_timeout='5s'")
    await client.query("SET LOCAL statement_timeout='10s'")
    const live = await client.query(`SELECT work.id FROM lingxios.agent_work_items work
      JOIN lingxios.agent_action_intents intent ON intent.idempotency_key=$3
      WHERE work.id=$1 AND work.fence=$2 AND work.status='leased' AND work.lease_expires_at>NOW()
        AND work.cancel_requested_at IS NULL AND work.tenant_id=$4 AND work.principal_id=$5
        AND intent.intent->>'workId'=work.id AND intent.intent->'action'->'args'=$6::jsonb
        AND intent.intent->'action'->>'action'='calendar.update'
        AND (intent.intent->>'requestVersion')::integer=jsonb_array_length(work.steer_inputs)+1
      FOR UPDATE OF work`, [work.id, work.fence, action.idempotencyKey, work.tenantId, work.principalId, JSON.stringify(action.args)])
    if (live.rows.length !== 1) throw new Error('calendar update requires the current live action intent')
    await assertCalendarAgent(client, work)
    const permissions = api.createPermissionService(client, { lockDependencies: true })
    await permissions.assertCan({ actorUserId: work.principalId!, companyId: work.tenantId,
      action: 'conversation:read', resource: { type: 'conversation', id: work.sessionId } })
    const room = await client.query('SELECT project_id FROM conversations WHERE id=$1 AND company_id=$2 FOR SHARE', [work.sessionId, work.tenantId])
    const projectId = room.rows[0]?.['project_id']
    if (typeof projectId !== 'string' || !projectId) throw new Error('calendar project scope is unavailable')
    await permissions.assertCan({ actorUserId: work.principalId!, companyId: work.tenantId, projectId,
      action: 'calendar:write', resource: { type: 'calendar_event', id: eventId } })
    const current = await client.query('SELECT id FROM calendar_events WHERE id=$1 AND company_id=$2 AND project_id=$3 FOR UPDATE', [eventId, work.tenantId, projectId])
    if (current.rows.length !== 1) throw new Error('calendar event is outside this project')
    let notification: NativeCalendarChanged | undefined
    const application = new api.CalendarApplication(client as unknown as NativeQueryable, { publish: async event => { notification = event } },
      { dispatch: async () => { throw new Error('calendar update cannot dispatch directly') } })
    const scope = { companyId: work.tenantId, projectId, userId: work.principalId! }
    // Compare the complete visible state: native timestamps lose sub-millisecond precision in JSON.
    if (!isDeepStrictEqual(await application.get(scope, eventId), expected)) throw new Error('calendar event changed; read it again before updating')
    const target = Object.hasOwn(patch, 'targetConversationId') ? patch.targetConversationId : (expected as Record<string, unknown>)['targetConversationId']
    if (typeof target === 'string') await permissions.assertCan({ actorUserId: work.principalId!, companyId: work.tenantId, projectId,
      action: 'conversation:write', resource: { type: 'conversation', id: target } })
    const event = await application.update(scope, eventId, patch)
    if (!notification || notification.type !== 'calendar.changed' || notification.kind !== 'event.updated'
      || notification.eventId !== eventId || notification.companyId !== work.tenantId || notification.workspaceId !== projectId) throw new Error('calendar update notification is invalid')
    await queueCalendarEvent(client, work, action, { ...notification, actorId: work.agentId })
    return { event, notification: 'queued' as const }
  })
}

export async function assertCalendarAgent(database: SqlQueryable, work: Pick<WorkItem, 'tenantId' | 'agentId' | 'sessionId'>) {
  const { rows } = await database.query(`SELECT p.id FROM participants p
    JOIN im_channel_bindings b ON b.company_id=p.company_id AND b.channel_id=$3
    WHERE p.company_id=$1 AND p.id=$2 AND p.kind='agent' AND p.departed_at IS NULL
      AND p.capabilities @> '["calendar"]'::jsonb AND b.profile->'members' ? p.id
      AND NOT EXISTS(SELECT 1 FROM learning_project_teacher_agents teacher WHERE teacher.company_id=p.company_id AND teacher.agent_id=p.id)
    FOR SHARE OF p,b`, [work.tenantId, work.agentId, work.sessionId])
  if (rows.length !== 1) throw new Error('calendar agent capability or conversation membership was revoked')
}

/** Written in the same transaction as the native mutation, including approved writes. */
export async function queueCalendarEvent(database: SqlQueryable, work: Pick<WorkItem, 'id'>, action: HostAction, event: NativeCalendarChanged) {
  await database.query('INSERT INTO lingxios.agent_calendar_outbox(id,work_id,event) VALUES($1,$2,$3::jsonb)',
    [action.idempotencyKey, work.id, JSON.stringify(event)])
}

export async function flushCalendarEvents(database: SqlQueryable, services: Pick<LingxiLoopServices, 'calendar'>): Promise<void> {
  const api = services.calendar?.writes
  if (api) await flushEventOutbox(database, 'agent_calendar_outbox', event => api.publish(api.CH_CALENDAR_EVENTS, event as NativeCalendarChanged))
}