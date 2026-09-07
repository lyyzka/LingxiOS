import { createHash } from 'node:crypto'
import { isDeepStrictEqual } from 'node:util'
import { withTransaction, type SqlPool, type SqlQueryable } from '../../control-plane/pg-store.js'
import type { ActionIntent } from '../../control-plane/stores.js'
import type { HostAction, WorkItem } from '../../protocol/types.js'
import { lockPendingApproval, recordExecutedApproval, inspectApproval, persistApproval, resumeApproved } from './approvals.js'
import { assertCalendarAgent, queueCalendarEvent } from './calendar-writes.js'
import type { LingxiLoopServices, NativeCalendarChanged, NativeQueryable } from './service-contracts.js'

type Services = Pick<LingxiLoopServices, 'calendar' | 'permissionService'>
type CalendarWork = Pick<WorkItem, 'id' | 'tenantId' | 'principalId' | 'agentId' | 'sessionId'>

/** Build the same scoped preview at request time and immediately before the approved mutation. */
async function calendarApproval(database: SqlQueryable, services: Services, work: CalendarWork, action: HostAction, approverId?: string) {
  const api = services.calendar?.writes
  if (!api || !work.principalId) throw new Error('calendar write bindings and original human principal are required')
  if (action.action !== 'calendar.create' && action.action !== 'calendar.delete') throw new Error('unsupported calendar approval')
  await assertCalendarAgent(database, work)
  const permissions = api.createPermissionService(database, { lockDependencies: true })
  for (const userId of new Set([work.principalId, ...(approverId ? [approverId] : [])])) {
    await permissions.assertCan({ actorUserId: userId, companyId: work.tenantId,
      action: 'conversation:read', resource: { type: 'conversation', id: work.sessionId } })
  }
  const room = await database.query('SELECT project_id FROM conversations WHERE id=$1 AND company_id=$2 FOR SHARE', [work.sessionId, work.tenantId])
  const projectId = room.rows[0]?.['project_id']
  if (typeof projectId !== 'string' || !projectId) throw new Error('calendar project scope is unavailable')
  const scope = { companyId: work.tenantId, projectId, userId: work.principalId }
  let notification: NativeCalendarChanged | undefined
  const application = new api.CalendarApplication(database as NativeQueryable,
    { publish: async event => { notification = event } }, { dispatch: async () => { throw new Error('approval cannot dispatch calendar tasks directly') } })
  const create = action.action === 'calendar.create'
  const { eventId, expected } = action.args
  if (!create && (Object.keys(action.args).some(key => !['eventId', 'expected'].includes(key))
    || typeof eventId !== 'string' || !eventId.trim() || eventId.length > 2000
    || !expected || typeof expected !== 'object' || Array.isArray(expected) || JSON.stringify(expected).length > 32_000)) throw new Error('calendar.delete requires eventId and the complete observed event')
  for (const userId of new Set([work.principalId, ...(approverId ? [approverId] : [])])) {
    await permissions.assertCan({ actorUserId: userId, companyId: work.tenantId, projectId,
      action: 'calendar:write', resource: create ? { type: 'project', id: projectId } : { type: 'calendar_event', id: eventId as string } })
  }
  const input = create ? api.createCalendarEventRequestSchema.parse(action.args) : undefined
  if (input?.kind === 'agent_task' && !input.targetConversationId) input.targetConversationId = work.sessionId
  if (input?.targetConversationId) {
    for (const userId of new Set([work.principalId, ...(approverId ? [approverId] : [])])) {
      await permissions.assertCan({ actorUserId: userId, companyId: work.tenantId, projectId,
        action: 'conversation:write', resource: { type: 'conversation', id: input.targetConversationId } })
    }
  }
  if (!create) {
    const target = await database.query('SELECT id FROM calendar_events WHERE id=$1 AND company_id=$2 AND project_id=$3 FOR UPDATE', [eventId, work.tenantId, projectId])
    if (target.rows.length !== 1 || !isDeepStrictEqual(await application.get(scope, eventId as string), expected)) throw new Error('calendar event changed; read it again before deleting')
  }
  // Dates are represented identically in persisted approval JSON and subsequent previews.
  const preview: Record<string, unknown> = JSON.parse(JSON.stringify({ projectId, conversationId: work.sessionId,
    ...(create ? { input } : { event: expected }) }))
  return { preview, apply: async () => {
    const id = create ? `ce-${createHash('sha256').update(action.idempotencyKey).digest('hex')}` : eventId as string
    const event = input ? await application.create(scope, input, { eventId: id }) : await application.delete(scope, id)
    const persisted = await database.query('SELECT id FROM calendar_events WHERE id=$1 AND company_id=$2 AND project_id=$3', [id, work.tenantId, projectId])
    if (persisted.rows.length !== (create ? 1 : 0) || (!create && !('ok' in event && event.ok === true))) throw new Error('calendar mutation postcondition was not observed')
    if (input) {
      const actual = await application.get(scope, id)
      const fields = JSON.parse(JSON.stringify(input)) as Record<string, unknown>
      if (actual.id !== id || !isDeepStrictEqual(event, actual)
        || !Object.entries(fields).every(([key, value]) => isDeepStrictEqual(Reflect.get(actual, key), value))) throw new Error('created calendar event does not match the approved fields')
    }
    if (!notification || notification.type !== 'calendar.changed' || notification.kind !== (create ? 'event.created' : 'event.deleted')
      || notification.eventId !== id || notification.companyId !== work.tenantId || notification.workspaceId !== projectId) throw new Error('calendar mutation notification is invalid')
    await queueCalendarEvent(database, work, action, { ...notification, actorId: work.agentId })
    return { eventId: id, ...(create ? { event } : { deleted: true }), notification: 'queued' as const }
  } }
}

export async function requestCalendarApproval(database: SqlPool, services: Services, work: Omit<WorkItem, 'leaseToken'>, action: HostAction) {
  return withTransaction(database, async client => {
    await client.query("SET LOCAL lock_timeout='5s'")
    await client.query("SET LOCAL statement_timeout='15s'")
    const { preview } = await calendarApproval(client, services, work, action)
    return persistApproval(client, work, action, { summary: action.action === 'calendar.create' ? 'Create the reviewed calendar event' : 'Delete the reviewed calendar event',
      scope: { projectId: preview['projectId'], conversationId: work.sessionId }, preview })
  })
}

export async function approveCalendar(database: SqlPool, services: Services, input: { companyId: string; userId: string; approvalId: string }) {
  const reviewed = await inspectApproval(database, services, input)
  if (!['calendar.create', 'calendar.delete'].includes(reviewed.action.action)) throw new Error('unsupported calendar approval')
  if (reviewed.status === 'EXECUTED') return resumeApproved(database, input, reviewed)
  if (reviewed.status !== 'PENDING') throw new Error('calendar approval is no longer pending')
  await withTransaction(database, async client => {
    await client.query("SET LOCAL lock_timeout='5s'")
    await client.query("SET LOCAL statement_timeout='15s'")
    const intent = await lockPendingApproval(client, input, reviewed)
    const permissions = services.calendar!.writes!.createPermissionService(client, { lockDependencies: true })
    await permissions.assertCan({ actorUserId: input.userId, companyId: input.companyId,
      action: 'agent_approval:resolve', resource: { type: 'approval', id: input.approvalId } })
    const work = { id: intent.workId, tenantId: intent.tenantId, agentId: intent.agentId, sessionId: intent.sessionId,
      ...(intent.principalId ? { principalId: intent.principalId } : {}) }
    const prepared = await calendarApproval(client, services, work, reviewed.action, input.userId)
    if (!isDeepStrictEqual(prepared.preview, reviewed.preview)) throw new Error('calendar approval preview is stale')
    const value = await prepared.apply()
    await recordExecutedApproval(client, input, reviewed, value, 'PENDING')
  })
  return resumeApproved(database, input, reviewed)
}
