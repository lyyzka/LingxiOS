import type { SqlQueryable } from '../../control-plane/pg-store.js'
import type { HostAction, WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices } from './service-contracts.js'

export const CALENDAR_METHODS = { list: ['from', 'to'], get: ['eventId'], dispatches: ['eventId'] } as const

export async function executeCalendar(db: SqlQueryable, services: Pick<LingxiLoopServices, 'calendar' | 'permissionService'>,
  work: Omit<WorkItem, 'leaseToken'>, action: HostAction) {
  const api = services.calendar
  const method = action.action.slice('calendar.'.length)
  if (!api || !action.action.startsWith('calendar.') || !Object.hasOwn(CALENDAR_METHODS, method)) throw new Error('unsupported calendar action')
  const allowed: readonly string[] = CALENDAR_METHODS[method as keyof typeof CALENDAR_METHODS]
  if (Object.keys(action.args).some(key => !allowed.includes(key))) throw new Error('unknown calendar argument')
  if (!work.principalId) throw new Error('calendar access requires the original human principal')
  const eventId = action.args['eventId']
  if (method !== 'list' && (typeof eventId !== 'string' || !eventId.trim() || eventId.length > 2000)) throw new Error('eventId is required')
  let range: { from?: Date; to?: Date } = {}
  if (method === 'list') {
    range = api.listCalendarEventsQuerySchema.parse(action.args)
    if (!range.from || !range.to || range.to.getTime() < range.from.getTime() || range.to.getTime() - range.from.getTime() > 366 * 86400_000) {
      throw new Error('from and to must define a range of at most 366 days')
    }
  }
  const conversation = { actorUserId: work.principalId, companyId: work.tenantId,
    action: 'conversation:read' as const, resource: { type: 'conversation' as const, id: work.sessionId } }
  await services.permissionService.assertCan(conversation)
  const { rows } = await db.query('SELECT project_id FROM conversations WHERE company_id=$1 AND id=$2', [work.tenantId, work.sessionId])
  const projectId = rows[0]?.['project_id']
  if (typeof projectId !== 'string' || !projectId) throw new Error('calendar project scope is unavailable')
  const authorization = { actorUserId: work.principalId, companyId: work.tenantId, projectId,
    action: 'calendar:read' as const, resource: method !== 'list'
      ? { type: 'calendar_event' as const, id: eventId as string } : { type: 'project' as const, id: projectId } }
  await services.permissionService.assertCan(authorization)
  const scope = { companyId: work.tenantId, projectId, userId: work.principalId }
  const value = method === 'list' ? await api.calendarApplication.list(scope, range)
    : method === 'dispatches' ? await api.calendarApplication.dispatches(scope, eventId as string) : await api.calendarApplication.get(scope, eventId as string)
  await services.permissionService.assertCan(conversation)
  await services.permissionService.assertCan(authorization)
  if (Array.isArray(value)) return method === 'dispatches'
    ? { dispatches: value.slice(0, 100), truncated: value.length > 100 }
    : { events: value.slice(0, 100), truncated: value.length > 100 }
  if (value.id !== eventId) throw new Error('invalid calendar read result')
  return value
}
