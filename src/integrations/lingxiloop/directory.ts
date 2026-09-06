import type { HostAction, WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices } from './service-contracts.js'

export const DIRECTORY_METHODS = { self: [], participants: ['kind'], statuses: [] } as const

export async function executeDirectory(work: Omit<WorkItem, 'leaseToken'>, action: HostAction, services: LingxiLoopServices) {
  const api = services.directory, method = action.action.slice('directory.'.length) as keyof typeof DIRECTORY_METHODS
  if (!api || !action.action.startsWith('directory.') || !Object.hasOwn(DIRECTORY_METHODS, method)) throw new Error('unsupported directory action')
  if (Object.keys(action.args).some(key => !DIRECTORY_METHODS[method].includes(key as never))) throw new Error('unknown directory argument')
  if (!work.principalId) throw new Error('persisted human authorization principal is required')
  await services.permissionService.assertCan({ actorUserId: work.principalId, companyId: work.tenantId, action: 'agent:read', resource: { type: 'agent', id: work.agentId } })
  if (method === 'self') {
    const result = await api.getAgentCliIdentity(work.agentId)
    const conversations = (result as { conversations?: unknown }).conversations
    if (!Array.isArray(conversations)) throw new Error('native directory identity is missing conversations')
    for (const conversation of conversations) {
      const id = (conversation as { id?: unknown })?.id
      if (typeof id !== 'string' || !id) throw new Error('native directory identity contains an invalid conversation')
      await services.permissionService.assertCan({ actorUserId: work.principalId, companyId: work.tenantId,
        action: 'conversation:read', resource: { type: 'conversation', id } })
    }
    return result
  }
  if (method === 'statuses') return api.listAgentCliStatuses(work.agentId)
  const kind = action.args['kind']
  if (kind !== undefined && kind !== 'agent' && kind !== 'human') throw new Error('directory participant kind must be agent or human')
  return api.listAgentCliParticipants(work.agentId, kind ?? null)
}
