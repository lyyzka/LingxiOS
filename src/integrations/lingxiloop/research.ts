import type { HostAction, WorkItem } from '../../protocol/types.js'
import { readResearch, searchResearch } from '../../research/index.js'
import type { LingxiLoopServices } from './service-contracts.js'

export const RESEARCH_METHODS = { search: ['query', 'limit'], read: ['url'] } as const

export async function executeResearch(work: Omit<WorkItem, 'leaseToken'>, action: HostAction, services: Pick<LingxiLoopServices, 'permissionService'>) {
  const method = action.action.slice('research.'.length)
  if (!work.principalId || !action.action.startsWith('research.') || !Object.hasOwn(RESEARCH_METHODS, method)) throw new Error('unsupported research action or missing principal')
  const allowed: readonly string[] = RESEARCH_METHODS[method as keyof typeof RESEARCH_METHODS]
  if (Object.keys(action.args).some(key => !allowed.includes(key))) throw new Error('unknown research argument')
  await services.permissionService.assertCan({ actorUserId: work.principalId, companyId: work.tenantId, action: 'agent:read', resource: { type: 'conversation', id: work.sessionId } })
  if (method === 'search') {
    if (typeof action.args['query'] !== 'string' || (action.args['limit'] !== undefined && typeof action.args['limit'] !== 'number')) throw new Error('invalid research search arguments')
    return searchResearch(action.args['query'], action.args['limit'] as number | undefined)
  }
  if (typeof action.args['url'] !== 'string') throw new Error('research URL is required')
  return readResearch(action.args['url'])
}
