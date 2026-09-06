import type { HostAction, WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices } from './service-contracts.js'

export const POLL_METHODS = { create: ['question', 'mode', 'options', 'expiresInMinutes'], vote: ['messageId', 'optionIds'], close: ['messageId'], show: ['messageId'] } as const

export async function executePoll(work: Omit<WorkItem, 'leaseToken'>, action: HostAction, services: Pick<LingxiLoopServices, 'pollApplication' | 'permissionService'>) {
  const polls = services.pollApplication
  const method = action.action.slice('polls.'.length)
  if (!polls || !work.principalId || !action.action.startsWith('polls.') || !Object.hasOwn(POLL_METHODS, method)) throw new Error('unsupported poll action or missing principal')
  const allowed: readonly string[] = POLL_METHODS[method as keyof typeof POLL_METHODS]
  const args = action.args
  if (Object.keys(args).some(key => !allowed.includes(key))) throw new Error('unknown poll argument')
  const scope = { companyId: work.tenantId, actorId: work.agentId }
  const messageId = args['messageId']
  if (method !== 'create') {
    if (typeof messageId !== 'string' || !messageId.trim()) throw new Error('messageId is required')
    if (await polls.conversationId(work.tenantId, messageId) !== work.sessionId) throw new Error('poll is outside the authorized conversation')
  }
  await services.permissionService.assertCan({ actorUserId: work.principalId, companyId: work.tenantId,
    action: method === 'show' ? 'poll:read' : method === 'create' ? 'poll:create' : method === 'vote' ? 'poll:vote' : 'poll:close',
    resource: method === 'create' ? { type: 'conversation', id: work.sessionId } : { type: 'poll', id: messageId as string },
  })
  if (method === 'create') {
    if (typeof args['question'] !== 'string' || !args['question'].trim() || args['question'].length > 280) throw new Error('invalid poll question')
    const options = args['options']
    if (!Array.isArray(options) || options.length < 2 || options.length > 10 || options.some(value => typeof value !== 'string' || !value.trim() || value.length > 120)) throw new Error('invalid poll options')
    const mode = args['mode'] ?? 'single'
    if (mode !== 'single' && mode !== 'multi') throw new Error('invalid poll mode')
    const expires = args['expiresInMinutes']
    if (expires != null && (typeof expires !== 'number' || !Number.isFinite(expires) || expires <= 0)) throw new Error('invalid poll expiry')
    return polls.create({ ...scope, conversationId: work.sessionId, question: args['question'], mode, options, expiresInMinutes: (expires ?? null) as number | null, idempotencyKey: action.idempotencyKey })
  }
  if (method === 'vote') {
    const optionIds = args['optionIds']
    if (!Array.isArray(optionIds) || optionIds.length > 10 || optionIds.some(value => typeof value !== 'string' || !value.trim())) throw new Error('invalid vote options')
    return polls.vote({ ...scope, messageId: messageId as string, voterKind: 'agent', optionIds })
  }
  if (method === 'close') return polls.close({ ...scope, messageId: messageId as string, reason: 'manual' })
  return polls.show(work.tenantId, messageId as string)
}
