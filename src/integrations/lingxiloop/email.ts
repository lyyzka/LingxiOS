import type { HostAction, WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices } from './service-contracts.js'

export const EMAIL_METHODS = {
  whoami: [], contacts: ['query'], inbox: ['unreadOnly', 'limit'], show: ['conversationId', 'limit'],
} as const
export const EMAIL_APPROVAL_METHODS = { send: ['to', 'cc', 'subject', 'body', 'attachmentClientMsgNos'], reply: ['conversationId', 'messageId', 'cc', 'body', 'attachmentClientMsgNos'] } as const

export async function executeEmail(work: Omit<WorkItem, 'leaseToken'>, action: HostAction, services: LingxiLoopServices) {
  const api = services.email
  const method = action.action.slice('email.'.length) as keyof typeof EMAIL_METHODS
  if (!api || !action.action.startsWith('email.') || !Object.hasOwn(EMAIL_METHODS, method)) throw new Error('unsupported or approval-required email action')
  if (Object.keys(action.args).some(key => !EMAIL_METHODS[method].includes(key as never))) throw new Error('unknown email argument')
  if (!work.principalId) throw new Error('persisted human authorization principal is required')
  const scope = { companyId: work.tenantId, userId: work.agentId }
  if (method === 'whoami' || method === 'contacts') await services.permissionService.assertCan({ actorUserId: work.principalId, companyId: work.tenantId, action: 'agent:read', resource: { type: 'agent', id: work.agentId } })
  switch (method) {
    case 'whoami': return api.getAgentEmailIdentity(scope)
    case 'contacts': return api.listAgentEmailContacts(scope, typeof action.args['query'] === 'string' ? action.args['query'] : '')
    case 'inbox': {
      const limit = action.args['limit'] === undefined ? 20 : Number(action.args['limit'])
      if (!Number.isInteger(limit) || limit < 1 || limit > 50 || action.args['unreadOnly'] !== undefined && typeof action.args['unreadOnly'] !== 'boolean') throw new Error('email inbox requires unreadOnly boolean and limit 1..50')
      const threads = await api.listAgentEmailInbox(scope, { unreadOnly: action.args['unreadOnly'] === true, limit })
      for (const thread of threads) {
        const conversationId = thread && typeof thread === 'object' ? (thread as Record<string, unknown>)['conversationId'] : undefined
        if (typeof conversationId !== 'string') throw new Error('native email inbox returned an invalid thread')
        await services.permissionService.assertCan({ actorUserId: work.principalId, companyId: work.tenantId, action: 'email:read', resource: { type: 'conversation', id: conversationId } })
      }
      return threads
    }
    case 'show': {
      const conversationId = action.args['conversationId'], limit = action.args['limit'] === undefined ? 50 : Number(action.args['limit'])
      if (typeof conversationId !== 'string' || !conversationId.trim() || !Number.isInteger(limit) || limit < 1 || limit > 50) throw new Error('email show requires conversationId and limit 1..50')
      await services.permissionService.assertCan({ actorUserId: work.principalId, companyId: work.tenantId, action: 'email:read', resource: { type: 'conversation', id: conversationId } })
      return api.getAgentEmailThread(scope, conversationId, limit)
    }
  }
}
