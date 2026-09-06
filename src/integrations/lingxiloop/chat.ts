import type { HostAction, WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices } from './service-contracts.js'
import { questionnaire } from './questionnaire.js'

export const CHAT_METHODS = { history: ['limit'], send: ['body', 'replyToClientMsgNo'], ask: ['title', 'items', 'submitLabel'] } as const

export async function executeChat(work: Omit<WorkItem, 'leaseToken'>, action: HostAction, services: LingxiLoopServices, channelType: number) {
  const method = action.action.slice('chat.'.length)
  if (!work.principalId || !action.action.startsWith('chat.') || !Object.hasOwn(CHAT_METHODS, method)) throw new Error('unsupported chat action or missing principal')
  const allowed: readonly string[] = CHAT_METHODS[method as keyof typeof CHAT_METHODS]
  if (Object.keys(action.args).some(key => !allowed.includes(key))) throw new Error('unknown chat argument')
  await services.permissionService.assertCan({ actorUserId: work.principalId, companyId: work.tenantId,
    action: method === 'history' ? 'conversation:read' : 'conversation:write', resource: { type: 'conversation', id: work.sessionId },
  })
  if (method === 'history') {
    if (!services.advanceAgentReadReceipt) throw new Error('native read receipt resource is required')
    const limit = action.args['limit'] ?? 50
    if (typeof limit !== 'number' || !Number.isInteger(limit) || limit < 1 || limit > 100) throw new Error('history limit must be an integer from 1 to 100')
    const messages = await services.wukongClient().syncMessages(work.sessionId, channelType, limit, work.agentId)
    if (messages.some(message => message.channelId !== work.sessionId || message.channelType !== channelType || !Number.isSafeInteger(message.messageSeq) || message.messageSeq < 0)) throw new Error('invalid native history scope or sequence')
    const readThroughSeq = messages.reduce((max, message) => Math.max(max, message.messageSeq), 0)
    if (readThroughSeq > 0) await services.advanceAgentReadReceipt({ companyId: work.tenantId, channelId: work.sessionId, agentId: work.agentId, readThroughSeq })
    return messages
  }
  if (method === 'ask') {
    const form = questionnaire(action.args)
    return services.wukongClient().sendMessage(work.sessionId, channelType, work.agentId, {
      version: 1, kind: 'questionnaire', clientMsgNo: `questionnaire-${action.idempotencyKey}`, body: form.title,
      refs: { runId: work.id, agentId: work.agentId }, data: { questionnaire: form },
      ...(work.threadId ? { replyToClientMsgNo: work.threadId } : {}),
    })
  }
  const body = action.args['body']
  const reply = action.args['replyToClientMsgNo'] ?? work.threadId
  if (typeof body !== 'string' || !body.trim()) throw new Error('message body is required')
  if (reply !== undefined && (typeof reply !== 'string' || !reply.trim())) throw new Error('invalid reply reference')
  return services.wukongClient().sendMessage(work.sessionId, channelType, work.agentId, {
    version: 1, kind: 'text', clientMsgNo: `action-${action.idempotencyKey}`, body,
    ...(reply ? { replyToClientMsgNo: reply } : {}), refs: { runId: work.id, agentId: work.agentId },
  })
}
