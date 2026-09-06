import type { HostAction, WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices } from './service-contracts.js'
import { questionnaire } from './questionnaire.js'

export const CHAT_METHODS = {
  metadata: [], history: ['limit', 'beforeSequence'], send: ['body', 'replyToClientMsgNo'], ask: ['title', 'items', 'submitLabel'],
  inbox: ['limit'], ack: [], search: ['query', 'limit'], react: ['messageId', 'emoji'],
  add_member: ['participantId'], set_topic: ['topic'], rename: ['title', 'expectedTitle'], list_mutes: [], set_muted: ['muted', 'until'],
} as const

export async function executeChat(work: Omit<WorkItem, 'leaseToken'>, action: HostAction, services: LingxiLoopServices, channelType: number) {
  const method = action.action.slice('chat.'.length)
  if (!work.principalId || !action.action.startsWith('chat.') || !Object.hasOwn(CHAT_METHODS, method)) throw new Error('unsupported chat action or missing principal')
  const allowed: readonly string[] = CHAT_METHODS[method as keyof typeof CHAT_METHODS]
  if (Object.keys(action.args).some(key => !allowed.includes(key))) throw new Error('unknown chat argument')
  await services.permissionService.assertCan({ actorUserId: work.principalId, companyId: work.tenantId,
    action: ['history', 'metadata', 'list_mutes', 'inbox', 'search'].includes(method) ? 'conversation:read'
      : ['add_member', 'set_topic', 'rename'].includes(method) ? 'conversation:manage' : 'conversation:write', resource: { type: 'conversation', id: work.sessionId },
  })
  if (method === 'metadata') {
    if (!services.conversations) throw new Error('native conversation metadata is unavailable')
    return services.conversations.getAgentConversationMetadata(work.agentId, work.sessionId)
  }
  if (method === 'list_mutes') {
    if (!services.conversations) throw new Error('native conversation controls are unavailable')
    return services.conversations.listAgentConversationMutes(work.agentId)
  }
  if (method === 'inbox') {
    const limit = action.args['limit'] ?? 50
    if (!services.messaging || !Number.isInteger(limit) || Number(limit) < 1 || Number(limit) > 50) throw new Error('chat inbox requires native messaging and limit 1..50')
    const items = await services.messaging.getAgentInbox({ companyId: work.tenantId, agentId: work.agentId, limit: Number(limit) })
    for (const item of items) {
      if (typeof item.channelId !== 'string' || !item.channelId) throw new Error('invalid native inbox conversation')
      await services.permissionService.assertCan({ actorUserId: work.principalId, companyId: work.tenantId,
        action: 'conversation:read', resource: { type: 'conversation', id: item.channelId } })
    }
    return items
  }
  if (method === 'ack') {
    if (!services.messaging || !await services.messaging.clearAgentChannelUnread({ companyId: work.tenantId, agentId: work.agentId, channelId: work.sessionId })) {
      throw new Error('native conversation unread state is unavailable')
    }
    return { ok: true }
  }
  if (method === 'search') {
    const query = action.args['query'], limit = action.args['limit'] ?? 10
    if (!services.messaging || typeof query !== 'string' || !query.trim() || query.length > 200 || !Number.isInteger(limit) || Number(limit) < 1 || Number(limit) > 50) throw new Error('chat search requires native messaging, query of 1..200 characters and limit 1..50')
    return services.messaging.searchAgentMessages({ companyId: work.tenantId, agentId: work.agentId, channelId: work.sessionId, query, limit: Number(limit) })
  }
  if (method === 'react') {
    const messageId = action.args['messageId'], emoji = action.args['emoji']
    if (!services.messaging || typeof messageId !== 'string' || !messageId.trim() || typeof emoji !== 'string' || !emoji.trim() || emoji.length > 64) throw new Error('chat react requires native messaging, messageId and emoji')
    const result = await services.messaging.toggleAgentChannelReaction({ companyId: work.tenantId, agentId: work.agentId, channelId: work.sessionId, messageId, emoji })
    if (result.kind !== 'updated') throw new Error(result.kind === 'message_not_found' ? 'reaction message not found' : 'reaction conversation not found')
    return result
  }
  if (['add_member', 'set_topic', 'rename', 'set_muted'].includes(method)) {
    const api = services.conversations
    if (!api) throw new Error('native conversation controls are unavailable')
    if (method === 'add_member') {
      const participantId = action.args['participantId']
      if (typeof participantId !== 'string' || !participantId.trim() || participantId.length > 200) throw new Error('participantId must contain 1..200 characters')
      return api.addAgentConversationMember(work.agentId, work.sessionId, participantId)
    }
    if (method === 'set_topic') {
      const topic = action.args['topic']
      if (topic !== null && (typeof topic !== 'string' || topic.length > 200)) throw new Error('topic must be null or at most 200 characters')
      return api.setAgentConversationTopic(work.agentId, work.sessionId, topic)
    }
    if (method === 'rename') {
      const title = action.args['title'], expected = action.args['expectedTitle']
      if (typeof title !== 'string' || !title.trim() || title.length > 80 || expected !== undefined && typeof expected !== 'string') throw new Error('rename requires title of 1..80 characters and optional expectedTitle')
      return api.setAgentConversationTitle(work.agentId, work.sessionId, title, expected)
    }
    const muted = action.args['muted'], until = action.args['until']
    if (typeof muted !== 'boolean' || until !== undefined && until !== null && (typeof until !== 'string' || !Number.isFinite(Date.parse(until)))) throw new Error('set_muted requires muted boolean and optional ISO timestamp')
    return api.setAgentConversationMuted(work.agentId, work.sessionId, muted, typeof until === 'string' ? new Date(until) : null)
  }
  if (method === 'history') {
    if (!services.advanceAgentReadReceipt) throw new Error('native read receipt resource is required')
    const limit = action.args['limit'] ?? 50
    if (typeof limit !== 'number' || !Number.isInteger(limit) || limit < 1 || limit > 100) throw new Error('history limit must be an integer from 1 to 100')
    const beforeSequence = action.args['beforeSequence'] ?? 0
    if (!Number.isSafeInteger(beforeSequence) || Number(beforeSequence) < 0) throw new Error('beforeSequence must be a non-negative safe integer')
    const messages = services.messaging ? await services.messaging.getAgentChannelHistory({ companyId: work.tenantId, agentId: work.agentId,
      channelId: work.sessionId, limit, beforeSequence: Number(beforeSequence) }) : await services.wukongClient().syncMessages(work.sessionId, channelType, limit, work.agentId)
    if (!messages || messages.some(message => message.channelId !== work.sessionId || !Number.isSafeInteger(message.messageSeq) || message.messageSeq < 0)) throw new Error('invalid native history scope or sequence')
    const readThroughSeq = messages.reduce((max, message) => Math.max(max, message.messageSeq), 0)
    if (readThroughSeq > 0) await services.advanceAgentReadReceipt({ companyId: work.tenantId, channelId: work.sessionId, agentId: work.agentId, readThroughSeq })
    return messages
  }
  if (method === 'ask') {
    const form = questionnaire(action.args)
    const payload = {
      version: 1, kind: 'questionnaire', clientMsgNo: `questionnaire-${action.idempotencyKey}`, body: form.title,
      refs: { runId: work.id, agentId: work.agentId }, data: { questionnaire: form },
      ...(work.threadId ? { replyToClientMsgNo: work.threadId } : {}),
    } as const
    if (!services.messaging) return services.wukongClient().sendMessage(work.sessionId, channelType, work.agentId, payload)
    const result = await services.messaging.sendAgentChannelMessage({ companyId: work.tenantId, agentId: work.agentId,
      channelId: work.sessionId, clientNonce: payload.clientMsgNo, payload })
    if (result.kind !== 'accepted') throw new Error(`native questionnaire send failed: ${result.kind}`)
    return result
  }
  const body = action.args['body']
  const reply = action.args['replyToClientMsgNo'] ?? work.threadId
  if (typeof body !== 'string' || !body.trim()) throw new Error('message body is required')
  if (reply !== undefined && (typeof reply !== 'string' || !reply.trim())) throw new Error('invalid reply reference')
  const payload = {
    version: 1, kind: 'text', clientMsgNo: `action-${action.idempotencyKey}`, body,
    ...(reply ? { replyToClientMsgNo: reply } : {}), refs: { runId: work.id, agentId: work.agentId },
  } as const
  if (!services.messaging) return services.wukongClient().sendMessage(work.sessionId, channelType, work.agentId, payload)
  const result = await services.messaging.sendAgentChannelMessage({ companyId: work.tenantId, agentId: work.agentId,
    channelId: work.sessionId, clientNonce: payload.clientMsgNo, payload })
  if (result.kind !== 'accepted') throw new Error(`native message send failed: ${result.kind}`)
  return result
}
