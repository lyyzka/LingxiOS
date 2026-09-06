import assert from 'node:assert/strict'
import test from 'node:test'
import { executeChat } from '../src/integrations/lingxiloop/chat.js'
import type { HostAction, WorkItem } from '../src/protocol/types.js'
import type { LingxiLoopServices } from '../src/integrations/lingxiloop/service-contracts.js'

const work = { id: 'w', fence: 1, homeEpoch: 1, tenantId: 'company', agentId: 'agent', sessionId: 'room', principalId: 'human',
  triggerRef: 'message', kind: 'turn', lane: 'interactive' } as Omit<WorkItem, 'leaseToken'>
const action = (name: string, args: Record<string, unknown> = {}): HostAction => ({ runId: 'w', cellId: 'c', callIndex: 0, action: name, args, idempotencyKey: name })

test('conversation controls stay in the current agent room and require human authorization', async () => {
  const calls: unknown[] = []
  const services = { permissionService: { assertCan: async (input: unknown) => calls.push(input) }, conversations: {
    getAgentConversationMetadata: async (...args: unknown[]) => { calls.push(args); return { title: 'Room' } },
    addAgentConversationMember: async () => ({}), setAgentConversationTopic: async () => ({}),
    setAgentConversationTitle: async (...args: unknown[]) => { calls.push(args); return { title: 'New' } },
    listAgentConversationMutes: async () => [], setAgentConversationMuted: async () => ({}),
  }, messaging: {
    getAgentChannelHistory: async () => [],
    sendAgentChannelMessage: async (input: unknown) => { calls.push(input); return { kind: 'accepted' as const, duplicate: false, messageId: 'sent', sequence: 2 } },
    getAgentInbox: async (input: unknown) => { calls.push(input); return [{ channelId: 'other-room', unread: 2 }] },
    clearAgentChannelUnread: async (input: unknown) => { calls.push(input); return true },
    searchAgentMessages: async (input: unknown) => { calls.push(input); return [{ id: 'message' }] },
    toggleAgentChannelReaction: async (input: unknown) => { calls.push(input); return { kind: 'updated' as const, reactions: [{ emoji: '👍', count: 1, users: ['agent'] }] } },
  }, wukongClient: () => ({ syncMessages: async () => [], sendMessage: async () => ({ messageId: 'm', messageSeq: 1 }) }) } as unknown as LingxiLoopServices
  assert.deepEqual(await executeChat(work, action('chat.metadata'), services, 2), { title: 'Room' })
  assert.deepEqual(await executeChat(work, action('chat.rename', { title: 'New', expectedTitle: 'Room' }), services, 2), { title: 'New' })
  assert.deepEqual(await executeChat(work, action('chat.inbox', { limit: 5 }), services, 2), [{ channelId: 'other-room', unread: 2 }])
  assert.deepEqual(await executeChat(work, action('chat.ack'), services, 2), { ok: true })
  assert.equal((await executeChat(work, action('chat.send', { body: 'Native message' }), services, 2) as { kind: string }).kind, 'accepted')
  assert.deepEqual(await executeChat(work, action('chat.search', { query: 'decision', limit: 5 }), services, 2), [{ id: 'message' }])
  assert.equal((await executeChat(work, action('chat.react', { messageId: 'message', emoji: '👍' }), services, 2) as { kind: string }).kind, 'updated')
  assert.deepEqual(calls, [
    { actorUserId: 'human', companyId: 'company', action: 'conversation:read', resource: { type: 'conversation', id: 'room' } }, ['agent', 'room'],
    { actorUserId: 'human', companyId: 'company', action: 'conversation:manage', resource: { type: 'conversation', id: 'room' } }, ['agent', 'room', 'New', 'Room'],
    { actorUserId: 'human', companyId: 'company', action: 'conversation:read', resource: { type: 'conversation', id: 'room' } },
    { companyId: 'company', agentId: 'agent', limit: 5 },
    { actorUserId: 'human', companyId: 'company', action: 'conversation:read', resource: { type: 'conversation', id: 'other-room' } },
    { actorUserId: 'human', companyId: 'company', action: 'conversation:write', resource: { type: 'conversation', id: 'room' } },
    { companyId: 'company', agentId: 'agent', channelId: 'room' },
    { actorUserId: 'human', companyId: 'company', action: 'conversation:write', resource: { type: 'conversation', id: 'room' } },
    { companyId: 'company', agentId: 'agent', channelId: 'room', clientNonce: 'action-chat.send', payload: {
      version: 1, kind: 'text', clientMsgNo: 'action-chat.send', body: 'Native message', refs: { runId: 'w', agentId: 'agent' },
    } },
    { actorUserId: 'human', companyId: 'company', action: 'conversation:read', resource: { type: 'conversation', id: 'room' } },
    { companyId: 'company', agentId: 'agent', channelId: 'room', query: 'decision', limit: 5 },
    { actorUserId: 'human', companyId: 'company', action: 'conversation:write', resource: { type: 'conversation', id: 'room' } },
    { companyId: 'company', agentId: 'agent', channelId: 'room', messageId: 'message', emoji: '👍' },
  ])
  await assert.rejects(executeChat(work, action('chat.set_muted', { muted: true, until: 'not-a-date' }), services, 2), /ISO timestamp/)
})
