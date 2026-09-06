import assert from 'node:assert/strict'
import test from 'node:test'
import { executeHandoff, resolveHandoffIngress } from '../src/integrations/lingxiloop/handoffs.js'
import type { LingxiLoopServices } from '../src/integrations/lingxiloop/service-contracts.js'
import type { HostAction, WorkItem } from '../src/protocol/types.js'

const work = { id: 'work', fence: 1, homeEpoch: 1, tenantId: 'company', agentId: 'source', sessionId: 'room',
  principalId: 'human', triggerRef: 'message', kind: 'turn', lane: 'interactive' } as Omit<WorkItem, 'leaseToken'>
const action = (name: string, args: Record<string, unknown>): HostAction => ({ runId: 'work', cellId: 'cell', callIndex: 0,
  action: name, args, idempotencyKey: 'handoff-key' })
const parentRequest = { version: 1 as const, workId: 'work', tenantId: 'company', sessionId: 'room', authorId: 'human',
  sourceRef: 'message', originalText: 'Only inspect; do not modify.', revisions: [], attachments: [],
  evidence: { version: 1 as const, id: 'work:evidence:1', items: [], capturedAt: 'now' } }

test('handoffs preserve native records while validating human scope and context messages', async () => {
  const calls: unknown[] = []
  const services = { permissionService: { assertCan: async (input: unknown) => { calls.push(input) } }, messaging: {
    missingAgentChannelMessageIds: async (input: unknown) => { calls.push(input); return [] },
  }, handoffs: {
    createHandoff: async (input: unknown) => { calls.push(input); return { id: 'handoff', sourceMessageId: 'message' } },
    updateHandoff: async (input: unknown) => { calls.push(input); return { status: 'completed' } },
    listHandoffs: async () => [],
  } } as unknown as LingxiLoopServices
  assert.deepEqual(await executeHandoff(work, action('handoffs.create', { toAgentId: 'target', title: 'Check result',
    contextMessageIds: ['message-1'], note: 'Verify it.' }), services), { id: 'handoff', sourceMessageId: 'message' })
  assert.deepEqual(calls, [
    { actorUserId: 'human', companyId: 'company', action: 'conversation:write', resource: { type: 'conversation', id: 'room' } },
    { companyId: 'company', agentId: 'source', channelId: 'room', messageIds: ['message-1'] },
    { companyId: 'company', conversationId: 'room', fromAgentId: 'source', toAgentId: 'target', title: 'Check result',
      contextMessageIds: ['message-1'], note: 'Verify it.', idempotencyKey: 'handoff-key' },
  ])
  await assert.rejects(executeHandoff(work, action('handoffs.create', { toAgentId: 'source', title: 'Self' }), services), /another target/)
})

test('handoff ingress restores its human principal only through the native record and source action', async () => {
  const permissions: unknown[] = []
  const services = { permissionService: { assertCan: async (input: unknown) => { permissions.push(input) } }, wukongClient: () => ({
    syncMessages: async () => [{ clientMsgNo: 'handoff:h1:created', messageSeq: 4, channelId: 'room', channelType: 2,
      fromUid: 'source', payload: { version: 1 as const, kind: 'handoff', refs: { handoffId: 'h1', toAgentId: 'target' } } }],
    sendMessage: async () => ({ messageId: 'unused', messageSeq: 1 }),
  }) } as Pick<LingxiLoopServices, 'wukongClient' | 'permissionService'>
  const database = { query: async (_sql: string, params?: readonly unknown[]) => {
    assert.deepEqual(params, ['h1', 'company', 'room', 'target', 'created'])
    return { rows: [{ parent_work_id: 'work', request_version: 1, request_snapshot: parentRequest,
      from_agent_id: 'source', to_agent_id: 'target', title: 'Verify result', note: 'Check carefully',
      context_message_ids: ['m1'], principal_id: 'human', name: 'Human' }], rowCount: 1 }
  } }
  assert.deepEqual(await resolveHandoffIngress(database, services, { companyId: 'company', agentId: 'target', channelId: 'room',
    clientMsgNo: 'handoff:h1:created' }, 2), { handoffId: 'h1', principalId: 'human', authorName: 'Human',
    text: 'Handoff: Verify result\nNote: Check carefully\nContext message IDs: m1', parentWorkId: 'work', rootWorkId: 'work',
    parentRequestVersion: 1, instructionAuthorId: 'source', parentRequest })
  assert.deepEqual(permissions, [{ actorUserId: 'human', companyId: 'company', action: 'conversation:read',
    resource: { type: 'conversation', id: 'room' } }])
  await assert.rejects(resolveHandoffIngress(database, services, { companyId: 'company', agentId: 'other', channelId: 'room',
    clientMsgNo: 'handoff:h1:created' }, 2), /committed handoff/)
})

test('terminal handoff ingress wakes the source with the persisted result', async () => {
  const nonce = `handoff:h1:completed:${'a'.repeat(64)}`
  const services = { permissionService: { assertCan: async () => {} }, wukongClient: () => ({
    syncMessages: async () => [{ clientMsgNo: nonce, messageSeq: 5, channelId: 'room', channelType: 2,
      fromUid: 'target', payload: { version: 1 as const, kind: 'handoff', refs: { handoffId: 'h1', toAgentId: 'source' } } }],
    sendMessage: async () => ({ messageId: 'unused', messageSeq: 1 }),
  }) } as Pick<LingxiLoopServices, 'wukongClient' | 'permissionService'>
  const database = { query: async (_sql: string, params?: readonly unknown[]) => {
    assert.deepEqual(params, ['h1', 'company', 'room', 'source', 'completed'])
    return { rows: [{ parent_work_id: 'work', request_version: 1, request_snapshot: parentRequest,
      from_agent_id: 'source', to_agent_id: 'target', title: 'Verify result', note: 'Looks good', status: 'completed',
      context_message_ids: ['m1'], principal_id: 'human', name: 'Human' }], rowCount: 1 }
  } }
  assert.equal((await resolveHandoffIngress(database, services, { companyId: 'company', agentId: 'source', channelId: 'room',
    clientMsgNo: nonce }, 2)).text, 'Completed handoff: Verify result\nNote: Looks good')
  await assert.rejects(resolveHandoffIngress(database, services, { companyId: 'company', agentId: 'source', channelId: 'room',
    clientMsgNo: 'handoff:h1:completed' }, 2), /invalid handoff ingress/)
})
