import assert from 'node:assert/strict'
import test from 'node:test'
import { executeDirectory } from '../src/integrations/lingxiloop/directory.js'
import type { HostAction, WorkItem } from '../src/protocol/types.js'
import type { LingxiLoopServices } from '../src/integrations/lingxiloop/service-contracts.js'

const work = { id: 'w', fence: 1, homeEpoch: 1, tenantId: 'company', agentId: 'agent', sessionId: 'room', principalId: 'human',
  triggerRef: 'message', kind: 'turn', lane: 'interactive' } as Omit<WorkItem, 'leaseToken'>
const action = (name: string, args: Record<string, unknown> = {}): HostAction => ({ runId: 'w', cellId: 'c', callIndex: 0, action: name, args, idempotencyKey: name })

test('directory discovery authorizes the human and derives the agent tenant natively', async () => {
  const calls: unknown[] = []
  const services = { permissionService: { assertCan: async (input: unknown) => calls.push(input) }, directory: {
    getAgentCliIdentity: async () => ({}), listAgentCliStatuses: async () => [],
    listAgentCliParticipants: async (...args: unknown[]) => { calls.push(args); return [{ id: 'peer' }] },
  } } as unknown as LingxiLoopServices
  assert.deepEqual(await executeDirectory(work, action('directory.participants', { kind: 'agent' }), services), [{ id: 'peer' }])
  assert.deepEqual(calls, [
    { actorUserId: 'human', companyId: 'company', action: 'agent:read', resource: { type: 'agent', id: 'agent' } },
    ['agent', 'agent'],
  ])
  await assert.rejects(executeDirectory(work, action('directory.participants', { kind: 'company' }), services), /agent or human/)
})

test('directory self reauthorizes every native conversation for the original human', async () => {
  const calls: unknown[] = []
  const services = { permissionService: { assertCan: async (input: unknown) => calls.push(input) }, directory: {
    getAgentCliIdentity: async () => ({ identity: { id: 'agent' }, conversations: [{ id: 'room' }, { id: 'other' }] }),
    listAgentCliStatuses: async () => [], listAgentCliParticipants: async () => [],
  } } as unknown as LingxiLoopServices
  await executeDirectory(work, action('directory.self'), services)
  assert.deepEqual(calls.slice(1), ['room', 'other'].map(id => ({ actorUserId: 'human', companyId: 'company',
    action: 'conversation:read', resource: { type: 'conversation', id } })))
})
