import assert from 'node:assert/strict'
import test from 'node:test'
import { preparePresentationApproval } from '../src/integrations/lingxiloop/presentation-approvals.js'
import type { LingxiLoopServices } from '../src/integrations/lingxiloop/service-contracts.js'
import type { HostAction, WorkItem } from '../src/protocol/types.js'

test('presentation outline approval binds the reviewed revision and reauthorizes the approver', async () => {
  const calls: unknown[] = []
  let revision = 2
  const services = { permissionService: { assertCan: async (request: unknown) => { calls.push(request) } }, presentations: {
    approvePresentationOutlineRequestSchema: { parse: (input: unknown) => input as { expectedRevision: number; idempotencyKey: string } },
    getPresentationForAgent: async () => ({ id: 'deck', title: 'Deck', status: 'awaitingOutlineApproval', outlineRevision: revision,
      outline: { pages: [{ title: 'One' }] } }),
    approvePresentationOutlineForAgent: async (_work: unknown, id: string, input: unknown) => { calls.push([id, input]); return { id, status: 'generating' } },
  } } as unknown as Pick<LingxiLoopServices, 'presentations' | 'permissionService'>
  const work = { id: 'work', fence: 1, homeEpoch: 1, tenantId: 'company', agentId: 'agent', sessionId: 'room',
    principalId: 'author', triggerRef: 'message', kind: 'turn', lane: 'interactive' } as Omit<WorkItem, 'leaseToken'>
  const action = { runId: 'work', cellId: 'cell', callIndex: 0, action: 'presentations.approve_outline',
    args: { presentationId: 'deck', expectedRevision: 2 }, idempotencyKey: 'key' } satisfies HostAction
  const prepared = await preparePresentationApproval(services, work, action, 'reviewer')
  assert.deepEqual(prepared.preview, { presentationId: 'deck', expectedRevision: 2, title: 'Deck', outline: { pages: [{ title: 'One' }] } })
  assert.deepEqual(await prepared.execute(), { id: 'deck', status: 'generating' })
  assert.deepEqual(calls, [
    { actorUserId: 'author', companyId: 'company', action: 'knowledge:write', resource: { type: 'conversation', id: 'room' } },
    { actorUserId: 'reviewer', companyId: 'company', action: 'knowledge:write', resource: { type: 'conversation', id: 'room' } },
    ['deck', { expectedRevision: 2, idempotencyKey: 'key' }],
  ])
  revision = 3
  await assert.rejects(preparePresentationApproval(services, work, action), /outline changed/)
})
