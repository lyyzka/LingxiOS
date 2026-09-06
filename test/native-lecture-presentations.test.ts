import assert from 'node:assert/strict'
import test from 'node:test'
import { createNativePresentationBridge } from '../src/integrations/lingxiloop/native-presentations.js'
import type { LectureDeckService } from '../src/lecture-deck/service.js'

test('LingxiLoop presentation actions use the package-owned lecture queue when no legacy service exists', async () => {
  const queued: unknown[] = [], operations: unknown[] = []
  const app = {
    async enqueueLecture(input: unknown) { queued.push(input); return { id: 'work', deckId: 'deck', revision: 1, status: 'planning' } },
    async enqueueLectureOperation(input: unknown) { operations.push(input); return { id: 'revision-work', deckId: 'deck', revision: 1, status: 'ready' } },
  }
  const record = { id: 'deck', tenantId: 'tenant', principalId: 'human', revision: 1, status: 'ready', request: { requirements: 'Teach', targetSlideCount: 3, durationMinutes: 10 } }
  const service = { get: async () => record, cancel: async () => ({ ...record, revision: 2, status: 'cancelled' }) } as unknown as LectureDeckService
  const bridge = createNativePresentationBridge(app, service)
  const work = { id: 'turn', fence: 1, companyId: 'tenant', authorizationUserId: 'human', agentId: 'agent', channelId: 'room', triggerClientMsgNo: 'message', reason: 'message' as const, executionRole: 'coordinator' as const, lane: 'learner' as const, leaseToken: 'token' }
  const created = await bridge.createPresentationForAgent(work, bridge.createPresentationRequestSchema.parse({ idempotencyKey: 'stable', requirements: 'Teach', targetSlideCount: 3 })) as { id: string }
  assert.equal(created.id, 'deck')
  assert.equal((queued[0] as { request: Record<string, unknown> }).request['idempotencyKey'], undefined)
  await bridge.revisePresentationForAgent(work, 'deck', bridge.revisePresentationRequestSchema.parse({ idempotencyKey: 'revise', instruction: 'Change page', scope: 'page', pageIds: ['pg_1'] }))
  assert.deepEqual((operations[0] as { request: unknown }).request, { instruction: 'Change page', scope: 'page', expectedRevision: 1, pageIds: ['pg_1'] })
  await assert.rejects(async () => bridge.createPresentationRequestSchema.parse({ idempotencyKey: 'x', requirements: 'x', targetSlideCount: 101 }), /targetSlideCount/)
})
