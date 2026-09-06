import assert from 'node:assert/strict'
import test from 'node:test'
import { enrichLingxiLoopContext } from '../src/integrations/lingxiloop/context.js'
import { deliverLingxiLoopEvent } from '../src/integrations/lingxiloop/delivery.js'
import { sweepLingxiLoopWatchdog } from '../src/integrations/lingxiloop/watchdog.js'
import type { SqlPool } from '../src/control-plane/pg-store.js'
import type { LingxiLoopServices } from '../src/integrations/lingxiloop/service-contracts.js'
import type { WorkItem } from '../src/protocol/types.js'

const work = { id: 'w', fence: 1, homeEpoch: 1, tenantId: 't', agentId: 'a', sessionId: 's', principalId: 'u',
  kind: 'turn', lane: 'interactive', triggerRef: 'm' } as Omit<WorkItem, 'leaseToken'>

test('LingxiLoop context syncs history, retrieval, learning, Canvas and approvals', async () => {
  let readThrough = 0
  const database = { query: async (sql: string) => sql.includes('knowledge_sources') ? { rows: [{ id: 'source', updated_at: 'v2' }] }
    : { rows: [{ id: 'approval', status: 'EXECUTED', result: { ok: true }, error: null }] } } as unknown as SqlPool
  const services = {
    wukongClient: () => ({ syncMessages: async () => [{ clientMsgNo: 'm', messageSeq: 7, timestamp: 1_700_000_000,
      channelId: 's', channelType: 2, fromUid: 'u', payload: { version: 1 as const, kind: 'text', body: 'question', data: { authorName: 'User' } } }], sendMessage: async () => ({ messageId: 'x', messageSeq: 1 }) }),
    advanceAgentReadReceipt: async ({ readThroughSeq }: { readThroughSeq: number }) => { readThrough = readThroughSeq },
    retrieveKnowledge: async () => [{ marker: 'S1', sourceId: 'source', sourceTitle: 'Title', chunkId: 'chunk', excerpt: 'Evidence' }],
    learning: { loadLearningTurnContext: async () => ({ project: { id: 'p' }, knowledgeUnits: [], due: [] }) },
    canvas: { getConversationCanvas: async () => ({ id: 'canvas' }), listCanvasAvailableAgents: async () => [{ id: 'peer' }] },
  } as unknown as LingxiLoopServices
  const result = await enrichLingxiLoopContext(database, { ...work, kind: 'resume', triggerRef: 'approval:approval' }, services, 2, {
    persona: { name: 'A', role: 'agent', instructions: '' }, capabilities: ['knowledge'],
    messages: [{ ref: 'fallback', authorId: 'u', authorName: 'U', authorKind: 'human', body: 'fallback', createdAt: '' }],
  })
  assert.equal(readThrough, 7)
  assert.equal(result.evidence?.[0]?.sourceVersion, 'v2')
  assert.equal(result.pendingApproval?.approved, true)
  assert.deepEqual(result.dynamic?.['product'], { knowledgeContext: [{ marker: 'S1', sourceId: 'source', sourceTitle: 'Title', chunkId: 'chunk', excerpt: 'Evidence' }], learnerId: 'u', learningContext: { project: { id: 'p' }, knowledgeUnits: [], due: [] }, canvas: { id: 'canvas' }, canvasRoster: [{ id: 'peer' }], contextDurationMs: result.dynamic?.['product'] && (result.dynamic['product'] as Record<string, unknown>)['contextDurationMs'] })
})

test('LingxiLoop delivery emits native stream chunks', async () => {
  const published: Array<Record<string, unknown>> = []
  const services = { publishAssistantStream: async (event: { chunks: Array<Record<string, unknown>> }) => { published.push(...event.chunks) } } as unknown as LingxiLoopServices
  await deliverLingxiLoopEvent({} as SqlPool, services, work, { runId: 'w', seq: 2, kind: 'model.delta', stage: 'delta', visibility: 'user',
    data: { delta: 'hello', partIndex: 0, partType: 'text', partStart: true } }, 2)
  assert.deepEqual(published.map(chunk => chunk['type']), ['part-start', 'text-delta'])
})

test('LingxiLoop watchdog trips and force-fences stale work', async () => {
  const calls: string[] = []
  const client = { query: async (sql: string) => { calls.push(sql); return sql.startsWith('UPDATE') ? { rows: [{ id: 'active' }] } : { rows: [] } }, release() {} }
  const database = { query: async () => ({ rows: [{ id: 'active' }] }), connect: async () => client } as unknown as SqlPool
  assert.deepEqual(await sweepLingxiLoopWatchdog(database, new Date(), 1_000, 1_000), { tripped: 1, fenced: 1 })
  assert.equal(calls.some(sql => sql.includes('agent_os_session_leases')), true)
})
