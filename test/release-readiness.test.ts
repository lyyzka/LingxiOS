import { durableProtocol } from './protocol-fixture.js'
import assert from 'node:assert/strict'
import test from 'node:test'
import { readFile } from 'node:fs/promises'
import { PGlite } from '@electric-sql/pglite'
import { checkStorage } from '../src/app/storage.js'
import { MemoryWorkStore, MemoryModelBudgetStore } from '../src/control-plane/memory-store.js'
import { AgentRuntime } from '../src/runtime/runtime.js'
import type { HostPort } from '../src/host/port.js'
import type { WorkCompletion, WorkItem } from '../src/protocol/types.js'

test('fresh schema checks reject missing execution tables and removed constraints', async () => {
  const db = new PGlite()
  const database = { async query(sql: string, values?: unknown[]) { const result = await db.query(sql, values); return { rows: result.rows as Record<string, unknown>[], rowCount: result.affectedRows ?? result.rows.length } } }
  try {
    await db.exec(await readFile(new URL('../../db/schema.sql', import.meta.url), 'utf8'))
    await checkStorage(database)
    await db.exec('ALTER TABLE lingxios.agent_work_items DROP CONSTRAINT agent_work_items_status_check')
    await assert.rejects(checkStorage(database), /required constraint is missing/)
    await db.exec('DROP TABLE lingxios.agent_steps')
    await assert.rejects(checkStorage(database), /agent_steps/)
  } finally { await db.close() }
})

test('claims filter unsupported work and bind retries to the normalized type set', async () => {
  const store = new MemoryWorkStore()
  const common = { tenantId: 't', principalId: 'p', agentId: 'a', lane: 'interactive' as const, triggerRef: 'm' }
  await store.enqueue({ ...common, id: 'custom', sessionId: 'custom', kind: 'custom_job' })
  await store.enqueue({ ...common, id: 'turn', sessionId: 'chat', kind: 'turn' })
  const work = await store.claim('worker', 'request1', ['resume','turn'])
  assert.equal(work?.id, 'turn')
  assert.deepEqual(await store.claim('worker', 'request1', ['turn','resume','turn']), work)
  await assert.rejects(store.claim('worker', 'request1', ['custom_job']), /task types changed/)
  assert.equal(await store.claim('worker', undefined, ['turn']), null)
})

test('processor structured and compaction calls share durable budgets across attempts without an observer', async () => {
  const budgets = new MemoryModelBudgetStore(), completions: WorkCompletion[] = []
  const work: WorkItem = { id: 'w', tenantId: 't', principalId: 'p', agentId: 'a', sessionId: 's', kind: 'test_processor', lane: 'background', triggerRef: 'm', fence: 1, homeEpoch: 1, leaseToken: 'token' }
  const host: HostPort = { ...durableProtocol(),
    claimWork: async () => null, heartbeat: async () => ({ ok: true }), loadContext: async () => { throw new Error('unused') },
    executeAction: async () => ({ ok: true }), loadSession: async () => null, saveSession: async () => {}, emitEvent: async () => {},
    commitResult: async () => {}, completeWork: async (_work, value) => { completions.push(value) }, yieldWork: async () => {},
    reserveModelCall: (item, callId, limits) => budgets.reserve(item.id, callId, limits),
    recordModelUsage: (item, callId, usage) => budgets.record(item.id, callId, usage.inputTokens, usage.outputTokens, usage.costMicros),
  }
  let calls = 0
  const model = { structured: async () => { calls++; return { value: {}, model: 'm', usage: { available: true, inputTokens: 10, outputTokens: 5 } } },
    compact: async () => { calls++; return { value: 'summary', model: 'm', usage: { available: true, inputTokens: 10, outputTokens: 5 } } },
    run: async () => { throw new Error('unused') } }
  const runtime = new AgentRuntime(host, model, { execute: async () => { throw new Error('unused') } }, { rootModelBudget: { maxModelCalls: 2 } })
  runtime.registerProcessor('test_processor', { async process(_item, context) {
    await context.model.structured({ instructions: 'first', input: {} })
    await context.model.compact({ instructions: 'second', items: [] })
  } })
  await runtime.runWork(work)
  assert.equal(completions[0]?.status, 'completed')
  await runtime.runWork({ ...work, fence: 2 })
  assert.equal(calls, 2)
  assert.match(completions[1]?.error ?? '', /budget exhausted/)
})
