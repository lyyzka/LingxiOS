import { durableProtocol } from './protocol-fixture.js'
import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { it } from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import { sweepQueuedWork } from '../src/control-plane/scheduler.js'
import type { SqlPool } from '../src/control-plane/pg-store.js'
import { AgentRuntime } from '../src/runtime/runtime.js'
import type { HostPort } from '../src/host/port.js'
import type { AssistantMessage, WorkItem } from '../src/protocol/types.js'
import type { ToolDefinition } from '../src/tools/catalog.js'
import { setTimeout as delay } from 'node:timers/promises'
import { executionModel, DEFAULT_MODEL_BUDGET } from '../src/model/execution.js'
import { ModelDriverError } from '../src/errors.js'
import type { ModelDriver } from '../src/model/driver.js'
import { DEFAULT_MODEL, DEFAULT_SMALL_MODEL } from '../src/model/openai.js'

it('routes small work without splitting root budgets or assigning approval decisions to a model', async () => {
  for (const kind of ['turn', 'resume', 'memory_synthesis']) {
    const work: WorkItem = { id: kind, tenantId: 't', agentId: 'a', sessionId: 's', kind,
      lane: kind === 'resume' ? 'approval' : 'interactive', triggerRef: 'm', fence: 1, homeEpoch: 1, leaseToken: 'token' }
    const calls: string[] = [], reservations: Array<number | undefined> = [], settled: string[] = []
    const usage = { available: true, inputTokens: 1, outputTokens: 1 }
    const driver = (modelId: string, maxOutputTokens: number): ModelDriver => ({ modelId, maxOutputTokens,
      run: async () => { calls.push(modelId + ':run'); return { text: 'Recorded result.', output: [], model: modelId, usage } },
      structured: async () => { calls.push(modelId + ':review'); return { value: {}, model: modelId, usage } },
      compact: async () => { calls.push(modelId + ':compact'); return { value: '{}', model: modelId, usage } } })
    const model = executionModel({ reserveModelCall: async (_work, _id, limits) => {
      reservations.push(limits.reservedOutputTokens)
      return { allowed: true, remainingCalls: 9, remainingTokens: 99999, remainingCostMicros: 99999, deadlineAt: new Date(Date.now() + 5000).toISOString() }
    }, recordModelUsage: async (_work, id, _usage, observation) => { settled.push(id + ':' + observation?.model) } },
    driver(DEFAULT_MODEL.id, 8192), work, { ...DEFAULT_MODEL_BUDGET, maxModelCalls: 3 }, undefined, driver(DEFAULT_SMALL_MODEL.id, 2048))
    await model.run({ instructions: '', items: [] })
    await model.structured({ instructions: '', input: {} })
    await model.compact({ instructions: '', items: [] })
    await assert.rejects(model.run({ instructions: '', items: [] }), /model budget exhausted/)
    const turnModel = kind === 'turn' ? DEFAULT_MODEL.id : DEFAULT_SMALL_MODEL.id
    const reviewModel = kind === 'memory_synthesis' ? DEFAULT_SMALL_MODEL.id : DEFAULT_MODEL.id
    assert.deepEqual(calls, [turnModel + ':run', reviewModel + ':review', DEFAULT_SMALL_MODEL.id + ':compact'])
    assert.deepEqual(reservations, [kind === 'turn' ? 8192 : 2048, kind === 'memory_synthesis' ? 2048 : 8192, 2048])
    assert.deepEqual(settled, [`${kind}:1:model:1:${turnModel}`, `${kind}:1:model:2:${reviewModel}`, `${kind}:1:model:3:${DEFAULT_SMALL_MODEL.id}`])
  }
})

it('continues beyond twelve steps, bounds concurrent reads, and stops an unstarted write at approval', async () => {
  const work: WorkItem = { id: 'long', tenantId: 't', agentId: 'a', sessionId: 's', principalId: 'u', kind: 'turn',
    lane: 'interactive', triggerRef: 'm', fence: 1, homeEpoch: 1, leaseToken: 'token' }
  const tools: ToolDefinition[] = ['read','write'].map(method => ({ name: `data__${method}`, action: `data.${method}`,
    description: method, parameters: { type: 'object', properties: { [method === 'read' ? 'query' : 'body']: { type: 'string' } }, additionalProperties: false },
    effect: method === 'read' ? 'read' : 'uncertain', approval: method === 'write' }))
  let calls = 0, active = 0, peak = 0, reads = 0, writes = 0, message: AssistantMessage | undefined
  const stepIds = new Set<string>()
  const host: HostPort = { ...durableProtocol(), claimWork: async () => null, heartbeat: async () => ({ ok: true }),
    loadContext: async () => ({ work, persona: { name: 'A', role: '', instructions: '' }, capabilities: ['data'], tools,
      messages: [{ ref: 'm', authorId: 'u', authorName: 'U', authorKind: 'human', body: 'Read the requested data.', createdAt: 'now' }] }),
    loadSession: async () => null, saveSession: async () => {}, emitEvent: async () => {}, yieldWork: async () => {},
    saveStep: async (_work, step) => { if (step.kind === 'data__read') stepIds.add(step.id) },
    executeAction: async (_work, action) => {
      if (action.action === 'task.inspect') return { ok: true, value: { requestVersion: 1, pending: [], truncated: false } }
      assert.match(action.cellId, /^step:/)
      assert.notEqual(action.cellId, 'provider-call')
      if (action.action === 'data.write') { writes++; return { ok: false, approval: { id: 'approval', status: 'PENDING' } } }
      active++; peak = Math.max(peak, active); await delay(1); active--; reads++
      return { ok: true, value: { query: action.args['query'] } }
    }, commitResult: async (_work, value) => { message = value }, completeWork: async (_work, value) => {
      assert.equal(value.goalOutcome?.status, 'awaiting_approval')
    } }
  const unexpected = async (): Promise<never> => { throw new Error('unexpected auxiliary call') }
  await new AgentRuntime(host, { structured: unexpected, compact: unexpected, run: async () => {
    calls++
    return calls <= 15 ? { text: '', output: Array.from({ length: 7 }, (_, i) => ({ type: 'function_call' as const,
      callId: `provider-${i}`, name: 'data__read', arguments: JSON.stringify({ query: `${calls}:${i}` }) })), usage: { available: true, inputTokens: 1, outputTokens: 1 } }
      : { text: 'Done.', output: [], usage: { available: true, inputTokens: 1, outputTokens: 1 } }
  } }, { execute: unexpected }).runWork(work)
  assert.deepEqual({ calls, reads, peak, steps: stepIds.size, body: message?.body }, { calls: 16, reads: 105, peak: 4, steps: 105, body: 'Done.' })
  await new AgentRuntime(host, { structured: unexpected, compact: unexpected, run: async () => ({ text: '',
    output: [0, 1].map(i => ({ type: 'function_call' as const, callId: `write-${i}`, name: 'data__write', arguments: '{"body":"save"}' })),
    usage: { available: true, inputTokens: 1, outputTokens: 1 } }) }, { execute: unexpected }).runWork(work)
  assert.equal(writes, 1)
})

it('reserves each provider attempt and settles an issued call after cancellation', async () => {
  const work: WorkItem = { id: 'usage', tenantId: 't', agentId: 'a', sessionId: 's', threadId: '', principalId: 'u', kind: 'turn',
    lane: 'interactive', triggerRef: 'm', fence: 2, homeEpoch: 1, leaseToken: 'token' }
  const controller = new AbortController(), reserved: string[] = [], settled: string[] = []
  let requests = 0
  const model = executionModel({ reserveModelCall: async (_work, id) => {
    reserved.push(id); return { allowed: true, remainingCalls: 9, remainingTokens: 99999, remainingCostMicros: 99999, deadlineAt: new Date(Date.now() + 5000).toISOString() }
  }, recordModelUsage: async (_work, id, usage, observation) => {
    settled.push(id); assert.equal(observation?.threadId, '')
    if (requests === 2) { assert.equal(controller.signal.aborted, true); assert.equal(usage.inputTokens, 9) }
  } }, { compact: async () => { throw new Error('unused') }, structured: async () => { throw new Error('unused') },
    run: async () => {
      if (++requests === 1) throw new ModelDriverError('unavailable', { kind: 'provider', status: 503, finishReasons: [] })
      controller.abort()
      return { text: 'done', output: [], usage: { available: true, inputTokens: 9, outputTokens: 2 } }
    } }, work, DEFAULT_MODEL_BUDGET)
  await model.run({ instructions: '', items: [], signal: controller.signal })
  assert.deepEqual(reserved, ['usage:2:model:1', 'usage:2:model:1:retry:2'])
  assert.deepEqual(settled, reserved)
})

it('preempts a healthy lower-priority lease and fences it after the grace period using real SQL', async () => {
  const db = new PGlite()
  const pool: SqlPool = {
    query: async (sql, args) => { const result = await db.query<Record<string, unknown>>(sql, args); return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length } },
    connect: async () => ({ query: pool.query, release() {} }),
  }
  try {
    await db.exec(await readFile(new URL('../../db/schema.sql', import.meta.url), 'utf8'))
    await db.exec(`INSERT INTO lingxios.agent_work_items(id,tenant_id,agent_id,session_id,kind,lane,trigger_ref,status,created_at,available_at,updated_at,lease_expires_at)
      VALUES ('active','t','a','s','routine','background','m','leased',NOW()-INTERVAL '10 minutes',NOW()-INTERVAL '10 minutes',NOW(),NOW()+INTERVAL '45 seconds'),
      ('waiting','t','a','s','turn','interactive','m2','queued',NOW()-INTERVAL '5 minutes',NOW()-INTERVAL '5 minutes',NOW(),NULL)`)
    const now = new Date()
    assert.deepEqual(await sweepQueuedWork(pool, now, 120_000, 30_000), { tripped: 1, fenced: 0 })
    assert.deepEqual(await sweepQueuedWork(pool, new Date(now.getTime() + 31_000), 120_000, 30_000), { tripped: 0, fenced: 1 })
    assert.deepEqual((await db.query('SELECT status,fence,preemptions FROM lingxios.agent_work_items WHERE id=$1', ['active'])).rows,
      [{ status: 'queued', fence: 1, preemptions: 1 }])
  } finally { await db.close() }
})
