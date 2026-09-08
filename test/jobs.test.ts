import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { it } from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import { createLingxiOS } from '../src/app/index.js'
import { PgModelBudgetStore, type SqlPool } from '../src/control-plane/pg-store.js'
import { resumeDependents } from '../src/control-plane/dependencies.js'
import { actionKeyOf, sessionKeyOf } from '../src/protocol/types.js'
import { modelPricing } from '../src/model/execution.js'
import type { ToolDefinition } from '../src/tools/definition.js'
import { snapshotEvidence } from '../src/context/evidence.js'
import { readRunReference, countPendingApprovals } from '../src/app/diagnostics.js'
import { snapshotRequest } from '../src/context/request.js'

it('commits child lineage atomically, resumes a child that finished before parking, propagates revisions and freezes model prices', async () => {
  const db = new PGlite()
  const pool: SqlPool = { query: async (sql, params) => {
    const result = await db.query<Record<string, unknown>>(sql, params)
    return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
  }, connect: async () => ({ query: pool.query, release() {} }) }
  await db.exec(await readFile(new URL('../../db/schema.sql', import.meta.url), 'utf8'))
  let fail = false
  const tool: ToolDefinition = { name: 'delegate__create', action: 'delegate.create', description: 'Delegate a bounded task', effect: 'transaction', approval: false,
    parameters: { type: 'object', properties: {}, additionalProperties: false }, parse: () => ({}), authorize: async () => {},
    async execute(context) {
      const value = await context.enqueueChild({ id: context.action.cellId, agentId: 'helper', sessionId: 'child-session', text: 'Assigned request' })
      if (fail) throw new Error('interrupted before receipt')
      return { ok: true, value, directive: { type: 'defer', reason: 'child', data: { taskRef: value.id } } }
    } }
  const control = await createLingxiOS({ database: pool, tools: [tool] })
  try {
    const identity = { runId: 'parent', tenantId: 'tenant', agentId: 'agent', sessionId: 'room', principalId: 'human' }
    await control.enqueue({ ...identity, id: 'parent', text: 'Original request', codeExecution: 'disabled', deliveryMode: 'action' })
    const host = control.connectWorker({ workerId: 'worker', workKinds: ['turn'] })
    let parent = (await host.claimWork())!
    await host.saveSession(parent, { key: sessionKeyOf(parent), tenantId: parent.tenantId, agentId: parent.agentId, sessionId: parent.sessionId,
      revision: 0, compactionEpoch: 0, history: [], appliedWorkIds: ['parent'], request: { version: 1, workId: 'parent', tenantId: 'tenant', sessionId: 'room',
        authorId: 'human', sourceRef: 'parent', originalText: 'Original request', revisions: [], codeExecution: 'disabled', deliveryMode: 'action',
        attachments: [], evidence: snapshotEvidence('parent:evidence:1', []) } })
    const saveProgress = (id: string, version: number, observedAt: string) => host.saveStep(parent,{ id, requestVersion: 1, kind: 'tool', input: { id }, artifacts: [],
      output: JSON.stringify({ receipts: [{ result: { ok: true, value: { version, observedAt } } }] }) })
    await saveProgress('first-progress',1,'2026-01-01')
    await db.query("UPDATE lingxios.agent_work_items SET last_progress_at='2001-01-01T00:00:00Z' WHERE id='parent'")
    await saveProgress('same-progress',1,'2026-02-01')
    assert.equal((await control.readRun(identity))?.lastProgressAt,'2001-01-01T00:00:00.000Z')
    await saveProgress('new-progress',2,'2026-02-01')
    assert.notEqual((await control.readRun(identity))?.lastProgressAt,'2001-01-01T00:00:00.000Z')
    const action = (cellId: string) => { const scope = { runId: 'parent', cellId, callIndex: 0 }; return { ...scope, action: tool.action, args: {}, idempotencyKey: actionKeyOf(scope) } }
    fail = true
    assert.equal((await host.executeAction(parent, action('rolled-back-child'))).executionState, 'no_effect')
    assert.deepEqual((await db.query("SELECT id FROM lingxios.agent_work_items WHERE id='rolled-back-child'")).rows, [])
    fail = false
    assert.equal((await host.executeAction(parent, action('child'))).ok, true)
    assert.equal((await host.executeAction(parent, action('child'))).ok, true)
    assert.equal((await host.executeAction(parent, action('sibling'))).ok, true)
    const child = (await host.claimWork())!
    assert.equal(child.id, 'child')
    assert.equal(child.principalId, 'human')
    assert.equal(child.meta?.['rootWorkId'], 'parent')
    assert.equal(child.meta?.['parentRequestVersion'], 1)
    assert.equal(child.meta?.['codeExecution'], 'disabled')
    assert.equal(child.meta?.['deliveryMode'], undefined)
    await host.completeWork(child, { status: 'completed', goalOutcome: { status: 'partial', verification: 'not_run', requestVersion: 1 } })
    const wait = { status: 'delegated' as const, taskRef: child.id, verification: 'not_run' as const, requestVersion: 1 }
    await assert.rejects(host.completeWork(parent, { status: 'completed', goalOutcome: wait }), /waitWork/)
    await host.waitWork(parent, wait)
    assert.equal((await control.readRun(identity))?.status, 'waiting')
    await resumeDependents(pool)
    assert.equal((await control.readRun(identity))?.status, 'waiting')
    const sibling = (await host.claimWork())!
    assert.equal(sibling.id, 'sibling')
    await host.completeWork(sibling, { status: 'completed', goalOutcome: { status: 'partial', verification: 'not_run', requestVersion: 1 } })
    await resumeDependents(pool)
    parent = (await host.claimWork())!
    assert.equal(parent.id, 'parent')
    assert.ok(parent.fence > 1)
    assert.equal((await host.executeAction(parent, action('cancelled-child'))).ok, true)
    await control.revise(identity, 'Updated request')
    assert.deepEqual((await db.query("SELECT status FROM lingxios.agent_work_items WHERE id='cancelled-child'")).rows, [{ status: 'cancelled' }])
    assert.equal(await control.cancel(identity), true)
    await assert.rejects(host.executeAction(parent, action('late-child')), /cancel/)

    const budgets = new PgModelBudgetStore(pool), pricing = modelPricing({ inputCostMicrosPerMillion: 2_000_000, outputCostMicrosPerMillion: 4_000_000 })
    const limits = { maxModelCalls: 3, maxTokens: 1000, maxCostMicros: 10_000, deadlineAt: '2099-01-01T00:00:00Z', pricing }
    await budgets.reserve('parent', 'priced-call', limits)
    await budgets.reserve('parent', 'priced-call', { ...limits, pricing: modelPricing({ inputCostMicrosPerMillion: 8_000_000, outputCostMicrosPerMillion: 9_000_000 }) })
    await budgets.record('parent', 'priced-call', 10, 5, 999, undefined, { callId: 'priced-call', purpose: 'agent-turn', workId: 'parent',
      tenantId: 'tenant', agentId: 'agent', sessionId: 'room', model: 'fixture', latencyMs: 1, status: 'succeeded', usage: { available: false, inputTokens: 10, outputTokens: 5 } })
    const observation = (await db.query<{ observation: { cost: unknown } }>("SELECT observation FROM lingxios.agent_model_budget_calls WHERE call_id='priced-call'")).rows[0]!.observation
    assert.deepEqual(observation.cost, { amountMicros: 40, usage: 'estimated', pricing })
    assert.deepEqual(await readRunReference(pool,'tenant','parent'),identity)
    assert.equal(await readRunReference(pool,'other','parent'),null)
    assert.equal(await countPendingApprovals(pool,'tenant','room'),0)
    assert.deepEqual((await control.listRuns({ tenantId: 'other' })).items,[])
    const first = await control.listRuns({ tenantId: 'tenant', limit: 2 })
    const second = await control.listRuns({ tenantId: 'tenant', limit: 2, cursor: first.nextCursor! })
    assert.equal(new Set([...first.items,...second.items].map(run => run.id)).size,4)
    assert.deepEqual((await control.listRuns({ tenantId: 'tenant',limit: 2,offset: 2 })).items.map(run => run.id),second.items.map(run => run.id))
    assert.deepEqual((await control.listRuns({ order: 'oldest' })).items.map(run => run.id),[...first.items,...second.items].map(run => run.id).reverse())
    assert.deepEqual((await control.listRuns({ order: 'id' })).items.map(run => run.id),['cancelled-child','child','parent','sibling'])
    assert.equal('meta' in first.items[0]!,false)
    assert.equal('lease_token_hash' in first.items[0]!,false)
    const report = await control.readOperations()
    assert.equal(report.runs,4)
    assert.equal(report.trend.reduce((sum,hour) => sum+hour.runs,0),4)
    await assert.rejects(control.listRuns({ cursor: 'invalid' }),/cursor/)
    await assert.rejects(control.listRuns({ cursor: first.nextCursor!,offset: 0 }),/pagination/)
    await db.query("UPDATE lingxios.agent_model_budget_calls SET failed_at=NOW(),attempts=12 WHERE call_id='priced-call'")
    assert.equal(await control.retryDelivery({ ...identity,threadId: 'another-thread' },'usage'),false)
    assert.equal(await control.retryDelivery(identity,'usage'),true)
    assert.equal(await control.retryDelivery(identity,'usage'),false)
  } finally { await control.stop(); await db.close() }
})

it('durable operation handoff releases the original conversation, then resumes without a sweep', async () => {
  const db = new PGlite()
  const pool: SqlPool = { query: async (sql, params) => {
    const result = await db.query<Record<string, unknown>>(sql, params)
    return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
  }, connect: async () => ({ query: pool.query, release() {} }) }
  await db.exec(await readFile(new URL('../../db/schema.sql', import.meta.url), 'utf8'))
  const tool: ToolDefinition = { name: 'delegate__create', action: 'delegate.create', description: 'Persist a long operation',
    effect: 'transaction', approval: false, parameters: { type: 'object', properties: {}, additionalProperties: false },
    parse: () => ({}), authorize: async () => {}, execute: async context => {
      const child = await context.enqueueChild({ id: 'long', agentId: 'agent', text: 'Long operation', executionClass: 'operation' })
      return { ok: true, value: child, directive: { type: 'defer', reason: 'child', data: { taskRef: child.id } } }
    } }
  const control = await createLingxiOS({ database: pool, tools: [tool], performance: { notifications: false } })
  const identity = { runId: 'front', tenantId: 'tenant', agentId: 'agent', sessionId: 'room', principalId: 'human' }
  try {
    await control.enqueue({ ...identity, id: 'front', text: 'Delegate work', executionClass: 'conversation' })
    const host = control.connectWorker({ workerId: 'worker', workKinds: ['turn'] })
    const parent = (await host.claimWork())!, context = await host.loadContext(parent)
    await host.saveSession(parent, { key: sessionKeyOf(parent), tenantId: parent.tenantId, agentId: parent.agentId,
      sessionId: parent.sessionId, revision: 0, compactionEpoch: 0, history: [], appliedWorkIds: [parent.id], request: snapshotRequest(context) })
    const scope = { runId: parent.id, cellId: 'delegate', callIndex: 0 }
    assert.equal((await host.executeAction(parent, { ...scope, action: tool.action, args: {}, idempotencyKey: actionKeyOf(scope) })).ok, true)
    await host.waitWork(parent, { status: 'delegated', taskRef: 'long', verification: 'not_run', requestVersion: 1 })
    const operation = (await host.claimWork(undefined, undefined, 'operation'))!
    assert.equal(operation.id, 'long'); assert.notEqual(sessionKeyOf(operation), sessionKeyOf(parent))
    await control.enqueue({ ...identity, id: 'short', text: 'Answer a new question', mode: 'chat' })
    const conversation = (await host.claimWork(undefined, undefined, 'conversation'))!
    assert.equal(conversation.id, 'short') // Same original session, while the long child remains leased.
    await host.completeWork(conversation, { status: 'completed', goalOutcome: { status: 'partial', verification: 'not_run', requestVersion: 1 } })
    assert.equal((await control.readRun(identity))?.status, 'waiting')
    await host.completeWork(operation, { status: 'completed', goalOutcome: { status: 'partial', verification: 'not_run', requestVersion: 1 } })
    assert.equal((await control.readRun(identity))?.status, 'queued') // No resumeDependents call, timer or notification required.
    assert.equal((await host.claimWork(undefined, undefined, 'conversation'))?.id, parent.id)
  } finally { await control.stop(); await db.close() }
})
