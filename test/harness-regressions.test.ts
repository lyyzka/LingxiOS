import assert from 'node:assert/strict'
import { it } from 'node:test'
import { readFile } from 'node:fs/promises'
import { PGlite } from '@electric-sql/pglite'
import { createLingxiOS } from '../src/app/index.js'
import type { SqlPool } from '../src/control-plane/pg-store.js'
import type { ToolDefinition } from '../src/tools/definition.js'
import { assembleHarness, type HarnessProfile } from '../src/harness/profile.js'
import { skillIndex } from '../src/skills/definition.js'
import { snapshotRequest } from '../src/context/request.js'
import { observationRef } from '../src/context/observations.js'
import { actionKeyOf, sessionKeyOf, type WorkItem } from '../src/protocol/types.js'
import { AgentRuntime } from '../src/runtime/runtime.js'
import { DefaultRuntimePolicy } from '../src/runtime/policy.js'
import { executionSnapshot } from '../src/runtime/execution-policy.js'
import { buildPromptContext } from '../src/prompts/provider.js'
import { businessActionDeliveryGap } from '../src/outcome/completion.js'
import { consumeAssistantMessage, createRunView, responseSegments } from '../src/ui/index.js'
import { checkStorage } from '../src/app/storage.js'
import { fitsModel } from '../src/model/profile.js'
import { OpenAIChatDriver } from '../src/model/openai.js'

const usage = { available: true, inputTokens: 10, outputTokens: 10 }
const schema = { type: 'object' as const, properties: { id: { type: 'string' } }, required: ['id'], additionalProperties: false as const }
const skill = { name: 'inspect', version: '1', description: 'Inspect a document', body: 'Read the full document before explaining it.', actions: ['documents.get'] }
const native: ToolDefinition = { name: 'documents__get', action: 'documents.get', description: 'Read documents', parameters: schema,
  effect: 'read', approval: false, deferred: true, parse: value => value as Record<string, unknown>, authorize: async () => {}, execute: async () => ({ ok: true, value: null }) }

it('assembles deterministically, rejects invalid dependencies, and evaluates hop capabilities once', () => {
  const a = { id: 'a', tools: [native], skills: [skill], rules: 'Document workflow.' }, b = { id: 'b', dependsOn: ['a'], tools: [] }
  const profile: HarnessProfile = { id: 'test', version: '1', mode: 'read', capabilities: [b, a] }
  assert.deepEqual(assembleHarness(profile), assembleHarness({ ...profile, capabilities: [a, b] }))
  assert.throws(() => assembleHarness({ ...profile, capabilities: [b] }), /missing capability/)
  assert.throws(() => assembleHarness({ ...profile, capabilities: [a, a] }), /duplicate/)
  assert.throws(() => assembleHarness({ ...profile, capabilities: [{ ...a, dependsOn: ['b'] }, b] }), /cyclic/)
  assert.notEqual(skillIndex(skill).hash, skillIndex({ ...skill, body: skill.body + ' Changed.' }).hash)
  let calls = 0
  const policy = new DefaultRuntimePolicy()
  policy.kernelCapabilities = () => { calls++; return calls === 1 ? [{ name: 'documents' }] : [] }
  const context = { work: { id: 'w', tenantId: 't', agentId: 'a', sessionId: 's', kind: 'turn', lane: 'interactive' as const, triggerRef: 'm', fence: 1, homeEpoch: 1, leaseToken: 'x', meta: { mode: 'read' } },
    persona: { name: 'A', role: '', instructions: '' }, capabilities: ['documents'], tools: [native], messages: [], harness: assembleHarness(profile).context }
  const snapshot = executionSnapshot(context, policy)
  const prompt = buildPromptContext(context, policy, 0, undefined, 'fixed', snapshot)
  assert.equal(calls, 1)
  assert.equal(snapshot.codeExecution, 'disabled')
  assert.match(prompt.systemInstructions, /documents/)
  assert.doesNotMatch(prompt.systemInstructions, /Use ipython|attach_file/)
  assert.ok(!JSON.stringify(prompt).includes(skill.body), 'only the skill index is loaded initially')
  assert.deepEqual(executionSnapshot(context, policy).tools, [])
})

it('enforces modes, scoped observations, skill revocation, exact obligations and committed presentation replay', async () => {
  const db = new PGlite()
  const pool: SqlPool = { query: async (sql, params) => { const result = await db.query<Record<string, unknown>>(sql, params); return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length } },
    connect: async () => ({ query: pool.query, release() {} }) }
  await db.exec(await readFile(new URL('../../db/schema.sql', import.meta.url), 'utf8'))
  await assert.rejects(createLingxiOS({ database: pool, modelBudget: { maxCostMicros: 1000 } }), /requires configured model token prices/)
  let revoked = false, grants = true, writes = 0
  const full = { text: 'x'.repeat(50_000) + 'TAIL_REQUIREMENT', status: 'active', count: 42 }
  const read: ToolDefinition = { ...native, authorize: async () => { if (revoked) throw new Error('revoked') }, execute: async () => ({ ok: true, value: full }) }
  const write: ToolDefinition = { ...native, action: 'documents.send', name: 'documents__send', effect: 'transaction', deferred: false,
    execute: async () => { writes++; return { ok: true, value: { sent: true } } }, verify: async () => ({ status: 'passed', evidence: {} }) }
  const profile: HarnessProfile = { id: 'reviewable', version: '1', mode: 'execute', capabilities: [{ id: 'docs', tools: [read, write], skills: [skill],
    presentations: [{ type: 'document', version: '1', description: 'Document facts', actions: ['documents.get'],
      authorize: async (_context, reference) => { if (revoked || reference !== 'd') throw new Error('unavailable') },
      resolve: async () => ({ fields: { status: full.status, count: full.count }, sources: [{ ref: 'd', version: 'v1', observedAt: '2026-09-08T00:00:00Z' }] }) }] }] }
  const app = await createLingxiOS({ database: pool, harness: profile, capabilityResolver: { resolve: async () => grants ? [{ name: 'documents' }] : [] },
    verifyRun: async ({ candidate }) => [{ checker: 'product:answer', status: candidate.body === 'verified answer' ? 'passed' : 'inconclusive', evidence: {} }] })
  try {
    const host = app.connectWorker({ workerId: 'test', workKinds: ['turn'] })
    const start = async (id: string, mode: 'chat' | 'read' | 'execute', obligations?: import('../src/outcome/obligations.js').DeliveryObligation[]) => {
      await app.enqueue({ id, tenantId: 't', agentId: 'a', sessionId: id, principalId: id, text: 'Inspect this document.', mode, ...(obligations ? { obligations } : {}) })
      const work = (await host.claimWork())!
      await host.saveSession(work, { key: sessionKeyOf(work), tenantId: 't', agentId: 'a', sessionId: id, history: [], appliedWorkIds: [id], revision: 0, compactionEpoch: 0,
        request: snapshotRequest(await host.loadContext(work)) })
      return work
    }
    const action = (work: WorkItem, cellId: string, name: string, args: Record<string, unknown>) => {
      const identity = { runId: work.id, cellId, callIndex: 0 }; return { ...identity, idempotencyKey: actionKeyOf(identity), action: name, args }
    }
    const chat = await start('chat', 'chat'), owner = await start('owner', 'read'), foreign = await start('foreign', 'read')
    assert.deepEqual((await host.loadContext(chat)).tools, [])
    assert.equal((await host.executeAction(chat, action(chat, 'read', read.action, { id: 'd' }))).ok, false)
    assert.equal((await host.executeAction(owner, action(owner, 'write', write.action, { id: 'd' }))).code, 'mode_forbidden')
    assert.equal(writes, 0)
    const readAction = action(owner, 'read', read.action, { id: 'd' })
    await host.executeAction(owner, readAction)
    const ref = observationRef(readAction.idempotencyKey, full), range = { actionKey: ref.actionKey, sha256: ref.sha256, start: ref.characters - 40, length: 40 }
    const reread = action(owner, 'tail', 'observations.read', range)
    assert.match(JSON.stringify((await host.executeAction(owner, reread)).value), /TAIL_REQUIREMENT/)
    assert.equal((await host.executeAction(foreign, action(foreign, 'foreign', 'observations.read', range))).ok, false)
    assert.equal((await host.executeAction(owner, action(owner, 'bad-hash', 'observations.read', { ...range, sha256: '0'.repeat(64) }))).ok, false)
    revoked = true
    assert.equal((await host.executeAction(owner, reread)).ok, false, 'receipt replay must reauthorize the original observation')
    revoked = false
    const index = skillIndex(skill), selection = { name: index.name, version: index.version, hash: index.hash }
    const load = action(owner, 'skill', 'skills.load', selection)
    assert.equal((await host.executeAction(owner, load)).ok, true)
    assert.equal((await host.executeAction(owner, action(owner, 'unknown-skill', 'skills.load', { ...selection, name: 'unknown' }))).ok, false)
    grants = false
    assert.equal((await host.executeAction(owner, load)).ok, false)
    const found = await host.executeAction(owner, action(owner, 'empty-catalog', 'catalog.discover', { query: '', cursor: 0 }))
    assert.deepEqual((found.value as { tools: unknown[] }).tools, [])
    grants = true
    assert.equal((await host.executeAction(owner, action(owner, 'forged-fields', 'presentation.render', { type: 'document', reference: 'd', count: 999 }))).ok, false)
    assert.equal((await host.executeAction(owner, action(owner, 'missing-source', 'presentation.render', { type: 'document', reference: 'missing' }))).ok, false)

    const delivery = await start('delivery', 'execute', [{ id: 'send', kind: 'external-delivery', action: write.action, args: { id: 'required' } },
      { id: 'answer', kind: 'answer-content', checker: 'product:answer' }])
    const candidate = { requestVersion: 1, body: 'verified answer', artifacts: [] }
    const checks = async () => (await host.verifyCandidate!(delivery, candidate)).records.filter(check => check.checker.startsWith('obligation:'))
    assert.deepEqual((await checks()).map(check => check.status), ['inconclusive', 'passed'])
    await host.executeAction(delivery, action(delivery, 'unrelated-send', write.action, { id: 'other' }))
    assert.equal((await checks())[0]?.status, 'inconclusive')
    await host.executeAction(delivery, action(delivery, 'required-send', write.action, { id: 'required' }))
    assert.ok((await checks()).every(check => check.status === 'passed'))
    assert.match(businessActionDeliveryGap({ ...snapshotRequest(await host.loadContext(delivery)), obligations: [] }, false)!, /no durable/)

    const display = await start('display', 'read')
    let calls = 0
    const runtime = new AgentRuntime(host, { run: async request => {
      assert.equal(request.codeExecution, 'disabled')
      if (++calls === 1) {
        assert.ok(!request.tools?.some(tool => tool.name === read.name), 'deferred schema is initially absent')
        return { text: '', output: [{ type: 'function_call', callId: 'discover', name: 'catalog__discover', arguments: '{"query":"documents","cursor":0}' }], usage }
      }
      assert.ok(request.tools?.some(tool => tool.name === read.name), 'discovery materializes the current authorized schema')
      return calls === 2 ? { text: '', output: [{ type: 'function_call', callId: 'render', name: 'presentation__render', arguments: '{"type":"document","reference":"d","annotation":"Count is 999 (model comment)"}' }], usage }
        : { text: 'verified answer', output: [], usage }
    }, structured: async () => ({ value: { missing: [] }, model: 'primary', usage }), compact: async () => { throw new Error('unexpected compaction') } },
    { execute: async () => { throw new Error('read mode cannot execute Python') } })
    await runtime.runWork(display)
    const state = await app.readRunState({ runId: display.id, tenantId: 't', principalId: display.principalId!, agentId: 'a', sessionId: display.sessionId })
    assert.equal(state?.run.status, 'succeeded', state?.run.error ?? undefined)
    assert.deepEqual(state?.message?.envelope.presentations?.[0]?.fields, { status: 'active', count: 42 })
    const view = consumeAssistantMessage(createRunView(display.id), state!.message!, { resultId: state!.run.resultId!, fence: state!.run.resultFence! })
    assert.deepEqual(consumeAssistantMessage(view, state!.message!, { resultId: state!.run.resultId!, fence: state!.run.resultFence! }), view)
    assert.equal(view.message?.envelope.presentations?.[0]?.hash.length, 64)
    assert.equal(responseSegments(state!.message!.envelope).at(-1)?.type, 'presentation')
    const originalHash = owner.meta!['harnessHash']
    await db.query("UPDATE lingxios.agent_work_items SET meta=jsonb_set(meta,'{harnessHash}','\"changed\"'::jsonb) WHERE id=$1", [owner.id])
    await assert.rejects(host.loadContext(owner), /harness version mismatch/)
    await db.query("UPDATE lingxios.agent_work_items SET meta=jsonb_set(meta,'{harnessHash}',$2::jsonb) WHERE id=$1", [owner.id, JSON.stringify(originalHash)])
    const program = await db.query<Record<string, unknown>>("SELECT data->'program' AS program FROM lingxios.agent_run_events WHERE run_id='display' AND kind='model.started' LIMIT 1")
    assert.equal((program.rows[0]?.['program'] as { hash: string }).hash.length, 64)
  } finally { await app.stop(); await db.close() }
})

it('migrates existing schema 7 without approving old semantics and rejects accidental reapplication', async () => {
  const db = new PGlite()
  try {
    await db.exec(await readFile(new URL('../../test/fixtures/schema-8.sql', import.meta.url), 'utf8'))
    await db.exec(`DROP TABLE lingxios.agent_memory_scopes;
      ALTER TABLE lingxios.agent_memory_evidence DROP COLUMN scope_epochs;
      ALTER TABLE lingxios.agent_approvals DROP COLUMN tool_contract_hash;
      UPDATE lingxios.schema_version SET version=7`)
    const migration = await readFile(new URL('../../db/migrations/008-governance.sql', import.meta.url), 'utf8')
    await db.exec(migration)
    assert.deepEqual((await db.query('SELECT version FROM lingxios.schema_version')).rows, [{ version: 8 }])
    const defaults = await db.query<Record<string, unknown>>("SELECT column_default FROM information_schema.columns WHERE table_schema='lingxios' AND table_name='agent_approvals' AND column_name='tool_contract_hash'")
    assert.equal(defaults.rows[0]?.['column_default'], null)
    await assert.rejects(db.exec(migration), /requires schema version 7/)
    await db.exec('ROLLBACK')
    await db.exec(await readFile(new URL('../../db/migrations/009-cognitive-memory-reset.sql',import.meta.url),'utf8'))
    for (const migration of ['010-im-collaboration', '011-performance-notifications', '012-async-admission']) {
      await db.exec(await readFile(new URL(`../../db/migrations/${migration}.sql`, import.meta.url), 'utf8'))
    }
    await checkStorage({ query: async (sql, params) => ({ rows: (await db.query<Record<string, unknown>>(sql, params)).rows, rowCount: null }) })
  } finally { await db.close() }
})

it('rejects unsupported model protocols before sending and budgets the full Chinese input', async () => {
  let calls = 0
  const model = new OpenAIChatDriver('primary', { apiKey: 'test', capabilities: { toolCalls: false, jsonObject: false }, fetchImpl: async () => { calls++; throw new Error('network must not run') } })
  await assert.rejects(model.run({ instructions: '', items: [], codeExecution: 'enabled' }), /does not support tool calls/)
  await assert.rejects(model.structured({ instructions: '', input: {} }), /does not support JSON/)
  assert.equal(calls, 0)
  assert.equal(fitsModel({ ...model, contextWindowTokens: 1000, profile: { ...model.profile, contextWindowTokens: 1000, maxOutputTokens: 10 },
    run: model.run.bind(model), compact: model.compact.bind(model), structured: model.structured.bind(model) }, '汉'.repeat(500)), false)
})
