import assert from 'node:assert/strict'
import { it } from 'node:test'
import { setTimeout as delay } from 'node:timers/promises'
import { mkdtemp, readdir, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createHash } from 'node:crypto'
import { AgentRuntime } from '../src/runtime/runtime.js'
import { AgentWorker } from '../src/worker/worker.js'
import { Wakeup } from '../src/control-plane/wakeup.js'
import { MemoryWorkStore } from '../src/control-plane/memory-store.js'
import { ByteBudget, ResourceQuota } from '../src/resource-quota.js'
import { MetricsRegistry } from '../src/metrics.js'
import { toolExecutor } from '../src/tools/executor.js'
import type { ToolDefinition } from '../src/tools/definition.js'
import { limitModel } from '../src/model/quota.js'
import { decodeHtml } from '../src/research/index.js'
import { extractDocumentText } from '../src/context/document-text.js'
import { inspectArtifacts, stageArtifact } from '../src/app/artifacts.js'
import type { ModelDriver } from '../src/model/driver.js'
import type { HostPort } from '../src/host/port.js'
import type { AssistantMessage, SessionRecord, SteerInput, WorkItem } from '../src/protocol/types.js'
import { durableProtocol } from './protocol-fixture.js'
import { snapshotEvidence } from '../src/context/evidence.js'

const deferred = <T = void>() => {
  let resolve!: (value: T) => void
  const promise = new Promise<T>(done => { resolve = done })
  return { promise, resolve }
}
const bounded = <T>(promise: Promise<T>) => Promise.race([promise, delay(3000).then(() => { throw new Error('audit regression timed out') })])
const work: WorkItem = { id: 'audit', tenantId: 't', agentId: 'a', sessionId: 's', principalId: 'u', kind: 'turn', lane: 'interactive',
  triggerRef: 'm', fence: 1, homeEpoch: 1, leaseToken: 'secret' }
const usage = { available: true, inputTokens: 10, outputTokens: 5 }
const answer = () => ({ text: 'Answer.', output: [{ role: 'assistant' as const, content: 'Answer.' }], usage })
const unexpected = async (): Promise<never> => { throw new Error('unexpected operation') }
function fixture(session: SessionRecord | null = null) {
  const committed = deferred<AssistantMessage>(), finished = deferred<unknown>()
  const steers: SteerInput[] = []
  const host: HostPort = { ...durableProtocol(), claimWork: async () => null,
    heartbeat: async () => ({ ok: true, steer: [...steers] }),
    loadContext: async () => ({ work, persona: { name: 'A', role: '', instructions: '' }, capabilities: [],
      messages: [{ ref: 'm', authorId: 'u', authorName: 'U', authorKind: 'human', body: 'Say hello.', createdAt: 'now' }] }),
    loadSession: async () => session, saveSession: async (_work, saved) => { session = structuredClone(saved) },
    emitEvent: async () => {}, executeAction: async () => ({ ok: true, value: { requestVersion: steers.length + 1, pending: [], truncated: false } }),
    commitResult: async (_work, message) => { committed.resolve(message) }, completeWork: async (_work, result) => { finished.resolve(result) }, yieldWork: async () => {} }
  return { host, committed, finished, steers }
}

it('HTML decoding stays linear on malformed tags and parsing is genuinely isolated/cancellable', async () => {
  const input = '<'.repeat(1_000_000)
  const began = performance.now()
  assert.equal(decodeHtml(input), input)
  assert.ok(performance.now() - began < 500, 'overlapping suffix scans returned')
  assert.equal(decodeHtml('<script>one<script>two'), '')
  assert.equal(decodeHtml('<p>A &amp;lt; B</p><style>hidden</style>'), 'A &lt; B')
  const stop = new AbortController()
  let ticks = 0
  const timer = setInterval(() => ticks++, 1)
  try {
    const cancelled = extractDocumentText(Buffer.from(input), 'html', stop.signal)
    stop.abort(new Error('audit cancellation'))
    await assert.rejects(cancelled, /audit cancellation/)
    assert.equal(await extractDocumentText(Buffer.from('<p>Visible</p>'), 'html'), 'Visible')
    assert.ok(ticks > 0)
  } finally { clearInterval(timer) }
})

it('lease recovery runs after registration and cannot block the next claim or its heartbeat', async () => {
  const recovery = deferred(), entered = deferred(), second = deferred()
  const f = fixture()
  let beats = 0, claims = 0
  f.host.recoverWork = async item => { if (item.id === work.id) { entered.resolve(); await recovery.promise }; return true }
  f.host.heartbeat = async () => { beats++; return { ok: true } }
  const runtime = new AgentRuntime(f.host, { run: unexpected, compact: unexpected, structured: unexpected }, { execute: unexpected }, { heartbeatMs: 5 })
  const worker = new AgentWorker({ workerId: 'audit-recovery', maxConcurrentRuns: 2, shutdownGraceMs: 1000, pollIdleMs: 5,
    host: { claimWork: async () => ++claims <= 2 ? { ...work, id: claims === 1 ? work.id : 'next', sessionId: String(claims) } : null },
    runtime: { runWork: async (item, signal) => { if (item.id === 'next') second.resolve(); await runtime.runWork(item, signal) } } })
  await worker.start()
  try { await bounded(entered.promise); await bounded(second.promise); await delay(25); assert.ok(beats > 0) }
  finally { recovery.resolve(); await worker.stop() }
})

it('long interactive operations cannot consume the reserved conversation slot', async () => {
  const store = new MemoryWorkStore(), release = deferred(), started = deferred()
  for (const id of ['long-a', 'long-b', 'short']) await store.enqueue({ ...work, id, sessionId: id,
    meta: { executionClass: id === 'short' ? 'conversation' : 'operation' } })
  const seen: string[] = []
  const worker = new AgentWorker({ workerId: 'admission', maxConcurrentRuns: 2, reservedInteractiveRuns: 1, shutdownGraceMs: 1000, pollIdleMs: 5,
    host: { claimWork: (_signal, lanes, executionClass) => store.claim('admission', undefined, undefined, lanes, executionClass) },
    runtime: { runWork: async item => { seen.push(item.id); if (item.id === 'short') started.resolve(); else await release.promise } } })
  await worker.start()
  try { await bounded(started.promise); assert.deepEqual(seen.slice(0, 2), ['long-a', 'short']) }
  finally { release.resolve(); await worker.stop() }
  await assert.rejects(store.claim('admission', 'identity', undefined, undefined, 'operation').then(async () =>
    store.claim('admission', 'identity', undefined, undefined, 'conversation')), /class changed/)
})

it('revise cancels an in-flight generation before the old provider finishes and accounts for it', async () => {
  const f = fixture(), entered = deferred(), old = deferred<ReturnType<typeof answer>>()
  let signal: AbortSignal | undefined, calls = 0
  const settlements: string[] = []
  const protocol = durableProtocol(observation => settlements.push(observation.purpose))
  f.host.reserveModelCall = protocol.reserveModelCall; f.host.recordModelUsage = protocol.recordModelUsage
  const model: ModelDriver = { compact: unexpected, structured: async () => ({ value: { missing: [] }, model: 'stub', usage }), run: async request => {
    if (++calls === 1) { signal = request.signal; entered.resolve(); return old.promise }
    assert.match(JSON.stringify(request.items), /New requirement/)
    return answer()
  } }
  const runtime = new AgentRuntime(f.host, model, { execute: unexpected }, { heartbeatMs: 60_000 })
  const wake = new Wakeup()
  let claimed = false
  f.host.claimWork = async () => { if (claimed) return null; claimed = true; return work }
  f.host.waitForWork = async (cursor, timeout, signal) => {
    if (cursor === String(wake.version)) await wake.wait(wake.version, timeout, signal)
    return String(wake.version)
  }
  const worker = new AgentWorker({ host: f.host, runtime, workerId: 'control-push', maxConcurrentRuns: 1, shutdownGraceMs: 1000 })
  await worker.start()
  await bounded(entered.promise)
  f.steers.push({ id: 'revise', text: 'New requirement', createdAt: 'now' })
  wake.notify() // The only run slot is occupied; the independent control watcher must still wake.
  try {
    const message = await bounded(f.committed.promise)
    assert.equal(signal?.aborted, true); assert.equal(message.envelope.requestVersion, 2)
    assert.equal(calls, 2); assert.equal(settlements.filter(purpose => purpose === 'agent-turn').length, 2)
  } finally { old.resolve(answer()); await worker.stop() }
})

it('soft compaction neither blocks first generation nor commit, and cancellation still settles its ledger', async () => {
  const history = Array.from({ length: 30 }, () => ({ role: 'user' as const, content: 'x'.repeat(2700) }))
  const session: SessionRecord = { key: '["t","a","s",null]', tenantId: 't', agentId: 'a', sessionId: 's', revision: 1,
    appliedWorkIds: [work.id], compactionEpoch: 0, history,
    request: { version: 1, workId: work.id, tenantId: 't', sessionId: 's', authorId: 'u', sourceRef: 'm',
      originalText: 'Say hello.', revisions: [], attachments: [], evidence: snapshotEvidence('e', []) } }
  const f = fixture(session), compact = deferred<{ value: string; model: string; usage: typeof usage }>()
  let summarySignal: AbortSignal | undefined, calls = 0
  const model: ModelDriver = { maxOutputTokens: 100, toolDefinitionTokens: 0, compact: async request => { summarySignal = request.signal; return compact.promise },
    structured: unexpected, run: async () => { calls++; return answer() } }
  const runtime = new AgentRuntime(f.host, model, { execute: unexpected }, { compaction: { softRatio: 0.6 } })
  const running = runtime.runWork(work)
  try { await bounded(f.committed.promise); assert.equal(calls, 1); assert.equal(summarySignal?.aborted, true) }
  finally { compact.resolve({ value: '{}', model: 'stub', usage }); await running; await runtime.drainBackground() }
})

it('foreground model has reserved admission and single-slot summaries are actually cancelled', async () => {
  const gate = deferred(), backgroundStarted = deferred()
  let cancelled = false, generated = 0, generationSignal: AbortSignal | undefined
  const source: ModelDriver = { structured: unexpected, run: async request => { generationSignal = request.signal; generated++; return answer() },
    compact: async request => {
      backgroundStarted.resolve()
      await new Promise<void>((resolve, reject) => {
        gate.promise.then(resolve)
        request.signal?.addEventListener('abort', () => { cancelled = true; reject(request.signal?.reason) }, { once: true })
      })
      return { value: '{}', model: 'stub', usage }
    } }
  const one = limitModel(source, new ResourceQuota(1))
  const summary = one.compact({ instructions: '', items: [], interruptible: true })
  await backgroundStarted.promise
  await one.run({ instructions: '', items: [] })
  await assert.rejects(summary, /foreground/)
  assert.equal(cancelled, true); assert.equal(generated, 1)
  const longGeneration = one.run({ instructions: '', items: [], admission: 'background' })
  const nextConversation = one.run({ instructions: '', items: [] })
  assert.equal(generationSignal?.aborted, false, 'foreground admission cannot fail an operation generation')
  await Promise.all([longGeneration, nextConversation])
  const two = new ResourceQuota(2, 16, 1), started: string[] = []
  const jobs = [two.run(async () => { started.push('background-1'); await gate.promise }, undefined, 'background'),
    two.run(async () => { started.push('background-2'); await gate.promise }, undefined, 'background')]
  await two.run(async () => { started.push('foreground') })
  assert.deepEqual(started, ['background-1', 'foreground'])
  gate.resolve(); await Promise.all(jobs)
  const metrics = new MetricsRegistry(), quota = new ResourceQuota(1, 16, 0, metrics, 'model')
  const pending = deferred(), stop = new AbortController()
  const operation = quota.run(() => pending.promise, stop.signal)
  stop.abort()
  assert.doesNotMatch(metrics.expose(), /agentos_cancel_resource_release_seconds_count/)
  pending.resolve(); await operation
  assert.match(metrics.expose(), /agentos_cancel_resource_release_seconds_count\{resource="model"\} 1/)
  assert.match(metrics.expose(), /agentos_resource_queue_wait_seconds_count/)
})

it('long native contracts keep resource and recovery limits explicit', () => {
  const tool: ToolDefinition = { name: 'long__read', action: 'long.read', description: 'Read', effect: 'read', approval: false,
    parameters: { type: 'object', properties: {}, additionalProperties: false }, parse: () => ({}), authorize: async () => {},
    execute: unexpected, execution: { class: 'operation', timeoutMs: 30_000, maxConcurrency: 2, cancellation: 'signal' } }
  const pool = { query: unexpected, connect: unexpected }
  assert.doesNotThrow(() => toolExecutor(pool, [tool]))
  assert.throws(() => toolExecutor(pool, [{ ...tool, effect: 'transaction' }]), /long-tool/)
  assert.throws(() => toolExecutor(pool, [{ ...tool, execution: { ...tool.execution!, timeoutMs: 30_001 } }]), /long-tool/)
  assert.throws(() => toolExecutor(pool, [{ ...tool, execution: { ...tool.execution!, maxConcurrency: 0 } }]), /capacity/)
  assert.throws(() => toolExecutor(pool, [{ ...tool, execution: { ...tool.execution!, cancellation: 'reconcile' } }]), /long-tool/)
})

it('streamed artifacts are quarantined until hash validation, cancellation cleans up, and byte limits recover', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'async-artifact-'))
  const bytes = Buffer.from('{"ok":true}')
  const artifact = { path: 'data.json', size: bytes.length, mime: 'application/json', sha256: createHash('sha256').update(bytes).digest('hex') }
  const chunks = async function* () { yield bytes.subarray(0, 3); yield bytes.subarray(3) }
  try {
    await stageArtifact(directory, work, artifact, chunks())
    assert.equal((await inspectArtifacts(directory, work, [artifact]))[0]?.status, 'passed')
    await assert.rejects(stageArtifact(directory, work, { ...artifact, sha256: '0'.repeat(64) }, chunks()), /commitment/)
    const stop = new AbortController()
    await assert.rejects(stageArtifact(directory, work, artifact, (async function* () { yield bytes.subarray(0, 3); stop.abort(); yield bytes.subarray(3) })(), stop.signal))
    const files = await readdir(directory, { recursive: true })
    assert.equal(files.filter(path => /[a-f0-9]{64}$/.test(path)).length, 2) // directory hash and one verified content hash
    const budget = new ByteBudget(10, 6), release = budget.acquire('a', 6)
    assert.throws(() => budget.acquire('a', 1), /byte budget/)
    const other = budget.acquire('b', 4)
    assert.throws(() => budget.acquire('c', 1), /byte budget/)
    release(); release(); other(); budget.acquire('a', 6)()
  } finally { await rm(directory, { recursive: true, force: true }) }
})
