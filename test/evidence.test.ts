import { MemoryStepStore } from '../src/control-plane/steps.js'
import { appendResearchEvidence } from '../src/context/research-evidence.js'
import { createTaskContract } from '../src/context/task-contract.js'
import assert from 'node:assert/strict'
import { it } from 'node:test'
import { snapshotEvidence, evidenceItems } from '../src/context/evidence.js'
import { createResponseEnvelope, snapshotArtifacts } from '../src/outcome/envelope.js'
import { responseSegments } from '../src/ui/index.js'
import { ControlPlaneService } from '../src/control-plane/service.js'
import { MemoryActionLedger, MemoryEventStore, MemorySessionStore, MemoryWorkStore } from '../src/control-plane/memory-store.js'
import { AgentRuntime } from '../src/runtime/runtime.js'
import type { HostPort } from '../src/host/port.js'
import { sessionKeyOf, type AssistantMessage } from '../src/protocol/types.js'
import type { ModelDriver } from '../src/model/driver.js'

it('delivers the latest recorded version of each artifact path', () => {
  const first = { path: 'report.txt', size: 3, mime: 'text/plain', sha256: 'a'.repeat(64) }
  const latest = { ...first, size: 4, sha256: 'b'.repeat(64) }
  const other = { ...first, path: 'notes.txt' }
  const inventory = snapshotArtifacts([first, other, latest, latest])
  assert.deepEqual(inventory, [latest, other])
  latest.size = 100
  assert.equal(inventory[0]!.size, 4)
  assert.throws(() => snapshotArtifacts([first, { ...first, sha256: 'invalid' }]), /invalid response artifacts/)
  for (const path of ['/outside', '../outside', 'folder/../outside', 'C:/outside', 'folder\\outside', 'folder//file', './report', 'bad\u0000name']) {
    assert.throws(() => snapshotArtifacts([{ ...first, path }]), /invalid response artifacts/)
  }
  assert.deepEqual(snapshotArtifacts([{ ...first, path: '报告/章节 1.txt' }]), [{ ...first, path: '报告/章节 1.txt' }])
})

it('restores prior artifact records without silently adding them to current delivery', async () => {
  let now = Date.now()
  const workStore = new MemoryWorkStore({}, () => now)
  const steps = new MemoryStepStore()
  const messages: AssistantMessage[] = []
  const service = new ControlPlaneService({ work: workStore, steps, sessions: new MemorySessionStore(), events: new MemoryEventStore(), actions: new MemoryActionLedger(),
    contextProvider: { loadContext: async () => ({ persona: { name: 'A', role: 'assistant', instructions: '' }, capabilities: [],
      messages: [{ ref: 'm', authorId: 'u', authorName: 'U', authorKind: 'human', body: 'Deliver the report', createdAt: 'now' }] }) },
    capabilityResolver: { resolve: async () => [] }, actionExecutor: { execute: async () => ({ ok: false }) },
    delivery: { onEvent: async () => {}, deliverMessage: async (_work, message) => { messages.push(message) } } })
  await service.enqueue({ id: 'artifacts', tenantId: 't', agentId: 'a', sessionId: 's', principalId: 'u', kind: 'turn', lane: 'interactive', triggerRef: 'm', meta: { text: 'Deliver the report' } })
  const first = (await service.claim('worker'))!
  const artifact = { path: 'report.txt', size: 4, mime: 'text/plain', sha256: 'a'.repeat(64) }
  await assert.rejects(service.recordEvent(first, { runId: first.id, seq: 1, kind: 'ipython.completed', stage: 'completed', visibility: 'internal',
    data: { artifacts: [{ ...artifact, path: '../outside' }] } }), /invalid kernel artifact event/)
  await service.recordEvent(first, { runId: first.id, seq: 1, kind: 'ipython.completed', stage: 'completed', visibility: 'internal',
    data: { callId: 'prior-cell', requestVersion: 1, output: 'prior output', artifacts: [artifact] } })
  await steps.save({ workId: first.id, fence: first.fence, leaseTokenHash: 'test' }, { id: 'prior-cell', kind: 'ipython', requestVersion: 1, input: { code: 'create()' }, output: 'prior output', artifacts: [artifact] })
  assert.deepEqual((await service.loadContext(first)).priorArtifacts, [])
  await workStore.requestPreempt(first.id)
  await service.yieldWork(first)
  now += 2000
  const resumed = (await service.claim('worker'))!
  assert.ok(resumed.fence > first.fence)
  assert.deepEqual((await service.loadContext(resumed)).priorArtifacts, [artifact])
  await assert.rejects(service.loadContext(first), /lease lost/)
  const host: HostPort = {
    claimWork: async () => null, heartbeat: work => service.heartbeat(work), loadContext: work => service.loadContext(work),
    executeAction: (work, action) => service.executeAction(work, action), emitEvent: (work, event) => service.recordEvent(work, event),
    loadSession: (work, key) => service.getSession(work, key), saveSession: async (work, session) => { session.revision = (await service.saveSession(work, session)).revision },
    commitResult: (work, message) => service.commitResult(work, message), completeWork: (work, completion) => service.complete(work, completion), yieldWork: work => service.yieldWork(work),
  }
  const model: ModelDriver = {
    run: async request => {
      assert.match(JSON.stringify(request.items), /report.txt/)
      assert.match(JSON.stringify(request.items), /not current delivery or proof of file availability/)
      const text = 'The previous report still needs file verification.'
      return { text, output: [{ role: 'assistant', content: text }], usage: { available: false, inputTokens: 0, outputTokens: 0 } }
    }, structured: async () => { throw new Error('unexpected') }, compact: async () => { throw new Error('unexpected') },
  }
  await new AgentRuntime(host, model, { execute: async () => { throw new Error('unexpected') } }).runWork(resumed)
  assert.equal(messages.length, 1)
  assert.deepEqual(messages[0]!.envelope!.artifacts, [])
})

it('freezes source versions, allows prose around citations and does not invent semantic support', () => {
  const items = [{ marker: 'S1', sourceId: 'source', sourceVersion: 'v1', chunkId: 'chunk', title: 'Title', excerpt: 'Only the first claim is supported.' }]
  const snapshot = snapshotEvidence('evidence-1', items)
  items[0]!.excerpt = 'Changed later'
  assert.equal(snapshot.items[0]!.excerpt, 'Only the first claim is supported.')
  const envelope = createResponseEnvelope('Explanation. [First claim](#cite-S1) Further discussion.',
    { status: 'partial', verification: 'not_run', requestVersion: 1 }, snapshot)
  assert.equal(envelope.citations[0]?.support, 'not_assessed')
  assert.deepEqual(envelope.citations[0]?.sources, [{ sourceId: 'source', sourceVersion: 'v1', chunkIds: ['chunk'] }])
  assert.deepEqual(responseSegments(envelope).map((part) => [part.type, part.text]), [
    ['text', 'Explanation. '], ['citation', 'First claim'], ['text', ' Further discussion.'],
  ])
  assert.throws(() => createResponseEnvelope('[Unknown](#cite-S2)', envelope.goalOutcome, snapshot), /unknown citation/)
  assert.throws(() => createResponseEnvelope('[Broken](#cite-S0)', envelope.goalOutcome, snapshot), /malformed/)
  assert.throws(() => snapshotEvidence('e', [items[0]!, { ...items[0]!, sourceVersion: 'v2', chunkId: 'other' }]), /conflicting/)
})

it('uses the original evidence across hops and rejects a tampered final envelope', async (t) => {
  const sessions = new MemorySessionStore()
  const messages: AssistantMessage[] = []
  let contextLoads = 0
  const service = new ControlPlaneService({
    work: new MemoryWorkStore(), sessions, events: new MemoryEventStore(), actions: new MemoryActionLedger(),
    contextProvider: { loadContext: async () => {
      contextLoads++
      return {
        persona: { name: 'A', role: 'assistant', instructions: '' }, capabilities: [],
        messages: [{ ref: 'm', authorId: 'u', authorName: 'U', authorKind: 'human', body: 'Explain with sources.', createdAt: 'now' }],
        evidence: [{ marker: contextLoads === 1 ? 'S1' : 'S2', sourceId: 'source', sourceVersion: contextLoads === 1 ? 'v1' : 'v2', chunkId: 'chunk', title: 'Title', excerpt: contextLoads === 1 ? 'ORIGINAL_EVIDENCE' : 'REPLACED_EVIDENCE' }],
      }
    } },
    capabilityResolver: { resolve: async () => [] }, actionExecutor: { execute: async () => ({ ok: false }) },
    delivery: { onEvent: async () => {}, deliverMessage: async (_work, message) => { messages.push(message) } },
  })
  await service.enqueue({ id: 'w', tenantId: 't', agentId: 'a', principalId: 'u', sessionId: 's', threadId: '', kind: 'turn', lane: 'interactive', triggerRef: 'm', meta: { text: 'Explain with sources.' } })
  const work = (await service.claim('worker'))!
  const host: HostPort = {
    claimWork: async () => null, heartbeat: (item) => service.heartbeat(item), loadContext: (item) => service.loadContext(item),
    executeAction: (item, action) => service.executeAction(item, action), emitEvent: (item, event) => service.recordEvent(item, event),
    loadSession: (item, key) => service.getSession(item, key), saveSession: async (item, session) => { session.revision = (await service.saveSession(item, session)).revision },
    commitResult: (item, message) => service.commitResult(item, message), completeWork: async () => {}, yieldWork: async () => {},
  }
  let calls = 0
  const model: ModelDriver = {
    run: async (request) => {
      calls++
      assert.match(JSON.stringify(request.items), /ORIGINAL_EVIDENCE/)
      assert.doesNotMatch(JSON.stringify(request.items), /REPLACED_EVIDENCE/)
      const text = calls === 1 ? '[Claim](#cite-S2)' : 'Explanation. [Claim](#cite-S1) More context.'
      return { text, output: [{ role: 'assistant', content: text }], usage: { available: false, inputTokens: 0, outputTokens: 0 } }
    }, structured: async () => { throw new Error('unexpected') }, compact: async () => { throw new Error('unexpected') },
  }
  await new AgentRuntime(host, model, { execute: async () => { throw new Error('unexpected') } }).runWork(work)
  assert.equal(calls, 2)
  assert.equal(messages.length, 1)
  await assert.rejects(service.commitResult(work, { ...messages[0]!, threadId: 'another-thread' }), /stream identity/)
  assert.equal(messages.length, 1)
  assert.equal(messages[0]!.envelope?.citations[0]?.sources[0]?.sourceVersion, 'v1')
  const unverified = structuredClone(messages[0]!)
  unverified.envelope!.goalOutcome = { status: 'satisfied', verification: 'passed', requestVersion: 1 }
  await assert.rejects(service.commitResult(work, unverified), /authoritative acceptance evidence/)
  assert.equal(messages.length, 1)
  assert.equal(messages[0]!.threadId, '')
  const tampered = structuredClone(messages[0]!)
  tampered.envelope!.citations[0]!.sources[0]!.sourceVersion = 'forged'
  await assert.rejects(service.commitResult(work, tampered), /inconsistent/)
  const { envelope: _envelope, ...withoutEnvelope } = messages[0]!
  await assert.rejects(service.commitResult(work, withoutEnvelope as AssistantMessage), /envelope is required/)
  await assert.rejects(service.commitResult(work, { ...messages[0]!, data: { goalOutcome: messages[0]!.envelope.goalOutcome } } as AssistantMessage), /stream identity/)
  assert.equal(messages.length, 1)
  const session = (await sessions.get(sessionKeyOf(work)))!
  assert.deepEqual(await service.getSession(work, session.key), session)
  await assert.rejects(service.getSession(work, 'other:a:s:-'), /outside/)
  await assert.rejects(service.getSession({ ...work, leaseToken: 'invalid' }, session.key), /lease lost/)
  await assert.rejects(service.saveSession(work, { ...session, tenantId: 'other' }), /invalid session/)
  await assert.rejects(service.saveSession(work, { ...session, threadId: 'other' }), /invalid session/)
  session.request!.originalText = 'rewritten request'
  await assert.rejects(service.saveSession(work, session), /invalid session record/)
  const contracted = (await service.getSession(work, session.key))!
  const contract = createTaskContract(contracted.request!.originalText, 1, { deliverables: ['Explain with sources'], constraints: [], actions: [], acceptance: ['Supported explanation'] })
  contracted.request!.contract = contract
  await service.saveSession(work, contracted)
  await assert.rejects(service.commitResult(work, messages[0]!), /inconsistent/)
  const withContract = structuredClone(messages[0]!)
  withContract.envelope!.taskContract = contract
  const replacedContract = structuredClone(withContract)
  replacedContract.envelope!.taskContract!.deliverables = ['Different deliverable']
  await assert.rejects(service.commitResult(work, replacedContract), /inconsistent/)
  assert.equal(messages.length, 1)
  const savedSession = (await sessions.get(sessionKeyOf(work)))!
  const missingSnapshot = t.mock.method(sessions, 'get', async () => null)
  await assert.rejects(service.commitResult(work, withContract), /saved request and evidence snapshot/)
  const { evidence: _evidence, ...requestWithoutEvidence } = savedSession.request!
  missingSnapshot.mock.mockImplementation(async () => ({ ...savedSession, request: requestWithoutEvidence as NonNullable<typeof savedSession.request> }))
  await assert.rejects(service.commitResult(work, withContract), /saved request and evidence snapshot/)
  missingSnapshot.mock.restore()
  await service.addSteer(work.id, 'Updated requirement before delivery')
  await assert.rejects(service.commitResult(work, withContract), /version is stale/)
  assert.equal(messages.length, 1)
})

it('promotes recorded research text into the next model input and validates final citations', async () => {
  const messages: AssistantMessage[] = []
  let completion: unknown
  const source = { text: 'Observed research finding.', finalUrl: 'https://example.com/paper', sha256: 'a'.repeat(64) }
  const service = new ControlPlaneService({ work: new MemoryWorkStore(), sessions: new MemorySessionStore(), events: new MemoryEventStore(), actions: new MemoryActionLedger(),
    contextProvider: { loadContext: async () => ({ persona: { name: 'A', role: 'assistant', instructions: '' }, capabilities: ['research'],
      messages: [{ ref: 'm', authorId: 'u', authorName: 'U', authorKind: 'human', body: 'Read the paper and cite its finding.', createdAt: 'now' }] }) },
    capabilityResolver: { resolve: async () => [{ name: 'research', methods: ['read'] }] }, actionExecutor: { execute: async () => ({ ok: true, value: source }) },
    delivery: { onEvent: async () => {}, deliverMessage: async (_work, message) => { messages.push(message) } } })
  await service.enqueue({ id: 'research', tenantId: 't', agentId: 'a', sessionId: 's', principalId: 'u', kind: 'turn', lane: 'interactive', triggerRef: 'm', meta: { text: 'Read the paper and cite its finding.' } })
  const work = (await service.claim('worker'))!
  const host: HostPort = {
    claimWork: async () => null, heartbeat: work => service.heartbeat(work), loadContext: work => service.loadContext(work),
    executeAction: (work, action) => service.executeAction(work, action), emitEvent: (work, event) => service.recordEvent(work, event),
    loadSession: (work, key) => service.getSession(work, key), saveSession: async (work, session) => { session.revision = (await service.saveSession(work, session)).revision },
    commitResult: (work, message) => service.commitResult(work, message), completeWork: (work, result) => { completion = result; return service.complete(work, result) }, yieldWork: work => service.yieldWork(work),
  }
  let calls = 0
  const model: ModelDriver = {
    run: async request => {
      calls++
      if (calls === 1) return { text: '', output: [{ type: 'function_call', callId: 'read', name: 'ipython', arguments: JSON.stringify({ code: 'host.research.read(url="https://example.com/paper")' }) }], usage: { available: false, inputTokens: 0, outputTokens: 0 } }
      assert.match(JSON.stringify(request.items), /Observed research finding/)
      assert.match(JSON.stringify(request.items), /S1/)
      const saved = (await service.getSession(work, '["t","a","s",null]'))!
      const altered = structuredClone(saved)
      altered.request!.evidence!.items[0]!.excerpt = 'Invented replacement'
      await assert.rejects(service.saveSession(work, altered), /evidence cannot be rewritten/)
      const forged = structuredClone(saved)
      forged.request!.evidence = appendResearchEvidence(saved.request!.evidence!, 'missing', { ok: true, value: { ...source, sha256: 'c'.repeat(64) } })
      await assert.rejects(service.saveSession(work, forged), /current research read intent/)
      const other = { runId: work.id, cellId: 'other', callIndex: 0, idempotencyKey: JSON.stringify([work.id, 'other', 0]), action: 'research.read', args: { url: source.finalUrl } }
      const receipt = await service.executeAction(work, other)
      const invented = structuredClone(saved)
      invented.request!.evidence = appendResearchEvidence(saved.request!.evidence!, other.idempotencyKey, { ...receipt, value: { ...source, sha256: 'b'.repeat(64) } })
      invented.request!.evidence.items.at(-1)!.excerpt = 'Invented addition'
      await assert.rejects(service.saveSession(work, invented), /does not match recorded/)
      const text = '[Observed research finding](#cite-S1).'
      return { text, output: [{ role: 'assistant', content: text }], usage: { available: false, inputTokens: 0, outputTokens: 0 } }
    }, structured: async () => { throw new Error('unexpected') }, compact: async () => { throw new Error('unexpected') },
  }
  await new AgentRuntime(host, model, { execute: async (_work, _run, _cell, _code, _signal, options) => {
    const action = { runId: work.id, cellId: 'read', callIndex: 0, idempotencyKey: JSON.stringify([work.id, 'read', 0]), action: 'research.read', args: { url: source.finalUrl } }
    await options?.onHostAction?.({ stage: 'started', action })
    const result = await service.executeAction(work, action)
    await options?.onHostAction?.({ stage: 'completed', action, result })
    return { executionId: 'read', stdout: '', stderr: '', result: null, durationMs: 1, truncated: false, artifacts: [], directives: [] }
  } }).runWork(work)
  assert.equal(calls, 2)
  assert.equal(messages.length, 1, JSON.stringify(completion))
  assert.deepEqual(messages[0]!.envelope!.citations[0]!.sources, [{ sourceId: source.finalUrl, sourceVersion: `sha256:${source.sha256}`, chunkIds: [JSON.stringify([work.id, 'read', 0])] }])
  assert.equal(messages[0]!.envelope!.citations[0]!.support, 'not_assessed')
})

it('keeps citation markers stable on repeated reads and preserves earlier source versions', () => {
  const initial = snapshotEvidence('initial', [])
  const firstResult = { ok: true, value: { text: 'First version', finalUrl: 'https://example.com/source', sha256: 'a'.repeat(64) } }
  const first = appendResearchEvidence(initial, 'read-1', firstResult)
  assert.equal(appendResearchEvidence(first, 'read-1', firstResult), first)
  assert.equal(appendResearchEvidence(first, 'read-2', firstResult), first)
  const next = appendResearchEvidence(first, 'read-3', { ok: true, value: { ...firstResult.value, sha256: 'b'.repeat(64), text: 'Revised version' } })
  assert.deepEqual(next.items.map(item => [item.marker, item.excerpt, item.sourceVersion]), [
    ['S1', 'First version', `sha256:${'a'.repeat(64)}`], ['S2', 'Revised version', `sha256:${'b'.repeat(64)}`],
  ])
  assert.deepEqual(first.items, next.items.slice(0, 1))
  firstResult.value.text = 'Caller mutation'
  assert.equal(first.items[0]!.excerpt, 'First version')
  assert.equal(appendResearchEvidence(next, 'failed', { ok: false, executionState: 'unknown' }), next)
  const envelope = createResponseEnvelope('[Earlier](#cite-S1) [Later](#cite-S2)', { status: 'partial', verification: 'not_run', requestVersion: 1 }, next)
  assert.deepEqual(envelope.citations.map(citation => citation.sources[0]!.sourceVersion), [`sha256:${'a'.repeat(64)}`, `sha256:${'b'.repeat(64)}`])
})

it('preserves truncation limits when promoting a research excerpt', () => {
  const result = { ok: true, value: { text: 'Only the beginning.', finalUrl: 'https://example.com/long', sha256: 'a'.repeat(64), truncated: true } }
  const snapshot = appendResearchEvidence(snapshotEvidence('initial', []), 'read', result)
  assert.equal(snapshot.items[0]!.truncated, true)
  const envelope = createResponseEnvelope('[Excerpt](#cite-S1)', { status: 'partial', verification: 'not_run', requestVersion: 1 }, snapshot)
  const [segment] = responseSegments(envelope)
  assert.ok(segment?.type === 'citation')
  assert.equal(segment.annotation.sources[0]!.truncated, true)
  assert.match(JSON.stringify(evidenceItems(snapshot)), /do not infer coverage of the full source/)
  assert.equal(appendResearchEvidence(snapshot, 'repeat', result), snapshot)
  assert.throws(() => snapshotEvidence('bad', [{ ...snapshot.items[0]!, truncated: 'yes' as unknown as boolean }]), /truncation flag/)
})
