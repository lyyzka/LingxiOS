import { snapshotEvidence } from '../src/context/evidence.js'
import { createTaskContract } from '../src/context/task-contract.js'
import assert from 'node:assert/strict'
import { it } from 'node:test'
import type { HostPort } from '../src/host/port.js'
import type { ModelDriver } from '../src/model/driver.js'
import type { KernelExecutor } from '../src/kernel/manager.js'
import type { ModelItem, SessionRecord, TurnContext, WorkCompletion } from '../src/protocol/types.js'
import { AgentRuntime } from '../src/runtime/runtime.js'
import { ApprovalPendingError, ModelDriverError } from '../src/errors.js'

for (const format of ['object', 'plain text'] as const) it(`preserves ${format} content and observed artifacts when assessment correction is exhausted`, async () => {
  const work: TurnContext['work'] = { id: 'w', tenantId: 't', agentId: 'a', sessionId: 's', kind: 'turn', lane: 'interactive', triggerRef: 'm', fence: 1, homeEpoch: 1, leaseToken: 'token' }
  const artifact = { path: 'answer.json', size: 12, mime: 'application/json', sha256: 'a'.repeat(64) }
  let turns = 0, committed = false
  const host: HostPort = { claimWork: async () => null, heartbeat: async () => ({ ok: true }),
    loadContext: async () => ({ work, persona: { name: 'A', role: '', instructions: '' }, capabilities: [],
      messages: [{ ref: 'm', authorId: 'u', authorName: 'U', authorKind: 'human', body: 'Create a file.', createdAt: 'now' }] }),
    executeAction: async () => ({ ok: true }), loadSession: async () => null, saveSession: async () => {}, emitEvent: async () => {},
    commitMessage: async (_work, message) => {
      assert.equal(message.envelope.goalOutcome.status, 'partial')
      assert.equal(message.body, 'File created.')
      assert.deepEqual(message.envelope.artifacts, [artifact])
      assert.equal(message.envelope.assessment, undefined)
      assert.match(JSON.stringify(message.envelope.goalOutcome.gaps), /protocol correction exhausted/)
      committed = true
    }, completeWork: async () => {}, yieldWork: async () => {} }
  const unexpected = async () => { throw new Error('unexpected auxiliary call') }
  const model: ModelDriver = { structured: unexpected, compact: unexpected, run: async () => {
    const usage = { available: false, inputTokens: 0, outputTokens: 0 }
    if (++turns === 1) return { usage, text: '', output: [{ type: 'function_call', callId: 'create', name: 'ipython', arguments: '{"code":"create_file()"}' }] }
    const candidate = format === 'plain text' ? 'File created.' : JSON.stringify({ body: 'File created.', status: 'satisfied', checks: [], gaps: [] })
    return { usage, text: candidate, output: [{ role: 'assistant', content: candidate }], finalCandidate: candidate }
  } }
  await new AgentRuntime(host, model, { execute: async () => ({ executionId: 'execution', stdout: '', stderr: '', result: null,
    artifacts: [artifact], directives: [], truncated: false, durationMs: 1 }) }).runWork(work)
  assert.equal(turns, 3)
  assert.equal(committed, true)
})

it('continues an actionable partial candidate before committing its result', async () => {
  const work: TurnContext['work'] = { id: 'w', tenantId: 't', agentId: 'a', sessionId: 's', kind: 'turn', lane: 'interactive', triggerRef: 'm', fence: 1, homeEpoch: 1, leaseToken: 'token' }
  let turns = 0, executed = 0, body = ''
  const host: HostPort = { claimWork: async () => null, heartbeat: async () => ({ ok: true }),
    loadContext: async () => ({ work, persona: { name: 'A', role: '', instructions: '' }, capabilities: [],
      messages: [{ ref: 'm', authorId: 'u', authorName: 'U', authorKind: 'human', body: 'Create a file.', createdAt: 'now' }] }),
    executeAction: async () => ({ ok: true, value: { requestVersion: 1, pending: [], truncated: false } }),
    loadSession: async () => null, saveSession: async () => {}, emitEvent: async () => {},
    commitMessage: async (_work, message) => { assert.equal(executed, 1); body = message.body },
    completeWork: async () => {}, yieldWork: async () => {} }
  const unexpected = async () => { throw new Error('unexpected auxiliary call') }
  const model: ModelDriver = { structured: unexpected, compact: unexpected, run: async request => {
    turns++
    const usage = { available: false, inputTokens: 0, outputTokens: 0 }
    if (turns === 2) {
      assert.match(JSON.stringify(request.items), /Continue any work/)
      return { text: '', output: [{ type: 'function_call', callId: 'create', name: 'ipython', arguments: '{"code":"create_file()"}' }], usage }
    }
    return { text: '', output: [], finalCandidate: JSON.stringify({ body: turns === 1 ? 'I will create it.' : 'File created.',
      status: turns === 1 ? 'partial' : 'satisfied', gaps: turns === 1 ? ['File has not been created yet'] : [],
      checks: [{ requirement: 'Create a file.', status: turns === 1 ? 'unknown' : 'met', basis: turns === 1 ? 'Pending execution' : 'Execution completed' }] }), usage }
  } }
  await new AgentRuntime(host, model, { execute: async () => {
    executed++
    return { executionId: 'execution', stdout: '', stderr: '', result: null, artifacts: [], directives: [], truncated: false, durationMs: 1 }
  } }).runWork(work)
  assert.equal(turns, 3)
  assert.equal(body, 'File created.')
})

it('drops optional recalled memory before allowing it to crowd out the original request', async () => {
  const work: TurnContext['work'] = { id: 'w', tenantId: 't', agentId: 'a', sessionId: 's', kind: 'turn', lane: 'interactive', triggerRef: 'm', fence: 1, homeEpoch: 1, leaseToken: 'token' }
  const original = 'Original requirement '.repeat(200)
  let committed = false
  let omitted = false
  const host: HostPort = {
    claimWork: async () => null, heartbeat: async () => ({ ok: true }),
    loadContext: async () => ({ work, persona: { name: 'A', role: '', instructions: '' }, capabilities: [],
      messages: [{ ref: 'm', authorId: 'u', authorName: 'U', authorKind: 'human', body: original, createdAt: 'now' }],
      memory: { id: 'large-memory', status: 'available', omitted: 0, items: [{ body: 'optional memory '.repeat(750) }] } }),
    executeAction: async (_work, action) => {
      assert.equal(action.action, 'task.inspect')
      return { ok: true, value: { requestVersion: 1, pending: [], truncated: false } }
    }, loadSession: async () => null, saveSession: async () => {},
    emitEvent: async (_work, event) => { if (event.kind === 'model.started') { omitted = event.data['memoryOmittedForBudget'] === true; assert.equal(event.data['memorySnapshot'], undefined) } },
    commitMessage: async (_work, message) => {
      assert.equal(message.body, 'Answer.')
      assert.deepEqual(message.envelope.goalOutcome, { status: 'satisfied', verification: 'not_run', requestVersion: 1 })
      committed = true
    }, completeWork: async () => {}, yieldWork: async () => {},
  }
  const unexpected = async () => { throw new Error('unexpected auxiliary call') }
  const model: ModelDriver = {
    run: async request => {
      assert.ok(request.items.some(item => 'role' in item && item.content === original))
      assert.doesNotMatch(JSON.stringify(request.items), /optional memory/)
      return { text: '', output: [], finalCandidate: JSON.stringify({
        body: 'Answer.', status: 'satisfied', gaps: [], checks: [{ requirement: 'Original requirement', status: 'met', basis: 'Answer supplied.' }],
      }), usage: { available: false, inputTokens: 0, outputTokens: 0 } }
    }, structured: unexpected, compact: unexpected,
  }
  await new AgentRuntime(host, model, { execute: unexpected }, { compaction: { contextWindowTokens: 30_000 } }).runWork(work)
  assert.equal(committed, true)
  assert.equal(omitted, true)
})

it('reviews complex candidates against original requirements, bounds corrections and discards reviews invalidated by steering', async () => {
  for (const mode of ['fixed', 'exhausted', 'steered', 'invalid'] as const) {
    const work: TurnContext['work'] = { id: 'w', tenantId: 't', agentId: 'a', sessionId: 's', kind: 'turn', lane: 'interactive', triggerRef: 'm', fence: 1, homeEpoch: 1, leaseToken: 'token' }
    const originalText = 'Compare both options and include costs.'
    const resourceChecks = [{ actionKey: 'recorded-check', result: { ok: true, value: {
      scope: 'observed_resource_fields', requestVersion: 1, action: 'resource.read', args: { id: 'r' },
      status: 'pass', expected: { cost: 10 }, observed: { cost: 10 },
    } } }]
    let session: SessionRecord = { key: '["t","a","s",null]', tenantId: 't', agentId: 'a', sessionId: 's', revision: 0, compactionEpoch: 0,
      history: [], appliedWorkIds: ['w'], request: { version: 1, workId: 'w', tenantId: 't', sessionId: 's', authorId: 'u', sourceRef: 'm',
        originalText, revisions: [], attachments: [], evidence: snapshotEvidence('e', []), resourceChecks,
        contract: createTaskContract(originalText, 1, { deliverables: ['Compare options'], constraints: [], actions: [], acceptance: ['Comparison provided'] }) } }
    let reviews = 0
    let turns = 0
    let body = ''
    let completion: WorkCompletion | undefined
    const deltas: string[] = []
    const host: HostPort = {
      claimWork: async () => null,
      heartbeat: async () => ({ ok: true, steer: mode === 'steered' && reviews ? [{ id: 'r', text: 'Just say hello.', createdAt: 'now' }] : [] }),
      loadContext: async () => ({ work, persona: { name: 'A', role: '', instructions: '' }, capabilities: [],
        messages: [{ ref: 'm', authorId: 'u', authorName: 'U', authorKind: 'human', body: originalText, createdAt: 'now' }] }),
      executeAction: async (_work, action) => {
        if (action.action === 'task.inspect') return { ok: true, value: { requestVersion: session.request!.revisions.length + 1, pending: [], truncated: false } }
        assert.equal(action.action, 'task.check_resource')
        return { ok: true, value: { ...resourceChecks[0]!.result.value, status: 'fail', observed: { cost: 20 } } }
      }, loadSession: async () => structuredClone(session),
      saveSession: async (_work, value) => { session = structuredClone(value) },
      emitEvent: async (_work, event) => { if (event.kind === 'model.delta') deltas.push(String(event.data['delta'])) },
      commitMessage: async (_work, message) => {
        body = message.body
        assert.notEqual(message.envelope?.goalOutcome.verification, 'passed')
        assert.deepEqual(message.envelope?.resourceChecks?.[0], resourceChecks[0])
        assert.equal(message.envelope?.resourceChecks?.length, mode === 'fixed' || mode === 'exhausted' ? 3 : 2)
      },
      completeWork: async (_work, value) => { completion = value }, yieldWork: async () => {},
    }
    const usage = { available: false, inputTokens: 0, outputTokens: 0 }
    const model: ModelDriver = {
      run: async request => {
        turns++
        assert.match(JSON.stringify(request.items), /Recorded resource observations/)
        if (turns === 2 && mode !== 'steered') assert.match(JSON.stringify(request.items), /include costs/)
        const text = turns === 1 ? 'Comparison without costs.' : mode === 'steered' ? 'Hello.' : 'Comparison with costs.'
        return { text: '', output: [], finalCandidate: JSON.stringify({
          body: text, status: 'satisfied', gaps: [], checks: [{ requirement: mode === 'steered' && turns === 2 ? 'Just say hello.' : originalText,
            status: 'met', basis: text }],
        }), usage }
      },
      structured: async request => {
        reviews++
        assert.equal((request.input as { originalText: string }).originalText, originalText)
        const observations = (request.input as { resourceChecks: typeof resourceChecks }).resourceChecks
        assert.deepEqual(observations[0], resourceChecks[0])
        assert.equal(observations.length, reviews + 1)
        assert.deepEqual(observations.at(-1)?.result.value.observed, { cost: 20 })
        assert.doesNotMatch(request.instructions, /Mission/)
        return { value: { missing: mode === 'fixed' && reviews === 2 ? []
          : [{ quote: mode === 'invalid' ? 'Invented requirement' : 'include costs', reason: 'Costs are missing' }] }, model: 'fixture', usage }
      }, compact: async () => { throw new Error('unexpected compaction') },
    }
    await new AgentRuntime(host, model, { execute: async () => { throw new Error('unexpected kernel') } }).runWork(work)
    assert.equal(completion?.status, 'completed')
    assert.deepEqual(deltas, [body])
    assert.equal(reviews, mode === 'fixed' || mode === 'exhausted' ? 2 : 1)
    if (mode === 'fixed') {
      assert.equal(completion?.goalOutcome?.status, 'partial')
      assert.equal(body, 'Comparison with costs.')
      assert.match(JSON.stringify(completion?.goalOutcome?.gaps), /did not confirm the expected fields/)
    }
    if (mode === 'exhausted') {
      assert.equal(body, 'Comparison with costs.')
      assert.match(completion?.goalOutcome?.gaps?.[0] ?? '', /Content acceptance correction budget exhausted/)
      assert.match(JSON.stringify(completion?.goalOutcome?.gaps), /did not confirm the expected fields/)
      assert.match(JSON.stringify(completion?.goalOutcome?.gaps), /include costs.*Costs are missing/)
    }
    if (mode === 'steered') {
      assert.equal(completion?.goalOutcome?.status, 'satisfied')
      assert.equal(body, 'Hello.')
      assert.equal(completion?.goalOutcome?.requestVersion, 2)
      assert.doesNotMatch(JSON.stringify(completion?.goalOutcome?.gaps ?? []), /Costs are missing|did not confirm the expected fields/)
    }
    if (mode === 'invalid') assert.match(JSON.stringify(completion?.goalOutcome?.gaps), /unavailable or returned invalid/)
  }
})

it('preserves history across prompt upgrades and exposes assigned action receipts with progress text', async () => {
  const context: TurnContext = {
    work: { id: 'w', tenantId: 't', agentId: 'a', sessionId: 's', kind: 'turn', lane: 'interactive', triggerRef: 'm', fence: 1, homeEpoch: 1, leaseToken: 'token' },
    persona: { name: 'Assistant', role: 'assistant', instructions: '' }, capabilities: ['files'], messages: [{ ref: 'm', authorId: 'u', authorName: 'User', authorKind: 'human', body: 'Save the requested file.', createdAt: 'now' }],
  }
  const session: SessionRecord = {
    key: '[\"t\",\"a\",\"s\",null]', tenantId: 't', agentId: 'a', sessionId: 's', history: [{ role: 'user', content: 'Preserve this requirement' }], appliedWorkIds: ['w'], revision: 1, compactionEpoch: 0,
    promptContext: { version: 2, epoch: 0, assembledAt: '', systemInstructions: '', persona: context.persona, capabilities: [], sourceVersions: { promptContract: 'old' } },
  }
  let completion: WorkCompletion | undefined
  let checkpoint: SessionRecord | undefined
  let calls = 0
  const host: HostPort = {
    claimWork: async () => null, heartbeat: async () => ({ ok: true }), loadContext: async () => context,
    executeAction: async () => ({ ok: true }), emitEvent: async () => {}, loadSession: async () => session,
    saveSession: async (_work, value) => { checkpoint = structuredClone(value) }, commitMessage: async (_work, message) => { assert.equal(message.body, 'Saved.') },
    completeWork: async (_work, result) => { completion = result }, yieldWork: async () => {},
  }
  const model: ModelDriver = {
    run: async (request) => {
      assert.deepEqual(request.items[0], { role: 'user', content: 'Preserve this requirement' })
      calls++
      const output: ModelItem[] = calls === 1
        ? [{ role: 'assistant', content: 'Saving.' }, { type: 'function_call', callId: 'c', name: 'ipython', arguments: '{"code":"result = host.files.save()"}' }]
        : [{ role: 'assistant', content: 'Saved.' }]
      if (calls === 2) {
        assert.deepEqual(request.items.slice(0, checkpoint!.history.length), checkpoint!.history)
        const receipt = request.items.find(item => 'type' in item && item.type === 'function_call_output')!
        assert.ok('type' in receipt && receipt.type === 'function_call_output')
        assert.deepEqual(JSON.parse(receipt.output).receipts, [{ action: 'files.save', idempotencyKey: '[\"w\",\"hop-1\",0]', result: { ok: true, value: { id: 'document' } } }])
      }
      return { text: calls === 1 ? 'Saving.' : 'Saved.', output, usage: { available: false, inputTokens: 0, outputTokens: 0 } }
    }, structured: async () => { throw new Error('unexpected') }, compact: async () => { throw new Error('unexpected') },
  }
  const kernels: KernelExecutor = { execute: async (_work, _run, _cell, _code, _signal, options) => {
    assert.equal(_cell, 'c')
    assert.deepEqual(checkpoint?.history.at(-1), { type: 'function_call', callId: 'c', name: 'ipython', arguments: '{"code":"result = host.files.save()"}' })
    const action = { runId: 'w', cellId: 'hop-1', callIndex: 0, action: 'files.save', args: {}, idempotencyKey: '[\"w\",\"hop-1\",0]' }
    await options?.onHostAction?.({ stage: 'started', action })
    await options?.onHostAction?.({ stage: 'completed', action, result: { ok: true, value: { id: 'document' } } })
    return { executionId: 'cell', stdout: '', stderr: '', result: null, durationMs: 1, truncated: false, artifacts: [], directives: [] }
  } }
  await new AgentRuntime(host, model, kernels).runWork(context.work)
  assert.deepEqual(completion, { status: 'completed', resultText: 'Saved.', goalOutcome: { status: 'partial', verification: 'not_run', requestVersion: 1, gaps: ['Goal acceptance has not been checked'] } })
  assert.equal(calls, 2)
  session.history.push({ type: 'function_call', callId: 'interrupted', name: 'ipython', arguments: '{"code":"host.files.save()"}' })
  await new AgentRuntime(host, model, kernels).runWork({ ...context.work, fence: 2 })
  assert.equal(calls, 2)
  assert.equal(completion?.status, 'failed')
  assert.match(completion?.error ?? '', /unresolved tool execution checkpoint/)
  host.recoverCell = async () => [{ action: 'files.save', idempotencyKey: '["w","interrupted",0]', result: {
    ok: false, executionState: 'unknown', error: 'receipt missing',
  } }]
  await new AgentRuntime(host, model, kernels).runWork({ ...context.work, fence: 3 })
  assert.equal(calls, 3)
  assert.equal(completion?.status, 'completed')
})

it('does not spend model-correction budget on provider failures', async () => {
  const work: TurnContext['work'] = { id: 'w', tenantId: 't', agentId: 'a', sessionId: 's', kind: 'turn', lane: 'interactive', triggerRef: 'm', fence: 1, homeEpoch: 1, leaseToken: 'token', meta: { text: 'answer' } }
  let calls = 0
  let completion: WorkCompletion | undefined
  const host: HostPort = {
    claimWork: async () => null, heartbeat: async () => ({ ok: true }),
    loadContext: async () => ({ work, persona: { name: '', role: '', instructions: '' }, capabilities: [],
      messages: [{ ref: 'm', authorId: 'u', authorName: 'U', authorKind: 'human', body: 'answer', createdAt: 'now' }] }),
    executeAction: async () => ({ ok: true }), emitEvent: async () => {}, loadSession: async () => null,
    saveSession: async () => {}, commitMessage: async () => {}, yieldWork: async () => {},
    completeWork: async (_work, value) => { completion = value },
  }
  const unavailable = async () => { throw new Error('unexpected') }
  const model: ModelDriver = {
    run: async () => { calls++; throw new ModelDriverError('provider unavailable', { kind: 'provider', finishReasons: [] }) },
    structured: unavailable, compact: unavailable,
  }
  await new AgentRuntime(host, model, { execute: unavailable }).runWork(work)
  assert.equal(calls, 1)
  assert.equal(completion?.status, 'failed')
})

it('records approval and input waiting without claiming goal completion or invented delegation', async () => {
  for (const mode of ['approval', 'user', 'handoff'] as const) {
    const work: TurnContext['work'] = { id: 'w', tenantId: 't', agentId: 'a', sessionId: 's', kind: 'turn', lane: 'interactive', triggerRef: 'm', fence: 1, homeEpoch: 1, leaseToken: 'token' }
    let completion: WorkCompletion | undefined
    const host: HostPort = {
      claimWork: async () => null, heartbeat: async () => ({ ok: true }),
      loadContext: async () => ({ work, persona: { name: '', role: '', instructions: '' }, capabilities: [], messages: [{ ref: 'm', authorId: 'u', authorName: 'User', authorKind: 'human', body: 'Save the requested file.', createdAt: 'now' }] }),
      executeAction: async () => ({ ok: true }), emitEvent: async (_work, event) => { assert.notEqual(event.kind, 'run.completed') },
      loadSession: async () => null, saveSession: async () => {},
      commitMessage: async () => { throw new Error('waiting must not commit a final answer') },
      completeWork: async (_work, result) => { completion = result }, yieldWork: async () => {},
    }
    const model: ModelDriver = {
      run: async () => ({ text: '', output: [{ type: 'function_call', callId: 'c', name: 'ipython', arguments: '{"code":"1"}' }], usage: { available: false, inputTokens: 0, outputTokens: 0 } }),
      structured: async () => { throw new Error('unexpected') }, compact: async () => { throw new Error('unexpected') },
    }
    const kernels: KernelExecutor = { execute: async () => {
      if (mode === 'approval') throw new ApprovalPendingError('approval-id', 'cell')
      return { executionId: 'cell', stdout: '', stderr: '', result: null, durationMs: 1, truncated: false, artifacts: [], directives: [{ type: 'defer', reason: mode }] }
    } }
    await new AgentRuntime(host, model, kernels).runWork(work)
    assert.equal(completion?.status, 'completed')
    assert.equal(completion?.goalOutcome?.status, mode === 'approval' ? 'awaiting_approval' : mode === 'user' ? 'awaiting_input' : 'blocked')
    assert.equal(completion?.goalOutcome?.verification, 'not_run')
    if (completion?.goalOutcome?.status === 'awaiting_approval') assert.equal(completion.goalOutcome.approvalId, 'approval-id')
  }
})


it('discards an unexecuted model candidate when steering arrives during generation', async () => {
  const work = { id: 'w', tenantId: 't', agentId: 'a', sessionId: 's', kind: 'turn', lane: 'interactive' as const, triggerRef: 'm', fence: 1, homeEpoch: 1, leaseToken: 'token' }
  let generated = false
  let calls = 0
  let committed = ''
  let saved: SessionRecord | undefined = {
    key: '[\"t\",\"a\",\"s\",null]', tenantId: 't', agentId: 'a', sessionId: 's', revision: 1, compactionEpoch: 0, history: [], appliedWorkIds: ['w'],
    request: { version: 1, workId: 'w', tenantId: 't', sessionId: 's', authorId: 'u', sourceRef: 'm', evidence: snapshotEvidence('w:evidence:1', []), attachments: [], originalText: 'Original', revisions: [],
      contract: createTaskContract('Original', 1, { deliverables: ['OBSOLETE_DELIVERABLE'], constraints: [], actions: [], acceptance: ['Original condition'] }) },
  }
  const host: HostPort = {
    claimWork: async () => null,
    heartbeat: async () => ({ ok: true, steer: generated ? [{ id: 'revision', text: 'Use the new requirement', createdAt: 'now' }] : [] }),
    loadContext: async () => ({ work, persona: { name: 'A', role: 'assistant', instructions: '' }, capabilities: [], messages: [{ ref: 'm', authorId: 'u', authorName: 'U', authorKind: 'human', body: 'Original', createdAt: 'now' }] }),
    executeAction: async () => { throw new Error('old action must not execute') }, emitEvent: async () => {},
    loadSession: async () => saved ? structuredClone(saved) : null, saveSession: async (_work, session) => { saved = structuredClone(session) },
    commitMessage: async (_work, message) => { committed = message.body }, completeWork: async () => {}, yieldWork: async () => {},
  }
  const model: ModelDriver = {
    run: async request => {
      calls++
      if (calls === 1) {
        assert.match(JSON.stringify(request.items), /OBSOLETE_DELIVERABLE/)
        generated = true
        return { text: '', output: [{ type: 'function_call', callId: 'old', name: 'ipython', arguments: '{"code":"raise Exception()"}' }], usage: { available: false, inputTokens: 0, outputTokens: 0 } }
      }
      assert.doesNotMatch(JSON.stringify(request.items), /OBSOLETE_DELIVERABLE/)
      assert.match(JSON.stringify(request.items), /Use the new requirement/)
      assert.equal(JSON.stringify(request.items).split('Use the new requirement').length - 1, 1)
      return { text: 'Revised answer', output: [{ role: 'assistant', content: 'Revised answer' }], usage: { available: false, inputTokens: 0, outputTokens: 0 } }
    }, structured: async () => { throw new Error('unexpected') }, compact: async () => { throw new Error('unexpected') },
  }
  await new AgentRuntime(host, model, { execute: async () => { throw new Error('old code must not execute') } }).runWork(work)
  assert.equal(calls, 2)
  assert.equal(committed, 'Revised answer')
  assert.equal(saved?.request?.revisions.length, 1)
  assert.equal(saved?.request?.contract, undefined)
  const revisions = structuredClone(saved?.request?.revisions)
  await new AgentRuntime(host, model, { execute: async () => { throw new Error('old code must not execute') } }).runWork({ ...work, fence: 2 })
  assert.equal(calls, 3)
  assert.deepEqual(saved?.request?.revisions, revisions)
})

it('commits a bounded partial delivery on hop exhaustion, retaining artifacts and honoring late signals', async () => {
  for (const mode of ['normal', 'steer', 'cancel', 'lease_lost'] as const) {
    const work: TurnContext['work'] = { id: 'w', tenantId: 't', agentId: 'a', sessionId: 's', kind: 'turn', lane: 'interactive', triggerRef: 'm', fence: 1, homeEpoch: 1, leaseToken: 'token' }
    let executed = false
    let committed = false
    let calls = 0
    let saved: SessionRecord | undefined
    let completion: WorkCompletion | undefined
    const artifact = { path: 'report.txt', size: 4, mime: 'text/plain', sha256: 'a'.repeat(64) }
    const host: HostPort = {
      claimWork: async () => null,
      heartbeat: async () => ({ ok: !(executed && mode === 'lease_lost'), cancelRequested: executed && mode === 'cancel',
        steer: executed && mode === 'steer' ? [{ id: 'r', text: 'Changed requirement', createdAt: 'now' }] : [] }),
      loadContext: async () => ({ work, persona: { name: 'A', role: 'assistant', instructions: '' }, capabilities: [],
        messages: [{ ref: 'm', authorId: 'u', authorName: 'U', authorKind: 'human', body: 'Create the requested report', createdAt: 'now' }] }),
      executeAction: async () => { throw new Error('unexpected') }, emitEvent: async () => {},
      loadSession: async () => null, saveSession: async (_work, session) => { saved = structuredClone(session) },
      commitMessage: async (_work, message) => {
        committed = true
        assert.deepEqual(saved?.history.at(-1), { role: 'assistant', content: message.body })
        assert.match(JSON.stringify(saved?.history), /private tool output/)
        assert.doesNotMatch(message.body, /private tool output/)
        assert.deepEqual(message.envelope?.artifacts, [artifact])
        assert.equal(message.envelope?.requestVersion, mode === 'steer' ? 2 : 1)
        assert.equal(message.envelope?.goalOutcome.status, 'partial')
        assert.equal(message.envelope?.goalOutcome.verification, 'not_run')
        assert.match(message.envelope?.goalOutcome.gaps?.[0] ?? '', /budget exhausted/)
      },
      completeWork: async (_work, result) => { completion = result }, yieldWork: async () => {},
    }
    const model: ModelDriver = {
      run: async () => { calls++; return { text: '', output: [{ type: 'function_call', callId: 'c', name: 'ipython', arguments: '{"code":"1"}' }], usage: { available: false, inputTokens: 0, outputTokens: 0 } } },
      structured: async () => { throw new Error('no extra grader') }, compact: async () => { throw new Error('unexpected') },
    }
    const kernels: KernelExecutor = { execute: async () => {
      executed = true
      return { executionId: 'cell', stdout: 'private tool output', stderr: '', result: null, durationMs: 1, truncated: false, artifacts: [artifact], directives: [] }
    } }
    assert.throws(() => new AgentRuntime(host, model, kernels, { maxHops: 0 }), /positive integer/)
    await new AgentRuntime(host, model, kernels, { maxHops: 1 }).runWork(work)
    assert.equal(calls, 1)
    assert.equal(committed, mode === 'normal' || mode === 'steer')
    assert.equal(completion?.status, mode === 'cancel' ? 'cancelled' : mode === 'lease_lost' ? undefined : 'completed')
    if (mode === 'steer') assert.equal(saved?.request?.revisions[0]?.text, 'Changed requirement')
  }
})

it('preserves unknown action outcomes in public events and model receipts', async () => {
  const work: TurnContext['work'] = { id: 'w', tenantId: 't', agentId: 'a', sessionId: 's', kind: 'turn', lane: 'interactive', triggerRef: 'm', fence: 1, homeEpoch: 1, leaseToken: 'token' }
  const result = { ok: false, executionState: 'unknown' as const, error: 'receipt unavailable' }
  let eventResult: unknown
  let calls = 0
  let completion: WorkCompletion | undefined
  const host: HostPort = {
    claimWork: async () => null, heartbeat: async () => ({ ok: true }),
    loadContext: async () => ({ work, persona: { name: 'A', role: 'assistant', instructions: '' }, capabilities: ['files'], messages: [{ ref: 'm', authorId: 'u', authorName: 'User', authorKind: 'human', body: 'Save the requested file.', createdAt: 'now' }] }),
    executeAction: async () => result, emitEvent: async (_work, event) => { if (event.kind === 'tool.completed') eventResult = event.data['result'] },
    loadSession: async () => null, saveSession: async () => {}, commitMessage: async () => {},
    completeWork: async (_work, value) => { completion = value }, yieldWork: async () => {},
  }
  const model: ModelDriver = {
    run: async request => {
      if (++calls === 1) return { text: '', output: [{ type: 'function_call', callId: 'c', name: 'ipython', arguments: '{"code":"host.files.save()"}' }], usage: { available: false, inputTokens: 0, outputTokens: 0 } }
      const output = request.items.find(item => 'type' in item && item.type === 'function_call_output')
      assert.ok(output && 'output' in output)
      assert.deepEqual(JSON.parse(output.output).receipts[0].result, result)
      return { text: 'The save outcome is unknown.', output: [{ role: 'assistant', content: 'The save outcome is unknown.' }], usage: { available: false, inputTokens: 0, outputTokens: 0 } }
    }, structured: async () => { throw new Error('unexpected') }, compact: async () => { throw new Error('unexpected') },
  }
  const kernels: KernelExecutor = { execute: async (_work, _run, _cell, _code, _signal, options) => {
    const action = { runId: 'w', cellId: 'hop-1', callIndex: 0, action: 'files.save', args: {}, idempotencyKey: '[\"w\",\"hop-1\",0]' }
    await options?.onHostAction?.({ stage: 'started', action })
    await options?.onHostAction?.({ stage: 'completed', action, result })
    return { executionId: 'cell', stdout: '', stderr: '', result: null, durationMs: 1, truncated: false, artifacts: [], directives: [] }
  } }
  await new AgentRuntime(host, model, kernels).runWork(work)
  assert.equal(completion?.status, 'completed')
  assert.deepEqual(eventResult, { status: 'unknown', error: 'receipt unavailable', reconciliationRequired: true })
})

it('keeps approval decisions separate from execution evidence when restoring a session', async () => {
  for (const approval of [
    { approvalId: 'approval', approved: true },
    { approvalId: 'approval', approved: true, error: 'execution unavailable' },
    { approvalId: 'approval', approved: false },
    { approvalId: 'approval', approved: true, result: { executionState: 'unknown' } },
  ]) {
    const work: TurnContext['work'] = { id: 'w', tenantId: 't', agentId: 'a', sessionId: 's', kind: 'resume', lane: 'interactive', triggerRef: 'm', fence: 1, homeEpoch: 1, leaseToken: 'token' }
    let observed = false
    const host: HostPort = {
      claimWork: async () => null, heartbeat: async () => ({ ok: true }),
      loadContext: async () => ({ work, persona: { name: 'A', role: 'assistant', instructions: '' }, capabilities: [], messages: [{ ref: 'm', authorId: 'u', authorName: 'User', authorKind: 'human', body: 'Save the requested file.', createdAt: 'now' }], pendingApproval: approval }),
      executeAction: async () => { throw new Error('unexpected execution') }, emitEvent: async () => {},
      loadSession: async () => null, saveSession: async () => {}, commitMessage: async () => {},
      completeWork: async () => {}, yieldWork: async () => {},
    }
    const model: ModelDriver = {
      run: async request => {
        const serialized = JSON.stringify(request.items)
        assert.doesNotMatch(serialized, /approved and executed/)
        assert.match(serialized, /decision alone does not establish execution/)
        assert.match(serialized, approval.approved ? /was approved/ : /was rejected/)
        if (approval.error) assert.match(serialized, /execution unavailable/)
        if (approval.result) assert.match(serialized, /unknown/)
        observed = true
        return { text: 'Execution remains unverified.', output: [{ role: 'assistant', content: 'Execution remains unverified.' }], usage: { available: false, inputTokens: 0, outputTokens: 0 } }
      }, structured: async () => { throw new Error('unexpected') }, compact: async () => { throw new Error('unexpected') },
    }
    await new AgentRuntime(host, model, { execute: async () => { throw new Error('unexpected') } }).runWork(work)
    assert.equal(observed, true)
  }
})

it('fails before model or side effects when the original request cannot be captured', async () => {
  const work: TurnContext['work'] = { id: 'w', tenantId: 't', agentId: 'a', sessionId: 's', kind: 'turn', lane: 'interactive', triggerRef: 'missing', fence: 1, homeEpoch: 1, leaseToken: 'token' }
  const calls: string[] = []
  let completion: WorkCompletion | undefined
  const unexpected = async () => { calls.push('unexpected'); throw new Error('unexpected execution') }
  const host: HostPort = {
    claimWork: async () => null, heartbeat: async () => ({ ok: true }),
    loadContext: async () => ({ work, persona: { name: 'A', role: 'assistant', instructions: '' }, capabilities: ['files'], messages: [] }),
    executeAction: unexpected, emitEvent: async () => {}, loadSession: async () => null,
    saveSession: unexpected, commitMessage: unexpected, yieldWork: unexpected,
    completeWork: async (_work, result) => { completion = result },
  }
  await new AgentRuntime(host, { run: unexpected, structured: unexpected, compact: unexpected }, { execute: unexpected }).runWork(work)
  assert.deepEqual(calls, [])
  assert.equal(completion?.status, 'failed')
  assert.match(completion?.error ?? '', /trigger message is required/)
  assert.equal(completion?.goalOutcome?.verification, 'inconclusive')
})
