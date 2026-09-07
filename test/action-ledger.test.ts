import { MemoryStepStore } from '../src/control-plane/steps.js'
import { MemoryModelBudgetStore } from '../src/control-plane/memory-store.js'
import { snapshotEvidence } from '../src/context/evidence.js'
import assert from 'node:assert/strict'
import { it } from 'node:test'
import { ControlPlaneService } from '../src/control-plane/service.js'
import { MemoryActionLedger, MemoryEventStore, MemorySessionStore, MemoryWorkStore } from '../src/control-plane/memory-store.js'

it('reserves before execution, blocks concurrent replay and rejects changed arguments', async () => {
  const actions = new MemoryActionLedger()
  let calls = 0
  let finish!: () => void
  const wait = new Promise<void>((resolve) => { finish = resolve })
  let started!: () => void
  const executing = new Promise<void>((resolve) => { started = resolve })
  const service = new ControlPlaneService({ modelBudgets: new MemoryModelBudgetStore(), steps: new MemoryStepStore(),
    work: new MemoryWorkStore(), sessions: new MemorySessionStore(), events: new MemoryEventStore(), actions,
    contextProvider: { loadContext: async () => ({ persona: { name: '', role: '', instructions: '' }, capabilities: [], messages: [] }) },
    capabilityResolver: { resolve: async () => [{ name: 'files', methods: ['save'] }] },
    delivery: { onEvent: async () => {}, deliverMessage: async () => {} },
    actionExecutor: { prepare: async () => {}, execute: async () => { calls++; started(); await wait; return { ok: true, value: 'saved' } } },
  })
  await service.enqueue({ id: 'w', tenantId: 't', agentId: 'a', sessionId: 's', principalId: 'u', kind: 'turn', lane: 'interactive', triggerRef: 'm' })
  const work = (await service.claim('worker'))!
  const action = { runId: 'w', cellId: 'c', callIndex: 0, action: 'files.save', args: { title: 'one' }, idempotencyKey: '[\"w\",\"c\",0]' }
  const first = service.executeAction(work, action)
  await executing
  assert.deepEqual(await actions.unsettled('w'), [{ actionKey: action.idempotencyKey, action: 'files.save', state: 'unknown' }])
  assert.deepEqual(await actions.unsettled('other'), [])
  assert.equal((await service.executeAction(work, action)).executionState, 'unknown')
  await assert.rejects(service.executeAction(work, { ...action, args: { title: 'two' } }), /identity reused/)
  finish()
  assert.deepEqual(await first, { ok: true, value: 'saved' })
  assert.deepEqual(await actions.unsettled('w'), [])
  assert.deepEqual(await service.executeAction(work, action), { ok: true, value: 'saved' })
  assert.equal(calls, 1)
})

it('does not overwrite intent or replay a receipt without an intent', async () => {
  const ledger = new MemoryActionLedger()
  const intent = { workId: 'w', tenantId: 't', principalId: 'u', agentId: 'a', sessionId: 's', threadId: null, requestVersion: 1,
    action: { runId: 'w', cellId: 'c', callIndex: 0, action: 'files.save', args: {}, idempotencyKey: 'intent' } }
  assert.equal(await ledger.reserve('intent', 'fingerprint', intent), 'started')
  assert.equal(await ledger.reserve('intent', 'fingerprint', intent), 'existing')
  await assert.rejects(ledger.reserve('intent', 'changed', intent), /identity reused/)
  await assert.rejects(ledger.record('orphan', { ok: true }), /intent is required/)
  await assert.rejects(ledger.reserve('wrong', 'fingerprint', intent), /must match/)
  await ledger.record('intent', { ok: false, executionState: 'unknown', error: 'lost' })
  const resolution = { id: 'resolution-1', actionKey: 'intent', result: { ok: true, value: { saved: true } },
    evidence: { source: 'authoritative-readback', version: 2 }, resolvedBy: 'operator:test' }
  assert.equal(await ledger.recordResolution(resolution), 'recorded')
  assert.equal(await ledger.recordResolution(resolution), 'existing')
  assert.deepEqual(await ledger.find('intent'), resolution.result)
  assert.deepEqual(await ledger.unsettled('w'), [])
  await assert.rejects(ledger.recordResolution({ ...resolution, evidence: { source: 'changed' } }), /identity reused/)
  const service = new ControlPlaneService({ modelBudgets: new MemoryModelBudgetStore(), steps: new MemoryStepStore(),
    work: new MemoryWorkStore(), sessions: new MemorySessionStore(), events: new MemoryEventStore(), actions: ledger,
    contextProvider: { loadContext: async () => ({ persona: { name: '', role: '', instructions: '' }, capabilities: [], messages: [] }) },
    capabilityResolver: { resolve: async () => [] }, actionExecutor: { prepare: async () => {}, execute: async () => ({ ok: true }) },
    delivery: { onEvent: async () => {}, deliverMessage: async () => {} },
  })
  await assert.rejects(service.resolveAction({ ...resolution, id: 'resolution-2' }, {
    tenantId: 'other', agentId: 'a', sessionId: 's', principalId: 'u',
  }), /not found/)
  assert.equal(await service.resolveAction({ ...resolution, id: 'resolution-2', result: { ok: false, error: 'not found' } }, {
    tenantId: 't', agentId: 'a', sessionId: 's', principalId: 'u',
  }), 'recorded')
})

it('does not treat a changed request or principal as a duplicate enqueue', async () => {
  const store = new MemoryWorkStore()
  const input = { id: 'w', tenantId: 't', agentId: 'a', sessionId: 's', principalId: 'u', kind: 'turn', lane: 'interactive' as const, triggerRef: 'm', meta: { text: 'original' } }
  await store.enqueue(input)
  await assert.rejects(store.enqueue({ ...input, principalId: 'other' }), /different request/)
  input.meta.text = 'changed after enqueue'
  await assert.rejects(store.enqueue(input), /different request/)
})

it('rejects actions from a stale request before reserving or executing them', async () => {
  const actions = new MemoryActionLedger()
  let calls = 0
  const service = new ControlPlaneService({ modelBudgets: new MemoryModelBudgetStore(), steps: new MemoryStepStore(),
    work: new MemoryWorkStore(), sessions: new MemorySessionStore(), events: new MemoryEventStore(), actions,
    contextProvider: { loadContext: async () => ({ persona: { name: '', role: '', instructions: '' }, capabilities: [], messages: [] }) },
    capabilityResolver: { resolve: async () => [{ name: 'files' }] },
    delivery: { onEvent: async () => {}, deliverMessage: async () => {} },
    actionExecutor: { prepare: async () => {}, execute: async () => { calls++; return { ok: true } } },
  })
  await service.enqueue({ id: 'w', tenantId: 't', agentId: 'a', sessionId: 's', principalId: 'u', kind: 'turn', lane: 'interactive', triggerRef: 'm', meta: { text: 'Save' } })
  const work = (await service.claim('worker'))!
  const session = { key: '[\"t\",\"a\",\"s\",null]', tenantId: 't', agentId: 'a', sessionId: 's', revision: 0, compactionEpoch: 0, history: [], appliedWorkIds: ['w'],
    request: { version: 1 as const, workId: 'w', tenantId: 't', sessionId: 's', authorId: 'u', sourceRef: 'm', evidence: snapshotEvidence('w:evidence:1', []), attachments: [], originalText: 'Save', revisions: [] as Array<{ id: string; text: string; createdAt: string }> } }
  session.revision = (await service.saveSession(work, session)).revision
  await service.addSteer('w', 'Only inspect, then save the corrected version')
  const action = { runId: 'w', cellId: 'c', callIndex: 0, action: 'files.save', args: {}, idempotencyKey: '[\"w\",\"c\",0]' }
  assert.deepEqual(await service.executeAction(work, action), { ok: false, error: 'request snapshot is stale or missing; process the latest user revisions before acting' })
  assert.equal(calls, 0)
  assert.equal(await actions.find(action.idempotencyKey), null)
  session.request.revisions = (await service.heartbeat(work)).steer!
  session.revision = (await service.saveSession(work, session)).revision
  assert.deepEqual(await service.executeAction(work, action), { ok: true })
  assert.equal(calls, 1)
  const intent = await actions.findIntent(action.idempotencyKey)
  assert.deepEqual(intent, { workId: 'w', tenantId: 't', principalId: 'u', agentId: 'a', sessionId: 's', threadId: null, requestVersion: 2, action })
  Object.assign(intent!.action.args, { mutated: true })
  assert.deepEqual((await actions.findIntent(action.idempotencyKey))?.action.args, {})
})
