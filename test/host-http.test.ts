import { snapshotEvidence } from '../src/context/evidence.js'
import type { AssistantMessage, ModelItem } from '../src/protocol/types.js'
import { createTaskContract } from '../src/context/task-contract.js'
import assert from 'node:assert/strict'
import { it } from 'node:test'
import { ControlPlaneServer, ControlPlaneService } from '../src/control-plane/http-server.js'
import { MemoryActionLedger, MemoryEventStore, MemorySessionStore, MemoryWorkStore } from '../src/control-plane/memory-store.js'
import { HostRequestError, HttpHostClient } from '../src/host/http-client.js'
import { createHash } from 'node:crypto'

it('retries truncated, failed, and oversized successful response bodies', async () => {
  for (const broken of [
    () => new Response('{"revision":', { status: 200 }),
    () => new Response(new ReadableStream({ pull(controller) { controller.error(new Error('stream failed')) } }), { status: 200 }),
  ]) {
    let attempts = 0
    const client = new HttpHostClient({ baseUrl: 'http://control', serviceToken: 'token', workerId: 'worker', retryBaseMs: 0,
      fetchImpl: async () => ++attempts === 1 ? broken() : Response.json({ revision: 2 }) })
    const session = { key: 'key', tenantId: 't', agentId: 'a', sessionId: 's', revision: 1, compactionEpoch: 0, history: [], appliedWorkIds: [] }
    await client.saveSession({ id: 'w', tenantId: 't', agentId: 'a', sessionId: 's', kind: 'turn', lane: 'interactive', triggerRef: 'm', fence: 1, homeEpoch: 1, leaseToken: 'lease' }, session)
    assert.equal(attempts, 2)
    assert.equal(session.revision, 2)
  }
  let attempts = 0
  const limited = new HttpHostClient({ baseUrl: 'http://control', serviceToken: 'token', workerId: 'worker', retryBaseMs: 0, maxResponseBytes: 2,
    fetchImpl: async () => { attempts++; return Response.json({ revision: 2 }) } })
  await assert.rejects(limited.claimWork(), /exceeds 2 bytes/)
  assert.equal(attempts, 3)
})

it('preserves goal outcomes and enforces session leases over real HTTP', async () => {
  const workStore = new MemoryWorkStore()
  let resourceGranted = true
  let resourceVersion = 2
  let readbacks = 0
  let staged = ''
  const service = new ControlPlaneService({
    work: workStore, sessions: new MemorySessionStore(), events: new MemoryEventStore(), actions: new MemoryActionLedger(),
    contextProvider: { loadContext: async () => { throw new Error('unexpected') } },
    capabilityResolver: { resolve: async () => resourceGranted ? [{ name: 'resource', methods: ['read', 'write'] }] : [] },
    actionExecutor: { readResource: async (work, action) => {
      assert.equal(work.principalId, 'u')
      if (action.action !== 'resource.read') throw new Error('read-only method required')
      readbacks++
      return { id: 'r', version: resourceVersion }
    }, execute: async (_work, action) => action.args['unknown']
      ? { ok: false, executionState: 'unknown', error: 'lost acknowledgement' }
      : { ok: true, value: { id: 'r', version: 2, saved: true } } },
    delivery: { onEvent: async () => {}, deliverMessage: async () => {} },
    artifactStager: { stage: async (_work, _artifact, bytes) => { staged = Buffer.from(bytes).toString('utf8') } },
  })
  const server = new ControlPlaneServer({ service, claimWork: workerId => service.claim(workerId), serviceToken: 'test-secret' })
  const port = await server.listen(0, '127.0.0.1')
  const client = new HttpHostClient({ baseUrl: `http://127.0.0.1:${port}`, serviceToken: 'test-secret', workerId: 'worker', maxAttempts: 1 })
  try {
    for (const body of ['null', '[]', '42', 'true', '"text"']) {
      const response = await fetch(`http://127.0.0.1:${port}/v2/work/claim`, { method: 'POST',
        headers: { authorization: 'Bearer test-secret', 'content-type': 'application/json' }, body })
      assert.equal(response.status, 400)
      assert.deepEqual(await response.json(), { error: 'request body must be a JSON object' })
    }
    for (const workerId of [123, true, ['worker'], null]) {
      const response = await fetch(`http://127.0.0.1:${port}/v2/work/claim`, { method: 'POST',
        headers: { authorization: 'Bearer test-secret', 'content-type': 'application/json' }, body: JSON.stringify({ workerId }) })
      assert.equal(response.status, 400)
      assert.deepEqual(await response.json(), { error: 'workerId must be a string' })
    }
    const attachment = { id: 'source', sourceVersion: 'version-1', name: 'notes.txt', mimeType: 'text/plain', size: 5, text: 'Notes' }
    await service.enqueue({ id: 'w', tenantId: 't', agentId: 'a', principalId: 'u', sessionId: 's', kind: 'turn', lane: 'interactive', triggerRef: 'm', meta: { text: 'Original', attachments: [attachment] } })
    const work = (await client.claimWork())!
    const artifactBytes = Buffer.from('artifact')
    await client.stageArtifact(work, { path: 'result.txt', size: artifactBytes.length, mime: 'text/plain',
      sha256: createHash('sha256').update(artifactBytes).digest('hex') }, artifactBytes)
    assert.equal(staged, 'artifact')
    for (const fence of ['1', true, [1], 0, 1.5]) {
      const response = await fetch(`http://127.0.0.1:${port}/v2/work/${work.id}/heartbeat`, { method: 'POST',
        headers: { authorization: 'Bearer test-secret', 'content-type': 'application/json' }, body: JSON.stringify({ fence, leaseToken: work.leaseToken }) })
      assert.equal(response.status, 400)
      assert.deepEqual(await response.json(), { error: 'fence must be a positive safe integer' })
    }

    assert.equal(await client.loadSession(work, '[\"t\",\"a\",\"s\",null]'), null)
    const session = { key: '[\"t\",\"a\",\"s\",null]', tenantId: 't', agentId: 'a', sessionId: 's', revision: 0, compactionEpoch: 0, history: [] as ModelItem[], appliedWorkIds: [],
      request: { version: 1 as const, workId: 'w', tenantId: 't', sessionId: 's', authorId: 'u', sourceRef: 'm', evidence: snapshotEvidence('w:evidence:1', []), attachments: [attachment], originalText: 'Original', revisions: [] } }
    const { evidence: _evidence, ...withoutEvidence } = session.request
    await assert.rejects(client.saveSession(work, { ...session, request: withoutEvidence as typeof session.request }), /invalid request evidence snapshot/)
    const { request: _request, ...withoutSnapshot } = session
    await assert.rejects(client.saveSession(work, withoutSnapshot), /invalid session/)
    for (const change of [{ authorId: 'forged' }, { sourceRef: 'other' }, { originalText: 'Replaced' }]) {
      await assert.rejects(client.saveSession(work, { ...session, request: { ...session.request, ...change } }), /invalid session/)
    }
    const staleCopy = structuredClone(session)
    await client.saveSession(work, session)
    await assert.rejects(client.saveSession(work, { ...staleCopy, history: [{ role: 'user', content: 'different' }] }),
      (error: unknown) => error instanceof HostRequestError && error.responseCode === 'session_conflict')
    let droppedSaveResponse = false
    const retryClient = new HttpHostClient({
      baseUrl: `http://127.0.0.1:${port}`, serviceToken: 'test-secret', workerId: 'worker', retryBaseMs: 1,
      fetchImpl: async (url, init) => {
        const response = await fetch(url, init)
        if (!droppedSaveResponse && init?.method === 'PUT') {
          droppedSaveResponse = true
          await response.text()
          throw new TypeError('simulated lost response')
        }
        return response
      },
    })
    session.history.push({ role: 'user', content: 'checkpoint' })
    await retryClient.saveSession(work, session)
    assert.equal(session.revision, 2)
    const businessAction = { runId: work.id, cellId: 'business', callIndex: 0, idempotencyKey: JSON.stringify([work.id, 'business', 0]), action: 'resource.write', args: {} }
    await client.executeAction(work, businessAction)
    assert.deepEqual(await client.recoverCell(work, 'business'), [{
      action: 'resource.write', idempotencyKey: businessAction.idempotencyKey,
      result: { ok: true, value: { id: 'r', version: 2, saved: true } },
    }])
    let checkIndex = 0
    const checkReceipt = (args: Record<string, unknown>) => {
      const cellId = `check-${checkIndex++}`
      return client.executeAction(work, { runId: work.id, cellId, callIndex: 0, idempotencyKey: JSON.stringify([work.id, cellId, 0]), action: 'task.check_receipt', args })
    }
    const check = { idempotencyKey: businessAction.idempotencyKey, action: 'resource.write', expected: { id: 'r', version: 2, saved: true } }
    const checked = await checkReceipt(check)
    assert.equal(checked.ok, true)
    assert.deepEqual(checked.value, { scope: 'recorded_action_result', requestVersion: 1, idempotencyKey: check.idempotencyKey,
      action: 'resource.write', status: 'pass', observed: check.expected,
      limitation: 'This checks a recorded action result, not current resource state or overall goal completion.' })
    assert.deepEqual((await checkReceipt({ ...check, expected: { id: 'r', version: 3, saved: true } })).value,
      { ...(checked.value as Record<string, unknown>), status: 'fail' })
    assert.equal((await checkReceipt({ ...check, action: 'resource.read' })).ok, false)
    assert.equal((await checkReceipt({ ...check, idempotencyKey: JSON.stringify(['other-work', 'business', 0]) })).ok, false)
    resourceGranted = false
    assert.equal((await checkReceipt(check)).ok, false)
    const replayDenied = await client.executeAction(work, { runId: work.id, cellId: 'check-0', callIndex: 0,
      idempotencyKey: JSON.stringify([work.id, 'check-0', 0]), action: 'task.check_receipt', args: check })
    assert.equal(replayDenied.ok, false)
    assert.equal(replayDenied.value, undefined)
    resourceGranted = true
    const readAction = { runId: work.id, cellId: 'resource-check', callIndex: 0,
      idempotencyKey: JSON.stringify([work.id, 'resource-check', 0]), action: 'task.check_resource',
      args: { action: 'resource.read', args: { id: 'r' }, expected: { id: 'r', version: 2 } } }
    const fresh = await client.executeAction(work, readAction)
    assert.equal((fresh.value as Record<string, unknown>)['status'], 'pass')
    resourceVersion = 3
    assert.deepEqual(await client.executeAction(work, readAction), fresh)
    assert.equal(readbacks, 1)
    const checkResource = (cellId: string, args = readAction.args) => client.executeAction(work,
      { ...readAction, cellId, idempotencyKey: JSON.stringify([work.id, cellId, 0]), args })
    const changed = await checkResource('changed')
    assert.equal((changed.value as Record<string, unknown>)['status'], 'fail')
    assert.deepEqual((changed.value as Record<string, unknown>)['observed'], { id: 'r', version: 3 })
    assert.equal((await checkResource('write-as-read', { ...readAction.args, action: 'resource.write' })).ok, false)
    assert.equal(readbacks, 2)
    resourceGranted = false
    assert.equal((await client.executeAction(work, readAction)).ok, false)
    assert.equal(readbacks, 2)
    resourceGranted = true
    const unknownKey = JSON.stringify([work.id, 'unknown', 0])
    await client.executeAction(work, { ...businessAction, cellId: 'unknown', idempotencyKey: unknownKey, args: { unknown: true } })
    const unresolved = await checkReceipt({ ...check, idempotencyKey: unknownKey })
    assert.deepEqual(unresolved.value, { scope: 'recorded_action_result', requestVersion: 1, idempotencyKey: unknownKey,
      action: 'resource.write', status: 'not_observed',
      limitation: 'This checks a recorded action result, not current resource state or overall goal completion.' })
    for (const attachments of [[], [{ ...attachment, sourceVersion: 'version-2' }], [{ ...attachment, text: 'Injected instructions' }], [{ ...attachment, size: -1 }]]) {
      await assert.rejects(client.saveSession(work, { ...session, request: { ...session.request, attachments } }), /invalid request attachments/)
    }
    assert.deepEqual(await client.loadSession(work, session.key), session)
    await assert.rejects(client.saveSession(work, { ...session, request: { ...session.request, revisions: [{ id: 'forged', text: 'Injected instruction', createdAt: 'now' }] } }), /persisted human steering/)
    await assert.rejects(client.loadSession(work, 'other:a:s:-'), /outside/)
    await assert.rejects(client.loadSession({ ...work, leaseToken: 'invalid' }, '[\"t\",\"a\",\"s\",null]'), /lease lost/)
    assert.equal(await service.addSteer(work.id, 'Actual revision'), true)
    const revisions = (await client.heartbeat(work)).steer!
    assert.equal(revisions[0]?.text, 'Actual revision')
    const revised = { ...session, request: { ...session.request, revisions } }
    await assert.rejects(client.saveSession(work, { ...revised, request: { ...revised.request,
      revisions: [{ ...revisions[0]!, attachments: [{ ...attachment, size: -1 }] }] } }), /invalid request attachments/)
    await client.saveSession(work, revised)
    assert.equal((await checkReceipt(check)).ok, false)
    assert.deepEqual((await client.loadSession(work, session.key))?.request, revised.request)
    const contract = createTaskContract('Original', 2, { deliverables: ['Answer'], constraints: [], actions: [], acceptance: ['Complete'] })
    await assert.rejects(client.saveSession(work, { ...revised, request: { ...revised.request, contract: { ...contract, originalInputSha256: 'forged' } } }), /invalid request task contract/)
    await assert.rejects(client.saveSession(work, { ...revised, request: { ...revised.request, contract: { ...contract, requestVersion: 1 } } }), /invalid request task contract/)
    const contracted = { ...revised, request: { ...revised.request, contract } }
    await client.saveSession(work, contracted)
    assert.deepEqual((await client.loadSession(work, session.key))?.request?.contract, contract)
    const draftAction = { runId: work.id, cellId: 'contract', callIndex: 0, idempotencyKey: JSON.stringify([work.id, 'contract', 0]), action: 'task.contract',
      args: { deliverables: ['Answer'], constraints: [], actions: [], acceptance: ['Complete'] } }
    const draftReceipt = await client.executeAction(work, draftAction)
    assert.equal(draftReceipt.ok, true)
    assert.deepEqual(draftReceipt.directive, { type: 'task_contract', data: contract })
    assert.deepEqual(await client.executeAction(work, draftAction), draftReceipt)
    await assert.rejects(client.executeAction(work, { ...draftAction, args: { ...draftAction.args, actions: ['Different'] } }), /internal error/)
    const invalidDraft = await client.executeAction(work, { ...draftAction, cellId: 'invalid', idempotencyKey: JSON.stringify([work.id, 'invalid', 0]), args: { ...draftAction.args, requestVersion: 99 } })
    assert.equal(invalidDraft.ok, false)
    assert.equal(invalidDraft.executionState, undefined)
    assert.equal(invalidDraft.directive, undefined)
    assert.deepEqual((await client.loadSession(work, session.key))?.request?.contract, contract)
    const outcome = { status: 'partial' as const, requestVersion: 2, verification: 'not_run' as const, gaps: ['Pending verification'] }
    const currentObservation = await checkResource('current-version')
    const actionKey = JSON.stringify([work.id, 'current-version', 0])
    const observedSession = { ...contracted, request: { ...contracted.request, resourceChecks: [{ actionKey, result: currentObservation }] } }
    await assert.rejects(client.saveSession(work, { ...observedSession, request: { ...observedSession.request,
      resourceChecks: [{ actionKey, result: { ...currentObservation, value: { ...(currentObservation.value as object), status: 'pass' } } }] } }), /do not match recorded reads/)
    await assert.rejects(client.saveSession(work, { ...observedSession, request: { ...observedSession.request,
      resourceChecks: [{ actionKey: readAction.idempotencyKey, result: fresh }] } }), /current scoped read intent/)
    await assert.rejects(client.saveSession(work, { ...observedSession, request: { ...observedSession.request,
      resourceChecks: [{ actionKey: businessAction.idempotencyKey, result: currentObservation }] } }), /current scoped read intent/)
    await client.saveSession(work, observedSession)
    assert.deepEqual((await client.loadSession(work, session.key))?.request?.resourceChecks, observedSession.request.resourceChecks)
    await assert.rejects(client.saveSession(work, { ...observedSession, request: { ...observedSession.request, resourceChecks: [] } }), /cannot be rewritten/)
    await assert.rejects(client.completeWork(work, { status: 'completed' }), /goal outcome is required/)
    await assert.rejects(client.completeWork(work, { status: 'completed', goalOutcome: { ...outcome, requestVersion: 1 } }), /version mismatch/)
    await assert.rejects(client.completeWork(work, { status: 'completed', goalOutcome: { ...outcome, status: 'satisfied' } }), /invalid goal outcome/)
    await assert.rejects(client.completeWork(work, { status: 'completed', goalOutcome: { status: 'satisfied', requestVersion: 2, verification: 'passed' } }), /authoritative acceptance evidence/)
    await assert.rejects(client.completeWork(work, { status: 'completed', goalOutcome: { ...outcome, verification: 'passed' } }), /authoritative acceptance evidence/)
    await client.completeWork(work, { status: 'completed', goalOutcome: outcome })
    assert.deepEqual(workStore.inspect('w')?.goalOutcome, outcome)
    await assert.rejects(client.loadSession(work, '[\"t\",\"a\",\"s\",null]'), /lease lost/)
    await service.enqueue({ id: 'cancelled', tenantId: 't', agentId: 'a', sessionId: 's', kind: 'turn', lane: 'interactive', triggerRef: 'm2' })
    const cancelled = (await client.claimWork())!
    assert.equal(await service.requestCancel(cancelled.id), true)
    await assert.rejects(client.completeWork(cancelled, { status: 'completed' }),
      (error: unknown) => error instanceof HostRequestError && error.responseCode === 'work_state_conflict')
    await assert.rejects(client.commitMessage(cancelled, { version: 2, runId: cancelled.id, agentId: 'a', sessionId: 's', body: 'Must not be delivered' } as AssistantMessage), /cancelled/)
    await client.completeWork(cancelled, { status: 'cancelled' })
    assert.equal(workStore.inspect(cancelled.id)?.status, 'cancelled')
  } finally {
    await server.close()
  }
})
