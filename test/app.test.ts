import { startWorker, createWorker } from '../src/worker/index.js'
import { setTimeout as delay } from 'node:timers/promises'
import assert from 'node:assert/strict'
import http from 'node:http'
import { mkdtemp, readFile, rm, writeFile, mkdir, symlink, readdir } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { it } from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import { executeRequest } from '../src/eval/index.js'
import { createLingxiOS, DefaultRuntimePolicy } from '../src/index.js'
import type { ContextProvider, PromptContext, TurnContext } from '../src/index.js'
import type { SqlPool } from '../src/control-plane/pg-store.js'
import { kernelHome } from '../src/kernel/manager.js'
import { persistArtifacts } from '../src/app/artifacts.js'

it('assembles the public app through HTTP model and Python, persisting results across reopen', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'lingxios-app-'))
  let db = new PGlite(join(directory, 'database'))
  let reviseBeforeCommit = false
  let exhaustBudget = false
  let missingArtifact = false
  let missingArtifactCalls = 0
  let remoteCalls: number | undefined
  const pool: SqlPool = {
    query: async (sql, params) => {
      if (reviseBeforeCommit && sql.includes('SELECT id FROM lingxios.agent_work_items')) {
        reviseBeforeCommit = false
        await db.query(`UPDATE lingxios.agent_work_items SET steer_inputs='[{"id":"late","text":"New requirement","createdAt":"now"}]'::jsonb WHERE id=$1`, [params?.[0]])
      }
      const result = await db.query<Record<string, unknown>>(sql, params)
      return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
    }, connect: async () => ({ query: pool.query, release: () => {} }),
  }
  const requests: Array<{ messages: Array<{ role: string; content: string }> }> = []
  let contentChecks = 0
  const server = http.createServer((req, res) => {
    const buffers: Buffer[] = []
    req.on('data', (chunk: Buffer) => buffers.push(chunk))
    req.on('end', async () => {
      const payload = JSON.parse(Buffer.concat(buffers).toString('utf8'))
      if (!payload.stream && payload.response_format?.type === 'json_object') {
        contentChecks++
        assert.match(payload.messages[0].content, /Check a candidate delivery/)
        res.writeHead(200, { 'content-type': 'application/json' })
        const input = JSON.parse(payload.messages.at(-1).content)
        const missing = input.revisions?.length ? [{ quote: input.revisions.at(-1).text, reason: 'Candidate ignores the revision.' }] : []
        res.end(JSON.stringify({ model: 'test', choices: [{ message: { content: JSON.stringify({ missing }) }, finish_reason: 'stop' }] }))
        return
      }
      requests.push(payload)
      if (remoteCalls !== undefined) remoteCalls++
      if (missingArtifact) {
        missingArtifactCalls++
        if (missingArtifactCalls === 2) await rm(join(kernelHome(options.kernel.homesRoot,
          { tenantId: 'tenant', agentId: 'assistant', sessionId: 'missing-artifact', homeEpoch: 1 }), 'missing.txt'))
        const delta = missingArtifactCalls === 1 ? { tool_calls: [{ index: 0, id: 'missing-artifact-cell', function: { name: 'ipython',
          arguments: JSON.stringify({ code: 'open("missing.txt", "w").write("result")' }) } }] } : { content: 'File prepared.' }
        res.writeHead(200, { 'content-type': 'text/event-stream' })
        res.end(`data: ${JSON.stringify({ choices: [{ delta, finish_reason: missingArtifactCalls === 1 ? 'tool_calls' : 'stop' }] })}\n\ndata: [DONE]\n\n`)
        return
      }
      const delta = exhaustBudget
        ? { tool_calls: [{ index: 0, id: `budget-${requests.length}`, function: { name: 'ipython', arguments: JSON.stringify({ code: 'print(2 + 2)' }) } }] }
        : (requests.length === 1 || remoteCalls === 1)
        ? { content: 'Calculating.', tool_calls: [{ index: 0, id: 'cell', function: { name: 'ipython', arguments: JSON.stringify({ code: 'host.task.contract(deliverables=["Calculation"], constraints=[], actions=[], acceptance=["Return the result"])\nprint(2 + 2)\nopen("answer.txt", "w").write("4")' }) } }] }
        : { content: '4' }
      res.writeHead(200, { 'content-type': 'text/event-stream' })
      res.end(`data: ${JSON.stringify({ choices: [{ delta, finish_reason: exhaustBudget || (requests.length === 1 || remoteCalls === 1) ? 'tool_calls' : 'stop' }] })}\n\ndata: [DONE]\n\n`)
    })
  })
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  assert.ok(address && typeof address === 'object')
  class InjectedPolicy extends DefaultRuntimePolicy {
    override productRules(candidate: PromptContext) {
      return `${super.productRules(candidate)}\n\nInjected policy marker.`
    }

    override dynamicContextItems(context: TurnContext) {
      return context.dynamic ? [{ role: 'user' as const, content: `Injected context: ${JSON.stringify(context.dynamic)}` }] : []
    }
  }
  const contextProvider: ContextProvider = { loadContext: async (work) => {
    const persona = { name: 'Injected assistant', role: 'assistant', instructions: 'Use injected context.' }
    return { persona, capabilities: [], dynamic: { tenant: work.tenantId },
      messages: [{ ref: work.triggerRef, authorId: work.principalId!, authorName: String(work.meta?.['authorName'] ?? 'User'), authorKind: 'human' as const, body: String(work.meta?.['text']), createdAt: work.createdAt ?? '' }],
      promptContextCandidate: { version: 3 as const, epoch: 0, assembledAt: '', systemInstructions: '', persona, capabilities: [], sourceVersions: { persona: 'injected-v1' } },
    }
  } }
  const options = { database: pool, model: { id: 'test', apiKey: 'test', baseUrl: `http://127.0.0.1:${address.port}` }, kernel: { homesRoot: join(directory, 'homes') }, worker: { healthPort: 0, pollIdleMs: 50 },
    homesRoot: join(directory, 'homes'), contextProvider, policy: new InjectedPolicy(), modelBudget: { maxModelCalls: 16 },
    modelTrace: { recordPayloads: true, redact: () => ({ redacted: true }), sampleRate: 1, retentionDays: 1 } }
  let app: Awaited<ReturnType<typeof createLingxiOS>> | undefined
  let controlApp: Awaited<ReturnType<typeof createLingxiOS>> | undefined
  let worker: ReturnType<typeof createWorker> | undefined
  try {
    await assert.rejects(createLingxiOS(options), /schema is missing/)
    await db.exec(await readFile(new URL('../../db/schema.sql', import.meta.url), 'utf8'))
    const stoppedApp = await createLingxiOS(options)
    await stoppedApp.stop()
    assert.equal('start' in stoppedApp, false)
    assert.equal('runNext' in stoppedApp, false)
    assert.throws(() => createWorker({ ...options, controlPlane: stoppedApp }), /application has stopped/)
    await assert.rejects(stoppedApp.listenControlPlane({ serviceToken: 'test', port: 0 }), /application has stopped/)
    const racingApp = await createLingxiOS({ database: pool })
    const opening = racingApp.listenControlPlane({ serviceToken: 'test', port: 0 })
    const closing = racingApp.stop()
    assert.equal(racingApp.stop(), closing)
    const closedPort = await opening
    await closing
    await assert.rejects(fetch(`http://127.0.0.1:${closedPort}/healthz`))
    app = await createLingxiOS(options)
    worker = createWorker({ ...options, controlPlane: app })
    const identity = { runId: 'request', tenantId: 'tenant', agentId: 'assistant', sessionId: 'session', principalId: 'user' }
    const evaluation = await executeRequest(app, worker, { id: identity.runId, ...identity, principalId: 'user', text: 'Calculate 2 + 2 using Python.' })
    assert.equal(evaluation.mode, 'runtime_execution')
    assert.equal(evaluation.workDequeued, true)
    assert.equal(evaluation.message?.body, '4')
    assert.equal(evaluation.delivery, 'observed')
    assert.equal(evaluation.externalDelivery, 'not_observed')
    assert.equal(evaluation.semanticQuality, 'not_assessed')
    const artifactIdentity = { ...identity, principalId: 'user' }
    const downloaded = await app.readArtifact(artifactIdentity, 'answer.txt')
    assert.equal(downloaded?.bytes.toString('utf8'), '4')
    assert.equal(downloaded?.artifact.path, 'answer.txt')
    for (const scope of [{ principalId: 'other' }, { tenantId: 'other' }, { threadId: 'other' }, { runId: 'other' }]) {
      assert.equal(await app.readArtifact({ ...artifactIdentity, ...scope }, 'answer.txt'), null)
    }
    assert.equal(await app.readArtifact(artifactIdentity, '../answer.txt'), null)
    const storedContract = (await db.query<{ request_snapshot: { contract: { deliverables: string[] } } }>('SELECT request_snapshot FROM lingxios.agent_os_sessions WHERE session_id=$1', [identity.sessionId])).rows[0]!.request_snapshot.contract
    assert.deepEqual(storedContract.deliverables, ['Calculation'])
    assert.deepEqual(evaluation.message?.envelope?.taskContract, storedContract)
    assert.match(JSON.stringify(requests[1]!.messages), /Derived task checklist/)

    await assert.rejects(executeRequest(app, worker, { id: identity.runId, ...identity, principalId: 'user', text: 'Calculate 2 + 2 using Python.' }), /already exists/)
    assert.equal(requests.length, 2)
    assert.equal((await app.readMessage(identity))?.body, '4')
    assert.equal(contentChecks, 1)
    const traces = (await db.query<{ data: Record<string, unknown>; expires_at: string }>(
      "SELECT data,expires_at FROM lingxios.agent_run_events WHERE run_id=$1 AND kind LIKE 'model.%' AND data ? 'input' OR run_id=$1 AND kind LIKE 'model.%' AND data ? 'output'", [identity.runId])).rows
    assert.ok(traces.length >= 2)
    assert.ok(traces.every(trace => JSON.stringify(trace.data).includes('redacted') && !JSON.stringify(trace.data).includes('Calculate 2 + 2')))
    assert.ok(traces.every(trace => trace.expires_at))
    assert.deepEqual(await app.readOutcome(identity), { status: 'satisfied', verification: 'inconclusive', requestVersion: 1 })
    assert.equal(evaluation.message?.envelope.assessment, undefined)
    assert.equal(await app.readMessage({ ...identity, tenantId: 'another' }), null)
    assert.equal(requests.length, 2)
    assert.match(requests[0]!.messages[0]!.content, /Runtime authorization/)
    assert.match(requests[0]!.messages[0]!.content, /Injected policy marker/)
    assert.match(JSON.stringify(requests[0]!.messages), /Injected context:.*tenant/)
    const output = requests[1]!.messages.find((item) => item.role === 'tool')!
    assert.equal(JSON.parse(output.content).stdout, '4\n')
    const { healthPort } = await worker.start()
    assert.equal((await fetch(`http://127.0.0.1:${healthPort}/readyz`)).status, 200)
    await assert.rejects(worker.start(), /already/)
    await worker.stop()
    await app.stop()
    await db.close()
    db = new PGlite(join(directory, 'database'))
    app = await createLingxiOS(options)
    worker = createWorker({ ...options, controlPlane: app })
    assert.equal((await app.readMessage(identity))?.body, '4')
    assert.equal(await worker.runNext(), false)
    // A committed result and terminal work state survive the same database reopen.
    const { model: _model, ...controlOptions } = options
    controlApp = await createLingxiOS(controlOptions)
    assert.equal('runNext' in controlApp, false)
    assert.equal('start' in controlApp, false)
    await assert.rejects(controlApp.listenControlPlane({ serviceToken: '', port: 0 }), /token is required/)
    const controlPort = await controlApp.listenControlPlane({ serviceToken: 'test-worker-secret', port: 0 })
    await assert.rejects(controlApp.listenControlPlane({ serviceToken: 'test-worker-secret', port: 0 }), /already/)
    const claimUrl = `http://127.0.0.1:${controlPort}/v5/work/claim`
    assert.equal((await fetch(claimUrl, { method: 'POST' })).status, 401)
    const recoveredClaim = await fetch(claimUrl, { method: 'POST', headers: { authorization: 'Bearer test-worker-secret', 'content-type': 'application/json' }, body: JSON.stringify({ protocol: 9, workerId: 'remote-worker', workKinds: ['turn','resume'] }) })
    assert.equal(recoveredClaim.status, 200)
    assert.equal(await recoveredClaim.json(), null)
    assert.equal(requests.length, 2)
    assert.deepEqual((await db.query('SELECT status,fence FROM lingxios.agent_work_items WHERE id=$1', [identity.runId])).rows, [{ status: 'succeeded', fence: 1 }])
    assert.equal((await app.readOutcome(identity))?.status, 'satisfied')
    assert.equal((await app.readArtifact(artifactIdentity, 'answer.txt'))?.bytes.toString('utf8'), '4')
    const committedPath = join(kernelHome(options.kernel.homesRoot, { ...identity, homeEpoch: 1 }), 'answer.txt')
    await writeFile(committedPath, '5')
    assert.equal((await app.readArtifact(artifactIdentity, 'answer.txt'))?.bytes.toString('utf8'), '4')
    await writeFile(committedPath, '4')
    const external = join(directory, 'external')
    await mkdir(external)
    await writeFile(join(external, 'answer.txt'), '4')
    await symlink(external, join(kernelHome(options.kernel.homesRoot, { ...identity, homeEpoch: 1 }), 'linked'), process.platform === 'win32' ? 'junction' : 'dir')
    const linkedMessage = structuredClone(evaluation.message!)
    linkedMessage.envelope!.artifacts = [{ ...downloaded!.artifact, path: 'linked/answer.txt' }]
    await assert.rejects(persistArtifacts(options.kernel.homesRoot, { ...identity, id: 'linked', homeEpoch: 1, fence: 1,
      kind: 'turn', lane: 'interactive', triggerRef: 'm' }, linkedMessage), /outside its committed home/)
    await rm(committedPath)
    assert.equal((await app.readArtifact(artifactIdentity, 'answer.txt'))?.bytes.toString('utf8'), '4')
    const committedWork = { ...identity, id: identity.runId, homeEpoch: 1, fence: 1, kind: 'turn', lane: 'interactive' as const, triggerRef: 'm' }
    await persistArtifacts(options.kernel.homesRoot, committedWork, evaluation.message!)
    const snapshotRoot = join(options.kernel.homesRoot, '.committed-artifacts')
    const savedFiles = (await readdir(snapshotRoot, { recursive: true })).filter(path => path.endsWith(downloaded!.artifact.sha256))
    assert.equal(savedFiles.length, 1)
    const snapshotPath = join(snapshotRoot, savedFiles[0]!)
    await writeFile(snapshotPath, '5')
    await assert.rejects(app.readArtifact(artifactIdentity, 'answer.txt'), /changed since commitment/)
    await assert.rejects(persistArtifacts(options.kernel.homesRoot, committedWork, evaluation.message!), /changed since commitment/)
    assert.equal(await readFile(snapshotPath, 'utf8'), '5')
    await writeFile(snapshotPath, '4')
    const recoveredHistory = (await db.query<{ history: unknown[] }>('SELECT history FROM lingxios.agent_os_sessions WHERE session_id=$1', [identity.sessionId])).rows[0]!.history
    assert.deepEqual(recoveredHistory.at(-1), { role: 'assistant', content: '4' })
    const recoveryEvents = (await db.query('SELECT seq,kind,visibility,data FROM lingxios.agent_run_events WHERE run_id=$1 AND seq>100000 ORDER BY seq', [identity.runId])).rows
    assert.deepEqual(recoveryEvents, [])
    assert.equal(await worker.runNext(), false)
    assert.deepEqual((await db.query('SELECT seq,kind,visibility,data FROM lingxios.agent_run_events WHERE run_id=$1 AND seq>100000 ORDER BY seq', [identity.runId])).rows, recoveryEvents)

    const remoteIdentity = { ...identity, runId: 'remote-request', sessionId: 'remote-session' }
    remoteCalls = 0
    await app.enqueue({ ...remoteIdentity, id: remoteIdentity.runId, principalId: 'user', text: 'Calculate 2 + 2 using Python.' })
    const remoteWorker = await startWorker({ ...process.env, AGENT_OS_CONTROL_PLANE_URL: `http://127.0.0.1:${controlPort}`,
      AGENT_OS_SERVICE_TOKEN: 'test-worker-secret', AGENT_OS_WORKER_ID: 'remote-worker', AGENT_OS_WORKER_PORT: '0',
      AGENT_OS_MODEL: 'test', AGENT_OS_MODEL_API_KEY: 'test', AGENT_OS_MODEL_BASE_URL: options.model.baseUrl,
      AGENT_OS_HOMES_ROOT: join(directory, 'remote-worker-homes'), AGENT_OS_POLL_IDLE_MS: '50', AGENT_OS_MAX_CONCURRENT_RUNS: '1' })
    try {
      const deadline = Date.now() + 10000
      while (!(await app.readMessage(remoteIdentity)) && Date.now() < deadline) await delay(25)
      assert.equal((await app.readMessage(remoteIdentity))?.body, '4')
      assert.equal((await app.readArtifact({ ...remoteIdentity, principalId: 'user' }, 'answer.txt'))?.bytes.toString('utf8'), '4')
      assert.equal(remoteCalls, 2)
    } finally {
      await remoteWorker.stop()
      remoteCalls = undefined
    }

    const lateIdentity = { ...identity, runId: 'late-request', sessionId: 'late-session' }
    await app.enqueue({ id: lateIdentity.runId, ...lateIdentity, principalId: 'user', text: 'Answer this request.' })
    reviseBeforeCommit = true
    assert.equal(await worker.runNext(), true)
    assert.equal(reviseBeforeCommit, false)
    assert.equal((await app.readMessage(lateIdentity))?.envelope?.goalOutcome.status, 'blocked')
    assert.deepEqual((await db.query('SELECT outbox.* FROM lingxios.agent_delivery_outbox outbox JOIN lingxios.agent_results result ON result.id=outbox.result_id WHERE result.work_id=$1', [lateIdentity.runId])).rows, [])
    exhaustBudget = true
    const partialIdentity = { ...identity, runId: 'partial-request', sessionId: 'partial-session' }
    const beforePartial = requests.length
    await app.enqueue({ id: partialIdentity.runId, ...partialIdentity, principalId: 'user', text: 'Calculate and explain the result.' })
    assert.equal(await worker.runNext(), true)
    assert.equal(requests.length - beforePartial, 16)
    const partialMessage = await app.readMessage(partialIdentity)
    assert.ok(partialMessage)
    assert.deepEqual(await app.readOutcome(partialIdentity), { status: 'partial', verification: 'not_run', requestVersion: 1,
      gaps: ['Execution stopped before verified completion', 'root work model budget exhausted'] })
    const partialHistory = (await db.query<{ history: unknown[] }>('SELECT history FROM lingxios.agent_os_sessions WHERE session_id=$1', [partialIdentity.sessionId])).rows[0]!.history
    assert.deepEqual(partialHistory.at(-1), { role: 'assistant', content: partialMessage.body })
    assert.equal(await worker.runNext(), false)

    missingArtifact = true
    const missingIdentity = { ...identity, runId: 'missing-artifact', sessionId: 'missing-artifact' }
    await app.enqueue({ ...missingIdentity, id: missingIdentity.runId, principalId: 'user', text: 'Create a result file.' })
    assert.equal(await worker.runNext(), true)
    assert.equal(missingArtifactCalls, 2)
    assert.equal((await app.readMessage(missingIdentity))?.body, 'File prepared.')
    assert.equal((await app.readOutcome(missingIdentity))?.status, 'satisfied')
    assert.equal((await app.readArtifact({ ...missingIdentity, principalId: 'user' }, 'missing.txt'))?.bytes.toString(), 'result')

  } finally {
    await worker?.stop()
    await controlApp?.stop()
    await app?.stop()
    await db.close()
    await new Promise<void>((resolve) => { server.close(() => resolve()); server.closeAllConnections() })
    await rm(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 })
  }
})
