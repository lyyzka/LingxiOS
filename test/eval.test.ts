import assert from 'node:assert/strict'
import { it } from 'node:test'
import { executeRequest, gradeResources } from '../src/eval/index.js'
import { readFile } from 'node:fs/promises'
import { PGlite } from '@electric-sql/pglite'
import { createLingxiOS } from '../src/index.js'
import { createWorker } from '../src/worker/index.js'
import type { SqlPool } from '../src/control-plane/pg-store.js'

it('observes committed authenticated evaluation results with and without a thread', async () => {
  const db = new PGlite()
  const pool: SqlPool = { query: async (sql, params) => {
    const result = await db.query<Record<string, unknown>>(sql, params)
    return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
  }, connect: async () => ({ query: pool.query, release() {} }) }
  await db.exec(await readFile(new URL('../../db/schema.sql', import.meta.url), 'utf8'))
  const app = await createLingxiOS({ database: pool })
  const usage = { available: false, inputTokens: 0, outputTokens: 0 }
  const worker = createWorker({ controlPlane: app, model: {
    run: async () => ({ text: 'Hello.', output: [{ role: 'assistant', content: 'Hello.' }], usage }),
    structured: async () => ({ value: { missing: [] }, model: 'fixture', usage }),
    compact: async () => { throw new Error('unexpected compaction') },
  } })
  try {
    for (const thread of [{}, { threadId: 'thread' }]) {
      const identity = { runId: thread.threadId ?? 'unthreaded', tenantId: 'tenant', agentId: 'agent',
        principalId: 'principal', sessionId: 'session', ...thread }
      const result = await executeRequest(app, worker, { id: identity.runId, ...identity, text: 'Say hello.' })
      assert.deepEqual(result.identity, identity)
      assert.equal(result.workDequeued, true)
      assert.equal(result.message?.body, 'Hello.')
      assert.equal(result.outcome?.status, 'satisfied')
      assert.equal(result.delivery, 'observed')
      assert.equal(result.externalDelivery, 'not_observed')
      assert.deepEqual(await app.readMessage(identity), result.message)
      assert.deepEqual(await app.readOutcome(identity), result.outcome)
      assert.equal(await app.readRun({ ...identity, principalId: 'other' }), null)
      assert.equal(await app.readRun({ ...identity, threadId: 'other' }), null)
    }
  } finally { await worker.stop(); await app.stop(); await db.close() }
})

it('grades observed resource state without treating missing or stale evidence as success', () => {
  const expected = [{ id: 'persisted', resource: 'document:1', expected: { body: 'complete answer', saved: true } }]
  const observed = { resource: 'document:1', requestVersion: 2, value: { body: 'complete answer', saved: true } }
  assert.deepEqual(gradeResources(2, expected, [observed]), [{ checkId: 'persisted', status: 'pass' }])
  assert.deepEqual(gradeResources(2, expected, [{ ...observed, value: { saved: true } }]), [{ checkId: 'persisted', status: 'fail' }])
  for (const observations of [[], [{ ...observed, requestVersion: 1 }], [observed, observed]]) {
    assert.equal(gradeResources(2, expected, observations)[0]?.status, 'not_observed')
  }
  assert.throws(() => gradeResources(2, [...expected, ...expected], []), /duplicate/)
  assert.throws(() => gradeResources(0, expected, []), /version/)
  const absent = [{ id: 'deleted', resource: 'document:1', expected: null }]
  assert.deepEqual(gradeResources(2, absent, [{ ...observed, value: null }]), [{ checkId: 'deleted', status: 'pass' }])
  assert.equal(gradeResources(2, absent, [{ ...observed, value: undefined }])[0]?.status, 'not_observed')
  const missingValue = JSON.parse('{"resource":"document:1","requestVersion":2}')
  assert.equal(gradeResources(2, absent, [missingValue])[0]?.status, 'not_observed')
  assert.throws(() => gradeResources(2, [{ ...absent[0]!, expected: undefined }], []), /invalid/)
  assert.throws(() => gradeResources(2, JSON.parse('[{"id":"deleted","resource":"document:1"}]'), [missingValue]), /invalid/)
})

it('runs recorded-observation CLI without claiming model quality or exposing input text', async () => {
  const { mkdtemp, writeFile, rm } = await import('node:fs/promises')
  const { tmpdir } = await import('node:os')
  const { join } = await import('node:path')
  const { fileURLToPath } = await import('node:url')
  const { spawnSync } = await import('node:child_process')
  const directory = await mkdtemp(join(tmpdir(), 'lingxios-eval-'))
  try {
    const path = join(directory, 'case.json')
    const input = { originalInput: 'private original request', requestVersion: 1,
      expectations: [{ id: 'saved', resource: 'doc', expected: true }],
      observations: [{ resource: 'doc', requestVersion: 1, value: true }] }
    const run = () => spawnSync(process.execPath, [fileURLToPath(new URL('../src/cli/eval.js', import.meta.url)), '--observations', path], { encoding: 'utf8' })
    await writeFile(path, JSON.stringify(input))
    let result = run()
    assert.equal(result.status, 0, result.stderr)
    const report = JSON.parse(result.stdout)
    assert.equal(report.mode, 'recorded_observations')
    assert.equal(report.semanticQuality, 'not_assessed')
    assert.equal(report.safetyCoverage, 'not_assessed')
    assert.equal(result.stdout.includes(input.originalInput), false)
    await writeFile(path, JSON.stringify({ ...input, observations: [] }))
    result = run()
    assert.equal(result.status, 1)
    assert.equal(JSON.parse(result.stdout).status, 'not_observed')
    await writeFile(path, '{private invalid secret')
    result = run()
    assert.equal(result.status, 2)
    assert.equal(result.stderr.includes('secret'), false)
  } finally { await rm(directory, { recursive: true, force: true }) }
})

it('reviews original inputs with a separate rubric without claiming calibrated quality', async () => {
  const { reviewAnswer } = await import('../src/eval/review.js')
  const input = { originalInput: 'Give only a hint', revisions: ['Now give the full derivation'], answer: 'A hint', rubric: 'Assess completeness against the latest request' }
  let calls = 0
  const model = { structured: async (request: { input: unknown; instructions: string }) => {
    calls++
    assert.deepEqual(request.input, input)
    assert.match(request.instructions, /ORIGINAL/)
    return { value: { verdict: 'does_not_meet', rationale: 'The revised request requires a full derivation.' }, model: 'fixture', usage: { available: false, inputTokens: 0, outputTokens: 0 } }
  }, run: async () => { throw new Error('unexpected') }, compact: async () => { throw new Error('unexpected') } }
  const result = await reviewAnswer(model, { ...input, privateMetadata: 'must not reach reviewer' } as typeof input)
  await assert.rejects(reviewAnswer({ id: '', apiKey: 'unused' }, input), /explicit model id must be non-empty/)
  assert.equal(result.verdict, 'does_not_meet')
  assert.equal(result.calibration, 'not_calibrated')
  assert.equal('status' in result, false)
  await assert.rejects(reviewAnswer({ ...model, contextWindowTokens: 10 }, input), /not truncated/)
  assert.equal(calls, 1)
  for (const [value, expected] of [
    [{ verdict: 'pass' }, /unsupported verdict/],
    [{ verdict: 'uncertain', rationale: '' }, /non-empty string/],
    [{ verdict: 'uncertain', rationale: 'a'.repeat(8001) }, /exceeds 8000/],
    [{ verdict: 'uncertain', rationale: 'unclear', extra: 'private' }, /only verdict and rationale/],
  ] as const) {
    await assert.rejects(reviewAnswer({ ...model, structured: async () => ({ value, model: 'fixture', usage: result.usage }) }, input), expected)
  }
})

it('sends semantic reviews through the real structured HTTP driver and rejects truncation', async () => {
  const http = await import('node:http')
  const { OpenAIChatDriver } = await import('../src/model/openai.js')
  const { reviewAnswer } = await import('../src/eval/review.js')
  const input = { originalInput: 'Explain the proof', revisions: [], answer: 'Only a claim', rubric: 'Require a justified derivation' }
  let finishReason = 'stop'
  let received: unknown
  const server = http.createServer((req, res) => {
    const chunks: Buffer[] = []
    req.on('data', (chunk: Buffer) => chunks.push(chunk))
    req.on('end', () => {
      received = JSON.parse(Buffer.concat(chunks).toString('utf8'))
      res.writeHead(200, { 'content-type': 'application/json' })
      res.end(JSON.stringify({ model: 'fixture-model', usage: { prompt_tokens: 100, completion_tokens: 20 }, choices: [{ finish_reason: finishReason,
        message: { content: JSON.stringify({ verdict: 'does_not_meet', rationale: 'The answer omits the requested derivation.' }) } }] }))
    })
  })
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve))
  try {
    const address = server.address()
    assert.ok(address && typeof address === 'object')
    const model = new OpenAIChatDriver('fixture-model', { apiKey: 'test', baseUrl: `http://127.0.0.1:${address.port}` })
    const result = await reviewAnswer({ id: 'fixture-model', apiKey: 'test', baseUrl: `http://127.0.0.1:${address.port}` }, input)
    assert.equal(result.calibration, 'not_calibrated')
    assert.deepEqual(result.usage, { available: true, inputTokens: 100, outputTokens: 20 })
    const body = received as { messages: Array<{ role: string; content: string }>; response_format: unknown }
    assert.deepEqual(body.response_format, { type: 'json_object' })
    assert.equal(body.messages[0]?.role, 'system')
    assert.deepEqual(JSON.parse(body.messages[1]!.content), input)
    finishReason = 'length'
    await assert.rejects(reviewAnswer(model, input), /did not finish normally/)
  } finally { await new Promise<void>(resolve => { server.close(() => resolve()); server.closeAllConnections() }) }
})

it('keeps false passes and uncertainty visible in calibration summaries', async () => {
  const { summarizeCalibration } = await import('../src/eval/calibration.js')
  assert.deepEqual(summarizeCalibration([]), { sampleCount: 0, agreements: 0, falsePasses: 0, falseFailures: 0, uncertain: 0, agreementRate: null, falsePassFraction: null })
  const samples = [
    { inputSha256: 'a'.repeat(64), expected: 'does_not_meet' as const, actual: 'meets_rubric' as const },
    { inputSha256: 'b'.repeat(64), expected: 'meets_rubric' as const, actual: 'does_not_meet' as const },
    { inputSha256: 'c'.repeat(64), expected: 'does_not_meet' as const, actual: 'uncertain' as const },
    { inputSha256: 'd'.repeat(64), expected: 'meets_rubric' as const, actual: 'meets_rubric' as const },
  ]
  assert.deepEqual(summarizeCalibration(samples), { sampleCount: 4, agreements: 1, falsePasses: 1, falseFailures: 1, uncertain: 1, agreementRate: 0.25, falsePassFraction: 0.25 })
  assert.throws(() => summarizeCalibration([samples[0]!, samples[0]!]), /duplicate/)
})

it('keeps independent resource observations scoped and separate from answer claims', async () => {
  const { reviewAnswer } = await import('../src/eval/review.js')
  const input = { originalInput: 'Create answer.json containing the sum', revisions: [], answer: 'The attachment is ready.', rubric: 'Deliver the requested JSON file',
    observations: [{ resource: 'artifact:answer.json', requestVersion: 1, value: { status: 'observed', source: 'app.readArtifact', json: { sum: 385 }, externalDelivery: 'not_observed' } }] }
  let calls = 0
  const model = { structured: async (request: { input: unknown; instructions: string }) => {
    calls++
    assert.deepEqual(request.input, input)
    assert.match(request.instructions, /independent resource reads/)
    assert.match(request.instructions, /not_observed remain unknown/)
    return { value: { verdict: 'uncertain', rationale: 'The file is observed; external delivery is unknown.' }, model: 'fixture', usage: { available: false, inputTokens: 0, outputTokens: 0 } }
  }, run: async () => { throw new Error('unexpected') }, compact: async () => { throw new Error('unexpected') } }
  assert.equal((await reviewAnswer(model, input)).verdict, 'uncertain')
  for (const observations of [
    [...input.observations, ...input.observations],
    [{ ...input.observations[0]!, requestVersion: 2 }],
    [{ ...input.observations[0]!, value: undefined }],
    [{ ...input.observations[0]!, value: 'x'.repeat(65_537) }],
  ]) await assert.rejects(reviewAnswer(model, { ...input, observations }), /review observations/)
  assert.equal(calls, 1)
})

it('records truncated-review diagnostics when rerunning one evaluation case', async () => {
  const { createServer } = await import('node:http')
  const { execFile } = await import('node:child_process')
  const { mkdtemp, readFile, rm } = await import('node:fs/promises')
  const { tmpdir } = await import('node:os')
  const { join, dirname, resolve } = await import('node:path')
  const { fileURLToPath } = await import('node:url')
  const directory = await mkdtemp(join(tmpdir(), 'lingxios-review-report-'))
  let reviewBudget: number | undefined
  let runtimeBudget: number | undefined
  const server = createServer((req, res) => {
    const chunks: Buffer[] = []
    req.on('data', (chunk: Buffer) => chunks.push(chunk))
    req.on('end', () => {
      const request = JSON.parse(Buffer.concat(chunks).toString('utf8'))
      if (request.stream) {
        runtimeBudget = request.max_tokens
        assert.equal(request.tool_choice, 'auto')
        assert.equal(request.response_format, undefined)
        assert.deepEqual(request.tools.map((tool: { function: { name: string } }) => tool.function.name), ['ipython', 'task__read_attachment', 'task__contract', 'task__ask', 'task__check_receipt', 'task__check_resource', 'task__inspect'])
        res.writeHead(200, { 'content-type': 'text/event-stream' })
        const delta = { content: '385' }
        res.end(`data: ${JSON.stringify({ model: 'fixture', choices: [{ index: 0, delta, finish_reason: 'tool_calls' }] })}\n\ndata: [DONE]\n\n`)
      } else {
        if (request.messages[0]?.content.includes('Review the answer')) reviewBudget = request.max_tokens
        res.writeHead(200, { 'content-type': 'application/json' })
        res.end(JSON.stringify({ choices: [{ finish_reason: 'length', message: { content: '{}' } }] }))
      }
    })
  })
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve))
  try {
    const address = server.address()
    assert.ok(address && typeof address === 'object')
    const output = join(directory, 'output')
    const result = await new Promise<{ code: number | string | null | undefined; stderr: string }>(resolve => {
      execFile(process.execPath, [fileURLToPath(new URL('../../scripts/eval-live.mjs', import.meta.url)),
        '--output', output, '--repeat', '1', '--case', 'chat-only'], {
        timeout: 30_000, env: { ...process.env, LINGXIOS_TEST_DATABASE_URL: '', AGENT_OS_MODEL: 'fixture',
          LINGXIOS_EVAL_REVIEW_MAX_OUTPUT_TOKENS: '8192', LINGXIOS_EVAL_REVIEW_TIMEOUT_MS: '90000',
          AGENT_OS_MODEL_API_KEY: 'fixture-secret', AGENT_OS_MODEL_BASE_URL: `http://127.0.0.1:${address.port}` },
      }, (error, _stdout, stderr) => resolve({ code: error?.code, stderr }))
    })
    assert.equal(result.code, 1, result.stderr)
    const summary = JSON.parse(await readFile(join(output, 'summary.json'), 'utf8'))
    assert.deepEqual(summary.selectedCases, ['chat-only'])
    assert.equal(summary.sampleCount, 1)
    assert.deepEqual(summary.budgets, { runtimeOutputTokens: 4096, reviewOutputTokens: 8192, reviewTimeoutMs: 90000, contextWindowTokens: 32768 })
    assert.equal(runtimeBudget, 4096)
    assert.equal(reviewBudget, 8192)
    const bytes = await readFile(join(output, 'chat-only-1.json'), 'utf8')
    assert.doesNotMatch(bytes, /fixture-secret/)
    const report = JSON.parse(bytes)
    assert.ok(report.checks.every((check: { status: string }) => check.status === 'pass'))
    assert.deepEqual(report.reviewFailureDiagnostics, { finishReasons: ['length'] })
    assert.equal(report.review, undefined)
  } finally {
    await new Promise<void>(resolve => { server.close(() => resolve()); server.closeAllConnections() })
    assert.equal(dirname(resolve(directory)), resolve(tmpdir()))
    await rm(directory, { recursive: true, force: true })
  }
})
