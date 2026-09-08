// Local framework benchmark. No provider credentials, network model calls, or product data.
// Run after npm run build. Results compare feature switches, not a historical release.
import assert from 'node:assert/strict'
import { readFile, writeFile } from 'node:fs/promises'
import http from 'node:http'
import { Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { setTimeout as delay } from 'node:timers/promises'
import { PGlite } from '@electric-sql/pglite'
import { createLingxiOS } from '../dist/src/app/index.js'
import { PgSessionStore } from '../dist/src/control-plane/pg-store.js'
import { HttpHostClient } from '../dist/src/host/http-client.js'
import { AgentRuntime } from '../dist/src/runtime/runtime.js'
import { AgentWorker } from '../dist/src/worker/worker.js'
import { OpenAIChatDriver } from '../dist/src/model/openai.js'
import { COMPACTION_INSTRUCTIONS } from '../dist/src/context/compiler.js'
import { createRunView, consumeRunStreamEvent } from '../dist/src/ui/index.js'
import { sessionKeyOf } from '../dist/src/protocol/types.js'
import { nullLogger } from '../dist/src/logging.js'
import { sqlOperation } from '../dist/src/control-plane/deadline-pool.js'

const repetitions = Number(process.env.LINGXIOS_BENCH_REPETITIONS ?? 20)
assert.ok(Number.isSafeInteger(repetitions) && repetitions >= 2 && repetitions <= 100)
const output = process.argv[2] ?? 'performance-benchmark.json'
const encoder = new TextEncoder()
const percentile = (values, p) => {
  const sorted = values.filter(Number.isFinite).sort((a, b) => a - b)
  return sorted.length ? Number(sorted[Math.ceil(sorted.length * p) - 1].toFixed(2)) : null
}
const summarize = rows => Object.fromEntries(['queueMs','providerToUiMs','ttftMs','commitToUiMs','totalMs','modelInputBytes','httpRequests','sqlWrites','sqlParameterBytes','modelCalls','stubInputTokens','stubOutputTokens']
  .map(key => [key, { p50: percentile(rows.map(row => row[key]), .5), p95: percentile(rows.map(row => row[key]), .95) }]))

async function benchmark(enabled) {
  const setup = performance.now(), db = new PGlite(), records = new Map()
  await db.exec(await readFile(new URL('../db/schema.sql', import.meta.url), 'utf8'))
  await db.exec('CREATE TABLE public.benchmark_writes(id TEXT PRIMARY KEY,work_id TEXT NOT NULL,value TEXT NOT NULL)')
  let tail = Promise.resolve(), writes = 0, parameterBytes = 0, httpRequests = 0, claimQuery
  const acquire = async () => {
    const previous = tail
    let release
    tail = new Promise(resolve => { release = resolve })
    await previous
    return release
  }
  const query = async (sql, params) => {
    if (sqlOperation(sql) === 'write') writes++
    parameterBytes += (params ?? []).reduce((sum, value) => sum + (typeof value === 'string' ? Buffer.byteLength(value) : 0), 0)
    if (sql.includes('FOR UPDATE OF work SKIP LOCKED') && sql.includes('LEFT JOIN lingxios.agent_os_session_routes')) claimQuery ??= { sql, params }
    const result = await db.query(sql, params)
    return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
  }
  const pool = {
    async query(sql, params) { const release = await acquire(); try { return await query(sql, params) } finally { release() } },
    async connect() {
      const release = await acquire()
      let committedWork
      return { async query(sql, params) {
        const result = await query(sql, params)
        if (sql.includes('INSERT INTO lingxios.agent_results(')) committedWork = params[1]
        if (sql === 'COMMIT' && committedWork) records.get(committedWork).commitAt = performance.now()
        return result
      }, release }
    },
  }
  const app = await createLingxiOS({ database: pool, logger: nullLogger,
    capabilityResolver: { resolve: async () => [{ name: 'bench', methods: ['write'] }] },
    tools: [{ name: 'bench__write', action: 'bench.write', effect: 'transaction', approval: false,
      description: 'Record one benchmark row in the disposable benchmark database.',
      parameters: { type: 'object', properties: { value: { type: 'string' } }, required: ['value'], additionalProperties: false },
      parse(input) { assert.equal(typeof input.value, 'string'); return input },
      async authorize(context) { assert.equal(context.work.tenantId, 'bench'); assert.equal(context.work.principalId, 'human') },
      async execute(context, input) {
        const id = context.action.idempotencyKey
        await context.database.query('INSERT INTO public.benchmark_writes VALUES($1,$2,$3)', [id, context.work.id, input.value])
        return { ok: true, value: { id, value: input.value } }
      },
      async verify(context, input, result) {
        const actual = (await context.database.query('SELECT value FROM public.benchmark_writes WHERE id=$1 AND work_id=$2', [result.id, context.work.id])).rows[0]
        return { status: actual?.value === input.value ? 'passed' : 'failed', evidence: { id: result.id, readable: Boolean(actual) } }
      },
    }],
    performance: { notifications: enabled, contextSnapshot: enabled, outboxConcurrency: enabled ? 4 : 1 },
    realtime: { allowDraft: () => enabled } })
  const port = await app.listenControlPlane({ serviceToken: 'benchmark', port: 0 })
  const host = new HttpHostClient({ baseUrl: `http://127.0.0.1:${port}`, serviceToken: 'benchmark', workerId: 'benchmark',
    fetchImpl: async (url, init) => { httpRequests++; return fetch(url, init) } })
  const model = new OpenAIChatDriver('sse-benchmark', { apiKey: 'stub', maxOutputTokens: 1024,
    contextWindowTokens: 128_000, fetchImpl: async (_url, init) => {
      const request = JSON.parse(String(init.body))
      const marker = /BENCH:([a-z]+-[0-9]+)/.exec(JSON.stringify(request.messages))?.[1]
      const row = records.get(marker)
      if (row) { row.modelCalls++; row.modelInputBytes += Buffer.byteLength(String(init.body)) }
      if (!request.stream) return Response.json({ model: 'stub', choices: [{ finish_reason: 'stop', message: {
        content: JSON.stringify(request.messages[0]?.content === COMPACTION_INSTRUCTIONS
          ? { observedResults: 'Historical conversation.', decisions: '', remainingWork: '', uncertainties: '' } : { missing: [] }),
      } }], usage: { prompt_tokens: 100, completion_tokens: 10 } })
      assert.ok(row, 'benchmark request must retain the original request marker')
      const hop = ++row.hops
      const needsTool = ['multihop','attachment','writing'].includes(row.scenario) && hop <= 2
      return new Response(new ReadableStream({ async start(controller) {
        const send = value => controller.enqueue(encoder.encode(`data: ${JSON.stringify(value)}\n\n`))
        await delay(20)
        if (needsTool) {
          send({ choices: [{ delta: { tool_calls: [{ index: 0, id: `call-${hop}`, type: 'function', function: {
            name: row.scenario === 'attachment' ? 'task__read_attachment' : row.scenario === 'writing' ? 'bench__write' : 'task__inspect',
            arguments: row.scenario === 'attachment' ? JSON.stringify({ id: 'source', sourceVersion: '1', offset: (hop - 1) * 512, limit: 512 })
              : row.scenario === 'writing' ? JSON.stringify({ value: `row-${hop}` }) : '{}',
          } }] }, finish_reason: 'tool_calls' }], usage: { prompt_tokens: 100, completion_tokens: 10 } })
        } else {
          row.providerAt = performance.now()
          const candidate = JSON.stringify({ body: 'Done.', status: 'satisfied', checks: [{ requirement: 'Reply Done.', status: 'met', basis: 'Reply provided.' }], gaps: [] })
          send({ choices: [{ delta: { content: '{"body":"Do', reasoning_content: 'PRIVATE' } }] })
          await delay(80)
          send({ choices: [{ delta: { content: candidate.slice('{"body":"Do'.length) }, finish_reason: 'stop' }],
            usage: { prompt_tokens: 100, completion_tokens: 10 } })
          row.modelEndedAt = performance.now()
        }
        controller.enqueue(encoder.encode('data: [DONE]\n\n')); controller.close()
      } }))
    } })
  const runtime = new AgentRuntime(host, model, { execute: async () => { throw new Error('unexpected Python') } }, {
    logger: nullLogger, performance: { checkpointDedup: enabled, promptCache: enabled, asyncCompaction: enabled, onDemandAttachments: enabled },
  })
  const worker = new AgentWorker({ host, workerId: 'benchmark', maxConcurrentRuns: 2, shutdownGraceMs: 5000,
    runtime: { runWork: async (work, signal) => { records.get(work.id).claimedAt = performance.now(); await runtime.runWork(work, signal) } } })
  const browser = http.createServer((request, response) => {
    const id = new URL(request.url, 'http://benchmark').pathname.slice(1), row = records.get(id)
    const stop = new AbortController()
    response.once('close', () => stop.abort())
    void app.streamRun(row.identity, { signal: stop.signal }).then(async stream => {
      response.writeHead(stream.status, Object.fromEntries(stream.headers))
      await pipeline(Readable.fromWeb(stream.body), response)
    }).catch(error => response.destroy(error))
  })
  await new Promise(resolve => browser.listen(0, '127.0.0.1', resolve))
  await worker.start()
  const setupMs = performance.now() - setup
  const results = []
  async function run(scenario, index) {
    const id = `${scenario}-${index}`, identity = { runId: id, tenantId: 'bench', agentId: 'agent', principalId: 'human', sessionId: id }
    if (scenario === 'history') await new PgSessionStore(pool).save({ key: sessionKeyOf(identity), ...identity,
      revision: 0, compactionEpoch: 0, appliedWorkIds: [], history: Array.from({ length: 90 }, () => ({ role: 'user', content: `BENCH:${id} ` + 'Past observation. '.repeat(60) })) })
    const row = { scenario, identity, hops: 0, modelCalls: 0, modelInputBytes: 0, start: performance.now(),
      initialWrites: writes, initialBytes: parameterBytes, initialHttp: httpRequests }
    records.set(id, row)
    await app.enqueue({ id, ...identity, text: `BENCH:${id} ${scenario === 'writing' ? 'Record two benchmark rows, then ' : ''}Reply Done.`,
      codeExecution: 'disabled', mode: scenario === 'short' || scenario === 'backlog' ? 'chat' : scenario === 'writing' ? 'execute' : 'read',
      ...(scenario === 'attachment' ? { attachments: [{ id: 'source', sourceVersion: '1', name: 'source.txt', mimeType: 'text/plain', size: 30_000, text: 'Source material. '.repeat(1800) }] } : {}) })
    const response = await fetch(`http://127.0.0.1:${browser.address().port}/${id}`, { signal: AbortSignal.timeout(30_000) })
    let view = createRunView(id), pending = ''
    const decoder = new TextDecoder()
    for await (const bytes of response.body) {
      pending += decoder.decode(bytes, { stream: true })
      let end
      while ((end = pending.indexOf('\n\n')) >= 0) {
        const frame = pending.slice(0, end); pending = pending.slice(end + 2)
        const data = frame.split('\n').find(line => line.startsWith('data: '))
        if (!data) continue
        assert.ok(!data.includes('PRIVATE'))
        view = consumeRunStreamEvent(view, JSON.parse(data.slice(6)))
        if ((view.draft || view.message) && !row.firstUiAt) row.firstUiAt = performance.now()
        if (view.message && !row.completeUiAt) row.completeUiAt = performance.now()
      }
    }
    assert.equal(view.message?.body, 'Done.', JSON.stringify(await app.readRunState(identity)))
    assert.equal(view.lifecycle, 'succeeded')
    assert.ok(row.completeUiAt >= row.commitAt, 'completion must follow commit')
    const usage = (await pool.query(`SELECT COUNT(*)::integer AS calls,SUM(input_tokens)::integer AS input,SUM(output_tokens)::integer AS output
      FROM lingxios.agent_model_budget_calls WHERE root_work_id=$1`, [id])).rows[0]
    assert.equal(row.modelCalls, usage.calls)
    if (['attachment','multihop','writing'].includes(scenario)) {
      const receipts = await pool.query(`SELECT COUNT(*)::integer AS count FROM lingxios.agent_action_intents intent
        JOIN lingxios.agent_action_ledger receipt USING(idempotency_key)
        WHERE intent.intent->>'workId'=$1 AND intent.intent->'action'->>'cellId' LIKE 'step:%'
          AND receipt.result->>'ok'='true'`, [id])
      assert.equal(receipts.rows[0].count, 2, 'benchmark tools must actually succeed')
    }
    const result = { scenario, queueMs: row.claimedAt - row.start, providerToUiMs: row.firstUiAt - row.providerAt,
      ttftMs: row.firstUiAt - row.start, commitToUiMs: row.completeUiAt - row.commitAt, totalMs: row.completeUiAt - row.start,
      modelCalls: row.modelCalls, modelInputBytes: row.modelInputBytes, stubInputTokens: usage.input, stubOutputTokens: usage.output,
      httpRequests: httpRequests - row.initialHttp, sqlWrites: writes - row.initialWrites, sqlParameterBytes: parameterBytes - row.initialBytes }
    results.push(result)
    return result
  }
  try {
    await run('short', 0) // warm-up, excluded below
    results.length = 0
    for (const scenario of ['short','multihop','attachment','history','writing']) {
      for (let i = 1; i <= repetitions; i++) await run(scenario, i)
      process.stdout.write(`${enabled ? 'enabled' : 'disabled'} ${scenario}: ${repetitions} completed\n`)
    }
    await Promise.all(Array.from({ length: 8 }, (_, i) => run('backlog', i)))
    await worker.stop()
    const memory = process.memoryUsage()
    let explain
    if (claimQuery) {
      await db.exec(`INSERT INTO lingxios.agent_work_items(id,tenant_id,agent_id,session_id,kind,lane,trigger_ref)
        SELECT 'profile-'||n,'profile','agent','profile-'||n,'turn','background','profile' FROM generate_series(1,1000) n`)
      await db.exec('ANALYZE lingxios.agent_work_items')
      explain = (await db.query(`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${claimQuery.sql}`, claimQuery.params)).rows[0]['QUERY PLAN']
    }
    return { enabled, setupMs, memory, rows: results,
      scenarios: Object.fromEntries(['short','multihop','attachment','history','writing','backlog'].map(scenario => [scenario, summarize(results.filter(row => row.scenario === scenario))])), explain }
  } finally {
    await worker.stop()
    await new Promise(resolve => { browser.close(resolve); browser.closeAllConnections() })
    await app.stop(); await db.close()
  }
}

const report = { measuredAt: new Date().toISOString(), node: process.version, platform: process.platform, repetitions,
  scope: 'Local PGlite, real worker HTTP and browser SSE, controlled OpenAI-compatible SSE stub. No real model A/B. Backlog counters overlap between concurrent runs. SQL writes count submitted statements, not WAL bytes; tokens are stub values. Memory is a process snapshot, not a peak or tenant attribution.',
  variants: [] }
for (const enabled of [false, true]) report.variants.push(await benchmark(enabled))
await writeFile(output, JSON.stringify(report, null, 2) + '\n')
process.stdout.write(`Saved ${output}\n`)
