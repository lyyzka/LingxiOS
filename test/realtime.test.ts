import assert from 'node:assert/strict'
import { it } from 'node:test'
import { readFile } from 'node:fs/promises'
import http from 'node:http'
import { Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { PGlite } from '@electric-sql/pglite'
import { createLingxiOS } from '../src/app/index.js'
import { AgentRuntime } from '../src/runtime/runtime.js'
import { OpenAIChatDriver } from '../src/model/openai.js'
import { HttpHostClient } from '../src/host/http-client.js'
import type { SqlPool } from '../src/control-plane/pg-store.js'
import type { RunStreamEvent } from '../src/app/realtime.js'
import { consumeRunStreamEvent, createRunView } from '../src/ui/index.js'
import { abortable } from '../src/deadline.js'
import { nullLogger } from '../src/logging.js'

it('streams safe body over worker HTTP and browser SSE before usage/verification, then replays the committed result', async t => {
  const db = new PGlite()
  await db.exec(await readFile(new URL('../../db/schema.sql', import.meta.url), 'utf8'))
  // PGlite has one connection. Hold it across BEGIN/COMMIT so concurrent SSE cannot see uncommitted rows.
  let tail = Promise.resolve()
  const acquire = async () => {
    let release!: () => void
    const previous = tail
    tail = new Promise<void>(resolve => { release = resolve })
    await previous
    return release
  }
  const query: SqlPool['query'] = async (sql, params) => {
    const result = await db.query<Record<string, unknown>>(sql, params)
    return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
  }
  const pool: SqlPool = { async query(sql, params) {
    const release = await acquire()
    try { return await query(sql, params) } finally { release() }
  }, async connect() { return { query, release: await acquire() } } }
  const logger = { ...nullLogger, warn: (message: string, fields?: Record<string, unknown>) => t.diagnostic(message + ' ' + JSON.stringify(fields)),
    error: (message: string, fields?: Record<string, unknown>) => t.diagnostic(message + ' ' + JSON.stringify(fields)) }
  const app = await createLingxiOS({ database: pool, logger, realtime: { allowDraft: () => true } })
  const identity = { runId: 'stream', tenantId: 'tenant', agentId: 'agent', sessionId: 'session', principalId: 'human' }
  await app.enqueue({ id: identity.runId, ...identity, text: 'Say hello', mode: 'chat' })
  const controlPort = await app.listenControlPlane({ serviceToken: 'test-service', port: 0 })
  const host = new HttpHostClient({ baseUrl: `http://127.0.0.1:${controlPort}`, serviceToken: 'test-service', workerId: 'stream-worker' })
  const work = (await host.claimWork())!
  assert.equal((await host.loadContext(work)).previewAllowed, true)
  let releaseModel!: () => void
  const providerGate = new Promise<void>(resolve => { releaseModel = resolve })
  const candidate = JSON.stringify({ body: 'Hello world', status: 'satisfied', checks: [{ requirement: 'Say hello', status: 'met', basis: 'Greeting supplied.' }], gaps: [] })
  let providerAt = 0, browserAt = 0, firstBody!: () => void, latestId = 0
  const bodySeen = new Promise<void>(resolve => { firstBody = resolve })
  const model = new OpenAIChatDriver('sse-stub', { apiKey: 'test', fetchImpl: async (_url, init) => {
    const request = JSON.parse(String(init?.body))
    if (!request.stream) return Response.json({ model: 'stub', choices: [{ finish_reason: 'stop', message: { content: '{"missing":[]}' } }],
      usage: { prompt_tokens: 10, completion_tokens: 10 } })
    return new Response(new ReadableStream<Uint8Array>({ async start(controller) {
      const encode = (value: unknown) => new TextEncoder().encode(`data: ${JSON.stringify(value)}\n\n`)
      providerAt = performance.now()
      controller.enqueue(encode({ choices: [{ delta: { content: '{"body":"Hello', reasoning_content: 'PRIVATE REASONING' } }] }))
      await providerGate
      controller.enqueue(encode({ model: 'stub', choices: [{ delta: { content: candidate.slice('{"body":"Hello'.length) }, finish_reason: 'stop' }],
        usage: { prompt_tokens: 10, completion_tokens: 10 } }))
      controller.enqueue(new TextEncoder().encode('data: [DONE]\n\n'))
      controller.close()
    } }))
  } })
  const browser = http.createServer((request, result) => {
    const closed = new AbortController()
    result.once('close', () => closed.abort())
    void app.streamRun(identity, { signal: closed.signal, lastEventId: request.headers['last-event-id'] as string | undefined ?? null }).then(async response => {
      result.writeHead(response.status, Object.fromEntries(response.headers))
      await pipeline(Readable.fromWeb(response.body!), result)
    }).catch(() => result.destroy())
  })
  await new Promise<void>(resolve => browser.listen(0, '127.0.0.1', resolve))
  const url = `http://127.0.0.1:${(browser.address() as { port: number }).port}`
  const stopBrowser = new AbortController()
  let view = createRunView(work.id), wire = ''
  const consume = (async () => {
    const response = await fetch(url, { signal: stopBrowser.signal })
    assert.equal(response.headers.get('x-accel-buffering'), 'no')
    const reader = response.body!.getReader(), decoder = new TextDecoder()
    let pending = ''
    try {
      while (true) {
        const next = await reader.read()
        if (next.done) break
        const chunk = decoder.decode(next.value, { stream: true })
        wire += chunk; pending += chunk
        let end: number
        while ((end = pending.indexOf('\n\n')) >= 0) {
          const frame = pending.slice(0, end); pending = pending.slice(end + 2)
          const line = frame.split('\n').find(value => value.startsWith('data: '))
          if (!line) continue
          const item = JSON.parse(line.slice(6)) as RunStreamEvent
          if (item.type === 'event') latestId = item.event.seq
          view = consumeRunStreamEvent(view, item)
          if (item.type === 'preview' && view.draft && !browserAt) { browserAt = performance.now(); firstBody() }
        }
      }
    } finally { await reader.cancel().catch(() => {}) }
  })()
  void consume.catch(() => {})
  const running = new AgentRuntime(host, model, { execute: async () => { throw new Error('Python must stay disabled') } }, { logger }).runWork(work)
  try {
    await abortable(bodySeen, AbortSignal.timeout(5000))
    assert.equal(view.draft, 'Hello')
    assert.equal(view.message, null)
    assert.equal((await app.readRunState(identity))?.run.status, 'leased')
    assert.equal((await pool.query('SELECT COUNT(*)::integer AS count FROM lingxios.agent_model_budget_calls WHERE observation IS NOT NULL')).rows[0]?.['count'], 0)
    assert.ok(!wire.includes('PRIVATE REASONING') && !wire.includes('Greeting supplied'))
    releaseModel()
    await running
    await abortable(consume, AbortSignal.timeout(5000))
    assert.equal((view as ReturnType<typeof createRunView>).message?.body, 'Hello world')
    assert.equal(view.lifecycle, 'succeeded')
    assert.equal(view.draft, '')
    const replay = await app.streamRun(identity, { lastEventId: String(latestId) })
    const replayed = await replay.text()
    assert.match(replayed, /Hello world/)
    assert.match(replayed, /candidateHash/)
    assert.doesNotMatch(replayed, /event: event/)
    await assert.rejects(app.streamRun({ ...identity, tenantId: 'other' }), /identity/)
    await assert.rejects(app.streamRun({ ...identity, principalId: 'other' }), /identity/)
    await assert.rejects(app.streamRun(identity, { lastEventId: '-1' }), /Last-Event-ID/)
    t.diagnostic(`SSE stub provider first content → UI draft: ${(browserAt - providerAt).toFixed(1)}ms; preview arrived before usage settlement`)
  } catch (error) {
    t.diagnostic('providerAt: ' + providerAt)
    t.diagnostic('internal phases: ' + JSON.stringify((await pool.query('SELECT kind FROM lingxios.agent_run_events ORDER BY seq')).rows))
    t.diagnostic('wire: ' + wire.slice(-2000))
    t.diagnostic('state: ' + JSON.stringify((await app.readRunState(identity))?.run))
    throw error
  } finally {
    releaseModel(); stopBrowser.abort()
    await running.catch(() => {}); await consume.catch(() => {})
    await new Promise<void>(resolve => { browser.close(() => resolve()); browser.closeAllConnections() })
    await app.stop(); await db.close()
  }
})
