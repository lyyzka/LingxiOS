import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { createServer } from 'node:http'
import { setTimeout as delay } from 'node:timers/promises'
import { Pool } from 'pg'
import { createLingxiOS, packageResources } from 'lingxios'
import { LectureDeckService, PostgresLectureRepository, FileLecturePublisher } from 'lingxios/lecture-deck'

assert.ok(process.env.LINGXIOS_LECTURE_TEST_DATABASE_URL, 'a fresh lecture PostgreSQL database is required')
const pool = new Pool({ connectionString: process.env.LINGXIOS_LECTURE_TEST_DATABASE_URL, max: 8 })
const directory = await mkdtemp(join(tmpdir(), 'lingxios-lecture-remote-'))
let app, controlPort, fault, intercepted, calls = 0, slideCalls = 0
const children = []
const model = createServer(async (req, res) => {
  const chunks = []; for await (const chunk of req) chunks.push(chunk)
  const body = JSON.parse(Buffer.concat(chunks)), input = JSON.parse(body.messages[1].content)
  calls++
  let value
  if (input.slides) value = { passed: true, issues: [] }
  else if (!input.pageId) value = { title: 'Remote lecture', audience: 'Learners', prerequisites: [], objectives: [{ id: 'o', description: 'Learn' }], chapters: [{ id: 'c', order: 0, title: 'Chapter', objectiveIds: ['o'], slideIds: ['pg_1', 'pg_2', 'pg_3'] }], targetSlideCount: 3, durationMinutes: 10, terminology: {} }
  else {
    slideCalls++
    value = { id: input.pageId, order: input.order, chapterId: input.chapter.id, role: input.order ? 'content' : 'cover', purpose: 'explain', title: `Page ${input.order + 1}`, conclusion: 'Grounded conclusion', visualKind: 'diagram',
      bodyHtml: '<svg role="img" aria-label="concept" viewBox="0 0 1280 720"><rect data-anchor-id="main" x="100" y="100" width="500" height="300" fill="#53d6c7"/></svg>', anchors: [{ id: 'main', x: 100, y: 100, width: 500, height: 300 }],
      steps: [{ id: 'explain', title: 'Explain', explanation: 'Explanation', anchorIds: ['main'], claimIds: ['claim'] }], bindings: [{ claimId: 'claim', snapshotId: input.evidence[0].id, evidenceMarkers: ['S1'], kind: 'source', statement: 'Claim' }] }
  }
  res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify({ model: 'fixture', usage: { prompt_tokens: 100, completion_tokens: 100 }, choices: [{ finish_reason: 'stop', message: { content: JSON.stringify(value) } }] }))
})
const proxy = createServer(async (req, res) => {
  const chunks = []; for await (const chunk of req) chunks.push(chunk)
  const bytes = Buffer.concat(chunks), body = bytes.length ? JSON.parse(bytes) : {}
  const hit = req.url.endsWith('/lecture') && fault?.matches(body.command)
  if (hit && fault.before) { intercepted = { url: req.url, body }; return }
  try {
    const response = await fetch(`http://127.0.0.1:${controlPort}${req.url}`, { method: req.method, headers: { authorization: req.headers.authorization, 'content-type': 'application/json' }, ...(bytes.length ? { body: bytes } : {}) })
    const text = await response.text()
    if (hit && response.ok) { intercepted = { url: req.url, body }; return }
    res.writeHead(response.status, { 'content-type': 'application/json' }).end(text)
  } catch { res.writeHead(502).end('{}') }
})
async function until(check) {
  const end = Date.now() + 30_000
  while (!await check()) { assert.ok(Date.now() < end, `lecture condition timed out: ${children.map(item => item.logs).join('\n')}`); await delay(25) }
}
function worker(id) {
  const child = spawn(process.execPath, ['dist/src/worker/main.js'], { windowsHide: true,
    env: { ...process.env, NODE_ENV: 'test', AGENT_OS_KERNEL_ISOLATION: 'process', AGENT_OS_CONTROL_PLANE_URL: `http://127.0.0.1:${proxy.address().port}`,
      AGENT_OS_SERVICE_TOKEN: 'lecture', AGENT_OS_WORKER_ID: id, AGENT_OS_WORKER_PORT: '0', AGENT_OS_POLL_IDLE_MS: '50',
      AGENT_OS_MAX_CONCURRENT_RUNS: '1', AGENT_OS_HOMES_ROOT: join(directory, id),
      AGENT_OS_MODEL: 'fixture', AGENT_OS_MODEL_API_KEY: 'fixture', AGENT_OS_MODEL_BASE_URL: `http://127.0.0.1:${model.address().port}` },
    stdio: ['ignore', 'ignore', 'pipe'] })
  const item = { child, exited: once(child, 'exit'), logs: '' }
  child.stderr.on('data', data => { item.logs = (item.logs + data).slice(-8000) }); children.push(item)
  return item
}
async function kill(item) { item.child.kill('SIGKILL'); await item.exited }
try {
  assert.equal((await pool.query("SELECT 1 FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema') LIMIT 1")).rows.length, 0)
  await pool.query(await readFile(packageResources().schema, 'utf8'))
  const publisher = new FileLecturePublisher(join(directory, 'published'))
  const unused = async () => { throw new Error('control plane must not author or review') }
  const service = new LectureDeckService({ repository: new PostgresLectureRepository(pool), publisher,
    author: { plan: unused, slide: unused }, reviewer: { review: unused },
    evidence: { search: async () => [{ marker: 'S1', sourceId: 'source', sourceVersion: 'v1', chunkId: 'chunk', title: 'Source', excerpt: 'Evidence' }] } })
  app = await createLingxiOS({ database: pool, lectureDeck: service, kernel: { homesRoot: join(directory, 'control') } })
  controlPort = await app.listenControlPlane({ serviceToken: 'lecture', port: 0 })
  model.listen(0, '127.0.0.1'); await once(model, 'listening')
  proxy.listen(0, '127.0.0.1'); await once(proxy, 'listening')
  const boundaries = [
    { name: 'before-checkpoint', before: true, matches: c => c?.operation === 'checkpoint' && c.value.stage === 'author-slide' },
    { name: 'after-checkpoint', matches: c => c?.operation === 'checkpoint' && c.value.stage === 'author-slide' },
    { name: 'after-intent', matches: c => c?.operation === 'claimPublication' },
    { name: 'after-file', matches: c => c?.operation === 'publish' },
    { name: 'before-terminal', before: true, matches: c => c?.operation === 'save' && c.record.status === 'ready' },
  ]
  for (const boundary of boundaries) {
    const identity = { tenantId: 't', principalId: 'p', agentId: 'a', sessionId: boundary.name }
    const created = await app.lectures.enqueueLecture({ ...identity, id: boundary.name, request: { requirements: 'Teach', targetSlideCount: 3 } })
    const scope = { ...identity, deckId: created.deckId }
    let original = worker(`${boundary.name}-original`)
    await until(async () => (await app.lectures.readLecture(scope)).status === 'awaiting_outline_approval')
    assert.equal(slideCalls, boundaries.indexOf(boundary) * 3 + (boundaries.indexOf(boundary) > 0 ? 1 : 0), 'only prior approved decks may have slides')
    fault = boundary; intercepted = undefined
    const approval = { ...identity, deckId: created.deckId, operation: 'approve_outline', idempotencyKey: `approve-${boundary.name}`, request: { expectedRevision: 1 } }
    await app.lectures.enqueueLectureOperation(approval)
    assert.equal((await app.lectures.enqueueLectureOperation(approval)).deduplicated, true)
    await until(() => intercepted)
    await kill(original)
    const old = intercepted, before = calls, oldRecord = await app.lectures.readLecture(scope)
    fault = undefined
    const staleWorkId = decodeURIComponent(old.url.split('/')[3])
    await pool.query("UPDATE lingxios.agent_work_items SET lease_expires_at=NOW()-INTERVAL '1 minute' WHERE id=$1", [staleWorkId])
    await pool.query("UPDATE lingxios.agent_os_session_leases SET expires_at=NOW()-INTERVAL '1 minute' WHERE work_id=$1", [staleWorkId])
    await pool.query("UPDATE lingxios.agent_os_workers SET last_seen_at=NOW()-INTERVAL '1 day'")
    const stale = await fetch(`http://127.0.0.1:${controlPort}${old.url}`, { method: 'POST', headers: { authorization: 'Bearer lecture', 'content-type': 'application/json' }, body: JSON.stringify(old.body) })
    assert.equal(stale.status, 409, 'expired worker write must be fenced')
    original = worker(`${boundary.name}-replacement`)
    try {
      await until(async () => (await app.lectures.readLecture(scope)).status === 'ready')
    } catch (error) {
      const state = await pool.query(`SELECT work.id,work.status,work.error,deck.record
        FROM lingxios.agent_work_items work JOIN lingxios.lecture_decks deck ON deck.id=work.meta->>'deckId'
        WHERE deck.tenant_id=$1 AND deck.id=$2`, [identity.tenantId, created.deckId])
      throw new Error(`${error instanceof Error ? error.message : error}\n${JSON.stringify(state.rows)}`)
    }
    const record = await app.lectures.readLecture(scope)
    assert.ok((await app.lectures.readLectureHtml(scope)).length > 1000)
    if (oldRecord.status === 'publishing') assert.equal(calls, before, 'publication recovery must not call any model')
    assert.ok((await pool.query('SELECT count(*)::int AS n FROM lingxios.agent_model_budget_calls WHERE root_work_id=$1', [created.id])).rows[0].n > 0,
      'every remote author/reviewer call must reserve against the root work budget')
    await assert.rejects(app.lectures.readLecture({ ...scope, principalId: 'other' }))
    await kill(original)
    console.log(`${boundary.name}: resumed revision ${record.revision} with fenced writes and immutable HTML`)
  }
} finally {
  for (const item of children) { if (item.child.exitCode === null && item.child.signalCode === null) await kill(item) }
  for (const server of [proxy, model]) { server.closeAllConnections(); if (server.listening) await new Promise(resolve => server.close(resolve)) }
  await app?.stop(); await pool.end()
  assert.equal(dirname(resolve(directory)), resolve(tmpdir())); await rm(directory, { recursive: true, force: true })
}
