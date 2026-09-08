import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { once } from 'node:events'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { createServer } from 'node:http'
import { Pool } from 'pg'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { fileURLToPath } from 'node:url'
import { createLingxiOS, packageResources } from '@lyyzka/lingxios'

const connectionString = process.env.LINGXIOS_WORKER_TEST_DATABASE_URL
assert.ok(connectionString, 'configure a fresh disposable PostgreSQL database for worker recovery')
const pool = new Pool({ connectionString, max: 4, connectionTimeoutMillis: 5000 })
const directory = await mkdtemp(join(tmpdir(), 'lingxios-worker-recovery-'))
const homesRoot = join(directory, 'homes')
let app
const children = []
let modelCalls = 0
let heldCompletion = false
let holdCompletion = true
const model = createServer(async (req, res) => {
  const chunks = []
  for await (const chunk of req) chunks.push(chunk)
  const request = JSON.parse(Buffer.concat(chunks).toString())
  if (!request.stream) {
    res.writeHead(200, { 'content-type': 'application/json' })
    res.end(JSON.stringify({ model: 'fixture', choices: [{ message: { content: '{"missing":[]}' }, finish_reason: 'stop' }] }))
    return
  }
  modelCalls++
  const delta = modelCalls === 1 ? { tool_calls: [{ index: 0, id: 'write-file', type: 'function',
    function: { name: 'ipython', arguments: JSON.stringify({ code: 'with open("answer.txt", "w") as f:\n    f.write("4")\nattach_file("answer.txt")' }) } }] }
    : { content: '4' }
  res.writeHead(200, { 'content-type': 'text/event-stream' })
  res.end(`data: ${JSON.stringify({ model: 'fixture', choices: [{ index: 0, delta, finish_reason: modelCalls === 1 ? 'tool_calls' : 'stop' }] })}\n\ndata: [DONE]\n\n`)
})
let controlPort
const proxy = createServer(async (req, res) => {
  const chunks = []
  for await (const chunk of req) chunks.push(chunk)
  try {
    const response = await fetch(`http://127.0.0.1:${controlPort}${req.url}`, {
      method: req.method, headers: { authorization: req.headers.authorization ?? '', 'content-type': 'application/json' },
      ...(chunks.length ? { body: Buffer.concat(chunks) } : {}),
    })
    if (holdCompletion && req.url.endsWith('/result') && response.ok) {
      heldCompletion = true
      return // Lose the acknowledgement after atomic message/work commit.
    }
    res.writeHead(response.status, { 'content-type': 'application/json' }).end(await response.text())
  } catch { res.writeHead(502).end('{}') }
})
async function until(check) {
  const deadline = Date.now() + 20_000
  while (!(await check())) {
    assert.ok(Date.now() < deadline, 'worker recovery condition timed out')
    await delay(25)
  }
}
function worker(id) {
  const child = spawn(process.execPath, [fileURLToPath(new URL('../dist/src/worker/main.js', import.meta.url))], {
    env: { ...process.env, AGENT_OS_CONTROL_PLANE_URL: `http://127.0.0.1:${proxy.address().port}`,
      AGENT_OS_SERVICE_TOKEN: 'recovery-test', AGENT_OS_WORKER_ID: id, AGENT_OS_WORKER_PORT: '0',
      AGENT_OS_POLL_IDLE_MS: '50', AGENT_OS_SHUTDOWN_GRACE_MS: '1000', AGENT_OS_MAX_CONCURRENT_RUNS: '1',
      AGENT_OS_MODEL: 'fixture', AGENT_OS_MODEL_API_KEY: 'test', AGENT_OS_MODEL_BASE_URL: `http://127.0.0.1:${model.address().port}`,
      AGENT_OS_HOMES_ROOT: homesRoot }, stdio: ['ignore', 'ignore', 'pipe'],
  })
  const entry = { child, exit: once(child, 'exit'), stderr: '' }
  child.stderr.on('data', chunk => { entry.stderr = (entry.stderr + chunk).slice(-16000) })
  children.push(entry)
  return entry
}
try {
  assert.equal((await pool.query("SELECT 1 FROM pg_namespace WHERE nspname='lingxios'")).rows.length, 0, 'use a fresh disposable database')
  assert.equal((await pool.query("SELECT 1 FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema') LIMIT 1")).rows.length, 0, 'use an empty disposable database')
  await pool.query(await readFile(packageResources().schema, 'utf8'))
  app = await createLingxiOS({ database: pool, homesRoot })
  controlPort = await app.listenControlPlane({ serviceToken: 'recovery-test', port: 0 })
  model.listen(0, '127.0.0.1'); await once(model, 'listening')
  proxy.listen(0, '127.0.0.1'); await once(proxy, 'listening')
  const identity = { runId: randomUUID(), tenantId: 'test', agentId: 'agent', sessionId: randomUUID(), principalId: 'human' }
  await app.enqueue({ ...identity, id: identity.runId, text: 'Write answer.txt containing 4 and attach it.' })
  const original = worker('original')
  await until(() => heldCompletion || original.child.exitCode !== null)
  assert.equal(heldCompletion, true, original.stderr)
  const committed = await app.readMessage(identity)
  assert.equal(committed?.body, '4')
  assert.equal((await app.readArtifact(identity, 'answer.txt'))?.bytes.toString(), '4')
  assert.equal(modelCalls, 2)
  original.child.kill('SIGKILL')
  await original.exit
  holdCompletion = false
  await pool.query("UPDATE lingxios.agent_work_items SET lease_expires_at=NOW()-INTERVAL '1 minute' WHERE id=$1", [identity.runId])
  await pool.query("UPDATE lingxios.agent_os_session_leases SET expires_at=NOW()-INTERVAL '1 minute'")
  await pool.query("UPDATE lingxios.agent_os_workers SET last_seen_at=NOW()-INTERVAL '1 day'")
  const replacement = worker('replacement')
  await until(async () => (await pool.query('SELECT 1 FROM lingxios.agent_os_workers WHERE worker_id=$1', ['replacement'])).rows.length === 1)
  assert.equal((await pool.query('SELECT status FROM lingxios.agent_work_items WHERE id=$1', [identity.runId])).rows[0].status, 'succeeded')
  assert.deepEqual(await app.readMessage(identity), committed)
  assert.deepEqual(await app.readOutcome(identity), { status: 'satisfied', verification: 'inconclusive', requestVersion: 1 })
  assert.equal((await app.readArtifact(identity, 'answer.txt'))?.bytes.toString(), '4')
  assert.equal(modelCalls, 2, 'recovery must not regenerate the committed response or file')
  assert.equal((await pool.query("SELECT count(*)::int AS count FROM lingxios.agent_run_events WHERE run_id=$1 AND kind='response.recovered'", [identity.runId])).rows[0].count, 0, 'an atomically committed work item needs no execution replay')
  // Windows kill() does not deliver a POSIX signal to the JS shutdown handler.
  replacement.child.kill(process.platform === 'win32' ? 'SIGKILL' : 'SIGTERM')
  assert.deepEqual(await replacement.exit, process.platform === 'win32' ? [null, 'SIGKILL'] : [0, null], replacement.stderr)
  console.log('Worker recovery passed: actual process killed after commit, replacement recovered message/artifact without model replay.')
} finally {
  for (const entry of children) {
    if (entry.child.exitCode === null && entry.child.signalCode === null) entry.child.kill('SIGKILL')
    await entry.exit
  }
  for (const server of [proxy, model]) {
    server.closeAllConnections()
    if (server.listening) await new Promise(resolve => server.close(resolve))
  }
  await app?.stop()
  await pool.end()
  assert.equal(dirname(resolve(directory)), resolve(tmpdir()))
  await rm(directory, { recursive: true, force: true })
}
