import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { createServer } from 'node:http'
import { setTimeout as delay } from 'node:timers/promises'
import { Pool } from 'pg'
import { createLingxiOS, packageResources } from '@lyyzka/lingxios'

assert.ok(process.env.LINGXIOS_CAPACITY_TEST_DATABASE_URL, 'a fresh capacity PostgreSQL database is required')
const pool = new Pool({ connectionString: process.env.LINGXIOS_CAPACITY_TEST_DATABASE_URL, max: 8 })
const directory = await mkdtemp(join(tmpdir(), 'lingxios-capacity-'))
const children = []
let active = 0, peak = 0, hold = true, app
const model = createServer(async (req, res) => {
  req.resume(); active++; peak = Math.max(peak, active)
  res.on('close', () => { active-- })
  while (hold && !res.destroyed) await delay(20)
  await delay(80)
  const content = '4'
  if (!res.destroyed) res.writeHead(200, { 'content-type': 'text/event-stream' }).end(`data: ${JSON.stringify({ model: 'fixture', choices: [{ index: 0, delta: { content }, finish_reason: 'stop' }] })}\n\ndata: [DONE]\n\n`)
})
async function until(check) {
  const end = Date.now() + 90_000
  while (!await check()) { assert.ok(Date.now() < end, children.map(item => item.logs).join('\n')); await delay(25) }
}
function worker(id, port) {
  const child = spawn(process.execPath, ['dist/src/worker/main.js'], { windowsHide: true,
    env: { ...process.env, NODE_ENV: 'test', AGENT_OS_KERNEL_ISOLATION: 'process', AGENT_OS_CONTROL_PLANE_URL: `http://127.0.0.1:${port}`,
      AGENT_OS_SERVICE_TOKEN: 'capacity', AGENT_OS_WORKER_ID: id, AGENT_OS_WORKER_PORT: '0', AGENT_OS_POLL_IDLE_MS: '50',
      AGENT_OS_MAX_CONCURRENT_RUNS: '2', AGENT_OS_SHUTDOWN_GRACE_MS: '1000', AGENT_OS_HOMES_ROOT: join(directory, id),
      AGENT_OS_MODEL: 'fixture', AGENT_OS_MODEL_API_KEY: 'fixture', AGENT_OS_MODEL_BASE_URL: `http://127.0.0.1:${model.address().port}` },
    stdio: ['ignore', 'ignore', 'pipe'] })
  const entry = { child, exited: once(child, 'exit'), logs: '' }
  child.stderr.on('data', data => { entry.logs = (entry.logs + data).slice(-8000) }); children.push(entry)
  return entry
}
try {
  assert.equal((await pool.query("SELECT 1 FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema') LIMIT 1")).rows.length, 0)
  await pool.query(await readFile(packageResources().schema, 'utf8'))
  app = await createLingxiOS({ database: pool, homesRoot: join(directory, 'control') })
  const port = await app.listenControlPlane({ serviceToken: 'capacity', port: 0 })
  model.listen(0, '127.0.0.1'); await once(model, 'listening')
  for (let i = 0; i < 100; i++) await app.enqueue({ id: `capacity-${i}`, tenantId: 't', agentId: 'a', sessionId: `s-${i}`, principalId: 'p', text: 'Return 4.', mode: 'chat', executionClass: i % 2 ? 'conversation' : 'operation' })
  assert.equal((await pool.query("SELECT count(*)::int AS n FROM lingxios.agent_work_items WHERE status='queued'")).rows[0].n, 100)
  const first = worker('capacity-a', port); worker('capacity-b', port)
  await until(() => active === 4)
  assert.equal((await pool.query("SELECT count(*)::int AS n FROM lingxios.agent_work_items WHERE status='leased'")).rows[0].n, 4)
  first.child.kill('SIGKILL'); await first.exited
  await pool.query("UPDATE lingxios.agent_work_items SET lease_expires_at=NOW()-INTERVAL '1 minute' WHERE leased_by='capacity-a'")
  await pool.query("UPDATE lingxios.agent_os_session_leases SET expires_at=NOW()-INTERVAL '1 minute' WHERE work_id IN (SELECT id FROM lingxios.agent_work_items WHERE leased_by='capacity-a')")
  await pool.query("UPDATE lingxios.agent_os_workers SET last_seen_at=NOW()-INTERVAL '1 day' WHERE worker_id='capacity-a'")
  hold = false; worker('capacity-replacement', port)
  await until(async () => (await pool.query("SELECT count(*)::int AS n FROM lingxios.agent_work_items WHERE status IN ('succeeded','partial','blocked','failed','cancelled')")).rows[0].n === 100)
  assert.deepEqual((await pool.query('SELECT status,count(*)::int AS n FROM lingxios.agent_work_items GROUP BY status')).rows, [{ status: 'succeeded', n: 100 }],
    JSON.stringify((await pool.query("SELECT id,meta->>'mode' AS mode,goal_outcome,error FROM lingxios.agent_work_items WHERE status<>'succeeded' LIMIT 3")).rows))
  assert.equal((await pool.query('SELECT count(*)::int AS n FROM lingxios.agent_results')).rows[0].n, 100)
  assert.ok(peak <= 4, `observed ${peak} concurrent model calls`)
  console.log('Capacity passed: 100 mixed conversation/operation tasks, two Workers x two slots, SIGKILL takeover, 100 unique committed messages.')
} finally {
  hold = false
  for (const item of children) { if (item.child.exitCode === null && item.child.signalCode === null) item.child.kill('SIGKILL'); await item.exited }
  model.closeAllConnections(); if (model.listening) await new Promise(resolve => model.close(resolve))
  await app?.stop(); await pool.end()
  assert.equal(dirname(resolve(directory)), resolve(tmpdir())); await rm(directory, { recursive: true, force: true })
}
