import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { randomUUID } from 'node:crypto'

// Run against an already-built image; no host mounts or published ports.
const image = process.argv[2]
assert.ok(image && !image.startsWith('-'), 'usage: node scripts/test-worker-image.mjs IMAGE')
const container = `lingxios-image-test-${randomUUID()}`
const result = spawnSync('docker', ['run', '--rm', '--name', container, '--network=none', '-i', image, 'node', '--input-type=module'], {
  encoding: 'utf8', timeout: 45_000,
  input: `
import assert from 'node:assert/strict'
import { spawn, spawnSync } from 'node:child_process'
import { once } from 'node:events'
import { createServer } from 'node:http'
import { setTimeout as delay } from 'node:timers/promises'
import { KernelManager } from './dist/src/kernel/manager.js'

assert.notEqual(process.getuid(), 0)
for (const name of ['lingxios', 'lingxios/worker', 'lingxios/lingxiloop', 'lingxios/eval', 'lingxios/ui']) await import(name)
const invalid = spawnSync(process.execPath, ['dist/src/worker/main.js'], { encoding: 'utf8', timeout: 5000 })
assert.equal(invalid.status, 1)
assert.match(invalid.stderr, /missing required environment variable: AGENT_OS_CONTROL_PLANE_URL/)
const kernels = new KernelManager({ execute: async () => { throw new Error('unexpected host call') } })
try {
  const work = { id: 'w', fence: 1, homeEpoch: 1, tenantId: 't', agentId: 'a', sessionId: 's', kind: 'turn', lane: 'interactive', triggerRef: 'm', leaseToken: 'token' }
  assert.equal((await kernels.execute(work, 'w', 'cell', 'print(6 * 7)')).stdout, '42\\n')
} finally { kernels.close() }

let claims = 0
const requests = []
const host = createServer((req, res) => {
  requests.push([req.method, req.url, req.headers.authorization])
  claims++
  req.resume()
  // Leave the next claim pending to exercise HTTP cancellation on SIGTERM.
  if (claims === 1) res.writeHead(200, { 'content-type': 'application/json' }).end('null')
})
host.listen(0, '127.0.0.1')
await once(host, 'listening')
const worker = spawn(process.execPath, ['dist/src/worker/main.js'], {
  env: { ...process.env, AGENT_OS_CONTROL_PLANE_URL: 'http://127.0.0.1:' + host.address().port,
    AGENT_OS_SERVICE_TOKEN: 'image-test', AGENT_OS_MODEL: 'unused-test-model', AGENT_OS_MODEL_API_KEY: 'unused-test-key',
    AGENT_OS_POLL_IDLE_MS: '50', AGENT_OS_SHUTDOWN_GRACE_MS: '1000' },
  stdio: ['ignore', 'pipe', 'pipe'],
})
let logs = ''
worker.stdout.on('data', chunk => { logs += chunk })
worker.stderr.on('data', chunk => { logs += chunk })
const exited = once(worker, 'exit')
try {
  let ready = false
  for (let i = 0; i < 100; i++) {
    try {
      const response = await fetch('http://127.0.0.1:5190/readyz')
      const status = await response.json()
      ready = response.ok && status.ok && status.activeRuns === 0 && claims >= 2
    } catch {}
    if (ready || worker.exitCode !== null) break
    await delay(50)
  }
  assert.ok(ready, logs)
  assert.ok(requests.every(request => JSON.stringify(request) === JSON.stringify(['POST', '/v2/work/claim', 'Bearer image-test'])))
  assert.equal((await fetch('http://127.0.0.1:5190/healthz')).status, 200)
  assert.equal((await fetch('http://127.0.0.1:5190/metrics')).status, 200)
  worker.kill('SIGTERM')
  const timeout = setTimeout(() => worker.kill('SIGKILL'), 5000)
  try { assert.deepEqual(await exited, [0, null], logs) } finally { clearTimeout(timeout) }
} finally {
  if (worker.exitCode === null && worker.signalCode === null) worker.kill('SIGKILL')
  await exited
  host.closeAllConnections()
  await new Promise(resolve => host.close(resolve))
}
console.log('worker image passed: non-root, exports, required config, Python kernel, authenticated polling, health and SIGTERM')
`,
})
process.stdout.write(result.stdout ?? '')
process.stderr.write(result.stderr ?? '')
if (result.error || result.status !== 0) spawnSync('docker', ['rm', '--force', container], { timeout: 10_000, stdio: 'ignore' })
assert.ifError(result.error)
assert.equal(result.status, 0, 'worker image smoke test failed')
