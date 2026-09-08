import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { fileURLToPath } from 'node:url'

// Run against an already-built image; no host mounts or published ports.
const image = process.argv[2]
assert.ok(image && !image.startsWith('-'), 'usage: node scripts/test-worker-image.mjs IMAGE')
const security = spawnSync('docker', ['info', '--format', '{{json .SecurityOptions}}'], { encoding: 'utf8', timeout: 10_000 })
assert.ifError(security.error)
assert.equal(security.status, 0, security.stderr)
const apparmor = JSON.parse(security.stdout).some(option => option.split(',').includes('name=apparmor'))
const container = `lingxios-image-test-${randomUUID()}`
const result = spawnSync('docker', ['run', '--rm', '--name', container, '--network=none', '--read-only', '--tmpfs', '/tmp:rw,noexec,nosuid,size=128m', '--tmpfs', '/data/homes:rw,nosuid,size=256m,uid=1000,gid=1000', '--cpus', '2', '--memory', '2g', '--pids-limit', '128', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges', '--security-opt', `seccomp=${fileURLToPath(new URL('../deploy/worker-seccomp.json', import.meta.url))}`, ...(apparmor ? ['--security-opt', 'apparmor=lingxios-worker'] : []), '-i', image, 'node', '--input-type=module'], {
  encoding: 'utf8', timeout: 45_000,
  input: `
import assert from 'node:assert/strict'
import { spawn, spawnSync } from 'node:child_process'
import { once } from 'node:events'
import { createServer } from 'node:http'
import { setTimeout as delay } from 'node:timers/promises'
import { KernelManager } from './dist/src/kernel/manager.js'
import { access, mkdir, readFile, writeFile } from 'node:fs/promises'
import { packageResources } from './dist/src/app/resources.js'
import { sandboxCommand } from './dist/src/kernel/isolation.js'

assert.notEqual(process.getuid(), 0)
for (const path of Object.values(packageResources())) await access(path)
await mkdir('/data/homes/own', { recursive: true })
await mkdir('/data/homes/sibling', { recursive: true })
await writeFile('/data/homes/sibling/secret', 'private')
await writeFile('/tmp/host-secret', 'private')
const sandbox = sandboxCommand('/data/homes/own', '/app/kernel/runner.py', 'python3',
  { memoryBytes: 512*1024*1024, cpuSeconds: 30, maxProcesses: 512, tmpBytes: 1024*1024 }, false, ['-I', '-c',
  'import os,socket,resource; assert not os.path.exists("/data/homes/sibling"); assert not os.path.exists("/tmp/host-secret"); assert not os.path.exists("/app"); assert not os.path.exists("/proc"); assert resource.getrlimit(resource.RLIMIT_AS)[1]==512*1024*1024; s=socket.socket(); s.settimeout(1); assert s.connect_ex(("192.0.2.1",80))!=0; print("isolated")'])
const isolation = spawnSync(sandbox.command, sandbox.args, { encoding: 'utf8', timeout: 10000 })
assert.equal(isolation.status, 0, isolation.stderr)
assert.equal(isolation.stdout.trim(), 'isolated')
if (${apparmor}) {
  assert.equal((await readFile('/proc/self/attr/current', 'utf8')).trim(), 'lingxios-worker (enforce)')
  const forbidden = spawnSync('unshare', ['--user', '--map-root-user', '--mount', '--propagation=slave', '--', 'mount', '-t', 'tmpfs', 'tmpfs', '/data/homes/own'], { encoding: 'utf8', timeout: 5000 })
  assert.ifError(forbidden.error)
  assert.notEqual(forbidden.status, 0, 'AppArmor must deny mounts outside Bubblewrap setup paths')
  assert.match(forbidden.stderr, /^mount: \/data\/homes\/own: cannot mount tmpfs/im, 'namespace setup must succeed before the mount is rejected')
}
for (const [code, expected] of [
  ['a=bytearray(1024*1024*1024)', /MemoryError/],
  ['open("/tmp/full", "wb").write(b"x"*(2*1024*1024))', /No space left/],
  ['while True: pass', null],
]) {
  const limited = sandboxCommand('/data/homes/own', '/app/kernel/runner.py', 'python3',
    { memoryBytes: 128*1024*1024, cpuSeconds: 1, maxProcesses: 512, tmpBytes: 1024*1024 }, false, ['-I', '-c', code])
  const exhausted = spawnSync(limited.command, limited.args, { encoding: 'utf8', timeout: 5000 })
  assert.notEqual(exhausted.status, 0)
  assert.equal(exhausted.error, undefined, 'kernel must terminate within its own CPU limit')
  if (expected) assert.match(exhausted.stderr, expected)
}
for (const name of ['lingxios', 'lingxios/worker', 'lingxios/eval', 'lingxios/ui']) await import(name)
const invalid = spawnSync(process.execPath, ['dist/src/worker/main.js'], { encoding: 'utf8', timeout: 5000 })
assert.equal(invalid.status, 1)
assert.match(invalid.stderr, /missing required environment variable: AGENT_OS_CONTROL_PLANE_URL/)
const kernels = new KernelManager({ execute: async () => { throw new Error('unexpected host call') } })
try {
  await kernels.check()
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
  assert.ok(requests.every(request => JSON.stringify(request) === JSON.stringify(['POST', '/v5/work/claim', 'Bearer image-test'])))
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
