import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { Pool } from 'pg'
import { setTimeout as delay } from 'node:timers/promises'
import { fileURLToPath } from 'node:url'

const root = fileURLToPath(new URL('../', import.meta.url))
const packageOnly = process.argv.includes('--package-only')

function run(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { cwd: root, env: { ...process.env, ...options.env }, windowsHide: true, shell: options.shell,
      stdio: options.capture ? ['ignore', 'pipe', 'pipe'] : 'inherit' })
    let output = ''
    child.stdout?.on('data', data => { output += data })
    child.stderr?.on('data', data => { output += data })
    child.on('error', reject)
    child.on('exit', code => code === 0 ? resolve(output) : reject(new Error(`${command} exited with ${code}${output ? `\n${output}` : ''}`)))
  })
}

await run(process.execPath, ['scripts/build.mjs'])
await run('npm', ['run', 'test:only'], { shell: process.platform === 'win32' })
if (packageOnly) process.exit(0)

const container = `lingxios-release-${randomUUID()}`
const password = randomUUID()
let admin
try {
  process.env.POSTGRES_PASSWORD = password
  await run('docker', ['run', '-d', '--rm', '--name', container, '-e', 'POSTGRES_PASSWORD', '-p', '127.0.0.1::5432', 'postgres:16.4-bookworm'], { capture: true })
  const binding = await run('docker', ['port', container, '5432/tcp'], { capture: true })
  const port = Number(binding.trim().split(':').at(-1))
  assert.ok(port > 0, 'Docker did not publish the PostgreSQL port')
  admin = new Pool({ host: '127.0.0.1', port, user: 'postgres', password, database: 'postgres', connectionTimeoutMillis: 1000 })
  for (let attempt = 0; ; attempt += 1) {
    try { await admin.query('SELECT 1'); break }
    catch (error) { if (attempt === 60) throw error; await delay(500) }
  }
  for (const [database, script, variable] of [
    ['gate_stores', 'test-postgres-stores.mjs', 'LINGXIOS_TEST_DATABASE_URL'],
    ['gate_recovery', 'test-worker-recovery.mjs', 'LINGXIOS_WORKER_TEST_DATABASE_URL'],
    ['gate_capacity', 'test-capacity.mjs', 'LINGXIOS_CAPACITY_TEST_DATABASE_URL'],
  ]) {
    await admin.query(`CREATE DATABASE ${database}`)
    await run(process.execPath, [`scripts/${script}`], { env: { [variable]: `postgresql://postgres:${password}@127.0.0.1:${port}/${database}` } })
  }
  const image = `lingxios:release-check-${randomUUID()}`
  try {
    await run('docker', ['build', '-t', image, '.'])
    await run(process.execPath, ['scripts/test-worker-image.mjs', image])
  } finally {
    await run('docker', ['image', 'rm', '-f', image], { capture: true }).catch(() => {})
  }
} finally {
  await admin?.end()
  await run('docker', ['rm', '-f', container], { capture: true }).catch(() => {})
  delete process.env.POSTGRES_PASSWORD
}
