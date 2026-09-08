import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { createHash, randomUUID } from 'node:crypto'
import { mkdir, readFile, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { Pool } from 'pg'
import { setTimeout as delay } from 'node:timers/promises'
import { fileURLToPath } from 'node:url'

const root = fileURLToPath(new URL('../', import.meta.url))
const packageOnly = process.argv.includes('--package-only')
const gates = []

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

const sha256 = value => createHash('sha256').update(value).digest('hex')
async function sourceSnapshot() {
  const paths = (await run('git', ['ls-files', '-z', '--cached', '--others', '--exclude-standard', '--',
    'src', 'test', 'scripts', 'db', 'kernel', 'deploy', '.github', 'package.json', 'package-lock.json', 'tsconfig.json', 'Dockerfile', '.dockerignore'], { capture: true }))
    .split('\0').filter(Boolean).sort()
  const files = await Promise.all([...new Set(paths)].map(async path => [path, await readFile(join(root, path)).then(sha256,
    error => { if (error.code === 'ENOENT') return null; throw error })]))
  return { sha256: sha256(JSON.stringify(files)), testSetSha256: sha256(JSON.stringify(files.filter(([path]) => path.startsWith('test/')))), files }
}
const source = await sourceSnapshot()
const commit = (await run('git', ['rev-parse', 'HEAD'], { capture: true })).trim()
async function recordQualification() {
  assert.equal((await sourceSnapshot()).sha256, source.sha256, 'source changed during release checks; no qualification emitted')
  const { releaseVersions } = await import('../dist/src/versions.js')
  const directory = join(root, 'release-results', 'qualifications')
  await mkdir(directory, { recursive: true })
  const path = join(directory, `${source.sha256}.json`)
  await writeFile(path, JSON.stringify({ version: 1, qualification: packageOnly ? 'package-gates' : 'deterministic-runtime-gates',
    commit, source, versions: releaseVersions, gates, completedAt: new Date().toISOString(),
    environment: { node: process.version, platform: process.platform, arch: process.arch },
    liveModel: 'not_run', productIntegration: 'not_run' }, null, 2) + '\n')
  console.log(`Release qualification: ${path}`)
}

await run(process.execPath, ['scripts/build.mjs'])
gates.push({ id: 'typecheck-build', status: 'passed' })
await run('npm', ['run', 'test:only'], { shell: process.platform === 'win32' })
gates.push({ id: 'unit-and-package-install', status: 'passed' })
if (packageOnly) { await recordQualification(); process.exit(0) }

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
    gates.push({ id: script, status: 'passed' })
  }
  const image = `lingxios:release-check-${randomUUID()}`
  try {
    await run('docker', ['build', '-t', image, '.'])
    await run(process.execPath, ['scripts/test-worker-image.mjs', image])
    gates.push({ id: 'linux-worker-image-and-sandbox', status: 'passed' })
  } finally {
    await run('docker', ['image', 'rm', '-f', image], { capture: true }).catch(() => {})
  }
} finally {
  await admin?.end()
  await run('docker', ['rm', '-f', container], { capture: true }).catch(() => {})
  delete process.env.POSTGRES_PASSWORD
}
await recordQualification()
