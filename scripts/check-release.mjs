import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { createHash, randomUUID } from 'node:crypto'
import { mkdir, readFile, writeFile } from 'node:fs/promises'
import { resolve, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { setTimeout as delay } from 'node:timers/promises'
import { Pool } from 'pg'

const root = fileURLToPath(new URL('../', import.meta.url))
const baseline = JSON.parse(await readFile(new URL('./release-baseline.json', import.meta.url)))
const deterministic = process.argv.includes('--deterministic')
const output = resolve(process.env.LINGXIOS_RELEASE_OUTPUT ?? join(root, 'release-results', randomUUID()))
await mkdir(output, { recursive: true })
const results = []
const source = resolve(process.env.LINGXILOOP_SOURCE ?? join(root, '../LingxiLoop/server/src'))
const container = `lingxios-release-${randomUUID()}`
const password = randomUUID()
const env = { ...process.env, LINGXILOOP_SOURCE: source }
function run(command, args, extraEnv = {}) {
  return new Promise((resolveRun, reject) => {
    const executable = process.platform === 'win32' && command === 'npm' ? 'npm.cmd' : command
    const child = spawn(executable, args, { cwd: root, env: { ...env, ...extraEnv }, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] })
    let text = ''
    child.stdout.on('data', data => { text += data })
    child.stderr.on('data', data => { text += data })
    child.on('error', reject)
    child.on('exit', code => resolveRun({ code, text }))
  })
}
async function gate(name, execute) {
  const started = Date.now()
  let result
  try { result = await execute() } catch (error) { result = { code: 1, text: String(error.message) } }
  await writeFile(join(output, `${name}.log`), result.text.replaceAll(password, '[redacted]'))
  results.push({ name, status: result.code === 0 ? 'passed' : 'failed', durationMs: Date.now() - started })
  console.log(`${name}: ${results.at(-1).status}`)
  return result.code === 0
}
let admin
try {
  const built = await gate('build', () => run(process.execPath, ['scripts/build.mjs']))
  assert.ok(built, 'build must pass before testing compiled output')
  await gate('package', () => process.platform === 'win32'
    ? run(process.env.ComSpec ?? 'cmd.exe', ['/d', '/s', '/c', 'npm run test:only'])
    : run('npm', ['run', 'test:only']))
  const pinned = await gate('native-baseline', async () => {
    const head = await run('git', ['-C', source, 'rev-parse', 'HEAD'])
    assert.equal(head.text.trim(), baseline.lingxiLoopCommit, 'native checkout must match release-baseline.json')
    const diff = await run('git', ['-C', source, 'diff', 'HEAD', '--', '.'])
    assert.equal(diff.text.trim(), '', 'native source must be unmodified')
    return head
  })
  await gate('contracts', () => run(process.execPath, ['scripts/check-lingxiloop-contracts.mjs']))
  const postgres = await gate('postgres-start', async () => {
    const result = await run('docker', ['run', '-d', '--rm', '--name', container, '-e', 'POSTGRES_PASSWORD', '-p', '127.0.0.1::5432', baseline.postgresImage], { POSTGRES_PASSWORD: password })
    assert.equal(result.code, 0, result.text)
    const port = await run('docker', ['port', container, '5432/tcp'])
    const number = Number(port.text.trim().split(':').at(-1))
    assert.ok(number > 0)
    env.LINGXIOS_RELEASE_PG_PORT = String(number)
    admin = new Pool({ host: '127.0.0.1', port: number, user: 'postgres', password, database: 'postgres', connectionTimeoutMillis: 1000 })
    for (let attempt = 0; ; attempt++) {
      try { await admin.query('SELECT 1'); break } catch (error) { if (attempt === 60) throw error; await delay(500) }
    }
    return { code: 0, text: `Disposable ${baseline.postgresImage}; one new database per gate.` }
  })
  const databaseGates = [
    ['postgres-stores', 'LINGXIOS_TEST_DATABASE_URL'], ['worker-recovery', 'LINGXIOS_WORKER_TEST_DATABASE_URL'],
    ['lecture-remote', 'LINGXIOS_LECTURE_TEST_DATABASE_URL'], ['capacity', 'LINGXIOS_CAPACITY_TEST_DATABASE_URL'],
    ['native-calendar', 'LINGXIOS_CALENDAR_TEST_DATABASE_URL'], ['native-documents', 'LINGXIOS_DOCUMENTS_TEST_DATABASE_URL'],
    ['native-document-content', 'LINGXIOS_DOCUMENT_CONTENT_TEST_DATABASE_URL'], ['native-evidence', 'LINGXIOS_TEST_DATABASE_URL'],
    ['native-canvas-work', 'LINGXIOS_CANVAS_TEST_DATABASE_URL'],
  ]
  for (const [name, variable] of databaseGates) await gate(name, async () => {
    assert.ok(postgres, 'PostgreSQL prerequisite failed')
    if (name.startsWith('native-')) assert.ok(pinned, 'native baseline prerequisite failed')
    const database = `gate_${name.replaceAll('-', '_')}`
    await admin.query(`CREATE DATABASE ${database}`)
    const url = `postgresql://postgres:${password}@127.0.0.1:${env.LINGXIOS_RELEASE_PG_PORT}/${database}`
    return run(process.execPath, [`scripts/test-${name}.mjs`], { [variable]: url })
  })
  for (const name of ['polls', 'presentations', 'teacher', 'missions', 'canvas', 'learning']) await gate(`native-${name}`, async () => {
    assert.ok(pinned, 'native baseline prerequisite failed')
    return run(process.execPath, [`scripts/test-native-${name}.mjs`])
  })
  await gate('image', async () => {
    const image = `lingxios:release-check-${randomUUID()}`
    const built = await run('docker', ['build', '-t', image, '.'])
    if (built.code !== 0) return built
    try { return await run(process.execPath, ['scripts/test-worker-image.mjs', image]) }
    finally { await run('docker', ['image', 'rm', image]) }
  })
  if (!deterministic) {
    await gate('live-model', async () => {
      assert.ok(postgres, 'PostgreSQL prerequisite failed')
      await admin.query('CREATE DATABASE gate_live')
      return run(process.execPath, ['scripts/eval-live.mjs', '--output', join(output, 'live'), '--repeat', String(baseline.liveRepeats)],
        { LINGXIOS_TEST_DATABASE_URL: `postgresql://postgres:${password}@127.0.0.1:${env.LINGXIOS_RELEASE_PG_PORT}/gate_live` })
    })
    await gate('human-and-product-acceptance', async () => {
      assert.ok(process.env.LINGXIOS_ACCEPTANCE_FILE, 'provide signed browser/content and complete native product acceptance evidence; fixture checks are insufficient')
      const evidence = JSON.parse(await readFile(process.env.LINGXIOS_ACCEPTANCE_FILE, 'utf8'))
      const head = (await run('git', ['rev-parse', 'HEAD'])).text.trim()
      assert.equal(evidence.commit, head)
      assert.ok(evidence.reviewer?.trim() && evidence.reviewedAt && evidence.evidenceUrl)
      for (const key of ['lectureBrowser', 'lectureContent', 'nativeDelivery', 'permissionRevocation', 'backupRestore']) assert.equal(evidence[key], 'passed', key)
      return { code: 0, text: JSON.stringify(evidence, null, 2) }
    })
  }
} finally {
  await admin?.end()
  await run('docker', ['rm', '-f', container])
  const lockSha256 = createHash('sha256').update(await readFile(join(root, 'package-lock.json'))).digest('hex')
  await writeFile(join(output, 'summary.json'), JSON.stringify({ version: 1, mode: deterministic ? 'deterministic-only' : 'release', baseline, lockSha256, results,
    releaseReady: !deterministic && results.every(result => result.status === 'passed') }, null, 2))
  process.exitCode = results.some(result => result.status !== 'passed') ? 1 : 0
  console.log(`Evidence: ${output}`)
}
