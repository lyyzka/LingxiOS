import { pdfFixture } from './pdf-fixture.js'
import assert from 'node:assert/strict'
import { execFile } from 'node:child_process'
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { promisify } from 'node:util'
import { it } from 'node:test'

it('installs a standalone tarball and executes packaged Python and document parsing outside the repository', async () => {
  const run = promisify(execFile)
  const root = fileURLToPath(new URL('../../', import.meta.url))
  const directory = await mkdtemp(join(tmpdir(), 'lingxios-package-'))
  const npm = process.env['npm_execpath'] ?? join(dirname(process.execPath), 'node_modules/npm/bin/npm-cli.js')
  try {
    const packed = await run(process.execPath, [npm, 'pack', '--json', '--ignore-scripts', '--pack-destination', directory], { cwd: root })
    const [tarball] = JSON.parse(packed.stdout) as Array<{ filename: string; integrity: string; files: Array<{ path: string }> }>
    assert.ok(tarball)
    assert.ok(tarball.files.some((file) => file.path === 'kernel/runner.py'))
    assert.ok(tarball.files.some((file) => file.path === 'dist/src/context/document-worker.js'))
    assert.ok(tarball.files.some((file) => file.path === 'db/schema.sql'))
    assert.ok(tarball.files.some((file) => file.path === 'db/migrations/010-im-collaboration.sql'))
    assert.ok(tarball.files.some((file) => file.path === 'dist/src/cli/eval.js'))
    assert.ok(!tarball.files.some((file) => file.path.startsWith('test/') || file.path.includes('pglite')))
    const pkg = JSON.parse(await readFile(join(root, 'package.json'), 'utf8')) as { name: string; version: string }
    // npm ci caches locked tarballs, not registry metadata. Reuse that graph so a clean CI cache also installs offline.
    const locked = JSON.parse(await readFile(join(root, 'package-lock.json'), 'utf8')) as { packages: Record<string, Record<string, unknown>> }
    const { devDependencies: _devDependencies, ...packageEntry } = locked.packages['']!
    const dependencies = { [pkg.name]: `file:${tarball.filename}` }
    const consumer = { name: 'lingxios-package-consumer', private: true, type: 'module', dependencies }
    const packages = { ...locked.packages, '': consumer,
      [`node_modules/${pkg.name}`]: { ...packageEntry, resolved: `file:${tarball.filename}`, integrity: tarball.integrity } }
    await writeFile(join(directory, 'package.json'), JSON.stringify(consumer))
    await writeFile(join(directory, 'package-lock.json'), JSON.stringify({ name: consumer.name, lockfileVersion: 3, requires: true, packages }))
    await run(process.execPath, [npm, 'ci', '--ignore-scripts', '--omit=dev', '--offline', '--no-audit', '--no-fund'], { cwd: directory })
    await writeFile(join(directory, 'consumer.mjs'), `
import assert from 'node:assert/strict'
import { access, mkdir, symlink, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { createHash } from 'node:crypto'
import http from 'node:http'
import { PGlite } from ${JSON.stringify(import.meta.resolve('@electric-sql/pglite'))}
import { readFile } from 'node:fs/promises'
import vm from 'node:vm'
import { createLingxiOS, doctor, packageResources, releaseVersions } from ${JSON.stringify(pkg.name)}
assert.equal(releaseVersions.runtime, ${JSON.stringify(pkg.version)})
// Internal runner conformance test; consumers use the high-level factory above.
const { KernelManager } = await import(new URL(${JSON.stringify(`./node_modules/${pkg.name}/dist/src/kernel/manager.js`)}, import.meta.url))
const { extractDocumentText } = await import(new URL(${JSON.stringify(`./node_modules/${pkg.name}/dist/src/context/document-text.js`)}, import.meta.url))
const parsedPdf = await extractDocumentText(Buffer.from(${JSON.stringify(pdfFixture(['中文安装验证'], true).toString('base64'))}, 'base64'), 'pdf')
assert.match(parsedPdf, /中文安装验证/)
assert.match(parsedPdf, /OCR are not represented/)
import { startWorker, createWorker } from ${JSON.stringify(`${pkg.name}/worker`)}
import { gradeResources } from ${JSON.stringify(`${pkg.name}/eval`)}
assert.deepEqual(gradeResources(1, [{ id: 'saved', resource: 'doc', expected: true }], [{ resource: 'doc', requestVersion: 1, value: true }]), [{ checkId: 'saved', status: 'pass' }])
assert.equal(typeof createLingxiOS, 'function')
assert.equal(typeof startWorker, 'function')
assert.equal(typeof createWorker, 'function')
for (const retired of ['lingxiloop','lecture-deck']) await assert.rejects(import(${JSON.stringify(pkg.name)} + '/' + retired), { code: 'ERR_PACKAGE_PATH_NOT_EXPORTED' })
const browserContext = vm.createContext({ structuredClone })
async function browserModule(url) {
  const module = new vm.SourceTextModule(await readFile(url, 'utf8'), { context: browserContext, identifier: url.href })
  await module.link((specifier, parent) => {
    assert.ok(specifier.startsWith('.'), 'browser export must not import server dependencies')
    return browserModule(new URL(specifier, parent.identifier))
  })
  return module
}
const ui = await browserModule(new URL(import.meta.resolve(${JSON.stringify(`${pkg.name}/ui`)})))
await ui.evaluate()
assert.equal(ui.namespace.createRunView('w').message, null)
assert.equal(JSON.stringify(ui.namespace.releaseVersions), JSON.stringify(releaseVersions))
await assert.rejects(startWorker({}), /missing required/)
for (const path of Object.values(packageResources())) await access(path)
const report = await doctor({ env: {} })
assert.deepEqual(report.versions, releaseVersions)
assert.equal(report.ready, false)
assert.equal(report.checks.find(item => item.name === 'database').status, 'not_run')
assert.equal(report.checks.find(item => item.name === 'model_configuration').status, 'failed')
const database = new PGlite()
await database.exec(await readFile(packageResources().schema, 'utf8'))
assert.equal((await database.query('SELECT version FROM lingxios.schema_version WHERE singleton=TRUE')).rows[0].version, releaseVersions.schema)
const pool = { query: async (sql, values) => {
  const result = await database.query(sql, values)
  return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
}, connect: async () => ({ query: pool.query, release() {} }) }
let modelCalls = 0
const modelServer = http.createServer((request, response) => {
  let body = ''
  request.on('data', chunk => { body += chunk })
  request.on('end', () => {
    if (!JSON.parse(body).stream) {
      response.writeHead(200, { 'content-type': 'application/json' })
      response.end(JSON.stringify({ model: 'test', choices: [{ message: { content: '{"missing":[]}' }, finish_reason: 'stop' }] }))
      return
    }
    modelCalls++
    assert.match(body, /Calculate six times seven/)
    const delta = modelCalls === 1 ? { tool_calls: [{ index: 0, id: 'calculate', function: { name: 'ipython', arguments: JSON.stringify({ code: 'print(6 * 7)' }) } }] } : { content: '42' }
    response.writeHead(200, { 'content-type': 'text/event-stream' })
    response.end('data: ' + JSON.stringify({ choices: [{ delta, finish_reason: modelCalls === 1 ? 'tool_calls' : 'stop' }] }) + '\\n\\ndata: [DONE]\\n\\n')
  })
})
await new Promise(resolve => modelServer.listen(0, '127.0.0.1', resolve))
const application = await createLingxiOS({ database: pool, homesRoot: join(process.cwd(), 'app-homes') })
for (const operation of [application.conversations.ingest, application.conversations.sync, application.graphs.enqueue,
  application.sharedState.apply, application.readConversationTrace]) assert.equal(typeof operation, 'function')
const localWorker = createWorker({ controlPlane: application, model: { id: 'fixture', apiKey: 'fixture', baseUrl: 'http://127.0.0.1:' + modelServer.address().port },
  kernel: { homesRoot: join(process.cwd(), 'app-homes') } })
try {
  const identity = { runId: 'packaged-work', tenantId: 'tenant', agentId: 'agent', sessionId: 'session', principalId: 'user' }
  await application.enqueue({ ...identity, id: identity.runId, principalId: 'user', text: 'Calculate six times seven using Python.' })
  assert.equal(application.runNext, undefined)
  assert.equal(await localWorker.runNext(), true)
  assert.equal((await application.readMessage(identity)).body, '42')
  assert.equal((await application.readMessage(identity)).version, releaseVersions.assistantMessage)
  assert.equal((await application.readOutcome(identity)).status, 'satisfied')
  assert.equal(modelCalls, 2)
  const control = await createLingxiOS({ database: pool, homesRoot: join(process.cwd(), 'remote-homes') })
  let remoteWorker
  try {
    modelCalls = 0
    const port = await control.listenControlPlane({ serviceToken: 'packaged-secret', port: 0 })
    const remoteIdentity = { ...identity, runId: 'remote-packaged-work', sessionId: 'remote-session' }
    await control.enqueue({ ...remoteIdentity, id: remoteIdentity.runId, principalId: 'user', text: 'Calculate six times seven using Python.' })
    remoteWorker = await startWorker({ ...process.env, AGENT_OS_CONTROL_PLANE_URL: 'http://127.0.0.1:' + port,
      AGENT_OS_SERVICE_TOKEN: 'packaged-secret', AGENT_OS_WORKER_ID: 'packaged-worker', AGENT_OS_WORKER_PORT: '0',
      AGENT_OS_MODEL: 'fixture', AGENT_OS_MODEL_API_KEY: 'fixture', AGENT_OS_MODEL_BASE_URL: 'http://127.0.0.1:' + modelServer.address().port,
      AGENT_OS_HOMES_ROOT: join(process.cwd(), 'remote-homes'), AGENT_OS_POLL_IDLE_MS: '50', AGENT_OS_MAX_CONCURRENT_RUNS: '1' })
    const deadline = Date.now() + 10000
    while (!(await control.readMessage(remoteIdentity)) && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 25))
    assert.equal((await control.readMessage(remoteIdentity))?.body, '42')
    assert.equal(modelCalls, 2)
  } finally {
    await remoteWorker?.stop()
    await control.stop()
  }
} finally {
  await localWorker.stop()
  await application.stop()
  await database.close()
  await new Promise(resolve => modelServer.close(resolve))
}
const kernels = new KernelManager({ execute: async () => { throw new Error('unexpected host call') } }, { maxKernels: 1 })
try {
  const work = { id: 'w', fence: 1, homeEpoch: 1, tenantId: 't', agentId: 'a', sessionId: 's', kind: 'turn', lane: 'interactive', triggerRef: 'm', leaseToken: 'token' }
  const result = await kernels.execute(work, 'w', 'cell', 'print(6 * 7)')
  assert.equal(result.stdout, '42\\n')
  const generated = await kernels.execute(work, 'w', 'file', ${JSON.stringify('import pathlib\ndef reject_whole_file(*args):\n    raise RuntimeError("whole-file allocation is forbidden")\npathlib.Path.read_bytes = reject_whole_file\nwith open("report.bin", "wb") as stream:\n    for _ in range(3):\n        stream.write(b"x" * (1024 * 1024))')})
  assert.deepEqual(generated.artifacts, [{ path: 'report.bin', size: 3 * 1024 * 1024, mime: 'application/octet-stream',
    sha256: createHash('sha256').update(Buffer.alloc(3 * 1024 * 1024, 'x')).digest('hex') }])
  const attached = await kernels.execute(work, 'w', 'attach', 'attach_file("report.bin")')
  assert.deepEqual(attached.artifacts, generated.artifacts)
  assert.deepEqual((await kernels.execute(work, 'w', 'unchanged', 'None')).artifacts, [])
  const racing = await kernels.execute(work, 'w', 'changing-file', ${JSON.stringify('pathlib.Path("race.txt").write_text("old")\nattach_file("race.txt")\noriginal_open = pathlib.Path.open\ndef changing_open(path, mode="r", *args, **kwargs):\n    if path.name == "race.txt" and mode == "rb":\n        with original_open(path, "w") as writer:\n            writer.write("changed-size")\n    return original_open(path, mode, *args, **kwargs)\npathlib.Path.open = changing_open\ntry:\n    try:\n        __import__("__main__").changed_artifacts({})\n        raise AssertionError("accepted a changing artifact")\n    except OSError as error:\n        assert "changed while being hashed" in str(error)\nfinally:\n    pathlib.Path.open = original_open')})
  assert.ok(racing.artifacts.some(artifact => artifact.path === 'race.txt' && artifact.size === 12))
  await assert.rejects(kernels.execute(work, 'w', 'missing-file', 'attach_file("missing.bin")'), /attached file must exist/)
  const home = await kernels.execute(work, 'w', 'home', 'str(pathlib.Path.cwd())')
  const external = join(process.cwd(), 'external')
  await mkdir(external)
  await writeFile(join(external, 'private.txt'), 'Must not be exported')
  for (const [index, expression] of [
    'os.remove(target)', 'os.rename(target, "stolen.txt")', 'os.open(target, os.O_WRONLY | os.O_TRUNC)',
    'os.link(target, "hardlink.txt")', 'os.symlink(target, "symlink.txt")',
  ].entries()) {
    await assert.rejects(kernels.execute(work, 'w', 'mutation-' + index,
      'import os; target = ' + JSON.stringify(join(external, 'private.txt')) + '; ' + expression), /disabled|inside this agent home/)
  }
  assert.equal(await readFile(join(external, 'private.txt'), 'utf8'), 'Must not be exported')
  await kernels.execute(work, 'w', 'local-mutation', 'import os; open("local.txt", "w").close(); os.rename("local.txt", "renamed.txt"); os.remove("renamed.txt")')
  await symlink(external, join(home.result, 'linked'), process.platform === 'win32' ? 'junction' : 'dir')
  await assert.rejects(kernels.execute(work, 'w', 'outside-file', 'attach_file("linked/private.txt")'), /attached file must exist/)
  const scan = await kernels.execute(work, 'w', 'scan', '__import__("__main__").changed_artifacts({})')
  assert.ok(Array.isArray(scan.result))
  assert.ok(scan.result.every(artifact => !artifact.path.startsWith('linked/')))
  kernels.close()
  const resumedKernels = new KernelManager({ execute: async () => { throw new Error('unexpected host call') } }, { maxKernels: 1 })
  try {
    const restored = await resumedKernels.execute({ ...work, fence: 2 }, 'w', 'restored-file', 'attach_file("report.bin")')
    assert.deepEqual(restored.artifacts, generated.artifacts)
  } finally { resumedKernels.close() }
} finally { kernels.close() }
console.log('package consumer passed')
`)
    const result = await run(process.execPath, ['--experimental-vm-modules', 'consumer.mjs'], { cwd: directory, timeout: 30_000 })
    assert.match(result.stdout, /package consumer passed/)
    const installedRoot = join(directory, 'node_modules', pkg.name)
    const installed = JSON.parse(await readFile(join(installedRoot, 'package.json'), 'utf8')) as { bin: Record<string, string> }
    assert.equal(installed.bin['lingxios-eval'], 'dist/src/cli/eval.js')
    await writeFile(join(directory, 'observations.json'), JSON.stringify({ originalInput: 'Save the document', requestVersion: 1,
      expectations: [{ id: 'saved', resource: 'doc', expected: true }], observations: [{ resource: 'doc', requestVersion: 1, value: true }] }))
    const evaluated = await run(process.execPath, [join(installedRoot, installed.bin['lingxios-eval']!), '--observations', 'observations.json'], { cwd: directory, timeout: 10_000 })
    assert.equal(JSON.parse(evaluated.stdout).status, 'pass')
    assert.equal(JSON.parse(evaluated.stdout).mode, 'recorded_observations')
  } finally {
    await rm(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 })
  }
})
