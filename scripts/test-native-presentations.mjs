import assert from 'node:assert/strict'
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises'
import { createRequire } from 'node:module'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import ts from 'typescript'
import { executePresentation } from '../dist/src/integrations/lingxiloop/presentations.js'

const root = fileURLToPath(new URL('../', import.meta.url))
const source = resolve(process.argv[2] ?? process.env.LINGXILOOP_SOURCE ?? join(root, '../LingxiLoop/server/src'))
const directory = await mkdtemp(join(tmpdir(), 'lingxios-native-presentations-'))
try {
  const requireNative = createRequire(join(source, '../package.json'))
  const input = await readFile(join(source, 'modules/presentations/contracts.ts'), 'utf8')
  const output = ts.transpileModule(input, { compilerOptions: { target: ts.ScriptTarget.ES2023, module: ts.ModuleKind.ESNext } }).outputText
    .replace("from 'zod'", `from ${JSON.stringify(pathToFileURL(requireNative.resolve('zod')).href)}`)
  await writeFile(join(directory, 'contracts.mjs'), output)
  const schemas = await import(pathToFileURL(join(directory, 'contracts.mjs')).href)
  const calls = []
  const api = { ...schemas }
  for (const name of ['create', 'get', 'cancel', 'retry', 'approveOutline', 'reviseOutline', 'revise']) {
    const exported = name === 'approveOutline' ? 'approvePresentationOutlineForAgent'
      : name === 'reviseOutline' ? 'revisePresentationOutlineForAgent' : `${name}PresentationForAgent`
    api[exported] = async (...args) => { calls.push({ name, args }); return { status: 'planning' } }
  }
  const services = { presentations: api, permissionService: { assertCan: async request => {
    assert.equal(request.actorUserId, 'human')
    assert.equal(request.companyId, 'tenant')
  } } }
  const work = { id: 'work', tenantId: 'tenant', sessionId: 'channel', agentId: 'agent', principalId: 'human', fence: 1, homeEpoch: 1, kind: 'turn', lane: 'interactive', triggerRef: 'message' }
  const call = (method, args) => executePresentation(work, { runId: 'work', cellId: 'cell', callIndex: 0, idempotencyKey: 'stable', action: `presentations.${method}`, args }, services)
  assert.deepEqual(await call('create', { requirements: 'Create a lecture', targetSlideCount: 24 }), { status: 'planning' })
  await call('get', { presentationId: 'deck' })
  await call('approve_outline', { presentationId: 'deck', expectedRevision: 3 })
  await call('revise_outline', { presentationId: 'deck', expectedRevision: 3, feedback: 'Explain further' })
  await call('revise', { presentationId: 'deck', instruction: 'Expand', scope: 'page', pageIds: ['page'] })
  await call('cancel', { presentationId: 'deck' })
  await call('retry', { presentationId: 'deck' })
  assert.deepEqual(calls.map(call => call.name), ['create', 'get', 'approveOutline', 'reviseOutline', 'revise', 'cancel', 'retry'])
  for (const call of calls) {
    assert.equal(call.args[0].authorizationUserId, 'human')
    assert.equal(call.args[0].channelId, 'channel')
    if (call.name !== 'get') assert.equal(call.args.at(-1).idempotencyKey, 'stable')
  }
  assert.equal(calls[2].args[2].expectedRevision, 3)
  assert.equal(calls[3].args[2].expectedRevision, 3)
  await assert.rejects(call('create', { requirements: 'Create', targetSlideCount: 2 }))
  await assert.rejects(call('revise_outline', { presentationId: 'deck', feedback: 'Missing revision' }))
  await assert.rejects(call('revise', { presentationId: 'deck', instruction: 'Edit', scope: 'page' }))
  await assert.rejects(call('create', { requirements: 'Create', authorizationUserId: 'forged' }), /unknown/)
  assert.equal(calls.length, 7)
  console.log('Presentation bridge and native input schemas passed; approval continuation is covered by package tests.')
} finally {
  assert.equal(dirname(directory), resolve(tmpdir()))
  await rm(directory, { recursive: true, force: true })
}
