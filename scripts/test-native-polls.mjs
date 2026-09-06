import assert from 'node:assert/strict'
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises'
import { createRequire } from 'node:module'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import ts from 'typescript'
import { PGlite } from '@electric-sql/pglite'
import { executePoll } from '../dist/src/integrations/lingxiloop/polls.js'

const root = fileURLToPath(new URL('../', import.meta.url))
const source = resolve(process.argv[2] ?? process.env.LINGXILOOP_SOURCE ?? join(root, '../LingxiLoop/server/src'))
const requireNative = createRequire(join(source, '../package.json'))
const directory = await mkdtemp(join(tmpdir(), 'lingxios-native-polls-'))
const db = new PGlite()
try {
  await writeFile(join(directory, 'package.json'), '{"type":"module"}')
  // Execute the reference domain and repository unchanged apart from TypeScript erasure
  // and resolving its existing zod dependency outside the temporary directory.
  for (const name of ['application', 'repository', 'contracts']) {
    const input = await readFile(join(source, `modules/polls/${name}.ts`), 'utf8')
    const output = ts.transpileModule(input, { compilerOptions: { target: ts.ScriptTarget.ES2023, module: ts.ModuleKind.ESNext } }).outputText
      .replace("from 'zod'", `from ${JSON.stringify(pathToFileURL(requireNative.resolve('zod')).href)}`)
    await writeFile(join(directory, `${name}.js`), output)
  }
  const { PollApplication } = await import(pathToFileURL(join(directory, 'application.js')).href)
  const baseline = await readFile(join(source, 'db/migrations/0001_v1_baseline.sql'), 'utf8')
  for (const table of ['im_polls', 'im_poll_votes']) {
    const definition = baseline.match(new RegExp(`CREATE TABLE public\\.${table} \\([\\s\\S]*?\\n\\);`))?.[0]
    assert.ok(definition, `missing native ${table} definition`)
    await db.exec(definition)
  }
  await db.exec(`ALTER TABLE im_polls ADD PRIMARY KEY(poll_client_msg_no);
    CREATE TABLE im_channel_bindings(company_id text,channel_id text,profile jsonb);
    INSERT INTO im_channel_bindings VALUES('t','s','{"channelType":2,"members":["a","u"]}');`)
  const queryable = database => ({ query: async (sql, args) => {
    const result = await database.query(sql, args)
    return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
  } })
  const publications = []
  const polls = new PollApplication(queryable(db), {
    transaction: callback => db.transaction(tx => callback(queryable(tx))),
    publishSnapshot: async (row, tallies, actorId) => { publications.push({ row, tallies, actorId }); return publications.length },
  })
  const services = { pollApplication: polls, permissionService: { assertCan: async request => {
    assert.equal(request.actorUserId, 'u')
    assert.equal(request.companyId, 't')
  } } }
  const work = { id: 'w', tenantId: 't', sessionId: 's', agentId: 'a', principalId: 'u', fence: 1, homeEpoch: 1, triggerRef: 'm', kind: 'turn', lane: 'interactive' }
  const call = (method, args, target = work) => executePoll(target, { runId: 'w', cellId: 'c', callIndex: 0, idempotencyKey: 'stable', action: `polls.${method}`, args }, services)
  const created = await call('create', { question: 'Choose', options: ['A', 'B'] })
  assert.equal((await call('create', { question: 'Choose', options: ['A', 'B'] })).messageId, created.messageId)
  await assert.rejects(call('create', { question: 'Changed', options: ['A', 'B'] }), /idempotency conflict/)
  const messageId = created.messageId
  await call('vote', { messageId, optionIds: [created.poll.options[0].id] })
  const shown = await call('show', { messageId })
  assert.deepEqual(shown.tallies, [{ optionId: created.poll.options[0].id, count: 1, voterIds: ['a'] }])
  await assert.rejects(call('show', { messageId }, { ...work, sessionId: 'other' }), /outside/)
  await assert.rejects(call('vote', { messageId, optionIds: [] }, { ...work, agentId: 'outsider' }), /not a member/)
  await assert.rejects(call('close', { messageId }, { ...work, agentId: 'u' }), /only the poll author/)
  await call('close', { messageId })
  assert.equal((await call('show', { messageId })).poll.closedReason, 'manual')
  await assert.rejects(call('vote', { messageId, optionIds: [] }), /closed/)
  assert.ok(publications.length >= 3)
  console.log('Native PollApplication and SQL passed create, dedupe, vote, show, close and domain authorization checks.')
  console.log('Publication transport and permissionService are test resources; live IM and real permission resolution remain separate gates.')
} finally {
  await db.close()
  assert.equal(dirname(directory), resolve(tmpdir()))
  await rm(directory, { recursive: true, force: true })
}
