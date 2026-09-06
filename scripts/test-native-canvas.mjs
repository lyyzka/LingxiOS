import assert from 'node:assert/strict'
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises'
import { createRequire } from 'node:module'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import ts from 'typescript'
import { PGlite } from '@electric-sql/pglite'
import { executeCanvas } from '../dist/src/integrations/lingxiloop/canvas.js'
import { ControlPlaneService } from '../dist/src/control-plane/service.js'
import { MemoryActionLedger, MemoryEventStore, MemorySessionStore, MemoryWorkStore } from '../dist/src/control-plane/memory-store.js'

const root = fileURLToPath(new URL('../', import.meta.url))
const source = resolve(process.argv[2] ?? process.env.LINGXILOOP_SOURCE ?? join(root, '../LingxiLoop/server/src'))
const directory = await mkdtemp(join(tmpdir(), 'lingxios-native-canvas-'))
try {
  const requireNative = createRequire(join(source, '../package.json'))
  const input = await readFile(join(source, 'modules/canvas/contracts.ts'), 'utf8')
  const output = ts.transpileModule(input, { compilerOptions: { target: ts.ScriptTarget.ES2023, module: ts.ModuleKind.ESNext } }).outputText
    .replace("from 'zod'", `from ${JSON.stringify(pathToFileURL(requireNative.resolve('zod')).href)}`)
  await writeFile(join(directory, 'contracts.mjs'), output)
  const schemas = await import(pathToFileURL(join(directory, 'contracts.mjs')).href)
  const calls = []
  const services = {
    canvas: { ...schemas,
      listCanvasAvailableAgents: async companyId => { assert.equal(companyId, 'tenant'); return [{ id: 'candidate', name: 'Candidate', role: 'Assistant', status: 'available' }] },
      getConversationCanvas: async () => ({ id: 'board', companyId: 'tenant', conversationId: 'channel' }),
      createCanvasFrame: async input => { calls.push(input); return { id: 'frame', revision: 1 } },
    },
    permissionService: { assertCan: async request => {
      assert.equal(request.actorUserId, 'human')
      assert.equal(request.companyId, 'tenant')
    } },
  }
  const work = { id: 'work', tenantId: 'tenant', sessionId: 'channel', agentId: 'agent', principalId: 'human', fence: 1, homeEpoch: 1, kind: 'turn', lane: 'interactive', triggerRef: 'message' }
  const call = frame => executeCanvas(work, { runId: 'work', cellId: 'cell', callIndex: 0, idempotencyKey: 'stable', action: 'canvas.create_frame', args: { frame } }, services)
  assert.deepEqual(await call({ type: 'markdown', title: 'Answer', content: '# Result' }), { id: 'frame', revision: 1 })
  assert.deepEqual(calls, [{ companyId: 'tenant', actorId: 'agent', actorKind: 'agent', idempotencyKey: 'stable', canvasId: 'board', frame: { canvasId: 'board', type: 'markdown', title: 'Answer', content: '# Result' } }])
  for (const frame of [{ type: 'unknown' }, { title: 'x'.repeat(201) }, { x: Infinity }, { width: 'wide' }, { content: 3 }, { actorId: 'forged' }, { canvasId: 'foreign' }]) {
    await assert.rejects(call(frame))
  }
  assert.equal(calls.length, 1)
  const commentWrites = []
  services.canvas.addCanvasComment = async input => { commentWrites.push(input); return { id: 'comment' } }
  const commentAction = { runId: 'work', cellId: 'comment', callIndex: 0, idempotencyKey: 'comment', action: 'canvas.add_comment', args: { body: ' Review this ' } }
  assert.deepEqual(await executeCanvas(work, commentAction, services), { id: 'comment' })
  assert.deepEqual(commentWrites, [{ companyId: 'tenant', actorId: 'agent', actorKind: 'agent', canvasId: 'board', body: 'Review this' }])
  for (const args of [{ body: '' }, { body: 'x'.repeat(8001) }, { body: 3 }, { body: 'x', frameId: 'foreign' }, { body: 'x', actorId: 'forged' }]) {
    await assert.rejects(executeCanvas(work, { ...commentAction, args }, services))
  }
  assert.equal(commentWrites.length, 1)
  console.log('Native Canvas comment schema and scoped adapter checks passed (comment writer mocked).')
  const discoveryAction = { runId: 'work', cellId: 'discovery', callIndex: 0, idempotencyKey: 'discovery', action: 'canvas.available_agents', args: {} }
  assert.deepEqual(await executeCanvas(work, discoveryAction, services), [{ id: 'candidate', name: 'Candidate', role: 'Assistant', status: 'available' }])
  await assert.rejects(executeCanvas(work, { ...discoveryAction, args: { companyId: 'foreign' } }, services), /unknown/)
  await assert.rejects(executeCanvas(work, discoveryAction, { ...services, permissionService: { assertCan: async request => { if (request.action === 'agent:read') throw new Error('agent read denied') } }, canvas: { ...services.canvas, listCanvasAvailableAgents: async () => { assert.fail('must authorize first') } } }), /agent read denied/)
  // Execute the original native frame application and SQL; only module locations change.
  for (const [name, path] of [
    ['collaboration-application', join(source, 'modules/canvas/collaboration-application.ts')],
    ['collaboration-repository', join(source, 'modules/canvas/collaboration-repository.ts')],
    ['frames-repository', join(source, 'modules/canvas/frames-repository.ts')],
    ['workspace-repository', join(source, 'modules/canvas/workspace-repository.ts')],
    ['frames-application', join(source, 'modules/canvas/frames-application.ts')],
    ['canvasLayout', resolve(source, '../../src/lib/canvasLayout.ts')],
  ]) {
    const nativeSource = await readFile(path, 'utf8')
    const compiled = ts.transpileModule(nativeSource, { compilerOptions: { target: ts.ScriptTarget.ES2023, module: ts.ModuleKind.ESNext } }).outputText
      .replace("'../../../../src/lib/canvasLayout.js'", "'./canvasLayout.mjs'")
      .replace("'./contracts.js'", "'./contracts.mjs'")
      .replace("'./repository.js'", "'./repository.mjs'")
    await writeFile(join(directory, `${name}.mjs`), compiled)
  }
  await writeFile(join(directory, 'repository.mjs'), "export * from './collaboration-repository.mjs'; export * from './frames-repository.mjs'; export * from './workspace-repository.mjs';")
  const { createCanvasFrameApplication } = await import(pathToFileURL(join(directory, 'frames-application.mjs')).href)
  const db = new PGlite()
  try {
    const baseline = await readFile(join(source, 'db/migrations/0001_v1_baseline.sql'), 'utf8')
    for (const table of ['participants', 'canvases', 'canvas_frames', 'canvas_agent_assignments', 'canvas_activity', 'canvas_comments', 'canvas_presence']) {
      const start = baseline.indexOf(`CREATE TABLE public.${table} (`)
      assert.ok(start >= 0)
      const end = baseline.indexOf('\n);', start)
      assert.ok(end > start)
      await db.exec(baseline.slice(start, end + 3))
      const primary = baseline.match(new RegExp(`ALTER TABLE ONLY public\\.${table}\\s+ADD CONSTRAINT ${table}_pkey PRIMARY KEY[^;]+;`))
      assert.ok(primary, `missing native primary key: ${table}`)
      await db.exec(primary[0])
    }
    const { createCanvasCollaborationApplication } = await import(pathToFileURL(join(directory, 'collaboration-application.mjs')).href)
    const unexpected = async () => { throw new Error('unexpected collaboration side effect') }
    const collaboration = createCanvasCollaborationApplication({ db, resolveCanvas: unexpected, requireFrame: unexpected, publishCanvas: unexpected, publishAssignments: unexpected, logActivity: unexpected })
    services.canvas.listCanvasAvailableAgents = collaboration.listCanvasAvailableAgents
    for (const [id, tenant, kind, capabilities, departed] of [
      ['eligible', 'tenant', 'agent', ['canvas'], null],
      ['foreign', 'other', 'agent', ['canvas'], null],
      ['departed', 'tenant', 'agent', ['canvas'], '2026-01-01'],
      ['no-canvas', 'tenant', 'agent', ['web'], null],
      ['human', 'tenant', 'human', ['canvas'], null],
    ]) {
      await db.query("INSERT INTO participants(id,company_id,kind,name,initial,avatar_bg,status,capabilities,departed_at) VALUES($1,$2,$3,$1,'A','blue','available',$4::jsonb,$5)", [id, tenant, kind, JSON.stringify(capabilities), departed])
    }
    assert.deepEqual(await executeCanvas(work, discoveryAction, services), [{ id: 'eligible', name: 'eligible', role: 'Learning Agent', status: 'available' }])
    await db.query("UPDATE participants SET capabilities='[]'::jsonb WHERE id='eligible'")
    assert.deepEqual(await executeCanvas(work, discoveryAction, services), [])
    const frameReferences = [...baseline.matchAll(/ALTER TABLE ONLY public\.canvas_\w+\s+ADD CONSTRAINT \w+ FOREIGN KEY \([^)]+\) REFERENCES public\.canvas_frames\(id\)[^;]+;/g)]
    assert.equal(frameReferences.length, 4)
    for (const reference of frameReferences) await db.exec(reference[0])
    await db.exec("INSERT INTO canvases(id,company_id,created_by,conversation_id,origin) VALUES('board','tenant','human','channel','conversation'); INSERT INTO canvas_agent_assignments(id,canvas_id,agent_id,assignment,color) VALUES('assignment','board','agent','Write answer','blue')")
    const events = []
    const native = createCanvasFrameApplication({
      db, transaction: fn => db.transaction(fn),
      resolveCanvas: async (companyId, actorId, canvasId) => {
        const { rows } = await db.query('SELECT * FROM canvases WHERE company_id=$1 AND id=$2', [companyId, canvasId])
        if (!rows[0]) throw new Error('canvas not found')
        return rows[0]
      },
      publishCanvas: async (...event) => { events.push(event) },
      logActivity: async input => input,
    })
    services.canvas.createCanvasFrame = native.createCanvasFrame
    const first = await call({ type: 'markdown', title: 'Answer', content: '# Result' })
    const retry = await call({ type: 'markdown', title: 'Answer', content: '# Result' })
    assert.deepEqual(retry, first)
    assert.equal(first.canvasId, 'board')
    assert.equal(first.createdBy, 'agent')
    assert.equal(first.content, '# Result')
    assert.deepEqual((await db.query('SELECT id,content,revision FROM canvas_frames')).rows, [{ id: first.id, content: '# Result', revision: 1 }])
    assert.deepEqual((await db.query('SELECT active_frame_id,status FROM canvas_agent_assignments')).rows, [{ active_frame_id: first.id, status: 'working' }])
    await assert.rejects(call({ content: 'x'.repeat(1024 * 1024 + 1) }), /exceeds 1 MiB/)
    assert.equal((await db.query('SELECT COUNT(*)::int AS count FROM canvas_frames')).rows[0].count, 1)
    assert.equal(events.length, 2) // Native retries republish; this is not exactly-once publication.
    services.canvas.updateCanvasFrame = native.updateCanvasFrame
    services.canvas.getConversationCanvas = async () => ({ id: 'board', companyId: 'tenant', conversationId: 'channel', frames: (await db.query('SELECT id FROM canvas_frames WHERE canvas_id=$1', ['board'])).rows })
    const commentEvents = [], commentActivities = []
    let failCommentPublication = false
    services.canvas.addCanvasComment = createCanvasCollaborationApplication({ db,
      resolveCanvas: async (companyId, actorId, canvasId) => {
        assert.equal(actorId, 'agent')
        const row = (await db.query('SELECT * FROM canvases WHERE company_id=$1 AND id=$2', [companyId, canvasId])).rows[0]
        if (!row) throw new Error('canvas not found')
        return row
      },
      requireFrame: native.requireFrame,
      publishCanvas: async (...event) => {
        if (failCommentPublication) throw new Error('comment publication unavailable')
        commentEvents.push(event)
      },
      publishAssignments: unexpected,
      logActivity: async input => { commentActivities.push(input); return input },
    }).addCanvasComment
    const posted = await executeCanvas(work, { ...commentAction, args: { body: '  Native persisted comment  ', frameId: first.id } }, services)
    assert.deepEqual((await db.query('SELECT id,canvas_id,frame_id,author_id,author_kind,body FROM canvas_comments')).rows,
      [{ id: posted.id, canvas_id: 'board', frame_id: first.id, author_id: 'agent', author_kind: 'agent', body: 'Native persisted comment' }])
    assert.deepEqual(commentEvents, [['tenant', { kind: 'comment.created', canvasId: 'board', comment: posted }]])
    assert.deepEqual(commentActivities, [{ companyId: 'tenant', canvasId: 'board', actorId: 'agent', actorKind: 'agent', frameId: first.id, action: 'comment_created' }])
    failCommentPublication = true
    await assert.rejects(executeCanvas(work, { ...commentAction, args: { body: 'Persisted before publication failure' } }, services), /comment publication unavailable/)
    assert.deepEqual((await db.query('SELECT body FROM canvas_comments ORDER BY body')).rows,
      [{ body: 'Native persisted comment' }, { body: 'Persisted before publication failure' }])
    assert.equal(commentEvents.length, 1)
    assert.equal(commentActivities.length, 1)
    const ledger = new MemoryActionLedger()
    const control = new ControlPlaneService({
      work: new MemoryWorkStore(), sessions: new MemorySessionStore(), events: new MemoryEventStore(), actions: ledger,
      contextProvider: { loadContext: async () => ({ persona: { name: '', role: '', instructions: '' }, capabilities: [], messages: [] }) },
      capabilityResolver: { resolve: async () => [{ name: 'canvas', methods: ['add_comment'] }] },
      delivery: { onEvent: async () => {}, deliverMessage: async () => {} },
      actionExecutor: { execute: async (current, action) => ({ ok: true, value: await executeCanvas(current, action, services) }) },
    })
    await control.enqueue(work)
    const claimed = await control.claim('comment-worker')
    const uncertainAction = { ...commentAction, idempotencyKey: JSON.stringify(['work', 'comment', 0]), args: { body: 'Persisted with unknown receipt' } }
    const receipt = await control.executeAction(claimed, uncertainAction)
    assert.deepEqual(receipt, { ok: false, executionState: 'unknown', error: 'comment publication unavailable' })
    assert.deepEqual(await ledger.find(uncertainAction.idempotencyKey), receipt)
    assert.deepEqual(await control.executeAction(claimed, uncertainAction), receipt)
    assert.deepEqual((await db.query("SELECT COUNT(*)::int AS count FROM canvas_comments WHERE body='Persisted with unknown receipt'")).rows, [{ count: 1 }])
    await db.query("DELETE FROM canvas_comments WHERE author_id='agent'")
    console.log('Native Canvas comment application/SQL passed: attributed frame comment, event shape, persisted write after publication failure, and unknown receipt replay without a second write (scope/publication callbacks and in-memory control stores are fixtures).')
    const update = (frameId, patch) => executeCanvas(work, { runId: 'work', cellId: 'edit', callIndex: 0, idempotencyKey: 'edit', action: 'canvas.update_frame', args: { frameId, patch } }, services)
    const updated = await update(first.id, { baseRevision: 1, content: 'Revised answer' })
    assert.equal(updated.revision, 2)
    assert.equal(updated.content, 'Revised answer')
    await assert.rejects(update(first.id, { baseRevision: 1, content: 'Stale overwrite' }), /revision conflict/)
    await assert.rejects(update(first.id, { content: 'Missing revision' }), /baseRevision/)
    await assert.rejects(update(first.id, { baseRevision: 2 }), /changed fields/)
    await assert.rejects(update(first.id, { baseRevision: 2, actorId: 'forged' }))
    await assert.rejects(update('foreign', { baseRevision: 2, content: 'Overwrite' }), /outside/)
    const allowed = services.permissionService.assertCan
    services.permissionService.assertCan = async request => { if (request.action === 'canvas:write') throw new Error('write revoked'); return allowed(request) }
    await assert.rejects(update(first.id, { baseRevision: 2, content: 'Denied overwrite' }), /write revoked/)
    assert.deepEqual((await db.query('SELECT content,revision FROM canvas_frames')).rows, [{ content: 'Revised answer', revision: 2 }])
    assert.equal(events.length, 3)
    services.permissionService.assertCan = allowed
    services.canvas.appendCanvasFrameContent = native.appendCanvasFrameContent
    const append = (frameId, content) => executeCanvas(work, { runId: 'work', cellId: 'append', callIndex: 0, idempotencyKey: 'append', action: 'canvas.append_content', args: { frameId, content } }, services)
    const appended = await append(first.id, '\nMore evidence')
    assert.equal(appended.content, 'Revised answer\nMore evidence')
    assert.equal(appended.revision, 3)
    for (const content of ['', 5, '文'.repeat(22000)]) await assert.rejects(append(first.id, content), /UTF-8/)
    await assert.rejects(append('foreign', 'No'), /outside/)
    services.permissionService.assertCan = async request => { if (request.action === 'canvas:write') throw new Error('write revoked'); return allowed(request) }
    await assert.rejects(append(first.id, 'No'), /write revoked/)
    assert.deepEqual((await db.query('SELECT content,revision FROM canvas_frames')).rows, [{ content: 'Revised answer\nMore evidence', revision: 3 }])
    assert.equal(events.length, 4)
    services.canvas.deleteCanvasFrame = native.deleteCanvasFrame
    const remove = frameId => executeCanvas(work, { runId: 'work', cellId: 'delete', callIndex: 0, idempotencyKey: 'delete', action: 'canvas.delete_frame', args: { frameId } }, services)
    await assert.rejects(remove(first.id), /write revoked/)
    assert.equal((await db.query('SELECT COUNT(*)::int AS count FROM canvas_frames')).rows[0].count, 1)
    services.permissionService.assertCan = allowed
    await assert.rejects(remove('foreign'), /outside/)
    await db.query("INSERT INTO canvas_comments(id,canvas_id,frame_id,author_id,author_kind,body) VALUES('comment','board',$1,'human','user','Keep this comment')", [first.id])
    await db.query("INSERT INTO canvas_activity(id,canvas_id,frame_id,actor_id,actor_kind,action) VALUES('activity','board',$1,'agent','agent','frame_created')", [first.id])
    await db.query("INSERT INTO canvas_presence(canvas_id,participant_id,participant_kind,status,frame_id) VALUES('board','agent','agent','working',$1)", [first.id])
    assert.deepEqual(await remove(first.id), { id: first.id, canvasId: 'board' })
    assert.equal((await db.query('SELECT COUNT(*)::int AS count FROM canvas_frames')).rows[0].count, 0)
    await assert.rejects(remove(first.id), /outside/)
    assert.equal(events.length, 5)
    // Record why the native presence API cannot be exposed as assignment completion.
    const statusApplication = createCanvasCollaborationApplication({ db,
      resolveCanvas: async () => (await db.query("SELECT * FROM canvases WHERE id='board'")).rows[0],
      requireFrame: native.requireFrame, publishCanvas: async () => {}, publishAssignments: async () => {}, logActivity: async input => input,
    })
    await statusApplication.setCanvasStatus({ companyId: 'tenant', actorId: 'agent', actorKind: 'agent', canvasId: 'board', status: 'completed' })
    assert.deepEqual((await db.query('SELECT status FROM canvas_agent_assignments')).rows, [{ status: 'completed' }])
    const lateFrame = await native.createCanvasFrame({ companyId: 'tenant', actorId: 'agent', actorKind: 'agent', canvasId: 'board', idempotencyKey: 'late-frame', frame: { content: 'Follow-up edit' } })
    assert.deepEqual((await db.query('SELECT status FROM canvas_agent_assignments')).rows, [{ status: 'completed' }], 'frame creation must not reopen a terminal assignment')
    await native.deleteCanvasFrame({ companyId: 'tenant', actorId: 'agent', actorKind: 'agent', frameId: lateFrame.id })
    await assert.rejects(executeCanvas(work, { runId: 'work', cellId: 'status', callIndex: 0, idempotencyKey: 'status', action: 'canvas.set_status', args: { status: 'completed' } }, services), /unsupported/)

    assert.deepEqual((await db.query('SELECT active_frame_id FROM canvas_agent_assignments')).rows, [{ active_frame_id: null }])
    for (const table of ['canvas_activity', 'canvas_comments', 'canvas_presence']) {
      assert.deepEqual((await db.query(`SELECT frame_id FROM ${table}`)).rows, [{ frame_id: null }])
    }

  } finally { await db.close() }
  console.log('Native Canvas agent discovery and creation/update/append/delete SQL, stable frame retry, revision conflicts, frame-reference cleanup and size bound passed in PGlite. Authorization and publication use test resources; native setCanvasStatus bypasses report checks and remains unexposed; native frame creation preserves terminal assignments; report/assignment execution remains unfinished.')
} finally {
  assert.equal(dirname(directory), resolve(tmpdir()))
  await rm(directory, { recursive: true, force: true })
}
