import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { readFile } from 'node:fs/promises'
import { createRequire } from 'node:module'
import { resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import ts from 'typescript'
import { PGlite } from '@electric-sql/pglite'
import { executeCanvasWork, assertCanvasWorker, reconcileCanvasWork } from '../dist/src/integrations/lingxiloop/canvas-work.js'
import { flushCanvasEvents } from '../dist/src/integrations/lingxiloop/canvas-events.js'
import { assertCanvasSummary } from '../dist/src/integrations/lingxiloop/canvas-summary.js'
import { PgActionLedger, PgWorkStore } from '../dist/src/control-plane/pg-store.js'
import { actionFingerprint } from '../dist/src/control-plane/service.js'
import { actionKeyOf } from '../dist/src/protocol/types.js'

const source = resolve(process.argv[2] ?? process.env.LINGXILOOP_SOURCE ?? fileURLToPath(new URL('../../LingxiLoop/server/src', import.meta.url)))
const transpile = input => ts.transpileModule(input, { compilerOptions: { target: ts.ScriptTarget.ES2023, module: ts.ModuleKind.ESNext } }).outputText
const asModule = code => 'data:text/javascript;base64,' + Buffer.from(code).toString('base64')
const orchestration = await import(asModule(transpile(await readFile(resolve(source, 'canvas/orchestration.ts'), 'utf8'))))
const assignmentSource = ts.createSourceFile('assignments.ts', await readFile(resolve(source, 'modules/canvas/assignments-application.ts'), 'utf8'), ts.ScriptTarget.Latest, true)
const assignmentMapper = assignmentSource.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === 'toAssignment')
assert.ok(assignmentMapper)
const { toAssignment } = await import(asModule(transpile(ts.createPrinter().printNode(ts.EmitHint.Unspecified, assignmentMapper, assignmentSource))))
const workspaceSource = ts.createSourceFile('repository.ts', await readFile(resolve(source, 'modules/canvas/assignments-repository.ts'), 'utf8'), ts.ScriptTarget.Latest, true)
const workspaceInsert = workspaceSource.statements.find(node => ts.isFunctionDeclaration(node) && node.name?.text === 'insertAgentWorkspace')
assert.ok(workspaceInsert)
const { insertAgentWorkspace } = await import(asModule(transpile(ts.createPrinter().printNode(ts.EmitHint.Unspecified, workspaceInsert, workspaceSource))))
const repository = asModule(transpile(await readFile(resolve(source, 'modules/evidence/repository.ts'), 'utf8')))
const evidence = await import(asModule(transpile(await readFile(resolve(source, 'modules/evidence/application.ts'), 'utf8')).replace("'./repository.js'", JSON.stringify(repository))))
const connectionString = process.env.LINGXIOS_CANVAS_TEST_DATABASE_URL
const nativeRequire = createRequire(resolve(source, '../package.json'))
const postgres = connectionString ? new (nativeRequire('pg').Pool)({ connectionString, max: 3, connectionTimeoutMillis: 5000 }) : undefined
const connection = await postgres?.connect()
const db = connection ? { query: (sql, params) => connection.query(sql, params), exec: sql => connection.query(sql), close: async () => { connection.release(); await postgres.end() } } : new PGlite()
let rejectReceipt = false
const pool = { query: async (sql, params) => {
  if (rejectReceipt && sql.startsWith('INSERT INTO lingxios.agent_action_ledger')) throw new Error('injected receipt write failure')
  const result = await db.query(sql, params)
  return { rows: result.rows, rowCount: result.rowCount ?? result.affectedRows ?? result.rows.length }
}, connect: async () => ({ query: pool.query, release() {} }) }
try {
  const existing = await db.query("SELECT tablename FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema')")
  assert.equal(existing.rows.length, 0, 'Canvas work checks require an empty disposable database')
  await db.exec(await readFile(new URL('../db/schema.sql', import.meta.url), 'utf8'))
  await db.exec('SET search_path=public')
  await db.exec(`CREATE TABLE companies(id text PRIMARY KEY);
    CREATE TABLE projects(id text,company_id text,PRIMARY KEY(id,company_id));
    CREATE TABLE project_memberships(company_id text,project_id text,user_id text,PRIMARY KEY(company_id,project_id,user_id));
    CREATE TABLE im_channel_bindings(company_id text,channel_id text,profile jsonb);
    CREATE TABLE learning_project_teacher_agents(company_id text,agent_id text);`)
  const baseline = (await readFile(resolve(source, 'db/migrations/0001_v1_baseline.sql'), 'utf8')).replaceAll('\r\n', '\n')
  for (const table of ['participants', 'canvases', 'canvas_agent_assignments', 'canvas_assignment_dependencies', 'canvas_frames', 'canvas_activity']) {
    const start = baseline.indexOf(`CREATE TABLE public.${table} (`)
    assert.ok(start >= 0)
    await db.exec(baseline.slice(start, baseline.indexOf('\n);', start) + 3))
    const primary = baseline.match(new RegExp(`ALTER TABLE ONLY public\\.${table}\\s+ADD CONSTRAINT \\w+ PRIMARY KEY[^;]+;`))
    assert.ok(primary)
    await db.exec(primary[0])
  }
  for (const name of ['canvases_id_company_id_key', 'canvas_agent_assignments_id_canvas_id_key', 'canvas_agent_assignments_canvas_id_agent_id_key', 'canvas_assignment_execution_role_check', 'canvas_assignment_verifier_not_self_check', 'canvas_assignment_verifies_assignment_id_fkey']) {
    if (name === 'canvas_assignment_execution_role_check') {
      const start = baseline.indexOf('ALTER TABLE public.canvas_agent_assignments\n')
      assert.ok(start >= 0)
      await db.exec(baseline.slice(start, baseline.indexOf(';', start) + 1))
    }
    const constraint = baseline.match(new RegExp(`ALTER TABLE ONLY public\\.\\w+\\s+ADD CONSTRAINT ${name}[^;]+;`))
    assert.ok(constraint, name)
    await db.exec(constraint[0])
  }
  for (const table of ['canvas_assignment_reports', 'evidence_records', 'evidence_links']) {
    const start = baseline.indexOf(`CREATE TABLE public.${table} (`)
    assert.ok(start >= 0)
    await db.exec(baseline.slice(start, baseline.indexOf('\n);', start) + 3))
  }
  await db.exec(baseline.match(/CREATE UNIQUE INDEX idx_canvas_report_assignment[^;]+;/)[0])
  await db.exec(baseline.match(/CREATE UNIQUE INDEX idx_canvases_one_per_conversation[^;]+;/)[0])
  await db.exec(`INSERT INTO companies VALUES('tenant'); INSERT INTO projects VALUES('project','tenant');
    INSERT INTO canvases(id,company_id,project_id,created_by,conversation_id,status,origin) VALUES('board','tenant','project','human','channel','active','agent_os');
    INSERT INTO im_channel_bindings VALUES('tenant','channel','{"members":["parent","builder","verifier","missing","cancel","dependent","handoff"]}');`)
  for (const id of ['parent', 'builder', 'verifier', 'missing', 'cancel', 'dependent', 'handoff']) await db.query(`INSERT INTO participants(id,company_id,kind,name,initial,avatar_bg,status,capabilities)
    VALUES($1,'tenant','agent',$1,'A','blue','available','["canvas"]'::jsonb)`, [id])
  let revoked = false
  const published = []
  let publicationFailure = true
  const services = { canvas: { orchestration: { ...orchestration, ...evidence, toAssignment, insertAgentWorkspace, CH_CANVAS: 'canvas', publish: async (channel, event) => {
    assert.equal(channel, 'canvas')
    if (publicationFailure) throw new Error('notification unavailable')
    published.push(event)
  },
    createPermissionService: (_db, options) => {
      assert.equal(options.lockDependencies, true)
      return { assertCan: async input => { assert.equal(input.actorUserId, 'human'); if (revoked) throw new Error('revoked') } }
    },
  } } }
  const stores = new PgWorkStore(pool), ledger = new PgActionLedger(pool)
  await stores.enqueue({ id: 'parent-work', tenantId: 'tenant', agentId: 'parent', sessionId: 'channel', principalId: 'human', kind: 'turn', lane: 'interactive', triggerRef: 'message',
    meta: { text: 'Build and independently verify the result.', attachments: [] } })
  const work = await stores.claim('worker')
  let counter = 0
  async function action(current, method, args) {
    const identity = { runId: current.id, cellId: `cell-${counter++}`, callIndex: 0 }
    const call = { ...identity, idempotencyKey: actionKeyOf(identity), action: `canvas.${method}`, args }
    const revisions = (await db.query('SELECT steer_inputs FROM lingxios.agent_work_items WHERE id=$1', [current.id])).rows[0].steer_inputs
    await ledger.reserve(call.idempotencyKey, actionFingerprint(current, call), { workId: current.id, tenantId: current.tenantId, principalId: current.principalId,
      agentId: current.agentId, sessionId: current.sessionId, threadId: current.threadId ?? null, requestVersion: revisions.length + 1, action: call })
    return call
  }
  const assign = await action(work, 'assign', { members: [{ agentId: 'builder', assignment: 'Build' }, { agentId: 'verifier', assignment: 'Verify', executionRole: 'verifier', verifiesAgentId: 'builder' }] })
  revoked = true
  await assert.rejects(executeCanvasWork(pool, services, work, assign), /revoked/)
  assert.equal((await db.query('SELECT COUNT(*)::int n FROM canvas_agent_assignments')).rows[0].n, 0)
  revoked = false
  rejectReceipt = true
  await assert.rejects(executeCanvasWork(pool, services, work, assign), /receipt write failure/)
  assert.equal((await db.query('SELECT COUNT(*)::int n FROM canvas_agent_assignments')).rows[0].n, 0)
  assert.equal((await db.query('SELECT COUNT(*)::int n FROM lingxios.agent_work_items')).rows[0].n, 1)
  rejectReceipt = false
  const result = await executeCanvasWork(pool, services, work, assign)
  assert.deepEqual(await ledger.find(assign.idempotencyKey), result)
  assert.equal(result.value.assignments.length, 2)
  assert.equal(await stores.hasPendingChild(work, result.value.assignments[0].taskRef, 1), true)
  assert.equal(await stores.hasPendingChild(work, result.value.assignments[0].taskRef, 2), false)
  assert.equal(await stores.hasPendingChild({ ...work, principalId: 'foreign' }, result.value.assignments[0].taskRef, 1), false)
  assert.equal(result.value.assignments[1].waitingForDependencies, true)
  await flushCanvasEvents(pool, services)
  assert.equal((await db.query('SELECT COUNT(*)::int n FROM lingxios.agent_canvas_outbox WHERE delivered_at IS NULL')).rows[0].n, 2)
  publicationFailure = false
  await db.query('UPDATE lingxios.agent_canvas_outbox SET available_at=NOW()')
  await flushCanvasEvents(pool, services)
  assert.equal(published.length, 1)
  assert.equal(published[0].kind, 'assignment.updated')
  assert.ok(['builder', 'verifier'].includes(published[0].assignment.agentId))
  await assert.rejects(executeCanvasWork(pool, services, work, assign), /already assigned/)
  const builder = await stores.claim('worker')
  assert.equal(builder.agentId, 'builder')
  assert.equal(await stores.claim('worker'), null)
  await assertCanvasWorker(pool, builder)
  const reportArgs = { finding: 'Implemented the result.', evidenceRefs: [], confidence: 0.7, unresolved: [] }
  const reportCall = await action(builder, 'submit_report', reportArgs)
  const report = await executeCanvasWork(pool, services, builder, reportCall)
  assert.ok(report.value.reportId)
  // A report alone never releases the verifier; durable execution must also finish.
  await reconcileCanvasWork(pool, services)
  assert.equal(await stores.claim('worker'), null)
  await db.query("UPDATE lingxios.agent_work_items SET status='completed',goal_outcome=$2::jsonb WHERE id=$1", [builder.id, JSON.stringify({ status: 'partial', verification: 'inconclusive', requestVersion: 1 })])
  await db.query('DELETE FROM lingxios.agent_os_session_leases WHERE work_id=$1', [builder.id])
  await reconcileCanvasWork(pool, services)
  const verifier = await stores.claim('worker')
  assert.equal(await stores.hasPendingChild(work, builder.id, 1), false)
  assert.equal(verifier.agentId, 'verifier')
  await assertCanvasWorker(pool, verifier)
  const foreignHandoff = await action(verifier, 'handoff', { toAgentId: 'handoff', task: 'Inspect the result', frameIds: ['foreign'] })
  await assert.rejects(executeCanvasWork(pool, services, verifier, foreignHandoff), /handoff frames/)
  const handoffCall = await action(verifier, 'handoff', { toAgentId: 'handoff', task: 'Inspect the result', context: 'Review the existing report' })
  const handed = await executeCanvasWork(pool, services, verifier, handoffCall)
  const handedWork = (await db.query('SELECT meta,steer_inputs FROM lingxios.agent_work_items WHERE id=$1', [handed.value.assignments[0].taskRef])).rows[0]
  assert.deepEqual(handedWork.steer_inputs, [])
  assert.equal(handedWork.meta.assignmentSteers.length, 1)
  assert.equal((await db.query('SELECT action FROM canvas_activity WHERE id=$1', [handed.value.activityId])).rows[0].action, 'handoff')
  const invalidReport = await action(verifier, 'submit_report', { ...reportArgs, verifiesReportId: 'foreign', verdict: 'supported', disconfirmingChecks: ['Checked a counterexample'] })
  await assert.rejects(executeCanvasWork(pool, services, verifier, invalidReport), /assigned builder/)
  assert.equal((await db.query('SELECT COUNT(*)::int n FROM canvas_assignment_reports')).rows[0].n, 1)
  const validReport = await action(verifier, 'submit_report', { ...reportArgs, evidenceRefs: [{ kind: 'report', id: report.value.reportId }], verifiesReportId: report.value.reportId, verdict: 'inconclusive', disconfirmingChecks: ['Insufficient observed output'] })
  assert.equal((await executeCanvasWork(pool, services, verifier, validReport)).value.verdict, 'inconclusive')
  assert.equal((await db.query('SELECT COUNT(*)::int n FROM evidence_links')).rows[0].n, 1)
  const more = await action(work, 'assign', { members: [{ agentId: 'missing', assignment: 'Missing report' }, { agentId: 'cancel', assignment: 'Cancel me' }, { agentId: 'dependent', assignment: 'Depends', dependsOnAgentIds: ['cancel'] }] })
  const created = (await executeCanvasWork(pool, services, work, more)).value.assignments
  const stop = await action(work, 'stop_assignment', { agentId: 'cancel' })
  await executeCanvasWork(pool, services, work, stop)
  await db.query("UPDATE lingxios.agent_work_items SET status='completed' WHERE id=$1", [created[0].taskRef])
  await reconcileCanvasWork(pool, services)
  assert.deepEqual((await db.query("SELECT agent_id,status FROM canvas_agent_assignments WHERE agent_id IN ('missing','cancel','dependent') ORDER BY agent_id")).rows,
    [{ agent_id: 'cancel', status: 'cancelled' }, { agent_id: 'dependent', status: 'cancelled' }, { agent_id: 'missing', status: 'failed' }])
  assert.equal((await db.query("SELECT status FROM lingxios.agent_work_items WHERE id=$1", [created[2].taskRef])).rows[0].status, 'cancelled')
  const modules = new Map()
  async function loadNative(file) {
    if (modules.has(file)) return modules.get(file)
    let code = transpile(await readFile(file, 'utf8'))
    for (const match of [...code.matchAll(/from ['"]([^'"]+)['"]/g)]) {
      if (!match[1].startsWith('.')) throw new Error('unexpected native dependency: ' + match[1])
      const url = await loadNative(resolve(file, '..', match[1].replace(/\.js$/, '.ts')))
      code = code.replace(match[0], 'from ' + JSON.stringify(url))
    }
    const url = asModule(code)
    modules.set(file, url)
    return url
  }
  const { createPermissionService } = await import(await loadNative(resolve(source, 'modules/access/public.ts')))
  await db.exec(`ALTER TABLE companies ADD COLUMN type text DEFAULT 'PERSONAL', ADD COLUMN status text DEFAULT 'ACTIVE', ADD COLUMN plan_id text DEFAULT 'plan';
    ALTER TABLE projects ADD COLUMN kind text DEFAULT 'PERSONAL_LEARNING', ADD COLUMN status text DEFAULT 'ACTIVE', ADD COLUMN plan_id text;
    ALTER TABLE project_memberships ADD COLUMN role text DEFAULT 'OWNER', ADD COLUMN status text DEFAULT 'ACTIVE';
    INSERT INTO project_memberships(company_id,project_id,user_id) VALUES('tenant','project','human');
    CREATE TABLE users(id text PRIMARY KEY,email text,email_verified_at timestamptz,deleted_at timestamptz,suspended_at timestamptz);
    INSERT INTO users(id) VALUES('human');
    CREATE TABLE conversations(id text,company_id text,project_id text,members jsonb,leader_id text);
    INSERT INTO conversations VALUES('channel','tenant','project','["human"]',NULL);
    CREATE TABLE company_memberships(company_id text,user_id text,role text,status text);
    INSERT INTO company_memberships VALUES('tenant','human','OWNER','ACTIVE');
    CREATE TABLE plans(id text PRIMARY KEY,code text,status text); INSERT INTO plans VALUES('plan','test','ACTIVE');
    CREATE TABLE entitlements(id text PRIMARY KEY,code text); INSERT INTO entitlements VALUES('conversation','conversation.core');
    CREATE TABLE plan_entitlements(plan_id text,entitlement_id text,value jsonb); INSERT INTO plan_entitlements VALUES('plan','conversation','true');`)
  services.canvas.orchestration.createPermissionService = createPermissionService
  for (const [revoke, restore] of [
    ["UPDATE project_memberships SET status='INACTIVE'", "UPDATE project_memberships SET status='ACTIVE'"],
    ["UPDATE conversations SET members='[]'", `UPDATE conversations SET members='["human"]'`],
    ["UPDATE plan_entitlements SET value='false'", "UPDATE plan_entitlements SET value='true'"],
    ["UPDATE participants SET capabilities='[]' WHERE id='parent'", `UPDATE participants SET capabilities='["canvas"]' WHERE id='parent'`],
  ]) {
    const denied = await action(work, 'steer_assignment', { agentId: 'verifier', text: 'Check a revised requirement' })
    await db.exec(revoke)
    await assert.rejects(executeCanvasWork(pool, services, work, denied))
    assert.equal((await db.query('SELECT jsonb_array_length(steer_inputs) n FROM lingxios.agent_work_items WHERE id=$1', [verifier.id])).rows[0].n, 0)
    await db.exec(restore)
  }
  const revised = await action(work, 'steer_assignment', { agentId: 'verifier', text: 'Check a revised requirement' })
  await executeCanvasWork(pool, services, work, revised)
  const steered = (await db.query('SELECT meta,steer_inputs,preempt_requested_at FROM lingxios.agent_work_items WHERE id=$1', [verifier.id])).rows[0]
  assert.deepEqual(steered.steer_inputs, [], 'agent instructions must not become user revisions')
  assert.equal(steered.meta.assignmentVersion, 2)
  assert.ok(steered.preempt_requested_at)
  await db.query("UPDATE lingxios.agent_work_items SET status='completed' WHERE id=$1", [verifier.id])
  await reconcileCanvasWork(pool, services)
  assert.equal((await db.query("SELECT status FROM canvas_agent_assignments WHERE agent_id='verifier'")).rows[0].status, 'failed', 'a report for the previous request version must not complete revised work')
  await db.query("UPDATE lingxios.agent_work_items SET status='failed',error='Unable to verify' WHERE id=$1", [handed.value.assignments[0].taskRef])
  await reconcileCanvasWork(pool, services)
  assert.equal((await db.query("SELECT COUNT(*)::int n FROM lingxios.agent_work_items WHERE kind='canvas_summary'")).rows[0].n, 1)
  await db.exec(`INSERT INTO conversations VALUES('new-channel','tenant','project','["human"]',NULL);
    INSERT INTO im_channel_bindings VALUES('tenant','new-channel','{"members":["parent","builder"]}');`)
  await stores.enqueue({ id: 'start-work', tenantId: 'tenant', agentId: 'parent', sessionId: 'new-channel', principalId: 'human', kind: 'turn', lane: 'interactive', triggerRef: 'new-message', meta: { text: 'Start a new workspace' } })
  const starter = await stores.claim('worker')
  const startCall = await action(starter, 'start_workspace', { title: 'New workspace', goal: 'Prepare a result', members: [{ agentId: 'builder', assignment: 'Prepare' }] })
  const started = await executeCanvasWork(pool, services, starter, startCall)
  assert.deepEqual((await db.query('SELECT goal,authorization_user_id FROM canvases WHERE id=$1', [started.value.canvasId])).rows,
    [{ goal: 'Prepare a result', authorization_user_id: 'human' }])
  await assert.rejects(executeCanvasWork(pool, services, starter, startCall), /already has a Canvas/)
  const stopWorkspace = await action(starter, 'stop_workspace', {})
  await executeCanvasWork(pool, services, starter, stopWorkspace)
  assert.equal((await db.query('SELECT status FROM lingxios.agent_work_items WHERE id=$1', [started.value.assignments[0].taskRef])).rows[0].status, 'cancelled')
  assert.equal((await db.query('SELECT status FROM canvases WHERE id=$1', [started.value.canvasId])).rows[0].status, 'stopped')
  assert.equal(await stores.claim('worker'), null, 'summary waits for the originating agent session lease')
  const completion = { status: 'completed', resultText: 'Partial findings; several assignments failed.', goalOutcome: { status: 'partial', verification: 'inconclusive', requestVersion: 1 } }
  assert.equal(await stores.complete(work.id, work.fence, createHash('sha256').update(work.leaseToken).digest('hex'), completion), true)
  const summary = await stores.claim('worker')
  assert.equal(summary.kind, 'canvas_summary')
  assert.equal(summary.meta.text, 'Build and independently verify the result.')
  await assertCanvasSummary(pool, summary)
  const incomplete = await action(summary, 'submit_report', { ...reportArgs, consumedReportIds: [report.value.reportId] })
  await assert.rejects(executeCanvasWork(pool, services, summary, incomplete), /every current assignment/)
  const reports = (await db.query('SELECT id FROM canvas_assignment_reports WHERE assignment_id IS NOT NULL')).rows.map(row => row.id)
  const summaryReport = await action(summary, 'submit_report', { ...reportArgs, finding: 'Builder supplied a finding; verification remains inconclusive and other work failed.',
    consumedReportIds: reports, conflictResolution: ['Retain the verifier uncertainty'], unresolved: ['Independent verification remains incomplete'] })
  await executeCanvasWork(pool, services, summary, summaryReport)
  assert.equal(await stores.complete(summary.id, summary.fence, createHash('sha256').update(summary.leaseToken).digest('hex'), completion), true)
  await reconcileCanvasWork(pool, services)
  assert.equal((await db.query("SELECT status FROM canvases WHERE id='board'")).rows[0].status, 'completed')
  assert.equal((await db.query('SELECT goal_outcome FROM lingxios.agent_work_items WHERE id=$1', [summary.id])).rows[0].goal_outcome.status, 'partial', 'workspace lifecycle does not prove whole-goal satisfaction')
  await reconcileCanvasWork(pool, services)
  assert.equal((await db.query("SELECT COUNT(*)::int n FROM lingxios.agent_work_items WHERE kind='canvas_summary'")).rows[0].n, 1)
  console.log('Canvas native tables/helpers/evidence and personal-project authorization passed: atomic assignment/receipt/outbox, dependency release, reports, handoff, cancellation, revocation, stale-report rejection and durable reporter lifecycle. Educational seats, concurrent PostgreSQL and public Python flow remain separate checks.')
} finally { await db.close() }
