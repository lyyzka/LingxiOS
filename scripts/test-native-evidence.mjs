import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { createRequire } from 'node:module'
import { dirname, relative, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import ts from 'typescript'
import { PGlite } from '@electric-sql/pglite'
import { recordAttempt } from '../dist/src/integrations/lingxiloop/learning-evidence.js'
import { proposeEvaluation } from '../dist/src/integrations/lingxiloop/learning-evaluation.js'
import { readAttempts } from '../dist/src/integrations/lingxiloop/learning-attempts.js'

const source = resolve(process.argv[2] ?? process.env.LINGXILOOP_SOURCE ?? fileURLToPath(new URL('../../LingxiLoop/server/src', import.meta.url)))
const nativeModules = new Map()
async function nativeModule(file) {
  const absolute = resolve(source, file)
  if (relative(source, absolute).startsWith('..')) throw new Error('native import is outside server source')
  if (nativeModules.has(absolute)) return nativeModules.get(absolute)
  const output = ts.transpileModule(await readFile(absolute, 'utf8'), { compilerOptions: { target: ts.ScriptTarget.ES2023, module: ts.ModuleKind.ESNext } }).outputText
  const parsed = ts.createSourceFile(absolute, output, ts.ScriptTarget.ES2023, true, ts.ScriptKind.JS)
  const imports = {}
  for (const statement of parsed.statements) {
    if ((!ts.isImportDeclaration(statement) && !ts.isExportDeclaration(statement)) || !statement.moduleSpecifier) continue
    const specifier = statement.moduleSpecifier.text
    if (specifier.startsWith('.')) imports[specifier] = await nativeModule(resolve(dirname(absolute), specifier.replace(/\.js$/, '.ts')))
    else if (!specifier.startsWith('node:')) throw new Error(`unsupported native dependency: ${specifier}`)
  }
  const url = await moduleUrl(absolute, imports)
  nativeModules.set(absolute, url)
  return url
}
async function moduleUrl(file, imports = {}) {
  const input = await readFile(resolve(source, file), 'utf8')
  let output = ts.transpileModule(input, { compilerOptions: { target: ts.ScriptTarget.ES2023, module: ts.ModuleKind.ESNext } }).outputText
  for (const [specifier, url] of Object.entries(imports)) output = output.replaceAll(`'${specifier}'`, JSON.stringify(url))
  return `data:text/javascript;base64,${Buffer.from(output).toString('base64')}`
}
const repository = await moduleUrl('modules/evidence/repository.ts')
const evidenceUrl = await moduleUrl('modules/evidence/application.ts', { './repository.js': repository })
const native = await import(evidenceUrl)
const inline = code => `data:text/javascript;base64,${Buffer.from(code).toString('base64')}`
const learningRepository = inline(`
  export * from ${JSON.stringify(await moduleUrl('modules/learning/missions-repository.ts'))};
  export * from ${JSON.stringify(await moduleUrl('modules/learning/evidence-repository.ts'))};
  export * from ${JSON.stringify(await moduleUrl('modules/learning/learning-state-repository.ts'))};
  export function listProjectLearningKnowledgeUnits() { throw Error('outside submission check'); }
  export function requireLearningCourseProjectScope() { throw Error('outside submission check'); }
`)
// Permission and unrelated lifecycle methods are explicit test seams; submission/evidence SQL is native.
const permissionUrl = inline(`
  export const requests = [];
  export function createPermissionService() { return { assertCan: async request => {
    requests.push(request);
    if (request.actorUserId !== 'learner') throw Error('fixture permission denied');
  } }; }
`)
const learning = await import(await moduleUrl('modules/learning/missions-application.ts', {
  '../access/public.js': permissionUrl,
  '../evidence/public.js': evidenceUrl,
  './repository.js': learningRepository,
  './errors.js': await moduleUrl('modules/learning/errors.ts'),
  './mission-lifecycle-application.js': inline(`
    export function getLearningMission() { throw Error('outside submission check'); }
    export { getLearningMission as addLearningMissionSteps, getLearningMission as completeLearningMission,
      getLearningMission as finishLearningMissionPlanning, getLearningMission as updateLearningMissionStep };
  `),
}))
const evaluation = await import(await moduleUrl('modules/learning/evaluation-application.ts', {
  '../access/public.js': permissionUrl,
  './errors.js': await moduleUrl('modules/learning/errors.ts'),
  './learning-state.js': await moduleUrl('modules/learning/learning-state.ts'),
  './project-scope-repository.js': await moduleUrl('modules/learning/project-scope-repository.ts'),
  './repository.js': learningRepository,
}))
const requireNative = createRequire(resolve(source, '../package.json'))
const realPermissionUrl = await nativeModule('modules/access/public.ts')
const realAccess = await import(realPermissionUrl)
const schemas = await import(await moduleUrl('modules/learning/contracts.ts', {
  zod: pathToFileURL(requireNative.resolve('zod')).href,
  './preset.js': await moduleUrl('modules/learning/preset.ts'),
}))
// An explicit URL must point to a disposable, empty database; this script creates native-shaped fixture tables.
const connectionString = process.env.LINGXIOS_TEST_DATABASE_URL
const pg = connectionString ? new (requireNative('pg').Pool)({ connectionString, max: 4, connectionTimeoutMillis: 5000 }) : null
const db = pg ? {
  query: (sql, params) => pg.query(sql, params),
  exec: sql => pg.query(sql),
  connect: () => pg.connect(),
  transaction: async callback => {
    const client = await pg.connect()
    try {
      await client.query('BEGIN')
      const result = await callback(client)
      await client.query('COMMIT')
      return result
    } catch (error) {
      await client.query('ROLLBACK')
      throw error
    } finally { client.release() }
  },
  close: () => pg.end(),
} : new PGlite()
try {
  // Execute unmodified native application/SQL against minimal tables, not the full product schema.
  await db.exec(`
    CREATE TABLE evidence_records(id text PRIMARY KEY,company_id text,project_id text,level text,derivation text,kind text,subject_user_id text,data jsonb,created_by_type text,created_by_id text,created_at timestamptz DEFAULT NOW());
    CREATE TABLE evidence_links(id text PRIMARY KEY,company_id text,project_id text,evidence_id text REFERENCES evidence_records(id),relation text,target_level text,target_kind text,target_id text,created_at timestamptz DEFAULT NOW(),UNIQUE(company_id,project_id,evidence_id,relation,target_kind,target_id));
    CREATE TABLE learning_attempts(id text PRIMARY KEY,company_id text,project_id text);
    INSERT INTO learning_attempts VALUES('attempt','tenant','project'),('foreign','other','project'),('elsewhere','tenant','other');
  `)
  const input = { id: 'evidence', companyId: 'tenant', projectId: 'project', level: 'L1', derivation: 'OBSERVED', kind: 'LEARNING_ATTEMPT', subjectUserId: 'learner', data: { answer: 'observed' }, createdBy: { type: 'USER', id: 'learner' } }
  const link = { relation: 'SOURCE', targetLevel: 'L1', targetKind: 'LEARNING_ATTEMPT', targetId: 'attempt' }
  const transaction = callback => db.transaction(callback)
  const record = await native.createEvidenceRecord(transaction, input, [link])
  assert.deepEqual(record, { ...input, createdAt: record.createdAt })
  assert.deepEqual(await native.createEvidenceRecord(transaction, input, [link]), record)
  assert.equal((await db.query('SELECT * FROM evidence_links')).rows.length, 1)
  for (const change of [{ data: { answer: 'changed' } }, { createdBy: { type: 'AGENT', id: 'learner' } }, { subjectUserId: 'other' }]) {
    await assert.rejects(native.createEvidenceRecord(transaction, { ...input, ...change }, [link]), /different content/)
  }
  for (const targetId of ['foreign', 'elsewhere', 'missing']) {
    await assert.rejects(native.createEvidenceRecord(transaction, { ...input, id: targetId }, [link, { ...link, targetId }]), /outside the current Project/)
  }
  await assert.rejects(native.createEvidenceRecord(transaction, { ...input, id: 'oversize', data: { answer: 'x'.repeat(32768) } }), /exceeds/)
  await assert.rejects(native.createEvidenceRecord(transaction, { ...input, id: 'too-many' }, Array(65).fill(link)), /limited to 64/)
  assert.deepEqual((await db.query('SELECT id FROM evidence_records')).rows, [{ id: 'evidence' }])
  assert.equal((await db.query('SELECT * FROM evidence_links')).rows.length, 1)
  const scope = { companyId: 'tenant', projectId: 'project', subjectUserId: 'learner', maximumLevel: 'L1' }
  assert.deepEqual(await native.readProductEvidenceChain(db, scope), [{ ...record, links: [link] }])
  for (const change of [{ companyId: 'other' }, { projectId: 'other' }, { subjectUserId: 'other' }, { maximumLevel: 'L0' }]) {
    assert.deepEqual(await native.readProductEvidenceChain(db, { ...scope, ...change }), [])
  }
  assert.throws(() => native.readProductEvidenceChain(db, { ...scope, maximumLevel: 'L4' }), /unavailable/)
  await db.exec(`
    CREATE TABLE projects(id text PRIMARY KEY,company_id text,kind text,name text,status text);
    CREATE TABLE conversations(id text PRIMARY KEY,company_id text,project_id text);
    CREATE TABLE courses(id text PRIMARY KEY,company_id text,project_id text,study_room_conversation_id text);
    CREATE TABLE learning_course_rooms(company_id text,course_id text,conversation_id text,purpose text);
    CREATE TABLE im_channel_bindings(company_id text,channel_id text,profile jsonb);
    CREATE TABLE learning_activities(id text PRIMARY KEY,company_id text,project_id text,status text);
    CREATE TABLE learning_mission_steps(id text PRIMARY KEY,company_id text,project_id text,mission_id text);
    CREATE TABLE learning_missions(id text PRIMARY KEY,company_id text,project_id text,conversation_id text,learner_id text);
    ALTER TABLE learning_attempts ADD COLUMN learner_id text, ADD COLUMN activity_id text, ADD COLUMN mission_step_id text, ADD COLUMN assistance text, ADD COLUMN evidence_id text REFERENCES evidence_records(id);
    INSERT INTO projects VALUES('project','tenant','TEACHING','Course','ACTIVE');
    INSERT INTO conversations VALUES('room','tenant','project');
    INSERT INTO learning_activities VALUES('activity','tenant','project','PUBLISHED'),('draft','tenant','project','DRAFT'),('foreign','other','project','PUBLISHED');
  `)
  const client = { query: async (sql, params) => {
    const result = await db.query(sql, params)
    return { ...result, rowCount: result.rowCount ?? result.affectedRows ?? result.rows.length }
  } }
  const submissionTransaction = callback => db.transaction(tx => callback({ query: async (sql, params) => {
    const result = await tx.query(sql, params)
    return { ...result, rowCount: result.rowCount ?? result.affectedRows ?? result.rows.length }
  } }))
  const metrics = []
  const infrastructure = { syncMessages: async request => {
    assert.deepEqual(request, { channelId: 'room', channelType: 2, limit: 100, loginUid: 'agent' })
    return [{ clientMsgNo: 'message', fromUid: 'learner', authoredByAgent: false }]
  }, metric: (...args) => metrics.push(args) }
  const command = { companyId: 'tenant', channelId: 'room', agentId: 'agent', activityId: 'activity', evidenceClientMsgNos: ['message'] }
  const submitted = await learning.recordLearningAttempt(client, submissionTransaction, infrastructure, command)
  assert.equal(submitted.learnerId, 'learner')
  const attempt = (await db.query('SELECT * FROM learning_attempts WHERE id=$1', [submitted.id])).rows[0]
  assert.equal(attempt.activity_id, 'activity')
  assert.equal(attempt.assistance, 'NONE')
  const chain = await native.readProductEvidenceChain(db, scope)
  assert.equal(chain.length, 2)
  const submittedEvidence = chain.find(item => item.id === attempt.evidence_id)
  assert.deepEqual(submittedEvidence.data, { conversationId: 'room', clientMsgNos: ['message'], documents: [], canvasFrames: [] })
  assert.deepEqual(submittedEvidence.links, [{ ...link, targetId: submitted.id }])
  for (const activityId of ['draft', 'foreign', 'missing']) {
    await assert.rejects(learning.recordLearningAttempt(client, submissionTransaction, infrastructure, { ...command, activityId }), /published activity or mission step/)
  }
  assert.equal((await db.query('SELECT * FROM evidence_records')).rows.length, 2)
  assert.equal((await db.query('SELECT * FROM evidence_links')).rows.length, 2)
  assert.equal((await db.query('SELECT * FROM learning_attempts WHERE evidence_id IS NOT NULL')).rows.length, 1)
  assert.deepEqual(metrics, [['learning.attempt.accepted', { source: 'message' }]])
  const permissions = (await import(permissionUrl)).requests
  assert.equal(permissions.length, 4)
  assert.ok(permissions.every(request => request.actorUserId === 'learner' && request.action === 'learning:submit' && request.projectId === 'project'))
  await db.exec(`CREATE TABLE participants(id text,company_id text,kind text,departed_at timestamptz);
    INSERT INTO participants VALUES('learner','tenant','human',NULL);`)
  const pool = { connect: async () => {
    const connection = pg ? await pg.connect() : db
    return { release() { if (pg) connection.release() }, query: async (sql, params) => {
    const result = await connection.query(sql, params)
    return { ...result, rowCount: result.rowCount ?? result.affectedRows ?? result.rows.length, command: sql.trim().split(/\s/)[0], oid: 0,
      fields: result.fields.map(field => ({ name: field.name, format: 'text', tableID: 0, columnID: 0, dataTypeID: field.dataTypeID, dataTypeSize: -1, dataTypeModifier: -1 })) }
  } } } }
  const bridgeMetrics = [], conversationPermissions = []
  const services = {
    learning: { createPermissionService: () => ({ assertCan: async () => {} }), ...await import(learningRepository), ...learning, inc: (...args) => bridgeMetrics.push(args) },
    permissionService: { assertCan: async request => conversationPermissions.push(request) },
    wukongClient: () => ({ syncMessages: async () => [{ clientMsgNo: 'message', fromUid: 'learner', channelId: 'room', channelType: 2, payload: { kind: 'text', body: 'answer' } }] }),
  }
  const work = { tenantId: 'tenant', principalId: 'learner', sessionId: 'room', agentId: 'agent' }
  const action = { action: 'learning.record_attempt', args: { activityId: 'activity', evidenceClientMsgNos: ['message'] } }
  const bridged = await recordAttempt(pool, services, work, action, 2)
  assert.equal(bridged.learnerId, 'learner')
  assert.equal((await db.query('SELECT * FROM learning_attempts WHERE id=$1', [bridged.id])).rows.length, 1)
  await assert.rejects(recordAttempt(pool, services, work, { ...action, args: { ...action.args, activityId: 'draft' } }, 2), /published activity or mission step/)
  assert.equal((await db.query('SELECT * FROM evidence_records')).rows.length, 3)
  assert.equal((await db.query('SELECT * FROM evidence_links')).rows.length, 3)
  assert.equal((await db.query('SELECT * FROM learning_attempts WHERE evidence_id IS NOT NULL')).rows.length, 2)
  assert.deepEqual(bridgeMetrics, [['learning.attempt.accepted', { source: 'message' }]])
  assert.deepEqual(conversationPermissions, Array(2).fill({ actorUserId: 'learner', companyId: 'tenant', action: 'learning:submit', resource: { type: 'conversation', id: 'room' } }))
  await db.exec(`
    ALTER TABLE learning_activities ADD COLUMN kind text DEFAULT 'PRACTICE', ADD COLUMN evaluation_mode text DEFAULT 'AGENT_FORMATIVE', ADD COLUMN target_level int DEFAULT 2;
    ALTER TABLE learning_attempts ADD COLUMN status text DEFAULT 'SUBMITTED';
    ALTER TABLE learning_mission_steps ADD COLUMN knowledge_unit_id text;
    CREATE TABLE learning_knowledge_units(id text PRIMARY KEY,company_id text,project_id text,target_level int);
    CREATE TABLE learning_activity_knowledge_units(company_id text,project_id text,activity_id text,knowledge_unit_id text);
    CREATE TABLE project_memberships(company_id text,project_id text,user_id text);
    CREATE TABLE learning_states(company_id text,project_id text,user_id text,knowledge_unit_id text,level int,status text,independent_evidence_count int,review_interval_days int,next_review_at timestamptz,last_evidence_at timestamptz,version int DEFAULT 1,updated_at timestamptz DEFAULT NOW(),UNIQUE(project_id,user_id,knowledge_unit_id));
    CREATE TABLE learning_evaluations(id text PRIMARY KEY,company_id text,project_id text,attempt_id text,demonstrated_level int,confidence numeric,rubric_results jsonb,feedback text,evaluator_id text,evaluator_kind text,status text,source_evidence_id text,verifier_evidence_id text);
    INSERT INTO learning_knowledge_units VALUES('unit','tenant','project',2);
    INSERT INTO learning_activity_knowledge_units VALUES('tenant','project','activity','unit');
    INSERT INTO project_memberships VALUES('tenant','project','learner');
  `)
  Object.assign(services.learning, evaluation, schemas)
  const evaluationAction = { action: 'learning.propose_evaluation', args: { attemptId: bridged.id, demonstratedLevel: 2, confidence: 0.9, rubricResults: [{ label: 'reasoning', score: 2, weight: 1 }] } }
  const accepted = await proposeEvaluation(pool, services, work, evaluationAction)
  assert.equal(accepted.status, 'ACCEPTED')
  assert.equal(accepted.decisions.length, 1)
  assert.equal(accepted.decisions[0].nextLevel, 2)
  assert.deepEqual((await db.query('SELECT level,status FROM learning_states')).rows, [{ level: 2, status: 'LEARNING' }])
  assert.deepEqual((await db.query('SELECT status FROM learning_attempts WHERE id=$1', [bridged.id])).rows, [{ status: 'EVALUATED' }])
  const pending = await proposeEvaluation(pool, services, work, { ...evaluationAction, args: { ...evaluationAction.args, attemptId: submitted.id, confidence: 0.5 } })
  assert.deepEqual({ status: pending.status, decisions: pending.decisions }, { status: 'PENDING', decisions: [] })
  assert.deepEqual((await db.query('SELECT status FROM learning_attempts WHERE id=$1', [submitted.id])).rows, [{ status: 'SUBMITTED' }])
  const previousMetrics = [...bridgeMetrics]
  // A real projection failure occurs after native evaluation insertion and must roll back that insertion.
  await db.exec('DELETE FROM project_memberships')
  await assert.rejects(proposeEvaluation(pool, services, work, evaluationAction), /learning state scope not found/)
  assert.deepEqual((await db.query('SELECT COUNT(*)::int AS count FROM learning_evaluations')).rows, [{ count: 2 }])
  assert.deepEqual(bridgeMetrics, previousMetrics)
  for (const rubricResults of [[], [{ label: 'reasoning', score: 5, weight: 1 }], [{ label: 'reasoning', score: 2, weight: 0 }]]) {
    await assert.rejects(proposeEvaluation(pool, services, work, { ...evaluationAction, args: { ...evaluationAction.args, rubricResults } }))
  }
  assert.deepEqual(bridgeMetrics.slice(1), [
    ['learning.state.changed', { status: 'LEARNING' }],
    ['learning.evaluation.proposed', { status: 'ACCEPTED' }],
    ['learning.evaluation.proposed', { status: 'PENDING' }],
  ])
  await db.exec(`ALTER TABLE learning_attempts ADD COLUMN submitted_at timestamptz DEFAULT NOW();
    ALTER TABLE learning_evaluations ADD COLUMN created_at timestamptz DEFAULT NOW();`)
  const listed = await readAttempts(pool, services, work, { action: 'learning.list_attempts', args: { activityId: 'activity' } })
  assert.equal(listed.truncated, false)
  assert.deepEqual(listed.attempts.map(item => item.id).sort(), [bridged.id, submitted.id].sort())
  const detail = await readAttempts(pool, services, work, { action: 'learning.get_attempt', args: { attemptId: bridged.id } })
  assert.equal(detail.evidence.data.clientMsgNos[0], 'message')
  assert.deepEqual(detail.evaluations.map(item => ({ id: item.id, status: item.status })), [{ id: accepted.evaluationId, status: 'ACCEPTED' }])
  assert.equal(detail.evaluationsTruncated, false)
  for (const attemptId of ['foreign', 'elsewhere', 'missing']) {
    await assert.rejects(readAttempts(pool, services, work, { action: 'learning.get_attempt', args: { attemptId } }), /not found for this principal/)
  }
  await assert.rejects(readAttempts(pool, services, work, { action: 'learning.list_attempts', args: { learnerId: 'other' } }), /unknown/)
  await db.query(`INSERT INTO learning_attempts(id,company_id,project_id,learner_id,activity_id,evidence_id)
    SELECT 'history-'||n,'tenant','project','learner','activity',$1 FROM generate_series(1,101) n`, [attempt.evidence_id])
  const bounded = await readAttempts(pool, services, work, { action: 'learning.list_attempts', args: {} })
  assert.equal(bounded.attempts.length, 100)
  assert.equal(bounded.truncated, true)
  await db.query(`INSERT INTO learning_evaluations(id,company_id,project_id,attempt_id,status)
    SELECT 'history-'||n,'tenant','project',$1,'PENDING' FROM generate_series(1,101) n`, [bridged.id])
  const boundedDetail = await readAttempts(pool, services, work, { action: 'learning.get_attempt', args: { attemptId: bridged.id } })
  assert.equal(boundedDetail.evaluations.length, 100)
  assert.equal(boundedDetail.evaluationsTruncated, true)
  const permissionCheck = services.permissionService.assertCan
  services.permissionService.assertCan = async () => { throw new Error('read denied') }
  await assert.rejects(readAttempts(pool, services, work, { action: 'learning.list_attempts', args: {} }), /read denied/)
  services.permissionService.assertCan = permissionCheck
  await db.exec("UPDATE participants SET departed_at=NOW() WHERE id='learner'")
  assert.deepEqual(await readAttempts(pool, services, work, { action: 'learning.list_attempts', args: {} }), { attempts: [], truncated: false })
  await db.exec(`
    UPDATE participants SET departed_at=NULL WHERE id='learner';
    ALTER TABLE projects ADD COLUMN plan_id text, ADD COLUMN created_by text;
    ALTER TABLE conversations ADD COLUMN members jsonb DEFAULT '["learner","agent"]', ADD COLUMN leader_id text;
    ALTER TABLE project_memberships ADD COLUMN role text, ADD COLUMN status text;
    INSERT INTO project_memberships VALUES('tenant','project','learner','STUDENT','ACTIVE');
    CREATE TABLE users(id text PRIMARY KEY,email text,email_verified_at timestamptz,deleted_at timestamptz,suspended_at timestamptz);
    CREATE TABLE companies(id text PRIMARY KEY,type text,status text,plan_id text);
    CREATE TABLE company_memberships(company_id text,user_id text,role text,status text);
    CREATE TABLE plans(id text PRIMARY KEY,code text,status text);
    CREATE TABLE entitlements(id text PRIMARY KEY,code text);
    CREATE TABLE plan_entitlements(plan_id text,entitlement_id text,value jsonb);
    INSERT INTO users VALUES('learner','learner@example.invalid',NOW(),NULL,NULL);
    INSERT INTO companies VALUES('tenant','PERSONAL','ACTIVE','plan');
    INSERT INTO company_memberships VALUES('tenant','learner','MEMBER','ACTIVE');
    INSERT INTO plans VALUES('plan','test','ACTIVE');
    INSERT INTO entitlements VALUES('learning','learning.core');
    INSERT INTO plan_entitlements VALUES('plan','learning','true');
  `)
  services.permissionService = realAccess.createPermissionService(client)
  Object.assign(services.learning, { createPermissionService: realAccess.createPermissionService },
    await import(await nativeModule('modules/learning/missions-application.ts')),
    await import(await nativeModule('modules/learning/evaluation-application.ts')))
  const authorized = await recordAttempt(pool, services, work, action, 2)
  assert.equal(authorized.learnerId, 'learner')
  assert.equal((await proposeEvaluation(pool, services, work, { ...evaluationAction, args: { ...evaluationAction.args, attemptId: authorized.id } })).status, 'ACCEPTED')
  const counts = async () => (await db.query(`SELECT
    (SELECT COUNT(*)::int FROM learning_attempts) AS attempts,
    (SELECT COUNT(*)::int FROM learning_evaluations) AS evaluations`)).rows
  const beforeDenied = await counts()
  for (const [change, restore] of [
    ["UPDATE company_memberships SET status='INACTIVE'", "UPDATE company_memberships SET status='ACTIVE'"],
    ["UPDATE project_memberships SET status='INACTIVE'", "UPDATE project_memberships SET status='ACTIVE'"],
    ["UPDATE project_memberships SET role='OBSERVER'", "UPDATE project_memberships SET role='STUDENT'"],
    ["UPDATE users SET suspended_at=NOW()", "UPDATE users SET suspended_at=NULL"],
    ["UPDATE plan_entitlements SET value='false'", "UPDATE plan_entitlements SET value='true'"],
    ["UPDATE projects SET company_id='other'", "UPDATE projects SET company_id='tenant'"],
  ]) {
    await db.exec(change)
    await assert.rejects(recordAttempt(pool, services, work, action, 2), error => error.name === 'ForbiddenError')
    await assert.rejects(proposeEvaluation(pool, services, work, evaluationAction), error => error.name === 'ForbiddenError')
    await db.exec(restore)
  }
  const preflightPermission = services.permissionService
  services.permissionService = { assertCan: async request => {
    await preflightPermission.assertCan(request)
    await db.exec("UPDATE project_memberships SET status='INACTIVE'")
  } }
  await assert.rejects(recordAttempt(pool, services, work, action, 2), error => error.name === 'ForbiddenError')
  await db.exec("UPDATE project_memberships SET status='ACTIVE'")
  await assert.rejects(proposeEvaluation(pool, services, work, evaluationAction), error => error.name === 'ForbiddenError')
  await db.exec("UPDATE project_memberships SET status='ACTIVE'")
  for (const read of [{ action: 'learning.list_attempts', args: {} }, { action: 'learning.get_attempt', args: { attemptId: authorized.id } }]) {
    await assert.rejects(readAttempts(pool, services, work, read), error => error.name === 'ForbiddenError')
    await db.exec("UPDATE project_memberships SET status='ACTIVE'")
  }
  services.permissionService = preflightPermission
  assert.deepEqual(await counts(), beforeDenied)
  if (pg) {
    const nativeProposal = services.learning.proposeLearningEvaluation
    let entered, release
    const authorizedWrite = new Promise(resolve => { entered = resolve })
    const hold = new Promise(resolve => { release = resolve })
    services.learning.proposeLearningEvaluation = async (...args) => { entered(); await hold; return nativeProposal(...args) }
    const pendingWrite = proposeEvaluation(pool, services, work, evaluationAction)
    const revoker = await pg.connect()
    let revocation
    try {
      await Promise.race([authorizedWrite, pendingWrite.then(() => { throw new Error('native proposal was not reached') })])
      const pid = (await revoker.query('SELECT pg_backend_pid() AS pid')).rows[0].pid
      revocation = revoker.query("UPDATE project_memberships SET status='INACTIVE'").then(() => null, error => error)
      const deadline = Date.now() + 5000
      let blocked = false
      while (Date.now() < deadline) {
        blocked = (await pg.query('SELECT cardinality(pg_blocking_pids($1)) > 0 AS blocked', [pid])).rows[0].blocked
        if (blocked) break
        await new Promise(resolve => setTimeout(resolve, 25))
      }
      assert.equal(blocked, true, 'revocation must wait for the native permission dependency locks')
      release()
      assert.equal((await pendingWrite).status, 'ACCEPTED')
      assert.equal(await revocation, null)
      services.learning.proposeLearningEvaluation = nativeProposal
      await assert.rejects(proposeEvaluation(pool, services, work, evaluationAction), error => error.name === 'ForbiddenError')
      console.log('PostgreSQL multi-connection check passed: revocation blocked until the authorized write committed, then subsequent writes were denied.')
    } finally {
      release()
      await pendingWrite.catch(() => {})
      if (revocation) await revocation
      revoker.release()
      services.learning.proposeLearningEvaluation = nativeProposal
    }
  }
  console.log(`Native learning submission, evaluation, evidence/state SQL and actual access policy/repository passed through package adapters on ${pg ? 'PostgreSQL' : 'PGlite'} (minimal relational schema; transport remains a fixture).`)
} finally {
  await db.close()
}
