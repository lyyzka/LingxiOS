import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { createRequire } from 'node:module'
import { resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import ts from 'typescript'
import { PGlite } from '@electric-sql/pglite'
import { executeDocumentContent, documentOperations, flushDocumentEvents } from '../dist/src/integrations/lingxiloop/document-content.js'
import { requestDocumentApproval, approveDocument } from '../dist/src/integrations/lingxiloop/document-approvals.js'

const source = resolve(process.env.LINGXILOOP_SOURCE ?? fileURLToPath(new URL('../../LingxiLoop/server/src', import.meta.url)))
const nativeRequire = createRequire(resolve(source, '../package.json'))
const modules = new Map()
async function loadNative(file) {
  if (modules.has(file)) return modules.get(file)
  let code = ts.transpileModule(await readFile(file, 'utf8'), { compilerOptions: { target: ts.ScriptTarget.ES2023, module: ts.ModuleKind.ESNext } }).outputText
  for (const match of [...code.matchAll(/from ['"]([^'"]+)['"]/g)]) {
    const dependency = match[1]
    const url = dependency.startsWith('.') ? await loadNative(resolve(file, '..', dependency.replace(/\.js$/, '.ts')))
      : dependency.startsWith('node:') ? dependency : pathToFileURL(nativeRequire.resolve(dependency)).href
    code = code.replace(match[0], 'from ' + JSON.stringify(url))
  }
  const url = 'data:text/javascript;base64,' + Buffer.from(code).toString('base64')
  modules.set(file, url)
  return url
}
const { DocumentsApplication } = await import(await loadNative(resolve(source, 'modules/documents/application.ts')))
const { createDocumentCollaborationApplication } = await import(await loadNative(resolve(source, 'modules/documents/collaboration-application.ts')))
const { createPermissionService } = await import(await loadNative(resolve(source, 'modules/access/public.ts')))
const connectionString = process.env.LINGXIOS_DOCUMENT_CONTENT_TEST_DATABASE_URL
const postgres = connectionString ? new (nativeRequire('pg').Pool)({ connectionString, max: 5, connectionTimeoutMillis: 5000 }) : undefined
const connection = await postgres?.connect()
const db = connection ? { query: (...args) => connection.query(...args), exec: sql => connection.query(sql), close: async () => { connection.release(); await postgres.end() } } : new PGlite()
try {
  assert.equal((await db.query("SELECT tablename FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema')")).rows.length, 0, 'requires an empty disposable database')
  await db.exec(await readFile(new URL('../db/schema.sql', import.meta.url), 'utf8'))
  const baseline = await readFile(resolve(source, 'db/migrations/0001_v1_baseline.sql'), 'utf8')
  for (const name of ['documents', 'document_snapshots', 'document_updates', 'approvals', 'participants']) {
    const definition = baseline.match(new RegExp(`CREATE TABLE public\\.${name} \\([\\s\\S]*?\\n\\);`))?.[0]
    assert.ok(definition, name)
    await db.exec(definition)
  }
  await db.exec(`
    CREATE SEQUENCE document_updates_id_seq;
    ALTER TABLE document_updates ALTER COLUMN id SET DEFAULT nextval('document_updates_id_seq');
    ALTER TABLE documents ADD PRIMARY KEY(id);
    ALTER TABLE document_updates ADD FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE;
    ALTER TABLE document_snapshots ADD FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE;
    ALTER TABLE approvals ADD PRIMARY KEY(id);
    CREATE UNIQUE INDEX approval_key ON approvals(idempotency_key);
    ALTER TABLE document_snapshots ADD PRIMARY KEY(document_id);
    CREATE TABLE conversations(id text PRIMARY KEY,company_id text,project_id text,members jsonb,leader_id text);
    INSERT INTO conversations VALUES('room','t','p','["human","agent"]','human');
    CREATE TABLE users(id text PRIMARY KEY,email text,email_verified_at timestamptz,deleted_at timestamptz,suspended_at timestamptz);
    INSERT INTO users(id) VALUES('human');
    CREATE TABLE companies(id text PRIMARY KEY,type text,status text,plan_id text);
    INSERT INTO companies VALUES('t','PERSONAL','ACTIVE','plan');
    CREATE TABLE projects(id text PRIMARY KEY,company_id text,kind text,plan_id text,status text,created_by text);
    INSERT INTO projects VALUES('p','t','PERSONAL_LEARNING',NULL,'ACTIVE','human');
    CREATE TABLE company_memberships(company_id text,user_id text,role text,status text);
    INSERT INTO company_memberships VALUES('t','human','OWNER','ACTIVE');
    CREATE TABLE project_memberships(company_id text,project_id text,user_id text,role text,status text);
    INSERT INTO project_memberships VALUES('t','p','human','OWNER','ACTIVE');
    CREATE TABLE plans(id text PRIMARY KEY,code text,status text);
    INSERT INTO plans VALUES('plan','test','ACTIVE');
    CREATE TABLE entitlements(id text PRIMARY KEY,code text);
    INSERT INTO entitlements VALUES('conversation','conversation.core'),('agent','agent.core');
    CREATE TABLE plan_entitlements(plan_id text,entitlement_id text,value jsonb);
    INSERT INTO plan_entitlements VALUES('plan','conversation','true'),('plan','agent','true');
    CREATE TABLE learning_project_teacher_agents(company_id text,agent_id text);
    CREATE TABLE im_channel_bindings(company_id text,channel_id text,profile jsonb);
    INSERT INTO im_channel_bindings VALUES('t','room','{"members":["human","agent"]}');
    INSERT INTO participants(id,company_id,kind,name,initial,avatar_bg,status,capabilities)
      VALUES('agent','t','agent','Agent','A','blue','available','["documents"]');
    INSERT INTO lingxios.agent_work_items(id,tenant_id,agent_id,session_id,principal_id,kind,lane,trigger_ref,fence,status,lease_expires_at)
      VALUES('work','t','agent','room','human','turn','interactive','message',1,'leased',NOW()+INTERVAL '1 hour');
  `)
  let failReceipt = false, failResume = false
  const query = async (sql, params) => {
    if (failReceipt && /(?:INSERT INTO|UPDATE) lingxios\.agent_action_ledger/.test(sql)) { failReceipt = false; throw new Error('injected receipt failure') }
    if (failResume && sql.includes('UPDATE approvals SET resumed_at')) { failResume = false; throw new Error('injected resume failure') }
    const value = await db.query(sql, params)
    return { ...value, rowCount: value.rowCount ?? value.affectedRows ?? value.rows.length }
  }
  const pool = { query, connect: async () => ({ query, release() {} }) }
  const published = []
  const publish = async (channel, event) => { published.push({ channel, event }) }
  const services = { permissionService: createPermissionService(pool), documents: { writes: {
    createPermissionService, CH_DOCS: 'docs', publish,
    content: { DocumentsApplication, createDocumentCollaborationApplication, normalizeStorageKey: () => null,
      storageKeyFromPublicUrl: () => null, signedUrlExpiresSoon: () => false, storage: { publicUrl: async () => { throw new Error('unused test storage') } },
      CH_DOC_UPDATE: 'updates', publish },
  } } }
  const work = { id: 'work', tenantId: 't', agentId: 'agent', principalId: 'human', sessionId: 'room', fence: 1, homeEpoch: 1, kind: 'turn', lane: 'interactive', triggerRef: 'message' }
  let index = 0
  const invoke = async (method, args) => {
    const key = 'action-' + index++
    const action = { runId: work.id, cellId: key, callIndex: 0, idempotencyKey: key, action: 'documents.' + method, args }
    await db.query('INSERT INTO lingxios.agent_action_intents(idempotency_key,fingerprint,intent) VALUES($1,$1,$2::jsonb)',
      [key, JSON.stringify({ workId: work.id, tenantId: 't', agentId: 'agent', principalId: 'human', sessionId: 'room', requestVersion: 1, action })])
    return executeDocumentContent(pool, services, work, action)
  }
  const created = await invoke('create', { title: 'Lesson', body: 'Original paragraph.' })
  assert.equal(created.result.document.createdBy, 'agent')
  assert.equal(published.length, 0, 'transaction never publishes uncommitted data')
  const first = await invoke('read', { documentId: created.documentId })
  assert.match(first.body, /Original paragraph\./)
  const edit = { documentId: created.documentId, expectedRevision: first.revision, operations: [{ kind: 'replace', find: 'Original', replace: 'Updated' }, { kind: 'append', text: 'Second paragraph.' }] }
  failReceipt = true
  await assert.rejects(invoke('edit', edit), /injected receipt failure/)
  assert.equal((await invoke('read', { documentId: created.documentId })).body, first.body, 'native Yjs update rolls back with receipt failure')
  const edited = await invoke('edit', edit)
  assert.equal(edited.result.replaced, 1)
  const current = await invoke('read', { documentId: created.documentId })
  assert.match(current.body, /Updated paragraph\./)
  assert.match(current.body, /Second paragraph\./)
  await assert.rejects(invoke('edit', edit), /document changed/)
  await db.exec("UPDATE project_memberships SET status='INACTIVE'")
  await assert.rejects(invoke('edit', { ...edit, expectedRevision: current.revision }), error => error.reason === 'PROJECT_MEMBERSHIP_INACTIVE')
  await db.exec("UPDATE project_memberships SET status='ACTIVE'")
  await assert.rejects(invoke('read', { documentId: 'foreign' }), /outside this project/)
  for (const operations of [[{ kind: 'replace', find: '', replace: 'x' }], [{ kind: 'image', src: 'javascript:alert(1)', alt: null, placement: { mode: 'end' } }], [{ kind: 'append', text: 'x', userId: 'foreign' }]]) assert.throws(() => documentOperations(operations))

  const action = { runId: work.id, cellId: 'delete', callIndex: 0, idempotencyKey: 'delete', action: 'documents.delete', args: { documentId: created.documentId, expectedRevision: current.revision } }
  const intent = { workId: work.id, tenantId: 't', agentId: 'agent', principalId: 'human', sessionId: 'room', threadId: null, requestVersion: 1, action }
  const request = { version: 1, workId: work.id, tenantId: 't', sessionId: 'room', authorId: 'human', sourceRef: 'message', originalText: 'Delete this document.', revisions: [], attachments: [], evidence: { id: 'empty', items: [] } }
  await db.query('INSERT INTO lingxios.agent_action_intents(idempotency_key,fingerprint,intent) VALUES($1,$1,$2::jsonb)', ['delete', JSON.stringify(intent)])
  await db.query('INSERT INTO lingxios.agent_os_sessions(session_key,tenant_id,agent_id,session_id,request_snapshot) VALUES($1,$2,$3,$4,$5::jsonb)', [JSON.stringify(['t', 'agent', 'room', null]), 't', 'agent', 'room', JSON.stringify(request)])
  const pending = await requestDocumentApproval(pool, services, work, action)
  await db.query('INSERT INTO lingxios.agent_action_ledger(idempotency_key,result) VALUES($1,$2::jsonb)', ['delete', JSON.stringify(pending)])
  await db.query("UPDATE lingxios.agent_work_items SET status='completed',goal_outcome=$1::jsonb WHERE id='work'", [JSON.stringify({ status: 'awaiting_approval', verification: 'not_run', requestVersion: 1, approvalId: pending.approval.id })])
  const decision = { companyId: 't', userId: 'human', approvalId: pending.approval.id }
  await db.exec("UPDATE users SET suspended_at=NOW() WHERE id='human'")
  await assert.rejects(approveDocument(pool, services, decision), error => error.reason === 'ACTOR_INACTIVE')
  await db.exec("UPDATE users SET suspended_at=NULL WHERE id='human'")
  failReceipt = true
  await assert.rejects(approveDocument(pool, services, decision), /injected receipt failure/)
  assert.equal((await db.query('SELECT id FROM documents')).rows.length, 1)
  failResume = true
  await assert.rejects(approveDocument(pool, services, decision), /injected resume failure/)
  assert.equal((await db.query('SELECT id FROM documents')).rows.length, 0)
  assert.equal((await db.query('SELECT id FROM document_updates')).rows.length, 0)
  assert.equal((await approveDocument(pool, services, decision)).status, 'resumed')
  assert.equal((await approveDocument(pool, services, decision)).status, 'already_resumed')
  const count = (await db.query('SELECT id FROM lingxios.agent_document_outbox')).rows.length
  for (let i = 0; i < count; i++) await flushDocumentEvents(pool, services)
  assert.ok(published.some(item => item.event.type === 'doc.update' && item.event.authorId === 'agent'))
  assert.ok(published.some(item => item.event.kind === 'document.deleted'))
  assert.equal((await db.query('SELECT id FROM lingxios.agent_document_outbox WHERE delivered_at IS NULL')).rows.length, 0)
  console.log('Native document create/edit/read/delete, Yjs persistence, permission revocation, atomic receipts, approval recovery and durable publication passed.')
} finally { await db.close() }
