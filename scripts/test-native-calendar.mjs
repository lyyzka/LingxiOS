import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { createRequire } from 'node:module'
import { resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import ts from 'typescript'
import { PGlite } from '@electric-sql/pglite'
import { updateCalendar, flushCalendarEvents } from '../dist/src/integrations/lingxiloop/calendar-writes.js'
import { requestCalendarApproval, approveCalendar } from '../dist/src/integrations/lingxiloop/calendar-approvals.js'
import { executeCalendar } from '../dist/src/integrations/lingxiloop/calendar.js'

const source = resolve(process.argv[2] ?? process.env.LINGXILOOP_SOURCE ?? fileURLToPath(new URL('../../LingxiLoop/server/src', import.meta.url)))
const nativeRequire = createRequire(resolve(source, '../package.json'))
const transpile = input => ts.transpileModule(input, { compilerOptions: { target: ts.ScriptTarget.ES2023, module: ts.ModuleKind.ESNext } }).outputText
const asModule = code => 'data:text/javascript;base64,' + Buffer.from(code).toString('base64')
const repository = asModule(transpile(await readFile(resolve(source, 'modules/calendar/repository.ts'), 'utf8')))
const application = transpile(await readFile(resolve(source, 'modules/calendar/application.ts'), 'utf8')).replace("'./repository.js'", JSON.stringify(repository))
const { CalendarApplication } = await import(asModule(application))
const schemas = transpile(await readFile(resolve(source, 'modules/calendar/contracts.ts'), 'utf8')).replace("'zod'", JSON.stringify(pathToFileURL(nativeRequire.resolve('zod')).href))
const { listCalendarEventsQuerySchema, updateCalendarEventRequestSchema, createCalendarEventRequestSchema } = await import(asModule(schemas))
const connectionString = process.env.LINGXIOS_CALENDAR_TEST_DATABASE_URL
const postgres = connectionString ? new (nativeRequire('pg').Pool)({ connectionString, max: 5, connectionTimeoutMillis: 5000 }) : undefined
const connection = await postgres?.connect()
const db = connection ? { query: (sql, params) => connection.query(sql, params), exec: sql => connection.query(sql), close: async () => { connection.release(); await postgres.end() } } : new PGlite()
const existing = await db.query("SELECT tablename FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema')")
if (existing.rows.length) { await db.close(); throw new Error('calendar test requires an empty disposable database') }
try {
  const baseline = await readFile(resolve(source, 'db/migrations/0001_v1_baseline.sql'), 'utf8')
  const definition = baseline.match(/CREATE TABLE public\.calendar_events \([\s\S]*?\n\);/)?.[0]
  assert.ok(definition)
  await db.exec(await readFile(new URL('../db/schema.sql', import.meta.url), 'utf8'))
  await db.exec(definition)
  const participantsDefinition = baseline.match(/CREATE TABLE public\.participants \([\s\S]*?\n\);/)?.[0]
  assert.ok(participantsDefinition)
  await db.exec(participantsDefinition)
  await db.exec(`
    CREATE TABLE conversations(id text,company_id text,project_id text);
    INSERT INTO conversations VALUES('room','t','p');
    CREATE TABLE company_memberships(company_id text,user_id text,status text,role text);
    INSERT INTO company_memberships VALUES('t','human','ACTIVE','MEMBER');
    INSERT INTO participants(id,company_id,kind,name,initial,avatar_bg,status,capabilities)
      VALUES('agent','t','agent','Agent','A','blue','available','["calendar"]'::jsonb);
    CREATE TABLE learning_project_teacher_agents(company_id text,agent_id text);
    CREATE TABLE im_channel_bindings(company_id text,channel_id text,profile jsonb);
    INSERT INTO im_channel_bindings VALUES('t','room','{"members":["human","agent"]}');
    INSERT INTO lingxios.agent_work_items(id,tenant_id,agent_id,session_id,principal_id,kind,lane,trigger_ref,fence,status,lease_expires_at) VALUES('work','t','agent','room','human','turn','interactive','message',1,'leased',NOW()+INTERVAL '1 hour');
    INSERT INTO calendar_events(id,company_id,project_id,created_by,title,start_at,is_private) VALUES
      ('shared','t','p','other','Shared','2026-09-06',false),
      ('mine','t','p','human','Mine','2026-09-07',true),
      ('private','t','p','other','Private','2026-09-08',true),
      ('foreign','other','p','human','Foreign','2026-09-09',false),
      ('other-project','t','other','human','Other project','2026-09-10',false);
    INSERT INTO calendar_events(id,company_id,project_id,created_by,title,start_at,recurrence) VALUES
      ('recurring','t','p','other','Daily','2026-08-01','{"freq":"daily","interval":1}');
  `)
  let failReceipt = false
  let failResume = false
  let denyTarget = false
  let revoked = false
  let revokeDuringRead = false
  const queryable = { query: async (sql, args) => {
    if (failReceipt && sql.includes('UPDATE lingxios.agent_action_ledger')) { failReceipt = false; throw new Error('injected receipt failure') }
    if (failResume && sql.includes('UPDATE approvals SET resumed_at')) { failResume = false; throw new Error('injected continuation failure') }
    const result = await db.query(sql, args)
    if (revokeDuringRead && sql.includes('FROM calendar_events')) revoked = true
    return { rows: result.rows, rowCount: result.rowCount ?? result.affectedRows ?? result.rows.length }
  } }
  const unused = async () => { throw new Error('reads must not publish or dispatch') }
  const services = { calendar: { calendarApplication: new CalendarApplication(queryable, { publish: unused }, { dispatch: unused }), listCalendarEventsQuerySchema },
    permissionService: { assertCan: async request => {
      assert.equal(request.actorUserId, 'human')
      assert.equal(request.companyId, 't')
      if (request.action === 'calendar:read') assert.equal(request.projectId, 'p')
      if (denyTarget && request.action === 'conversation:write') throw new Error('target denied')
      if (revoked) throw new Error('revoked')
    } },
  }
  const work = { fence: 1, kind: 'turn', lane: 'interactive', triggerRef: 'message', homeEpoch: 1, id: 'work', tenantId: 't', principalId: 'human', agentId: 'agent', sessionId: 'room' }
  const action = { action: 'calendar.list', args: { from: '2026-09-01T00:00:00Z', to: '2026-09-30T00:00:00Z' } }
  assert.deepEqual((await executeCalendar(queryable, services, work, action)).events.map(event => event.id), ['recurring', 'shared', 'mine'])
  const get = { action: 'calendar.get', args: { eventId: 'mine' } }
  assert.equal((await executeCalendar(queryable, services, work, get)).title, 'Mine')
  for (const eventId of ['private', 'foreign', 'other-project']) await assert.rejects(executeCalendar(queryable, services, work, { ...get, args: { eventId } }), error => error.code === 'event_not_found')
  for (const args of [{}, { ...action.args, projectId: 'foreign' }, { ...action.args, from: 'invalid' }, { ...action.args, to: '2028-01-01' }]) await assert.rejects(executeCalendar(queryable, services, work, { ...action, args }))
  await assert.rejects(executeCalendar(queryable, services, { ...work, principalId: undefined }, action), /original human/)
  await db.exec("INSERT INTO calendar_events(id,company_id,project_id,created_by,title,start_at) SELECT 'many-'||n,'t','p','human','Many','2026-09-20'::timestamptz FROM generate_series(1,101) n")
  const listed = await executeCalendar(queryable, services, work, action)
  assert.equal(listed.events.length, 100)
  assert.equal(listed.truncated, true)
  revokeDuringRead = true
  await assert.rejects(executeCalendar(queryable, services, work, get), /revoked/)
  revoked = false
  revokeDuringRead = false
  const pool = { ...queryable, connect: async () => ({ query: queryable.query, release() {} }) }
  let published
  services.calendar.writes = {
    CalendarApplication, updateCalendarEventRequestSchema, createCalendarEventRequestSchema,
    createPermissionService: (client, options) => { assert.equal(client.query, queryable.query); assert.deepEqual(options, { lockDependencies: true }); return services.permissionService },
    CH_CALENDAR_EVENTS: 'calendar', publish: async (channel, event) => {
      assert.equal(channel, 'calendar')
      assert.equal((await db.query("SELECT title FROM calendar_events WHERE id='mine'")).rows[0].title, 'Updated')
      published = event
      throw new Error('notification transport failed')
    },
  }
  const expected = await executeCalendar(queryable, services, work, get)
  const update = { action: 'calendar.update', args: { eventId: 'mine', expected, patch: { title: 'Updated' } } }
  let updateIndex = 0
  const applyUpdate = async action => {
    const idempotencyKey = 'update-' + updateIndex++
    const invocation = { ...action, runId: work.id, cellId: idempotencyKey, callIndex: 0, idempotencyKey }
    await db.query('INSERT INTO lingxios.agent_action_intents(idempotency_key,fingerprint,intent) VALUES($1,$1,$2::jsonb)', [idempotencyKey, JSON.stringify({ workId: work.id, tenantId: 't', principalId: 'human', requestVersion: 1, action: invocation })])
    return updateCalendar(pool, services, work, invocation)
  }
  const result = await applyUpdate(update)
  assert.equal(result.event.title, 'Updated')
  assert.equal(result.notification, 'queued')
  assert.equal(published, undefined)
  await flushCalendarEvents(pool, services)
  assert.equal((await db.query('SELECT delivered_at FROM lingxios.agent_calendar_outbox')).rows[0].delivered_at, null)
  assert.deepEqual(published, { type: 'calendar.changed', kind: 'event.updated', eventId: 'mine', companyId: 't', workspaceId: 'p', actorId: 'agent' })
  await assert.rejects(applyUpdate(update), /event changed/)
  const refreshed = await executeCalendar(queryable, services, work, get)
  await assert.rejects(applyUpdate({ ...update, args: { ...update.args, expected: refreshed, patch: { endAt: '2020-01-01' } } }), /endAt/)
  await assert.rejects(applyUpdate({ ...update, args: { ...update.args, expected: refreshed, patch: { title: '' } } }))
  denyTarget = true
  await assert.rejects(applyUpdate({ ...update, args: { ...update.args, expected: refreshed, patch: { targetConversationId: 'room' } } }), /target denied/)
  denyTarget = false
  revoked = true
  await assert.rejects(applyUpdate({ ...update, args: { ...update.args, expected: refreshed } }), /revoked/)
  assert.equal((await db.query("SELECT title FROM calendar_events WHERE id='mine'")).rows[0].title, 'Updated')
  revoked = false
  const approvalDefinition = baseline.match(/CREATE TABLE public\.approvals \([\s\S]*?\n\);/)?.[0]
  assert.ok(approvalDefinition)
  await db.exec(approvalDefinition)
  await db.exec('ALTER TABLE approvals ADD PRIMARY KEY(id); CREATE UNIQUE INDEX calendar_approval_key ON approvals(idempotency_key)')
  let approvalIndex = 0
  const sessionKey = JSON.stringify(['t', 'agent', 'room', null])
  const pending = async (method, args) => {
    const id = 'approval-work-' + approvalIndex++
    const requestWork = { ...work, id }
    const invocation = { action: 'calendar.' + method, args, runId: id, cellId: id, callIndex: 0, idempotencyKey: id }
    const intent = { workId: id, tenantId: 't', agentId: 'agent', principalId: 'human', sessionId: 'room', threadId: null, requestVersion: 1, action: invocation }
    const request = { version: 1, workId: id, tenantId: 't', sessionId: 'room', authorId: 'human', sourceRef: 'message', originalText: 'Please ' + method + ' this event', revisions: [], attachments: [], evidence: { id: 'empty', items: [] } }
    await db.query(`INSERT INTO lingxios.agent_work_items(id,tenant_id,agent_id,session_id,principal_id,kind,lane,trigger_ref,fence,status,lease_expires_at)
      VALUES($1,'t','agent','room','human','turn','interactive','message',1,'leased',NOW()+INTERVAL '1 hour')`, [id])
    await db.query('INSERT INTO lingxios.agent_action_intents(idempotency_key,fingerprint,intent) VALUES($1,$1,$2::jsonb)', [id, JSON.stringify(intent)])
    await db.query(`INSERT INTO lingxios.agent_os_sessions(session_key,tenant_id,agent_id,session_id,request_snapshot)
      VALUES($1,'t','agent','room',$2::jsonb) ON CONFLICT(session_key) DO UPDATE SET request_snapshot=EXCLUDED.request_snapshot,history='[]'::jsonb`, [sessionKey, JSON.stringify(request)])
    const receipt = await requestCalendarApproval(pool, services, requestWork, invocation)
    assert.ok(receipt.approval?.id)
    await db.query('INSERT INTO lingxios.agent_action_ledger(idempotency_key,result) VALUES($1,$2::jsonb)', [id, JSON.stringify(receipt)])
    await db.query(`UPDATE lingxios.agent_work_items SET status='completed',goal_outcome=$2::jsonb WHERE id=$1`, [id,
      JSON.stringify({ status: 'awaiting_approval', verification: 'not_run', requestVersion: 1, approvalId: receipt.approval.id })])
    return { input: { companyId: 't', userId: 'human', approvalId: receipt.approval.id }, workId: id }
  }
  const createArgs = { title: 'Approved event', startAt: '2026-09-12T10:00:00Z', isPrivate: true }
  const creation = await pending('create', createArgs)
  assert.equal((await db.query("SELECT id FROM calendar_events WHERE title='Approved event'")).rows.length, 0)
  await db.exec("UPDATE participants SET capabilities='[]'::jsonb WHERE id='agent'")
  await assert.rejects(approveCalendar(pool, services, creation.input), /capability/)
  await db.exec(`UPDATE participants SET capabilities='["calendar"]'::jsonb WHERE id='agent'`)
  revoked = true
  await assert.rejects(approveCalendar(pool, services, creation.input), /revoked/)
  revoked = false
  failReceipt = true
  await assert.rejects(approveCalendar(pool, services, creation.input), /injected receipt failure/)
  assert.equal((await db.query("SELECT id FROM calendar_events WHERE title='Approved event'")).rows.length, 0)
  assert.equal((await db.query('SELECT id FROM lingxios.agent_calendar_outbox WHERE work_id=$1', [creation.workId])).rows.length, 0)
  assert.equal((await db.query('SELECT status FROM approvals WHERE id=$1', [creation.input.approvalId])).rows[0].status, 'PENDING')
  failResume = true
  await assert.rejects(approveCalendar(pool, services, creation.input), /injected continuation failure/)
  assert.equal((await db.query('SELECT status FROM approvals WHERE id=$1', [creation.input.approvalId])).rows[0].status, 'EXECUTED')
  const created = (await db.query("SELECT id,created_by FROM calendar_events WHERE title='Approved event'")).rows
  assert.equal(created.length, 1)
  assert.equal(created[0].created_by, 'human')
  assert.equal((await approveCalendar(pool, services, creation.input)).status, 'resumed')
  assert.equal((await approveCalendar(pool, services, creation.input)).status, 'already_resumed')
  assert.equal((await db.query("SELECT id FROM calendar_events WHERE title='Approved event'")).rows.length, 1)
  const observed = await services.calendar.calendarApplication.get({ companyId: 't', projectId: 'p', userId: 'human' }, created[0].id)
  const deletion = await pending('delete', { eventId: observed.id, expected: observed })
  await db.query('UPDATE calendar_events SET title=$2 WHERE id=$1', [observed.id, 'Changed after approval'])
  await assert.rejects(approveCalendar(pool, services, deletion.input), /event changed/)
  assert.equal((await db.query('SELECT id FROM calendar_events WHERE id=$1', [observed.id])).rows.length, 1)
  await db.query('UPDATE calendar_events SET title=$2 WHERE id=$1', [observed.id, observed.title])
  await db.query("UPDATE lingxios.agent_work_items SET cancel_requested_at=NOW() WHERE id=$1", [deletion.workId])
  await assert.rejects(approveCalendar(pool, services, deletion.input), /expired or changed/)
  await db.query('UPDATE lingxios.agent_work_items SET cancel_requested_at=NULL WHERE id=$1', [deletion.workId])
  failReceipt = true
  await assert.rejects(approveCalendar(pool, services, deletion.input), /injected receipt failure/)
  assert.equal((await db.query('SELECT id FROM calendar_events WHERE id=$1', [observed.id])).rows.length, 1)
  assert.equal((await approveCalendar(pool, services, deletion.input)).status, 'resumed')
  assert.equal((await approveCalendar(pool, services, deletion.input)).status, 'already_resumed')
  assert.equal((await db.query('SELECT id FROM calendar_events WHERE id=$1', [observed.id])).rows.length, 0)
  assert.equal((await db.query('SELECT id FROM lingxios.agent_calendar_outbox WHERE work_id=$1', [deletion.workId])).rows.length, 1)
  const publishedEvents = []
  services.calendar.writes.publish = async (channel, event) => { assert.equal(channel, 'calendar'); publishedEvents.push(event) }
  await db.exec('UPDATE lingxios.agent_calendar_outbox SET available_at=NOW()')
  for (let i = 0; i < 4; i++) await flushCalendarEvents(pool, services)
  assert.equal(publishedEvents.length, 3)
  assert.deepEqual(publishedEvents.map(event => event.kind).sort(), ['event.created', 'event.deleted', 'event.updated'])
  assert.equal((await db.query('SELECT id FROM lingxios.agent_calendar_outbox WHERE delivered_at IS NULL')).rows.length, 0)
  const expired = await pending('create', createArgs)
  await db.query("UPDATE approvals SET expires_at=NOW()-INTERVAL '1 minute' WHERE id=$1", [expired.input.approvalId])
  await assert.rejects(approveCalendar(pool, services, expired.input), /expired or changed/)
  const revised = await pending('create', createArgs)
  await db.query(`UPDATE lingxios.agent_work_items SET steer_inputs='[{"id":"revision","text":"Cancel this creation","createdAt":"2026-09-06T10:00:00Z"}]'::jsonb WHERE id=$1`, [revised.workId])
  await assert.rejects(approveCalendar(pool, services, revised.input), /current request/)
  assert.equal((await db.query("SELECT id FROM calendar_events WHERE title='Approved event'")).rows.length, 0)
  // A non-abortable native Redis command must not block task claiming or accumulate commands.
  await db.query('UPDATE lingxios.agent_calendar_outbox SET delivered_at=NULL,available_at=NOW() WHERE work_id=$1', [creation.workId])
  let releasePublication, startedPublication
  const publicationStarted = new Promise(resolve => { startedPublication = resolve })
  const blockedPublication = new Promise(resolve => { releasePublication = resolve })
  let outstandingCalls = 0
  services.calendar.writes.publish = async () => { outstandingCalls++; startedPublication(); await blockedPublication }
  const draining = flushCalendarEvents(pool, services)
  await publicationStarted
  await flushCalendarEvents(pool, services)
  await draining
  assert.equal(outstandingCalls, 1)
  await flushCalendarEvents(pool, services)
  assert.equal(outstandingCalls, 1)
  releasePublication()
  const deadline = Date.now() + 1000
  while ((await db.query('SELECT id FROM lingxios.agent_calendar_outbox WHERE delivered_at IS NULL')).rows.length && Date.now() < deadline) await new Promise(resolve => setImmediate(resolve))
  assert.equal((await db.query('SELECT id FROM lingxios.agent_calendar_outbox WHERE delivered_at IS NULL')).rows.length, 0)

  const nativeModules = new Map()
  async function nativeModule(file) {
    if (nativeModules.has(file)) return nativeModules.get(file)
    let code = transpile(await readFile(file, 'utf8'))
    for (const match of [...code.matchAll(/from ['"]([^'"]+)['"]/g)]) {
      assert.ok(match[1].startsWith('.'), 'unexpected native authorization dependency')
      code = code.replace(match[0], 'from ' + JSON.stringify(await nativeModule(resolve(file, '..', match[1].replace(/\.js$/, '.ts')))))
    }
    const url = asModule(code)
    nativeModules.set(file, url)
    return url
  }
  const { createPermissionService } = await import(await nativeModule(resolve(source, 'modules/access/public.ts')))
  await db.exec(`
    ALTER TABLE conversations ADD COLUMN members jsonb DEFAULT '["human"]', ADD COLUMN leader_id text;
    CREATE TABLE users(id text PRIMARY KEY,email text,email_verified_at timestamptz,deleted_at timestamptz,suspended_at timestamptz);
    INSERT INTO users(id) VALUES('human');
    CREATE TABLE companies(id text PRIMARY KEY,type text,status text,plan_id text);
    INSERT INTO companies VALUES('t','PERSONAL','ACTIVE','plan');
    CREATE TABLE projects(id text PRIMARY KEY,company_id text,kind text,plan_id text,status text,created_by text);
    INSERT INTO projects VALUES('p','t','PERSONAL_LEARNING',NULL,'ACTIVE','human');
    UPDATE company_memberships SET role='OWNER';
    CREATE TABLE project_memberships(company_id text,project_id text,user_id text,role text,status text);
    INSERT INTO project_memberships VALUES('t','p','human','OWNER','ACTIVE');
    CREATE TABLE plans(id text PRIMARY KEY,code text,status text);
    INSERT INTO plans VALUES('plan','test','ACTIVE');
    CREATE TABLE entitlements(id text PRIMARY KEY,code text);
    INSERT INTO entitlements VALUES('conversation','conversation.core'),('agent','agent.core');
    CREATE TABLE plan_entitlements(plan_id text,entitlement_id text,value jsonb);
    INSERT INTO plan_entitlements VALUES('plan','conversation','true'),('plan','agent','true');
  `)
  services.calendar.writes.createPermissionService = createPermissionService
  services.permissionService = createPermissionService(queryable)
  const nativeApproval = await pending('create', createArgs)
  for (const [revoke, restore, reason] of [
    ["UPDATE project_memberships SET status='INACTIVE'", "UPDATE project_memberships SET status='ACTIVE'", 'PROJECT_MEMBERSHIP_INACTIVE'],
    ["UPDATE conversations SET members='[]'", "UPDATE conversations SET members='[\"human\"]'", 'RESOURCE_MEMBERSHIP_REQUIRED'],
    ["UPDATE plan_entitlements SET value='false'", "UPDATE plan_entitlements SET value='true'", 'ENTITLEMENT_MISSING'],
    ["UPDATE project_memberships SET role='OBSERVER'", "UPDATE project_memberships SET role='OWNER'", 'ROLE_NOT_ALLOWED'],
    ["UPDATE users SET suspended_at=NOW()", "UPDATE users SET suspended_at=NULL", 'ACTOR_INACTIVE'],
  ]) {
    await db.exec(revoke)
    await assert.rejects(approveCalendar(pool, services, nativeApproval.input), error => error.reason === reason)
    assert.equal((await db.query("SELECT id FROM calendar_events WHERE title='Approved event'")).rows.length, 0)
    await db.exec(restore)
  }
  assert.equal((await approveCalendar(pool, services, nativeApproval.input)).status, 'resumed')
  assert.equal((await db.query("SELECT id FROM calendar_events WHERE title='Approved event'")).rows.length, 1)
  if (postgres) {
    const concurrent = await pending('create', { ...createArgs, title: 'Concurrent approval' })
    const competing = await Promise.allSettled([approveCalendar(postgres, services, concurrent.input), approveCalendar(postgres, services, concurrent.input)])
    assert.ok(competing.some(result => result.status === 'fulfilled'))
    for (const result of competing) if (result.status === 'rejected') assert.match(result.reason.message, /expired or changed|not waiting/)
    assert.equal((await db.query("SELECT id FROM calendar_events WHERE title='Concurrent approval'")).rows.length, 1)
    assert.equal((await approveCalendar(postgres, services, concurrent.input)).status, 'already_resumed')
    const locked = await pending('create', { ...createArgs, title: 'Locked authorization' })
    const revoker = await postgres.connect()
    try {
      await revoker.query("SET lock_timeout='200ms'")
      services.calendar.writes.CalendarApplication = class extends CalendarApplication {
        async create(...args) {
          await assert.rejects(revoker.query("UPDATE project_memberships SET status='INACTIVE' WHERE user_id='human'"), error => error.code === '55P03')
          await assert.rejects(revoker.query("UPDATE participants SET capabilities='[]'::jsonb WHERE id='agent'"), error => error.code === '55P03')
          await assert.rejects(revoker.query("UPDATE im_channel_bindings SET profile='{}' WHERE channel_id='room'"), error => error.code === '55P03')
          return super.create(...args)
        }
      }
      assert.equal((await approveCalendar(postgres, services, locked.input)).status, 'resumed')
      assert.equal((await db.query("SELECT id FROM calendar_events WHERE title='Locked authorization'")).rows.length, 1)
    } finally { revoker.release(); services.calendar.writes.CalendarApplication = CalendarApplication }
  }
  console.log('Native calendar application/schema/SQL and native authorization passed: read/update scope, versioned create/delete approvals, revocation, expiry/cancellation/revision denial, transactional rollback, continuation recovery and durable bounded notifications. ' + (postgres ? 'PostgreSQL parallel approvals and authorization locks passed. ' : '') + 'Uses a minimal personal-project schema; educational seats and scheduler dispatch are separate checks.')
} finally { await db.close() }
