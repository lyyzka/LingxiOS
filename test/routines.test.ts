import assert from 'node:assert/strict'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import http from 'node:http'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { it } from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import type { SqlPool } from '../src/control-plane/pg-store.js'
import type { HostAction, WorkItem } from '../src/protocol/types.js'
import type { LingxiLoopServices } from '../src/integrations/lingxiloop/service-contracts.js'
import { approveRoutine, requestRoutineApproval } from '../src/integrations/lingxiloop/approvals.js'
import { assertRoutineWork, executeRoutine, nextRoutineRun, scheduleRoutines } from '../src/integrations/lingxiloop/routines.js'
import { createLingxiLoop } from '../src/integrations/lingxiloop/index.js'
import { LINGXILOOP_CAPABILITY_METHODS, LingxiLoopRuntimePolicy } from '../src/integrations/lingxiloop/policy.js'
import { startWorker } from '../src/worker/index.js'
import { setTimeout as delay } from 'node:timers/promises'

it('approves routines separately from activation, fences stale previews, and scopes durable scheduled work', async () => {
  const db = new PGlite()
  let failure: 'receipt' | 'enqueue' | undefined
  let denied: 'revoked' | 'transient' | undefined
  const database: SqlPool = { query: async (sql, params) => {
    if (failure === 'receipt' && sql.startsWith('UPDATE lingxios.agent_action_ledger')) throw new Error('injected receipt failure')
    if (failure === 'enqueue' && sql.startsWith('INSERT INTO lingxios.agent_routine_runs')) throw new Error('injected enqueue failure')
    const result = await db.query<Record<string, unknown>>(sql, params)
    return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
  }, connect: async () => ({ query: database.query, release() {} }) }
  const services: Pick<LingxiLoopServices, 'permissionService'> = { permissionService: { assertCan: async request => {
    if (request.action === 'agent_run:control') {
      assert.deepEqual(request.resource, { type: 'conversation', id: 's' })
      if (denied === 'transient') throw new Error('database unavailable')
      if (denied === 'revoked') throw Object.assign(new Error('revoked'), { name: 'ForbiddenError', status: 403 })
    }
  } } }
  const work: WorkItem = { id: 'w', tenantId: 't', agentId: 'a', sessionId: 's', threadId: 'thread', principalId: 'u', kind: 'turn', lane: 'interactive', triggerRef: 'm', fence: 1, homeEpoch: 1, leaseToken: 'token' }
  let serial = 0
  const action = (name: string, args: Record<string, unknown>): HostAction => ({ runId: work.id, cellId: 'c', callIndex: serial, idempotencyKey: `key-${++serial}`, action: `routines.${name}`, args })
  const create = { kind: 'summary', title: 'Daily summary', instructions: 'Summarize the current project', schedule: { time: '09:00' }, timezone: 'Asia/Shanghai' }
  const prepare = async (candidate: HostAction) => {
    await database.query("UPDATE lingxios.agent_work_items SET status='leased',goal_outcome=NULL WHERE id='w'")
    await database.query('INSERT INTO lingxios.agent_action_intents(idempotency_key,fingerprint,intent) VALUES($1,$2,$3)',
      [candidate.idempotencyKey, 'fingerprint', JSON.stringify({ workId: 'w', tenantId: 't', agentId: 'a', sessionId: 's', threadId: 'thread', principalId: 'u', requestVersion: 1, action: candidate })])
    const result = await requestRoutineApproval(database, services, work, candidate)
    assert.ok(result.approval)
    await database.query("UPDATE lingxios.agent_work_items SET status='waiting',goal_outcome=$1 WHERE id='w'", [JSON.stringify({ status: 'awaiting_approval', approvalId: result.approval.id, requestVersion: 1 })])
    await database.query('INSERT INTO lingxios.agent_action_ledger(idempotency_key,result) VALUES($1,$2)', [candidate.idempotencyKey, JSON.stringify(result)])
    return { companyId: 't', userId: 'reviewer', approvalId: result.approval.id }
  }
  const plans = async () => (await database.query('SELECT * FROM lingxios.agent_routines ORDER BY id')).rows
  const jobs = async () => (await database.query("SELECT * FROM lingxios.agent_work_items WHERE kind='routine' ORDER BY id")).rows
  const due = () => database.query("UPDATE lingxios.agent_routines SET next_run_at=NOW()-INTERVAL '1 day' WHERE status='active'")
  try {
    await db.exec(await readFile(new URL('../../db/schema.sql', import.meta.url), 'utf8'))
    await db.exec(`CREATE TABLE conversations(id text,company_id text,project_id text);
      CREATE TABLE im_channel_bindings(company_id text,channel_id text,profile jsonb);
      CREATE TABLE participants(company_id text,id text,kind text,departed_at timestamptz,capabilities jsonb);
      CREATE TABLE learning_project_teacher_agents(company_id text,agent_id text);
      CREATE TABLE approvals(id text,company_id text,agent_id text,channel_id text,source text,work_id text,authorization_user_id text,idempotency_key text UNIQUE,action text,args jsonb,summary text,requested_by text,scope jsonb,preview jsonb,expires_at timestamptz,
        status text DEFAULT 'PENDING',resolved_at timestamptz,resolved_by text,executed_at timestamptz,result jsonb,error text,resumed_at timestamptz);
      INSERT INTO conversations VALUES('s','t','p');
      INSERT INTO im_channel_bindings VALUES('t','s','{"members":["a","u","reviewer"],"channelType":2}');
      INSERT INTO participants VALUES('t','a','agent',NULL,'["routines","knowledge"]'),('t','u','human',NULL,'[]'),('t','reviewer','human',NULL,'[]');
      INSERT INTO lingxios.agent_work_items(id,tenant_id,agent_id,session_id,thread_id,principal_id,kind,lane,trigger_ref,fence,status,lease_expires_at)
        VALUES('w','t','a','s','thread','u','turn','interactive','m',1,'leased',NOW()+INTERVAL '1 hour')`)
    await database.query('INSERT INTO lingxios.agent_os_sessions(session_key,tenant_id,agent_id,session_id,thread_id,request_snapshot) VALUES($1,$2,$3,$4,$5,$6)',
      ['session', 't', 'a', 's', 'thread', JSON.stringify({ workId: 'w', tenantId: 't', sessionId: 's', authorId: 'u', originalText: 'Schedule the project summary', revisions: [] })])
    for (const args of [{ ...create, kind: 'teacher_digest' }, { ...create, timezone: '+08:00' }, { ...create, schedule: { everyMinutes: 4 } },
      { ...create, schedule: { everyMinutes: 5, time: '09:00' } }, { ...create, schedule: { time: '24:00' } }, { ...create, principalId: 'other' }]) {
      await assert.rejects(requestRoutineApproval(database, services, work, action('create', args)))
    }
    assert.deepEqual(await plans(), [])
    const decision = await prepare(action('create', create))
    assert.deepEqual(await plans(), [])
    failure = 'receipt'
    await assert.rejects(approveRoutine(database, services, decision), /receipt failure/)
    assert.deepEqual(await plans(), [])
    failure = undefined
    assert.deepEqual(await approveRoutine(database, services, decision), { status: 'resumed', workId: 'w' })
    assert.deepEqual(await approveRoutine(database, services, decision), { status: 'already_resumed', workId: 'w' })
    const [plan] = await plans()
    assert.ok(plan)
    assert.deepEqual([plan['status'], plan['next_run_at'], plan['principal_id'], plan['thread_id']], ['paused', null, 'u', 'thread'])
    assert.equal(await scheduleRoutines(database, services), 0)
    const id = String(plan['id'])
    const stale = await prepare(action('activate', { routineId: id }))
    await executeRoutine(database, services, work, action('pause', { routineId: id }))
    await assert.rejects(approveRoutine(database, services, stale), /stale/)
    const activation = await prepare(action('activate', { routineId: id }))
    denied = 'revoked'
    await assert.rejects(approveRoutine(database, services, activation), /revoked/)
    denied = undefined
    await approveRoutine(database, services, activation)
    const listed = await executeRoutine(database, services, work, action('list', {})) as { routines: unknown[]; truncated: boolean }
    assert.deepEqual(listed, { routines: await plans(), truncated: false })
    assert.deepEqual(await executeRoutine(database, services, { ...work, threadId: 'other' }, action('list', {})), { routines: [], truncated: false })
    await assert.rejects(executeRoutine(database, services, { ...work, threadId: 'other' }, action('pause', { routineId: id })), /outside/)
    await due()
    failure = 'enqueue'
    await assert.rejects(scheduleRoutines(database, services), /enqueue failure/)
    assert.deepEqual(await jobs(), [])
    failure = undefined
    assert.equal(await scheduleRoutines(database, services), 1)
    await due()
    assert.equal(await scheduleRoutines(database, services), 0)
    const [job] = await jobs()
    assert.ok(job)
    assert.deepEqual([job['principal_id'], job['session_id'], job['thread_id'], (job['meta'] as Record<string, unknown>)['text']], ['u', 's', 'thread', create.instructions])
    const scheduled = { ...work, id: String(job['id']), kind: 'routine', lane: 'background' as const }
    await assertRoutineWork(database, services, scheduled)
    await assert.rejects(assertRoutineWork(database, services, { ...scheduled, principalId: 'reviewer' }), /obsolete/)
    await executeRoutine(database, services, work, action('pause', { routineId: id }))
    await assert.rejects(assertRoutineWork(database, services, scheduled), /obsolete/)
    assert.equal((await jobs())[0]!['status'], 'cancelled')
    await approveRoutine(database, services, await prepare(action('activate', { routineId: id })))
    await due()
    denied = 'transient'
    await assert.rejects(scheduleRoutines(database, services), /database unavailable/)
    assert.equal((await plans())[0]!['status'], 'active')
    denied = 'revoked'
    assert.equal(await scheduleRoutines(database, services), 0)
    assert.equal((await plans())[0]!['pause_reason'], 'authorization_or_scope_changed')
    denied = undefined
    await db.exec("INSERT INTO learning_project_teacher_agents VALUES('t','a')")
    await assert.rejects(executeRoutine(database, services, work, action('list', {})), /outside/)
    for (const [from, expected] of [['2026-03-08T06:00:00Z', '2026-03-08T07:30:00.000Z'], ['2026-11-01T05:45:00Z', '2026-11-01T06:30:00.000Z']]) {
      assert.equal(await nextRoutineRun(database, { time: from!.includes('03-08') ? '02:30' : '01:30' }, 'America/New_York', new Date(from!)), expected)
    }
    assert.equal(await nextRoutineRun(database, { everyMinutes: 5 }, 'Asia/Shanghai', new Date('2026-01-01T00:00:00.123Z')), '2026-01-01T00:05:00.123Z')
    await db.exec(`DELETE FROM learning_project_teacher_agents;
      ALTER TABLE participants ADD COLUMN name text DEFAULT 'Assistant', ADD COLUMN role text DEFAULT 'assistant', ADD COLUMN system_prompt text DEFAULT '';
      ALTER TABLE approvals ADD CONSTRAINT approvals_work_id_fkey FOREIGN KEY(work_id) REFERENCES lingxios.agent_work_items(id) ON DELETE CASCADE`)
    await approveRoutine(database, services, await prepare(action('activate', { routineId: id })))
    await database.query("UPDATE lingxios.agent_work_items SET status='succeeded' WHERE id='w'")
    await due()
    const directory = await mkdtemp(join(tmpdir(), 'lingxios-routines-'))
    let calls = 0, deliveries = 0, failDelivery = false
    const server = http.createServer((req, res) => {
      const chunks: Buffer[] = []
      req.on('data', chunk => chunks.push(Buffer.from(chunk)))
      req.on('end', () => {
        const prompt = Buffer.concat(chunks).toString('utf8')
        const request = JSON.parse(prompt)
        if (!request.stream && request.response_format?.type === 'json_object') {
          const instructions = String(request.messages[0]?.content)
          assert.match(instructions, /^(Maintain compact learning memory\.|Independently audit every proposed memory change)/)
          res.writeHead(200, { 'content-type': 'application/json' })
          res.end(JSON.stringify({ model: 'test', choices: [{ message: {
            content: instructions.startsWith('Maintain') ? '{"changes":[]}' : '{"approved":true,"confidence":0.9}',
          }, finish_reason: 'stop' }] }))
          return
        }
        const tool = ++calls % 2 === 1
        assert.match(prompt, /Summarize the current project/)
        if (!tool) assert.match(prompt, /Daily summary/)
        const delta = tool ? { tool_calls: [{ index: 0, id: 'routines-read', function: { name: 'ipython', arguments: JSON.stringify({ code: 'print(host.routines.list())\nprint(host.knowledge.list_sources())' }) } }] } : { content: JSON.stringify({ body: 'Scheduled summary.', status: 'satisfied', gaps: [], checks: [{ requirement: 'Summarize the current project', status: 'met', basis: 'Summary supplied from the recorded reads.' }] }) }
        res.writeHead(200, { 'content-type': 'text/event-stream' })
        res.end(`data: ${JSON.stringify({ choices: [{ delta, finish_reason: tool ? 'tool_calls' : 'stop' }] })}\n\ndata: [DONE]\n\n`)
      })
    })
    await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve))
    const address = server.address() as { port: number }
    const unused = async (): Promise<never> => { throw new Error('unexpected knowledge action') }
    const app = await createLingxiLoop({ database, services: { ...services,
      knowledge: { listKnowledgeSourcesForAgent: async native => { assert.equal(native.reason, 'routine'); assert.equal(native.authorizationUserId, 'u'); return [] }, addKnowledgeText: unused, addKnowledgeUrl: unused, addKnowledgeFile: unused, retryKnowledgeSourceForAgent: unused, setKnowledgeSourceEnabled: unused, deleteKnowledgeSourceForAgent: unused },
      wukongClient: () => ({ syncMessages: async () => [], sendMessage: async (channel, type, sender, message) => {
        if (failDelivery) throw new Error('transport unavailable')
        assert.deepEqual([channel, type, sender, message.body, message.replyToClientMsgNo], ['s', 2, 'a', 'Scheduled summary.', 'thread'])
        deliveries++
        return { messageId: message.clientMsgNo, messageSeq: deliveries }
      } }),
    }, worker: { id: 'routine-test' }, model: { id: 'test', apiKey: 'test', baseUrl: `http://127.0.0.1:${address.port}` }, kernel: { homesRoot: directory },
    policy: new LingxiLoopRuntimePolicy({ capabilityMethods: LINGXILOOP_CAPABILITY_METHODS }) })
    try {
      const deadline = Date.now() + 2_000
      while (!await app.runNext() && Date.now() < deadline) await delay(25)
      assert.equal(calls > 0, true)
      assert.equal(calls, 2, JSON.stringify(await jobs()))
      const deliveryDeadline = Date.now() + 2_000
      while (deliveries === 0 && Date.now() < deliveryDeadline) await delay(25)
      assert.equal(deliveries, 1)
      const completed = (await jobs()).find(row => row['status'] === 'succeeded')!
      const runIdentity = { runId: String(completed['id']), tenantId: 't', agentId: 'a', sessionId: 's', threadId: 'thread' }
      assert.equal((await app.readMessage(runIdentity))?.body, 'Scheduled summary.')
      assert.equal(await app.readDelivery(runIdentity), 'delivered')
      await due()
      assert.equal(await scheduleRoutines(database, services), 1)
      failDelivery = true
      const port = await app.listenControlPlane({ serviceToken: 'routine-worker-secret', port: 0 })
      const worker = await startWorker({ ...process.env, AGENT_OS_CONTROL_PLANE_URL: `http://127.0.0.1:${port}`,
        AGENT_OS_SERVICE_TOKEN: 'routine-worker-secret', AGENT_OS_WORKER_ID: 'routine-test', AGENT_OS_WORKER_PORT: '0',
        AGENT_OS_MODEL: 'test', AGENT_OS_MODEL_API_KEY: 'test', AGENT_OS_MODEL_BASE_URL: `http://127.0.0.1:${address.port}`,
        AGENT_OS_HOMES_ROOT: directory, AGENT_OS_POLL_IDLE_MS: '50', AGENT_OS_MAX_CONCURRENT_RUNS: '1' })
      try {
        const deadline = Date.now() + 10_000
        while ((await jobs()).some(row => row['status'] === 'queued' || row['status'] === 'leased') && Date.now() < deadline) await delay(25)
      } finally { await worker.stop() }
      assert.equal(calls, 4)
      assert.equal(deliveries, 1)
      failDelivery = false
      denied = 'revoked'
      await database.query('UPDATE lingxios.agent_delivery_outbox SET available_at=NOW() WHERE delivered_at IS NULL')
      await app.runNext()
      assert.equal(deliveries, 1)
      denied = undefined
      await executeRoutine(database, services, work, action('pause', { routineId: id }))
      await database.query('UPDATE lingxios.agent_delivery_outbox SET available_at=NOW() WHERE delivered_at IS NULL')
      await app.runNext()
      assert.equal(deliveries, 1)
    } finally {
      await app.stop()
      await new Promise<void>(resolve => server.close(() => resolve()))
      await rm(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 })
    }
  } finally { await db.close() }
})
