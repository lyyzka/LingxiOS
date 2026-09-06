import assert from 'node:assert/strict'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import http from 'node:http'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import ts from 'typescript'
import { PGlite } from '@electric-sql/pglite'
import { startMission, assertMissionCoordinatorWork } from '../dist/src/integrations/lingxiloop/learning-missions.js'
import { createLingxiLoop } from '../dist/src/integrations/lingxiloop/index.js'

const source = resolve(process.argv[2] ?? process.env.LINGXILOOP_SOURCE ?? fileURLToPath(new URL('../../LingxiLoop/server/src', import.meta.url)))
const input = await readFile(resolve(source, 'modules/learning/missions-repository.ts'), 'utf8')
const output = ts.transpileModule(input, { compilerOptions: { target: ts.ScriptTarget.ES2023, module: ts.ModuleKind.ESNext } }).outputText
const repository = await import(`data:text/javascript;base64,${Buffer.from(output).toString('base64')}`)
const db = new PGlite()
let failEnqueue = false, failSend = false
const database = { query: async (sql, params) => {
  if (failEnqueue && sql.startsWith('INSERT INTO lingxios.agent_work_items')) throw new Error('injected enqueue failure')
  const result = await db.query(sql, params)
  return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
}, connect: async () => ({ query: database.query, release() {} }) }
const messages = [{ clientMsgNo: 'human-message', fromUid: 'learner', channelId: 'room', channelType: 2, payload: { version: 1, kind: 'text', body: 'Help me learn fractions' } }]
const cards = [], permissions = [], metrics = []
const services = {
  learning: { ...repository, inc: (...args) => metrics.push(args) },
  permissionService: { assertCan: async request => { permissions.push(request) } },
  wukongClient: () => ({ syncMessages: async () => messages, sendMessage: async (...args) => {
    if (failSend) throw new Error('transport unavailable')
    cards.push(args)
    return { messageId: args[3].clientMsgNo, messageSeq: cards.length }
  } }),
}
const work = { id: 'request', tenantId: 'tenant', agentId: 'assistant', sessionId: 'room', principalId: 'learner', triggerRef: 'human-message', kind: 'turn', lane: 'interactive', fence: 1, homeEpoch: 1 }
const action = { runId: work.id, cellId: 'cell', callIndex: 0, idempotencyKey: 'key', action: 'learning.start_mission', args: { goal: 'Compare fractions', successCriteria: 'Explain two equivalent fractions' } }
const jobs = async () => (await database.query('SELECT * FROM lingxios.agent_work_items')).rows
try {
  await db.exec(await readFile(new URL('../db/schema.sql', import.meta.url), 'utf8'))
  // Native-shaped relational fixtures, not a substitute for native authorization tests.
  await db.exec(`
    CREATE TABLE projects(id text,company_id text,kind text,name text,status text);
    CREATE TABLE conversations(id text,company_id text,project_id text,members jsonb);
    CREATE TABLE courses(id text,company_id text,project_id text,study_room_conversation_id text);
    CREATE TABLE learning_course_rooms(company_id text,course_id text,conversation_id text,purpose text);
    CREATE TABLE participants(id text,company_id text,kind text,name text,departed_at timestamptz,capabilities jsonb,preset_key text);
    CREATE TABLE learning_missions(id text PRIMARY KEY,company_id text,project_id text,learner_id text,conversation_id text,trigger_client_msg_no text,goal text,success_criteria text,kind text,coordinator_agent_id text,created_by text,status text DEFAULT 'PLANNING',created_at timestamptz DEFAULT NOW(),updated_at timestamptz DEFAULT NOW(),UNIQUE(company_id,project_id,learner_id,conversation_id,trigger_client_msg_no));
    CREATE TABLE learning_mission_steps(id text,company_id text,project_id text,mission_id text,kind text,description text,success_criteria text,knowledge_unit_id text,status text,position int,outcome text,completion_evidence_id text,completion_attempt_id text,created_at timestamptz);
    INSERT INTO projects VALUES('project','tenant','TEACHING','Course','ACTIVE');
    INSERT INTO conversations VALUES('room','tenant','project','["assistant","nova","learner"]');
    INSERT INTO courses VALUES('course','tenant','project','room');
    INSERT INTO participants VALUES('learner','tenant','human','Learner',NULL,'[]',NULL),('assistant','tenant','agent','Assistant',NULL,'["canvas","learning"]',NULL),('nova','tenant','agent','Nova',NULL,'["canvas","learning"]','nova');
  `)
  await assert.rejects(startMission(database, services, work, { ...action, args: { ...action.args, principalId: 'other' } }, 2), /unknown/)
  messages[0].fromUid = 'other'
  await assert.rejects(startMission(database, services, work, action, 2), /persisted principal/)
  messages[0].fromUid = 'learner'
  messages[0].payload.refs = { agentId: 'assistant' }
  await assert.rejects(startMission(database, services, work, action, 2), /persisted principal/)
  assert.deepEqual((await database.query('SELECT id FROM learning_missions')).rows, [])
  assert.deepEqual(await jobs(), [])
  delete messages[0].payload.refs
  messages[0].channelId = 'foreign'
  await assert.rejects(startMission(database, services, work, action, 2), /persisted principal/)
  messages[0].channelId = 'room'
  failEnqueue = true
  await assert.rejects(startMission(database, services, work, action, 2), /enqueue failure/)
  assert.deepEqual((await database.query('SELECT id FROM learning_missions')).rows, [])
  assert.deepEqual(await jobs(), [])
  assert.equal(cards.length, 0)
  failEnqueue = false
  const mission = await startMission(database, services, work, action, 2)
  assert.deepEqual([mission.coordinatorAgentId, mission.learnerId, mission.status, mission.goal], ['nova', 'learner', 'PLANNING', action.args.goal])
  assert.equal(cards.length, 1)
  assert.deepEqual(cards[0].slice(0, 3), ['room', 2, 'assistant'])
  assert.deepEqual([cards[0][3].kind, cards[0][3].data.missionId, cards[0][3].data.courseId, cards[0][3].data.suppressAgentWake], ['learning_mission', mission.id, 'course', true])
  const [job] = await jobs()
  assert.deepEqual([job.kind, job.agent_id, job.principal_id, job.thread_id, job.trigger_ref, job.meta.text], ['mission_coordinator', 'nova', 'learner', 'human-message', 'human-message', messages[0].payload.body])
  const coordinated = { ...work, id: job.id, agentId: 'nova', kind: 'mission_coordinator', meta: job.meta, threadId: job.thread_id }
  assert.equal(await assertMissionCoordinatorWork(database, services, coordinated), mission.id)
  await db.exec("UPDATE learning_missions SET coordinator_agent_id='assistant'")
  await assert.rejects(assertMissionCoordinatorWork(database, services, coordinated), /assignment or scope changed/)
  await db.exec("UPDATE learning_missions SET coordinator_agent_id='nova'")
  for (const status of ['PAUSED', 'CANCELLED']) {
    await database.query('UPDATE learning_missions SET status=$1', [status])
    await assert.rejects(assertMissionCoordinatorWork(database, services, coordinated), /no longer runnable/)
  }
  await db.exec("UPDATE learning_missions SET status='PLANNING'")
  assert.equal((await startMission(database, services, work, action, 2)).id, mission.id)
  assert.equal((await jobs()).length, 1)
  assert.deepEqual(metrics, [['learning.mission.created', { mode: 'agent' }], ['learning.mission.deduplicated', undefined]])
  assert.ok(permissions.some(item => item.actorUserId === 'learner' && item.resource.type === 'project' && item.resource.id === 'project' && item.action === 'learning:submit'))
  failSend = true
  await assert.rejects(startMission(database, services, work, action, 2), /transport unavailable/)
  assert.equal((await jobs()).length, 1)
  assert.equal((await database.query('SELECT id FROM learning_missions')).rows.length, 1)
  failSend = false
  await db.exec("UPDATE courses SET study_room_conversation_id='other'; INSERT INTO learning_course_rooms VALUES('tenant','course','room','lab')")
  await assert.rejects(startMission(database, services, work, action, 2), /explicit learner request/)
  messages[0].clientMsgNo = 'next-message'
  const explicit = await startMission(database, services, { ...work, agentId: 'nova', triggerRef: 'next-message', threadId: 'existing-thread' }, { ...action, args: { ...action.args, explicit: true } }, 2)
  assert.equal(explicit.kind, 'PROJECT')
  assert.equal((await jobs()).length, 1, 'current coordinator must not enqueue itself')
  await db.exec(`ALTER TABLE participants ADD COLUMN role text DEFAULT 'assistant', ADD COLUMN system_prompt text DEFAULT '';
    CREATE TABLE learning_project_teacher_agents(company_id text,agent_id text);
    CREATE TABLE im_channel_bindings(company_id text,channel_id text,profile jsonb);
    INSERT INTO im_channel_bindings VALUES('tenant','room','{"members":["learner","assistant","nova"],"channelType":2}');
    CREATE TABLE approvals(id text,work_id text REFERENCES lingxios.agent_work_items(id) ON DELETE CASCADE)`)
  const directory = await mkdtemp(join(tmpdir(), 'lingxios-native-missions-'))
  let calls = 0
  const server = http.createServer((req, res) => {
    const chunks = []
    req.on('data', chunk => chunks.push(chunk))
    req.on('end', () => {
      const prompt = Buffer.concat(chunks).toString('utf8')
      assert.ok(prompt.includes('Help me learn fractions'))
      if (calls < 2) assert.ok(prompt.includes(mission.id))
      const tool = ++calls % 2 === 1
      const code = calls === 1 ? `print(host.learning.get_mission(missionId=${JSON.stringify(mission.id)}))`
        : 'print(host.learning.start_mission(goal="Compare fractions", successCriteria="Explain equivalent fractions", explicit=True))'
      const delta = tool ? { tool_calls: [{ index: 0, id: 'mission-action', function: { name: 'ipython', arguments: JSON.stringify({ code }) } }] } : { content: JSON.stringify({ body: 'Nova: Mission is ready for planning.', status: 'blocked',
        checks: [{ requirement: 'Help me learn fractions', status: 'unknown', basis: 'The fixture verified Mission setup only.' }], gaps: ['Planning and learning work have not been performed by this fixture.'] }) }
      res.writeHead(200, { 'content-type': 'text/event-stream' })
      res.end(`data: ${JSON.stringify({ choices: [{ delta, finish_reason: tool ? 'tool_calls' : 'stop' }] })}\n\ndata: [DONE]\n\n`)
    })
  })
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve))
  const unavailable = async () => { throw new Error('unexpected mutation') }
  const app = await createLingxiLoop({ database, services: { ...services,
    learning: { ...services.learning, createPermissionService: unavailable, proposeLearningEvaluation: unavailable, learningScoreBreakdownSchema: { parse: unavailable }, recordLearningAttempt: unavailable, findLearningDocumentEvidence: unavailable, findLearningCanvasEvidence: unavailable, createKnowledgeUnits: unavailable, draftActivity: unavailable,
      loadLearningTurnContext: async (native, actor) => {
        assert.ok(['handoff', 'message', 'routine'].includes(native.reason))
        assert.equal(actor, 'learner')
        return { project: { id: 'project' }, learnerId: 'learner', knowledgeUnits: [], due: [] }
      },
      getMission: async (id, company, project, learner, conversation) => {
        const stored = await repository.findLearningMission(database, company, project, id)
        assert.equal(stored.learnerId, learner); assert.equal(stored.conversationId, conversation)
        return stored
      }, getActivity: unavailable, updateMissionStep: unavailable, addMissionSteps: unavailable, finishMissionPlanning: unavailable, completeMission: unavailable,
    },
    knowledge: { listKnowledgeSourcesForAgent: unavailable, addKnowledgeText: unavailable, addKnowledgeUrl: unavailable, addKnowledgeFile: unavailable, retryKnowledgeSourceForAgent: unavailable, setKnowledgeSourceEnabled: unavailable, deleteKnowledgeSourceForAgent: unavailable },
  }, model: { id: 'test', apiKey: 'test', baseUrl: `http://127.0.0.1:${server.address().port}` }, kernel: { homesRoot: directory } })
  try {
    assert.equal(await app.runNext(), true)
    assert.equal(calls, 2, JSON.stringify(await jobs()))
    const run = { tenantId: 'tenant', agentId: 'nova', sessionId: 'room', threadId: 'human-message', runId: job.id }
    assert.equal((await app.readMessage(run))?.body, 'Nova: Mission is ready for planning.')
    assert.equal(await app.readDelivery(run), 'delivered')
    assert.deepEqual([cards.at(-1)[2], cards.at(-1)[3].replyToClientMsgNo], ['nova', 'human-message'])
    messages[0].clientMsgNo = 'via-host'
    messages[0].payload.refs = { agentId: 'assistant' }
    const jobsBeforeRejectedInput = (await jobs()).length
    await assert.rejects(app.receive({ companyId: 'tenant', agentId: 'assistant', channelId: 'room', clientMsgNo: 'via-host' }), /agent-authored/)
    await assert.rejects(app.receive({ companyId: 'tenant', agentId: 'assistant', channelId: 'room', clientMsgNo: 'via-host', continuation: { runId: job.id, requestVersion: 1 } }), /agent-authored/)
    assert.equal((await jobs()).length, jobsBeforeRejectedInput)
    delete messages[0].payload.refs
    const received = await app.receive({ companyId: 'tenant', agentId: 'assistant', channelId: 'room', clientMsgNo: 'via-host' })
    assert.equal(await app.runNext(), true)
    assert.equal(calls, 6, 'the second fixture response is retried because it identifies Nova in Assistant’s turn')
    const delegated = (await jobs()).filter(item => item.kind === 'mission_coordinator' && item.trigger_ref === 'via-host')
    assert.equal(delegated.length, 1)
    assert.deepEqual([delegated[0].agent_id, delegated[0].principal_id, delegated[0].meta.text], ['nova', 'learner', messages[0].payload.body])
    const receipt = (await database.query(`SELECT ledger.result FROM lingxios.agent_action_ledger ledger
      JOIN lingxios.agent_action_intents intent ON intent.idempotency_key=ledger.idempotency_key
      WHERE intent.intent->>'workId'=$1 AND intent.intent->'action'->>'action'='learning.start_mission'`, [received.id])).rows
    assert.equal(receipt.length, 1)
    assert.equal(receipt[0].result.ok, true)
    assert.equal(receipt[0].result.value.id, delegated[0].meta.missionId)
  } finally {
    await app.stop()
    await new Promise(resolve => server.close(resolve))
    await rm(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 })
  }
  console.log('Packaged Mission creation passed with unchanged native repository SQL: scoped human source, native coordinator choice, atomic namespaced enqueue, deduplication, native card publication and Python coordinator delivery. Full native permission execution and coordinator model quality remain separate gates.')
} finally { await db.close() }
