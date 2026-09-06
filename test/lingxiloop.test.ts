import { configureTeacherDigest, scheduleTeacherDigests } from '../src/integrations/lingxiloop/teacher-digest.js'
import { teacherReportingFixture } from './teacher-reporting-fixture.js'
import assert from 'node:assert/strict'
import http from 'node:http'
import { setTimeout as delay } from 'node:timers/promises'
import { startWorker } from '../src/worker/index.js'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { it } from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import { createLingxiLoop } from '../src/integrations/lingxiloop/index.js'
import type { LingxiLoopServices, NativeTextMessage, NativeCalendarChanged, NativeCalendarCreate } from '../src/integrations/lingxiloop/service-contracts.js'
import type { SqlPool } from '../src/control-plane/pg-store.js'
import { executeCanvas } from '../src/integrations/lingxiloop/canvas.js'
import { executeKnowledge } from '../src/integrations/lingxiloop/actions.js'

const unexpectedMission = async (): Promise<never> => { throw new Error('unexpected Mission creation') }
const unusedMissionServices = { createPermissionService: () => ({ assertCan: unexpectedMission }), proposeLearningEvaluation: unexpectedMission, learningScoreBreakdownSchema: { parse: () => { throw new Error("unused schema") } }, recordLearningAttempt: unexpectedMission, findLearningDocumentEvidence: unexpectedMission, findLearningCanvasEvidence: unexpectedMission, createKnowledgeUnits: unexpectedMission, draftActivity: unexpectedMission, findLearningRoomState: unexpectedMission, findEligibleLearningMissionCoordinator: unexpectedMission, upsertLearningMission: unexpectedMission, findLearningMission: unexpectedMission, inc: () => {} }

it('binds native-shaped resources through the packaged ingress/action/delivery path', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'lingxios-loop-'))
  const db = new PGlite()
  let failRejection = false
  let failApprovedResume = false
  let failMemoryRecall = true
  const database: SqlPool = { query: async (sql, params) => {
    if (failMemoryRecall && sql.includes('FROM lingxios.agent_memories') && !sql.includes('WHERE FALSE')) { failMemoryRecall = false; throw new Error('injected memory recall outage') }
    if (failApprovedResume && sql.includes('UPDATE approvals SET resumed_at')) { failApprovedResume = false; throw new Error('injected continuation failure') }
    if (failRejection && sql.includes('UPDATE lingxios.agent_action_ledger SET result')) { failRejection = false; throw new Error('injected rejection failure') }
    const result = await db.query<Record<string, unknown>>(sql, params)
    return { command: 'TEST', oid: 0, fields: [], rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
  }, connect: async () => ({ query: database.query, release: () => {} }) }
  await db.exec(await readFile(new URL('../../db/schema.sql', import.meta.url), 'utf8'))
  await db.exec(`
    CREATE TABLE participants(company_id text,id text,kind text,name text,role text,system_prompt text,capabilities jsonb,departed_at timestamptz);
    CREATE TABLE learning_project_teacher_agents(company_id text,agent_id text);
    CREATE TABLE im_channel_bindings(company_id text,channel_id text,profile jsonb);
    CREATE TABLE approvals(id text PRIMARY KEY, company_id text, agent_id text, channel_id text, source text, work_id text,
      authorization_user_id text, idempotency_key text UNIQUE, action text, args jsonb, summary text, requested_by text,
      scope jsonb, preview jsonb, expires_at timestamptz, status text DEFAULT 'PENDING', resolved_at timestamptz, resolved_by text, executed_at timestamptz, result jsonb, error text, resumed_at timestamptz);
    ALTER TABLE approvals ADD CONSTRAINT approvals_work_id_fkey FOREIGN KEY(work_id) REFERENCES lingxios.agent_work_items(id) ON DELETE CASCADE;
    CREATE TABLE knowledge_sources(id text PRIMARY KEY,company_id text,project_id text,visibility_scope text,owner_user_id text,deleted_at timestamptz);
    INSERT INTO knowledge_sources VALUES('source','t','project','PROJECT','u',NULL);
    CREATE TABLE knowledge_fixture(title text,body text,principal text,request_ref text);
    CREATE TABLE conversations(id text,company_id text,project_id text);
    INSERT INTO conversations VALUES('s','t','project');
    CREATE TABLE calendar_events(id text,company_id text,project_id text,created_by text,payload jsonb);
    INSERT INTO calendar_events VALUES('event','t','project','u',NULL),('scheduled-event','t','project','u',NULL);
    CREATE TABLE documents(id text,company_id text,project_id text,title text,conversation_id text);
    INSERT INTO documents VALUES('doc','t','project','Document','s');
    INSERT INTO participants VALUES('t','a','agent','Assistant','assistant','', '["knowledge","canvas","learning","documents","calendar"]'::jsonb,NULL),('t','u','human','User','human','', '[]'::jsonb,NULL);
    CREATE TABLE learning_attempts(id text PRIMARY KEY,company_id text,project_id text,learner_id text,activity_id text,mission_step_id text,assistance text,status text,submitted_at timestamptz,evidence_id text);
    CREATE TABLE evidence_records(id text PRIMARY KEY,company_id text,project_id text,data jsonb,created_by_type text,created_by_id text,created_at timestamptz);
    CREATE TABLE learning_evaluations(id text PRIMARY KEY,company_id text,project_id text,attempt_id text,demonstrated_level int,confidence numeric,rubric_results jsonb,feedback text,evaluator_id text,evaluator_kind text,status text,source_evidence_id text,verifier_evidence_id text,created_at timestamptz);
    INSERT INTO learning_attempts VALUES('attempt','t','project','u','activity',NULL,'NONE','EVALUATED',NOW(),'evidence');
    INSERT INTO evidence_records VALUES('evidence','t','project','{}','AGENT','a',NOW());
    INSERT INTO im_channel_bindings VALUES('t','s','{"channelType":2,"members":["a","u"]}');
  `)
  const delivered: Array<Parameters<ReturnType<LingxiLoopServices['wukongClient']>['sendMessage']>[3]> = []
  let approvalMethod = ''
  let inputMode: 'ask' | 'reply' | undefined
  let teacherCalls: number | undefined
  let teacherQueries = 0
  let teacherDigest = false
  let teacherDenied = false
  let teacherWrite = false
  let teacherDraft = false
  let teacherActivity = false
  let teacherApproval: 'publication' | 'membership' | 'END' | 'ENTER_READ_ONLY' | undefined
  let teacherApprovalResume = false
  let teacherApprovalWrites = 0
  let attachmentText = 'Original attachment text'
  let rejectionReply = false
  let approvedReply = false
  let allowApprovedWrite = false
  let failApprovedWrite = false
  let approvedWrites = 0
  let sourceEnabled = true
  let sourceExists = true
  let knowledgeCalls = 0
  let pollReads = 0
  let calendarTitle = 'Meeting'
  let approvedCalendarId = ''
  let canvasReads = 0
  let sendFailures = 1
  let deliveryDenied = false
  let denyProjectRead = false
  const unexpected = async (): Promise<never> => { throw new Error('unexpected service call') }
  const services: LingxiLoopServices = {
    calendar: {
      writes: {
        CalendarApplication: class {
          async create(scope: { companyId: string; projectId: string; userId: string }, input: NativeCalendarCreate, options?: { eventId?: string }) {
            const event = { ...input, id: options!.eventId!, startAt: input.startAt.toISOString() }
            await database.query('INSERT INTO calendar_events(id,company_id,project_id,payload) VALUES($1,$2,$3,$4::jsonb)', [event.id, scope.companyId, scope.projectId, JSON.stringify(event)])
            await this.events.publish({ type: 'calendar.changed', kind: 'event.created', eventId: event.id, companyId: scope.companyId, workspaceId: scope.projectId, actorId: scope.userId })
            return event
          }
          async delete(scope: { companyId: string; projectId: string; userId: string }, id: string) {
            assert.equal((await database.query('DELETE FROM calendar_events WHERE id=$1 AND company_id=$2 AND project_id=$3', [id, scope.companyId, scope.projectId])).rowCount, 1)
            await this.events.publish({ type: 'calendar.changed', kind: 'event.deleted', eventId: id, companyId: scope.companyId, workspaceId: scope.projectId, actorId: scope.userId })
            return { ok: true as const }
          }
          constructor(_db: unknown, private events: { publish(event: NativeCalendarChanged): Promise<void> }) {}
          get = (scope: { companyId: string; projectId: string; userId: string }, id: string) => services.calendar!.calendarApplication.get(scope, id)
          async update(scope: { companyId: string; projectId: string; userId: string }, id: string, patch: { title?: string }) {
            calendarTitle = patch.title ?? calendarTitle
            await this.events.publish({ type: 'calendar.changed', kind: 'event.updated', eventId: id, companyId: scope.companyId, workspaceId: scope.projectId, actorId: scope.userId })
            return this.get(scope, id)
          }
        },
        updateCalendarEventRequestSchema: { parse: value => value as { title: string } },
        createCalendarEventRequestSchema: { parse: value => { const input = value as { title: string; startAt: string }; return { title: input.title, startAt: new Date(input.startAt), kind: 'personal', allDay: false, status: 'active', isPrivate: false } } },
        createPermissionService: () => services.permissionService,
        CH_CALENDAR_EVENTS: 'calendar', publish: async (channel, event) => { assert.equal(channel, 'calendar'); assert.equal(event.actorId, 'a') },
      },
      calendarApplication: {
        dispatches: async (_scope, id) => id === 'scheduled-event' ? [{ eventId: id, scheduledFor: '2026-09-06T10:00:00.000Z',
          status: 'dispatched', conversationId: 's', messageId: 'calendar-message' }] : [],
        list: async scope => { assert.deepEqual(scope, { companyId: 't', projectId: 'project', userId: 'u' }); return [{ id: 'event', title: calendarTitle, startAt: '2026-09-06T10:00:00Z' }] },
        get: async (scope, id) => {
          assert.equal(scope.userId, 'u')
          if (id === 'scheduled-event') return { id, createdBy: 'u', kind: 'agent_task' as const, title: 'Scheduled task', description: null,
            assigneeId: 'a', targetConversationId: 's', agentPrompt: 'Prepare the scheduled report.', startAt: '2026-09-06T10:00:00Z' }
          if (id === 'event') return { id, createdBy: 'u', kind: 'personal' as const, title: calendarTitle, description: null,
            assigneeId: null, targetConversationId: null, agentPrompt: null, startAt: '2026-09-06T10:00:00Z' }
          const result = await database.query('SELECT payload FROM calendar_events WHERE id=$1', [id])
          assert.equal(result.rows.length, 1)
          return result.rows[0]!['payload'] as { id: string; createdBy: string; kind: 'personal' | 'agent_task'; title: string;
            description: string | null; assigneeId: string | null; targetConversationId: string | null; agentPrompt: string | null; startAt: string }
        },
      },
      listCalendarEventsQuerySchema: { parse: value => { const range = value as { from: string; to: string }; return { from: new Date(range.from), to: new Date(range.to) } } },
    },
    documents: {
      listRecentAgentDocumentCreations: async () => [],
      writes: {
        createPermissionService: () => services.permissionService,
        renameDocumentRequestSchema: { parse: value => value as { title: string } },
        renameDocument: async (client, companyId, projectId, documentId, title) => {
          const result = await client.query('UPDATE documents SET title=$1 WHERE id=$2 AND company_id=$3 AND project_id=$4', [title, documentId, companyId, projectId])
          return result.rowCount === 1
        },
        CH_DOCS: 'docs', publish: async (channel, event) => { assert.equal(channel, 'docs'); assert.equal(event.actorId, 'a'); assert.equal(event.documentId, 'doc') },
      },
      listAgentDocuments: async scope => {
        assert.equal(scope.projectId, 'project')
        return [{ id: 'doc', title: 'Document', createdBy: 'a', conversationId: 's', createdAt: 'now', updatedAt: 'now' }]
      },
      readAgentDocument: async (scope, id) => {
        assert.deepEqual([scope, id], [{ companyId: 't', projectId: 'project', userId: 'u' }, 'doc'])
        const title = (await db.query<{ title: string }>('SELECT title FROM documents WHERE id=$1', [id])).rows[0]!.title
        return { id: 'doc', title, createdBy: 'a', conversationId: 's', createdAt: 'now', updatedAt: 'now', body: 'Document text' }
      },
    },
    learning: { ...unusedMissionServices, updateMissionStep: unexpected, addMissionSteps: unexpected, finishMissionPlanning: unexpected, completeMission: unexpected,
      createPermissionService: () => ({ assertCan: async input => { assert.equal(input.actorUserId, 'u'); assert.equal(input.action, 'learning:read') } }),
      loadLearningTurnContext: async () => undefined, getMission: unexpected, getActivity: unexpected,
      findLearningRoomState: async () => ({ companyId: 't', projectId: 'project', purpose: 'study' }) },
    teacher: { ...teacherReportingFixture, findTeacherScopeBinding: async () => ({ company_id: 't', agent_id: 'a', project_id: 'project', course_id: 'course', course_title: 'Course', course_status: 'ACTIVE', room_id: 's', room_status: 'active', agent_name: 'Teacher', has_teacher: true }),
    loadTeacherOverviewRows: async (_db, scope, days) => { assert.equal(scope.teacherUserId, 'u'); assert.equal(days, 7); teacherQueries++; return { distribution: [], missions: [], activity: [{ learners: 5 }], attention: [], coverage: [] } },
    findTeacherTurnCounts: async () => ({ learners: 0, objectives: 0, activities: 0, pending_reviews: 0 }),
      findTeacherObjectiveApprovalTarget: async () => ({ status: 'DRAFT', updatedAt: 'v1', label: null }),
      findTeacherCourseApprovalTarget: async () => ({ status: 'ACTIVE', updatedAt: 'v1', label: null }),
      findTeacherMembershipApprovalTarget: async () => ({ enabled: false, label: null }),
      assertTeacherApprovalFresh: async (input, client) => { assert.ok(client); assert.equal(input.preview['currentVersion'], teacherApproval === 'membership' ? false : 'v1') },
      requireLearningCourseRole: async (_client, input) => { if (teacherDenied) throw Object.assign(new Error('forbidden'), { name: 'ForbiddenError', status: 403 }); assert.deepEqual(input, { companyId: 't', courseId: 'course', userId: 'u', role: 'teacher' }) },
      setLearningCourseMembershipRecord: async (client, input) => {
        assert.deepEqual(input, { companyId: 't', courseId: 'course', userId: 'new-teacher', role: 'teacher', enabled: true })
        await client.query("INSERT INTO project_memberships VALUES('t','project','new-teacher','ACTIVE','TEACHER')")
        return 'updated'
      },
      enqueueLearningEffect: async (client, input) => {
        assert.deepEqual(input, { companyId: 't', courseId: 'course', kind: 'teacher_room.sync' })
        await client.query('INSERT INTO teacher_effect_fixture VALUES($1)', [input.kind])
      },
      createLearningActivity: async (client, transaction, input) => {
        assert.deepEqual(input, { companyId: 't', courseId: 'course', actorId: 'u', actorKind: 'teacher', title: 'Fraction practice', instructions: 'Compare fractions', type: 'PRACTICE', objectiveIds: ['objective'], evaluationMode: 'TEACHER_REQUIRED', targetLevel: 2, rubric: [] })
        const activity = { id: 'activity', courseId: 'course', status: 'DRAFT', title: input.title, instructions: input.instructions, type: input.type, objectiveIds: input.objectiveIds }
        await transaction(async db => { await db.query('INSERT INTO teacher_activities_fixture VALUES($1::jsonb)', [JSON.stringify(activity)]) })
        return activity
      },
      createLearningObjectives: async (client, transaction, input) => {
        assert.deepEqual(input, { companyId: 't', courseId: 'course', actorId: 'u', actorKind: 'teacher', objectives: [{ title: 'Explain fractions', successCriteria: 'Compare two fractions', targetLevel: 3, prerequisiteIds: [] }] })
        const objective = { id: 'objective', courseId: 'course', title: 'Explain fractions', successCriteria: 'Compare two fractions', targetLevel: 3, position: 0, status: 'DRAFT', prerequisiteIds: [] }
        await transaction(async db => { await db.query('INSERT INTO teacher_objectives_fixture VALUES($1::jsonb)', [JSON.stringify(objective)]) })
        return [objective]
      },
      updateTeacherCourseMetadata: async (client, input) => {
        const result = await client.query('UPDATE teacher_metadata_fixture SET name=$1 RETURNING *', [input.title])
        await client.query("UPDATE participants SET name=$1 WHERE company_id='t' AND id='a'", ['Pulse · ' + input.title])
        return result.rows[0]
      },
      setLearningObjectiveStatus: async (client, input) => {
        assert.equal(input.teacherId, 'u')
        teacherApprovalWrites++
        await client.query("UPDATE learning_knowledge_units SET status=$1 WHERE company_id='t' AND id=$2", [input.status, input.objectiveId])
      },
      ProjectLifecycleApplication: class {
        async executeInTransaction(client: import('../src/control-plane/pg-store.js').SqlQueryable, input: { actorUserId: string; command: string }) {
          assert.equal(input.actorUserId, 'u')
          teacherApprovalWrites++
          const status = input.command === 'END' ? 'COURSE_ENDED' : 'READ_ONLY'
          await client.query("UPDATE projects SET status=$1 WHERE company_id='t' AND id='project'", [status])
          if (status === 'READ_ONLY') await client.query("UPDATE learning_course_teacher_rooms SET status='closed'")
          return { ok: true as const, status, applied: true }
        }
      } },
    canvas: { addCanvasComment: async () => { throw new Error("unused comment") }, canvasCommentRequestSchema: { parse: () => { throw new Error("unused comment schema") } }, listCanvasAvailableAgents: async () => [], deleteCanvasFrame: async () => { throw new Error('unexpected delete') }, appendCanvasFrameContent: async () => { throw new Error('unexpected append') }, updateCanvasFrame: async () => { throw new Error('unexpected update') }, canvasFrameUpdateRequestSchema: { parse: (input: unknown) => input as Record<string, unknown> }, createCanvasFrame: async () => { throw new Error('unexpected write') }, canvasFrameCreateRequestSchema: { parse: (input: unknown) => input as Record<string, unknown> }, getConversationCanvas: async (...args) => { assert.deepEqual(args, ['t', 's', 'u']); canvasReads++; return { id: 'current-canvas', frames: [] } } },
    storage: { readObjectBounded: async () => Buffer.from(attachmentText) },
    pollApplication: { conversationId: async () => 's', create: unexpected, vote: unexpected, close: unexpected, show: async () => { pollReads++; return { id: 'p' } } },
    knowledge: {
      listKnowledgeSourcesForAgent: async () => sourceExists ? [{ id: 'source', title: 'Title', enabled: sourceEnabled }] : [],
      addKnowledgeText: async (work, input) => {
        knowledgeCalls++
        assert.equal(work.authorizationUserId, 'u')
        assert.equal(work.companyId, 't')
        assert.equal(work.channelId, 's')
        assert.equal(work.threadRootClientMsgNo, 'thread-root')
        assert.equal(JSON.parse(input.idempotencyKey)[0], work.id)
        await database.query('INSERT INTO knowledge_fixture VALUES($1,$2,$3,$4)', [input.title, input.text, work.authorizationUserId, work.triggerClientMsgNo])
        return { id: 'source', status: 'queued' }
      },
      addKnowledgeUrl: unexpected, addKnowledgeFile: unexpected, retryKnowledgeSourceForAgent: unexpected,
      setKnowledgeSourceEnabled: async (work, _id, enabled) => {
        assert.equal(allowApprovedWrite, true)
        assert.equal(work.authorizationUserId, 'u')
        approvedWrites++
        sourceEnabled = enabled
        if (failApprovedWrite) throw new Error('lost acknowledgement')
        return { enabled }
      },
      deleteKnowledgeSourceForAgent: async (work) => {
        assert.equal(allowApprovedWrite, true)
        assert.equal(work.authorizationUserId, 'u')
        approvedWrites++
        sourceExists = false
        await database.query("UPDATE knowledge_sources SET deleted_at=NOW() WHERE id='source'")
        if (failApprovedWrite) throw new Error('lost acknowledgement')
        return { deleted: true }
      },
    },
    permissionService: { assertCan: async (request) => {
      if (denyProjectRead && request.resource.type === 'project') throw new Error('project read denied')
      if (deliveryDenied && request.action === 'conversation:write') throw new Error('permission revoked')
      assert.equal(request.actorUserId, 'u')
      assert.equal(request.companyId, 't')
      return {}
    } },
    wukongClient: () => ({
      syncMessages: async () => [{ clientMsgNo: 'calendar-dispatch:73fba3a26d0cb420bc4788aa83e43a27a3acb0eb3f77e8bb68dcac663781b379', messageSeq: 3,
        channelId: 's', channelType: 2, fromUid: 'calendar', payload: { version: 1, kind: 'system', data: { calendarEventId: 'scheduled-event', scheduledFor: '2026-09-06T10:00:00.000Z' } } },
        { clientMsgNo: approvalMethod || 'm', messageSeq: 1, channelId: 's', channelType: 2, fromUid: 'u', payload: { version: 1, kind: 'text', body: approvalMethod?.startsWith('calendar-') ? 'Please ' + approvalMethod.slice(9) + ' the calendar event.' : teacherApproval === 'membership' ? 'Add new-teacher as a teacher.' : teacherApproval ? 'Publish the fractions objective.' : teacherActivity ? 'Draft fraction practice linked to the objective.' : teacherDraft ? 'Draft a fractions objective with a comparison success criterion.' : teacherWrite ? 'Rename this course to Updated course.' : approvalMethod ? 'Apply the requested knowledge source change.' : 'Save this source.', replyToClientMsgNo: 'thread-root' } },
        { clientMsgNo: 'attachment', messageSeq: 2, channelId: 's', channelType: 2, fromUid: 'u', payload: { version: 1, kind: 'attachment', data: { key: 'attachments/t/file', name: 'notes.txt', mime: 'text/plain', size: Buffer.byteLength(attachmentText) } } }],
      sendMessage: async (channel, type, author, payload) => {
        if (sendFailures > 0) { sendFailures--; throw new Error('temporary transport failure') }
        assert.deepEqual([channel, type, author], ['s', 2, 'a'])
        delivered.push(payload)
        return { messageId: 'reply', messageSeq: 2 }
      },
    }),
  }
  let calls = 0
  let contentReviews = 0
  let memorySynthesisCalls = 0
  const server = http.createServer((req, res) => {
    const buffers: Buffer[] = []
    req.on('data', (chunk: Buffer) => buffers.push(chunk))
    req.on('end', async () => {
      const requestBody = JSON.parse(Buffer.concat(buffers).toString('utf8'))
      if (req.url === '/embeddings') {
        res.writeHead(200, { 'content-type': 'application/json' })
        res.end(JSON.stringify({ model: 'embedding-test', data: requestBody.input.map((_text: string, index: number) => ({ index, embedding: [1, 0] })) }))
        return
      }
      if (!requestBody.stream && requestBody.response_format?.type === 'json_object') {
        const instructions = String(requestBody.messages[0]?.content)
        if (instructions.startsWith('Maintain compact learning memory.') || instructions.startsWith('Independently audit every proposed memory change')) {
          memorySynthesisCalls++
          res.writeHead(200, { 'content-type': 'application/json' })
          res.end(JSON.stringify({ model: 'test', choices: [{ message: { content: instructions.startsWith('Maintain') ? '{"changes":[]}' : '{"approved":true,"confidence":0.9}' }, finish_reason: 'stop' }] }))
          return
        }
        contentReviews++
        assert.match(JSON.stringify(requestBody.messages), /resource-review:/)
        res.writeHead(200, { 'content-type': 'application/json' })
        res.end(JSON.stringify({ model: 'test', choices: [{ message: { content: '{"missing":[]}' }, finish_reason: 'stop' }] }))
        return
      }
      calls++
      const active = await db.query<{ request: { originalText: string; revisions: { text: string }[] } }>(`SELECT session.request_snapshot AS request
        FROM lingxios.agent_os_sessions session JOIN lingxios.agent_work_items work ON work.id=session.request_snapshot->>'workId'
        WHERE work.status='leased'`)
      assert.equal(active.rows.length, 1)
      const snapshot = active.rows[0]!.request
      const finalDelta = (delta: { content?: string; tool_calls?: unknown[] }) => delta.tool_calls ? delta : { content: JSON.stringify({
        body: delta.content, status: rejectionReply ? 'blocked' : 'satisfied', gaps: rejectionReply ? ['The human rejected the requested change.'] : [],
        checks: [snapshot.originalText, ...snapshot.revisions.map(revision => revision.text)].map(requirement => ({
          requirement, status: rejectionReply ? 'unmet' : 'met', basis: rejectionReply ? 'The action was rejected.' : 'The controlled native fixture produced the recorded result.',
        })),
      }) }
      if (calls === 1) {
        assert.match(JSON.stringify(requestBody.messages), /memory:unavailable/)
        assert.match(JSON.stringify(requestBody.messages), /Save this source/)
      }
      if (teacherApproval) {
        const code = teacherApproval === 'END' || teacherApproval === 'ENTER_READ_ONLY' ? `host.teacher.transition_course(command="${teacherApproval}")` : teacherApproval === 'membership' ? 'host.teacher.set_teacher_membership(userId="new-teacher", enabled=True)' : 'host.teacher.publish_objective(objectiveId="objective")'
        const delta = teacherApprovalResume ? { content: teacherApproval === 'END' ? 'Course ended.' : teacherApproval === 'membership' ? 'Teacher added; channel sync queued.' : 'Objective published.' } : { tool_calls: [{ index: 0, id: 'teacher-approval', function: { name: 'ipython', arguments: JSON.stringify({ code }) } }] }
        if (teacherApprovalResume) assert.match(Buffer.concat(buffers).toString('utf8'), /Recorded action result/)
        res.writeHead(200, { 'content-type': 'text/event-stream' })
        res.end(`data: ${JSON.stringify({ choices: [{ delta: finalDelta(delta), finish_reason: teacherApprovalResume ? 'stop' : 'tool_calls' }] })}\n\ndata: [DONE]\n\n`)
        return
      }
      if (teacherCalls !== undefined) {
        teacherCalls++
        const prompt = Buffer.concat(buffers).toString('utf8')
        assert.match(prompt, /registered teacher operations agent/)
        const code = teacherDigest ? 'print(host.teacher.overview(windowDays=7))\ntry:\n    host.task.ask(question="Do you want details?")\nexcept Exception:\n    print("questions denied")\nelse:\n    raise RuntimeError("unexpected question permission")\ntry:\n    host.teacher.get_attempt(attemptId="private")\nexcept Exception:\n    print("drilldown denied")\nelse:\n    raise RuntimeError("unexpected drilldown permission")' : teacherActivity ? 'print(host.teacher.draft_activity(title="Fraction practice", instructions="Compare fractions", type="PRACTICE", objectiveIds=["objective"]))' : teacherDraft ? 'print(host.teacher.draft_objectives(objectives=[{"title":"Explain fractions","successCriteria":"Compare two fractions","targetLevel":3}]))' : teacherWrite ? 'print(host.teacher.update_course(title="Updated course"))' : 'print(host.teacher.overview(windowDays=7))\ntry:\n    host.knowledge.list_sources()\nexcept Exception:\n    print("knowledge denied")\nelse:\n    raise RuntimeError("unexpected knowledge access")'
        const delta = teacherCalls === 1 ? { tool_calls: [{ index: 0, id: 'teacher-read', function: { name: 'ipython', arguments: JSON.stringify({ code }) } }] } : { content: teacherActivity ? 'Activity drafted.' : teacherDraft ? 'Objective drafted.' : teacherWrite ? 'Course updated.' : 'Five learners observed.' }
        if (teacherCalls === 2) { if (teacherDraft || teacherActivity) assert.match(prompt, /DRAFT/); else if (teacherWrite) assert.match(prompt, /Updated course/); else { assert.match(prompt, teacherDigest ? /questions denied/ : /knowledge denied/); if (teacherDigest) assert.match(prompt, /drilldown denied/); assert.match(prompt, /learners/) } }
        res.writeHead(200, { 'content-type': 'text/event-stream' })
        res.end(`data: ${JSON.stringify({ choices: [{ delta: finalDelta(delta), finish_reason: teacherCalls === 1 ? 'tool_calls' : 'stop' }] })}\n\ndata: [DONE]\n\n`)
        return
      }
      if (inputMode) {
        if (inputMode === 'reply') assert.match(Buffer.concat(buffers).toString('utf8'), /Reply attachment details/)
        const delta = inputMode === 'ask' ? { tool_calls: [{ index: 0, id: 'native-input-ask', function: { name: 'ipython', arguments: JSON.stringify({ code: 'host.task.ask(question="Please attach the details")' }) } }] } : { content: 'Details received.' }
        res.writeHead(200, { 'content-type': 'text/event-stream' })
        res.end(`data: ${JSON.stringify({ choices: [{ delta: finalDelta(delta), finish_reason: inputMode === 'ask' ? 'tool_calls' : 'stop' }] })}\n\ndata: [DONE]\n\n`)
        return
      }
      if (approvalMethod?.startsWith('calendar-') && !approvedReply) {
        const code = approvalMethod === 'calendar-create' ? 'host.calendar.create(title="Approved meeting", startAt="2026-09-12T10:00:00Z")'
          : 'current = host.calendar.get(eventId=' + JSON.stringify(approvedCalendarId) + ')\nhost.calendar.delete(eventId=current["id"], expected=current)'
        const delta = { tool_calls: [{ index: 0, id: approvalMethod, function: { name: 'ipython', arguments: JSON.stringify({ code }) } }] }
        res.writeHead(200, { 'content-type': 'text/event-stream' })
        res.end(`data: ${JSON.stringify({ choices: [{ delta: finalDelta(delta), finish_reason: 'tool_calls' }] })}\n\ndata: [DONE]\n\n`)
        return
      }
      if (calls === 1) assert.match(Buffer.concat(buffers).toString('utf8'), /Original attachment text/)
      if (calls === 2 && !approvalMethod) {
        const prompt = JSON.parse(Buffer.concat(buffers).toString('utf8')) as { messages: Array<{ role: string; content?: string }> }
        assert.ok(prompt.messages.some(message => message.role === 'tool' && message.content?.includes('knowledge_source_fields') && message.content.includes('pass')))
        assert.ok(prompt.messages.some(message => message.role === 'user' && message.content?.startsWith('Recalled memory snapshot')
          && message.content.includes('Source saved for later reference') && message.content.includes('source_refs')))
      }
      if (rejectionReply) assert.match(Buffer.concat(buffers).toString('utf8'), /rejected by the human reviewer/)
      if (approvedReply) assert.match(Buffer.concat(buffers).toString('utf8'), /Recorded action result/)
      const delta = approvedReply ? { content: 'Approved change recorded.' } : rejectionReply ? { content: 'Change declined.' } : approvalMethod ? { tool_calls: [{ index: 0, id: 'approval-call', function: { name: 'ipython', arguments: JSON.stringify({ code: approvalMethod.endsWith('delete_source') ? 'host.knowledge.delete_source(sourceId="source")' : 'host.knowledge.set_source_enabled(sourceId="source", enabled=False)' }) } }] } : calls === 1 ? { tool_calls: [{ index: 0, id: 'c', function: { name: 'ipython', arguments: JSON.stringify({ code: 'saved = host.knowledge.add_text(title="Title", text="Body")\npoll = host.polls.show(messageId="p")\nresource = host.task.check_resource(action="polls.show", args={"messageId": "p"}, expected={"id": "p"})\nchecked = host.knowledge.check_source(sourceId="source", expected={"enabled": True})\ncanvas = host.canvas.current()\nnoted = host.memory.note(body="Source saved for later reference", scope="course")\nmemories = host.memory.list(scope="course")\ntemporary = host.memory.note(body="Temporary note")\npinned = host.memory.pin(id=temporary["id"], expectedVersion=temporary["version"], pinned=True)\nhost.memory.delete(id=pinned["id"], expectedVersion=pinned["version"])\nassert host.calendar.list(**{"from":"2026-09-01T00:00:00Z", "to":"2026-10-01T00:00:00Z"})["events"][0]["id"] == "event"\nassert host.calendar.get(eventId="event")["title"] == "Meeting"\ncurrent_event = host.calendar.get(eventId="event")\nassert host.calendar.update(eventId="event", expected=current_event, patch={"title":"Revised meeting"})["event"]["title"] == "Revised meeting"\nassert host.documents.list()["documents"][0]["id"] == "doc"\nassert host.documents.read(documentId="doc")["body"] == "Document text"\ncurrent_doc = host.documents.read(documentId="doc")\nassert host.documents.rename(documentId="doc", expectedTitle=current_doc["title"], title="Reviewed")["title"] == "Reviewed"\nlearning_check = host.task.check_resource(action="learning.get_attempt", args={"attemptId": "attempt"}, expected={"status": "EVALUATED"})' }) } }] } : { content: 'Source queued.' }
      res.writeHead(200, { 'content-type': 'text/event-stream' })
      res.end(`data: ${JSON.stringify({ choices: [{ delta: finalDelta(delta), finish_reason: !approvedReply && !rejectionReply && (approvalMethod || calls === 1) ? 'tool_calls' : 'stop' }] })}\n\ndata: [DONE]\n\n`)
    })
  })
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  assert.ok(address && typeof address === 'object')
  const app = await createLingxiLoop({ database, services, worker: { id: 'loop-test' }, model: { id: 'test', apiKey: 'test', baseUrl: `http://127.0.0.1:${address.port}` }, kernel: { homesRoot: directory } })
  const invalidOptions = { database, services, model: { id: 'test', apiKey: 'test' } }
  await db.exec('ALTER TABLE approvals DROP CONSTRAINT approvals_work_id_fkey')
  await assert.rejects(createLingxiLoop(invalidOptions), /native approvals must reference/)
  await db.exec('ALTER TABLE approvals ADD CONSTRAINT approvals_work_id_fkey FOREIGN KEY(work_id) REFERENCES lingxios.agent_work_items(id) ON DELETE CASCADE')
  await assert.rejects(createLingxiLoop({ ...invalidOptions, services: { ...services, pollApplication: {} as NonNullable<LingxiLoopServices['pollApplication']> } }), /missing native poll export/)
  await assert.rejects(createLingxiLoop({ ...invalidOptions, services: { ...services, presentations: {} as NonNullable<LingxiLoopServices['presentations']> } }), /missing native presentation export/)
  for (const missing of ['createPermissionService', 'proposeLearningEvaluation', 'recordLearningAttempt', 'findLearningDocumentEvidence', 'findLearningCanvasEvidence', 'createKnowledgeUnits', 'draftActivity', 'findLearningRoomState', 'findEligibleLearningMissionCoordinator', 'upsertLearningMission', 'findLearningMission', 'inc', 'loadLearningTurnContext', 'getMission', 'getActivity', 'addMissionSteps', 'updateMissionStep', 'finishMissionPlanning', 'completeMission']) {
    const learning = { ...unusedMissionServices, updateMissionStep: async () => null, addMissionSteps: async () => null, finishMissionPlanning: async () => null, completeMission: async () => null, loadLearningTurnContext: async () => undefined, getMission: async () => null, getActivity: async () => null }
    Reflect.deleteProperty(learning, missing)
    await assert.rejects(createLingxiLoop({ ...invalidOptions, services: { ...services, learning } }), new RegExp(`missing native learning export: ${missing}`))
  }
  try {
    assert.equal('enqueue' in app, false)
    const calendarClientMsgNo = 'calendar-dispatch:73fba3a26d0cb420bc4788aa83e43a27a3acb0eb3f77e8bb68dcac663781b379'
    const calendarRequest = await app.receiveCalendarDispatch({ companyId: 't', agentId: 'a', channelId: 's', clientMsgNo: calendarClientMsgNo })
    assert.equal((await app.receiveCalendarDispatch({ companyId: 't', agentId: 'a', channelId: 's', clientMsgNo: calendarClientMsgNo })).deduplicated, true)
    assert.equal(await app.cancel({ runId: calendarRequest.id, tenantId: 't', agentId: 'a', sessionId: 's', principalId: 'u', threadId: calendarClientMsgNo }), true)
    await assert.rejects(app.receiveCalendarDispatch({ companyId: 't', agentId: 'a', channelId: 's', clientMsgNo: 'forged' }), /committed calendar dispatch/)
    const input = { companyId: 't', agentId: 'a', channelId: 's', clientMsgNo: 'm', principalId: 'forged', attachmentClientMsgNos: ['attachment'] }
    const request = await app.receive(input)
    assert.equal((await app.receive(input)).deduplicated, true)
    assert.equal(await app.runNext(), true)
    const deliveryIdentity = { runId: request.id, tenantId: 't', agentId: 'a', sessionId: 's' }
    assert.equal(await app.readDelivery(deliveryIdentity), 'pending')
    assert.equal(await app.readDelivery({ ...deliveryIdentity, tenantId: 'other' }), null)
    assert.equal((await app.readOutcome(deliveryIdentity))?.status, 'satisfied')
    assert.equal((await app.readMessage(deliveryIdentity))?.body, 'Source queued.')
    const memoryEvidence = await database.query('SELECT source_run_id,principal_id,request_version,assistant_text,status FROM lingxios.agent_memory_evidence WHERE source_run_id=$1', [request.id])
    assert.deepEqual(memoryEvidence.rows, [{ source_run_id: request.id, principal_id: 'u', request_version: 1, assistant_text: 'Source queued.', status: 'pending' }])
    assert.equal(delivered.length, 0)
    await database.query('UPDATE lingxios.agent_delivery_outbox SET available_at=NOW() WHERE run_id=$1', [request.id])
    assert.equal(await app.runNext(), true)
    assert.equal(memorySynthesisCalls, 2)
    assert.deepEqual((await database.query('SELECT status FROM lingxios.agent_memory_evidence WHERE source_run_id=$1', [request.id])).rows, [{ status: 'processed' }])
    assert.deepEqual((await database.query("SELECT status FROM lingxios.agent_work_items WHERE kind='memory_synthesis'")).rows, [{ status: 'completed' }])
    assert.equal(await app.readDelivery(deliveryIdentity), 'delivered')
    for (const scope of [{ agentId: 'other' }, { sessionId: 'other' }, { runId: 'missing' }]) {
      assert.equal(await app.readDelivery({ ...deliveryIdentity, ...scope }), null)
    }
    await database.query(`INSERT INTO lingxios.agent_messages(run_id,tenant_id,agent_id,session_id,message)
      VALUES('historical','t','a','s','{"body":"Old message"}'::jsonb)`)
    assert.equal(await app.readDelivery({ ...deliveryIdentity, runId: 'historical' }), 'not_observed')
    assert.equal(knowledgeCalls, 1)
    assert.equal(pollReads, 3)
    assert.equal(contentReviews, 1)
    const resourceChecks = await database.query(
      "SELECT result->'value'->>'status' AS status,result->'value'->>'scope' AS scope FROM lingxios.agent_action_ledger WHERE idempotency_key IN (SELECT idempotency_key FROM lingxios.agent_action_intents WHERE intent->'action'->>'action'='task.check_resource')")
    assert.deepEqual(resourceChecks.rows, Array(4).fill({ status: 'pass', scope: 'observed_resource_fields' }))
    const finalChecks = (await app.readMessage(deliveryIdentity))?.envelope?.resourceChecks
    assert.equal(finalChecks?.length, 4)
    assert.equal(finalChecks?.[2]?.actionKey, JSON.stringify([request.id, 'resource-review:1:1', 0]))
    const learningCheck = finalChecks?.[3]?.result.value as Record<string, unknown>
    assert.deepEqual({ action: learningCheck['action'], args: learningCheck['args'], observed: learningCheck['observed'], status: learningCheck['status'] },
      { action: 'learning.get_attempt', args: { attemptId: 'attempt' }, observed: { status: 'EVALUATED' }, status: 'pass' })
    assert.equal(finalChecks?.[0]?.actionKey, JSON.stringify([request.id, 'c', 2]))
    assert.equal((finalChecks?.[0]?.result.value as Record<string, unknown>)['requestVersion'], 1)
    assert.deepEqual((finalChecks?.[0]?.result.value as Record<string, unknown>)['expected'], { id: 'p' })
    assert.equal(canvasReads, 1)
    const savedMemories = await database.query('SELECT scope_type,scope_id,body,version,source_refs FROM lingxios.agent_memories')
    assert.equal(savedMemories.rows.length, 1)
    assert.deepEqual({ ...savedMemories.rows[0], source_refs: undefined }, {
      scope_type: 'course', scope_id: 's', body: 'Source saved for later reference', version: 1, source_refs: undefined,
    })
    assert.equal((savedMemories.rows[0]!['source_refs'] as Array<Record<string, unknown>>)[0]!['workId'], request.id)
    const memoryChanges = await database.query(`SELECT i.intent->'action'->>'action' AS action,l.result->>'ok' AS ok
      FROM lingxios.agent_action_intents i JOIN lingxios.agent_action_ledger l USING(idempotency_key)
      WHERE i.intent->>'workId'=$1 AND i.intent->'action'->>'action' IN ('memory.pin','memory.delete') ORDER BY action`, [request.id])
    assert.deepEqual(memoryChanges.rows, [{ action: 'memory.delete', ok: 'true' }, { action: 'memory.pin', ok: 'true' }])
    const memoryTrace = await database.query("SELECT data->'memorySnapshot' AS snapshot FROM lingxios.agent_run_events WHERE run_id=$1 AND kind='model.started' ORDER BY seq", [request.id])
    assert.equal((memoryTrace.rows[0]!['snapshot'] as Record<string, unknown>)['status'], 'unavailable')
    assert.match(JSON.stringify(memoryTrace.rows[1]!['snapshot']), /Source saved for later reference/)
    const checks = await database.query(`SELECT result->'value'->>'scope' AS scope,result->'value'->>'status' AS status,
      result->'value'->'observed' AS observed FROM lingxios.agent_action_ledger
      WHERE idempotency_key=$1`, [JSON.stringify([request.id, 'c', 3])])
    assert.deepEqual(checks.rows, [{ scope: 'knowledge_source_fields', status: 'pass', observed: { enabled: true } }])
    assert.deepEqual((await database.query('SELECT * FROM knowledge_fixture')).rows, [{ title: 'Title', body: 'Body', principal: 'u', request_ref: 'm' }])
    assert.equal(delivered[0]?.body, 'Source queued.')
    assert.equal(delivered[0]?.replyToClientMsgNo, 'thread-root')
    assert.equal(delivered[0]?.clientMsgNo, `agent-${request.id}`)
    assert.deepEqual((await database.query('SELECT delivered_at IS NOT NULL AS acknowledged FROM lingxios.agent_delivery_outbox')).rows, [{ acknowledged: true }])
    // Simulate a lost local acknowledgement after the external send succeeded.
    await database.query('UPDATE lingxios.agent_delivery_outbox SET delivered_at=NULL,available_at=NOW() WHERE run_id=$1', [request.id])
    const recovered = await createLingxiLoop(invalidOptions)
    try {
      assert.equal(await recovered.runNext(), false)
      assert.deepEqual(delivered[1], delivered[0])
      assert.equal(calls, 2)
      assert.equal(await recovered.runNext(), false)
      assert.equal(delivered.length, 2)
      await database.query('UPDATE lingxios.agent_delivery_outbox SET delivered_at=NULL,available_at=NOW() WHERE run_id=$1', [request.id])
      sendFailures = 1
      assert.equal(await recovered.runNext(), false)
      assert.deepEqual((await database.query('SELECT available_at>NOW() AS delayed,claim_token IS NULL AS released FROM lingxios.agent_delivery_outbox')).rows, [{ delayed: true, released: true }])
      assert.equal(await recovered.runNext(), false)
      assert.equal(delivered.length, 2)
      await database.query('UPDATE lingxios.agent_delivery_outbox SET available_at=NOW() WHERE run_id=$1', [request.id])
      assert.equal(await recovered.runNext(), false)
      assert.deepEqual(delivered[2], delivered[0])
      await database.query('UPDATE lingxios.agent_delivery_outbox SET delivered_at=NULL,available_at=NOW() WHERE run_id=$1', [request.id])
      deliveryDenied = true
      assert.equal(await recovered.runNext(), false)
      assert.equal(delivered.length, 3)
      assert.deepEqual((await database.query('SELECT delivered_at FROM lingxios.agent_delivery_outbox')).rows, [{ delivered_at: null }])
      deliveryDenied = false
      await database.query('UPDATE lingxios.agent_delivery_outbox SET available_at=NOW() WHERE run_id=$1', [request.id])
      await database.query('UPDATE lingxios.agent_work_items SET cancel_requested_at=NOW() WHERE id=$1', [request.id])
      assert.equal(await recovered.runNext(), false)
      assert.equal(delivered.length, 3)
      await database.query(`UPDATE lingxios.agent_work_items SET cancel_requested_at=NULL,steer_inputs='[{"id":"new","text":"changed","createdAt":"now"}]'::jsonb WHERE id=$1`, [request.id])
      assert.equal(await recovered.runNext(), false)
      assert.equal(delivered.length, 3)
    } finally { await recovered.stop() }
    const work = { id: 'w', fence: 1, homeEpoch: 1, tenantId: 't', agentId: 'a', sessionId: 's', principalId: 'u', triggerRef: 'm', kind: 'turn', lane: 'interactive' as const }
    const sourceCheck = { runId: 'w', cellId: 'check', callIndex: 0, idempotencyKey: JSON.stringify(['w', 'check', 0]),
      action: 'knowledge.check_source', args: { sourceId: 'source', expected: { enabled: true, title: 'Title' } } }
    const observedSource = await executeKnowledge(work, sourceCheck, services, 2) as Record<string, unknown>
    assert.equal(observedSource['status'], 'pass')
    assert.deepEqual(observedSource['observed'], sourceCheck.args.expected)
    assert.ok(Number.isFinite(Date.parse(String(observedSource['observedAt']))))
    sourceEnabled = false
    assert.equal((await executeKnowledge(work, sourceCheck, services, 2) as Record<string, unknown>)['status'], 'fail')
    sourceExists = false
    assert.equal((await executeKnowledge(work, sourceCheck, services, 2) as Record<string, unknown>)['status'], 'not_observed')
    sourceEnabled = true
    sourceExists = true
    await assert.rejects(executeKnowledge(work, { ...sourceCheck, args: { sourceId: 'source', expected: { deleted: true } } }, services, 2), /expected must contain/)
    await assert.rejects(executeKnowledge(work, sourceCheck, { ...services, permissionService: { assertCan: async () => { throw new Error('read denied') } } }, 2), /read denied/)
    const action = { runId: 'w', cellId: 'c', callIndex: 0, idempotencyKey: '[\"w\",\"c\",0]', action: 'knowledge.add_text', args: { title: 'Title', text: 'Body', userId: 'forged' } }
    await assert.rejects(executeKnowledge(work, action, services, 2), /unknown knowledge argument/)
    await assert.rejects(executeKnowledge(work, { ...action, action: 'knowledge.delete_source', args: {} }, services, 2), /approval-required/)
    await assert.rejects(app.receive({ ...input, companyId: 'other' }), /not a member/)
    assert.equal(knowledgeCalls, 1)
    const { executeChat } = await import('../src/integrations/lingxiloop/chat.js')
    const advances: unknown[] = []
    const chatServices = { ...services, advanceAgentReadReceipt: async (input: unknown) => { advances.push(input) } }
    const history = await executeChat(work, { ...action, action: 'chat.history', args: { limit: 5 } }, chatServices, 2)
    assert.ok(Array.isArray(history))
    assert.deepEqual(advances, [{ companyId: 't', channelId: 's', agentId: 'a', readThroughSeq: 3 }])
    await executeChat({ ...work, threadId: 'thread-root' }, { ...action, action: 'chat.send', args: { body: 'Progress' } }, chatServices, 2)
    assert.equal(delivered.at(-1)?.clientMsgNo, `action-${action.idempotencyKey}`)
    assert.equal(delivered.at(-1)?.replyToClientMsgNo, 'thread-root')
    const question = { name: 'format', prompt: 'Which format?', input: { label: 'Your preference' } }
    const asked = await executeChat(work, { ...action, action: 'chat.ask', args: { title: 'Preference', items: [question] } }, chatServices, 2)
    assert.deepEqual(asked, { messageId: 'reply', messageSeq: 2 })
    assert.equal(delivered.at(-1)?.kind, 'questionnaire')
    assert.deepEqual(delivered.at(-1)?.data?.['questionnaire'], { title: 'Preference', items: [{ ...question, choices: [] }] })
    await assert.rejects(executeChat(work, { ...action, action: 'chat.ask', args: { items: [question, question] } }, chatServices, 2), /unique/)
    await assert.rejects(executeChat(work, { ...action, action: 'chat.ask', args: { items: [{ prompt: 'Empty' }] } }, chatServices, 2), /choices or freeform/)
    for (const invalid of [{ ...question, required: 'false' }, { ...question, hidden: true }, { ...question, input: { label: 'Reply', secret: true } }]) {
      await assert.rejects(executeChat(work, { ...action, action: 'chat.ask', args: { items: [invalid] } }, chatServices, 2), /boolean|unknown/)
    }
    await assert.rejects(executeChat(work, { ...action, action: 'chat.history', args: { limit: 101 } }, chatServices, 2), /limit/)
    await assert.rejects(executeChat(work, { ...action, action: 'chat.send', args: { body: 'Spoof', channelId: 'foreign' } }, chatServices, 2), /unknown/)
    for (const method of ['delete_source', 'set_source_enabled']) {
      approvalMethod = method
      const queued = await app.receive({ ...input, clientMsgNo: method })
      const beforeApproval: number = calls
      assert.equal(await app.runNext(), true)
      assert.equal(calls, beforeApproval + 1)
      const outcome = await app.readOutcome({ runId: queued.id, tenantId: 't', agentId: 'a', sessionId: 's' })
      assert.equal(outcome?.status, 'awaiting_approval')
      assert.ok(outcome && outcome.status === 'awaiting_approval')
      const reviewed = await app.inspectApproval({ companyId: 't', userId: 'u', approvalId: outcome.approvalId })
      assert.equal(reviewed.status, 'PENDING')
      assert.equal(reviewed.action.action, `knowledge.${method}`)
      assert.equal(await app.readMessage({ runId: queued.id, tenantId: 't', agentId: 'a', sessionId: 's' }), null)
      assert.equal(await app.runNext(), false)
      failRejection = true
      await assert.rejects(app.rejectApproval({ companyId: 't', userId: 'u', approvalId: outcome.approvalId }), /injected rejection failure/)
      assert.equal((await app.readOutcome({ runId: queued.id, tenantId: 't', agentId: 'a', sessionId: 's' }))?.status, 'awaiting_approval')
      assert.equal((await app.inspectApproval({ companyId: 't', userId: 'u', approvalId: outcome.approvalId })).status, 'PENDING')
      assert.deepEqual(await app.rejectApproval({ companyId: 't', userId: 'u', approvalId: outcome.approvalId }), { status: 'resumed', workId: queued.id })
      assert.deepEqual(await app.rejectApproval({ companyId: 't', userId: 'u', approvalId: outcome.approvalId }), { status: 'already_rejected', workId: queued.id })
      rejectionReply = true
      assert.equal(await app.runNext(), true)
      rejectionReply = false
      assert.equal((await app.readMessage({ runId: queued.id, tenantId: 't', agentId: 'a', sessionId: 's' }))?.body, 'Change declined.')
      assert.equal((await app.readOutcome({ runId: queued.id, tenantId: 't', agentId: 'a', sessionId: 's' }))?.status, 'blocked')
      assert.equal(await app.runNext(), true) // The committed response schedules its memory work.
      assert.equal(await app.runNext(), false)
    }

    for (const method of ['approve-delete_source', 'approve-set_source_enabled', 'unknown-set_source_enabled', 'unknown-delete_source']) {
      approvalMethod = method
      sourceExists = true
      sourceEnabled = true
      await database.query("UPDATE knowledge_sources SET deleted_at=NULL,visibility_scope='PROJECT',owner_user_id='u'")
      failApprovedWrite = method.startsWith('unknown')
      const queued = await app.receive({ ...input, clientMsgNo: method })
      assert.equal(await app.runNext(), true)
      const waiting = await app.readOutcome({ runId: queued.id, tenantId: 't', agentId: 'a', sessionId: 's' })
      assert.ok(waiting && waiting.status === 'awaiting_approval')
      const decision = { companyId: 't', userId: 'u', approvalId: waiting.approvalId }
      const beforeWrites = approvedWrites
      sourceEnabled = false
      await assert.rejects(app.approveKnowledge(decision), /fresh approval preview/)
      assert.equal(approvedWrites, beforeWrites)
      sourceEnabled = true
      allowApprovedWrite = true
      if (!failApprovedWrite) {
        failApprovedResume = true
        await assert.rejects(app.approveKnowledge(decision), /injected continuation failure/)
        assert.equal(approvedWrites, beforeWrites + 1)
        assert.equal((await app.readOutcome({ runId: queued.id, tenantId: 't', agentId: 'a', sessionId: 's' }))?.status, 'awaiting_approval')
        assert.equal((await database.query('SELECT status FROM approvals WHERE id=$1', [waiting.approvalId])).rows[0]?.['status'], 'EXECUTED')
      }
      assert.deepEqual(await app.approveKnowledge(decision), failApprovedWrite ? { status: 'reconciliation_required' } : { status: 'resumed', workId: queued.id })
      assert.equal(approvedWrites, beforeWrites + 1)
      assert.deepEqual(await app.approveKnowledge(decision), failApprovedWrite ? { status: 'reconciliation_required' } : { status: 'already_resumed', workId: queued.id })
      assert.equal(approvedWrites, beforeWrites + 1)
      const approval = (await database.query('SELECT status,result FROM approvals WHERE id=$1', [waiting.approvalId])).rows[0]!
      assert.equal(approval['status'], failApprovedWrite ? 'APPROVED' : 'EXECUTED')
      if (method.endsWith('delete_source') && !failApprovedWrite) assert.deepEqual(approval['result'], { deleted: true, externalAssetCleanup: 'not_verified' })
      if (failApprovedWrite && method.endsWith('delete_source')) {
        await database.query("UPDATE knowledge_sources SET deleted_at=NULL")
        assert.deepEqual(await app.reconcileKnowledgeApproval(decision), { status: 'not_observed' })
        await database.query("UPDATE knowledge_sources SET deleted_at=NOW(),visibility_scope='PRIVATE',owner_user_id='other'")
        assert.deepEqual(await app.reconcileKnowledgeApproval(decision), { status: 'not_observed' })
        await database.query("UPDATE knowledge_sources SET owner_user_id='u',deleted_at='2000-01-01'")
        assert.deepEqual(await app.reconcileKnowledgeApproval(decision), { status: 'not_observed' })
        await database.query("UPDATE knowledge_sources SET deleted_at=NOW()")
        denyProjectRead = true
        await assert.rejects(app.reconcileKnowledgeApproval(decision), /project read denied/)
        assert.equal((await app.inspectApproval(decision)).status, 'APPROVED')
        denyProjectRead = false
        assert.deepEqual(await app.reconcileKnowledgeApproval(decision), { status: 'resumed', workId: queued.id })
        assert.deepEqual((await database.query('SELECT result FROM approvals WHERE id=$1', [waiting.approvalId])).rows[0]?.['result'],
          { deleted: true, externalAssetCleanup: 'not_verified', reconciliation: 'postcondition_observed', executionAttribution: 'not_verified' })
      } else if (failApprovedWrite) {
        sourceExists = false
        assert.deepEqual(await app.reconcileKnowledgeApproval(decision), { status: 'not_observed' })
        sourceExists = true
        sourceEnabled = true
        assert.deepEqual(await app.reconcileKnowledgeApproval(decision), { status: 'not_observed' })
        assert.equal((await app.inspectApproval(decision)).status, 'APPROVED')
        sourceEnabled = false
        assert.deepEqual(await app.reconcileKnowledgeApproval(decision), { status: 'resumed', workId: queued.id })
        assert.deepEqual(await app.reconcileKnowledgeApproval(decision), { status: 'already_resumed', workId: queued.id })
        assert.deepEqual((await database.query('SELECT result FROM approvals WHERE id=$1', [waiting.approvalId])).rows[0]?.['result'],
          { enabled: false, reconciliation: 'postcondition_observed', executionAttribution: 'not_verified' })
      }
      approvedReply = true
      assert.equal(await app.runNext(), true)
      approvedReply = false
      assert.equal((await app.readMessage({ runId: queued.id, tenantId: 't', agentId: 'a', sessionId: 's' }))?.body, 'Approved change recorded.')
      assert.equal(approvedWrites, beforeWrites + 1)

      assert.equal(await app.runNext(), true)
      assert.equal(await app.runNext(), false)
    }
    for (const method of ['create', 'delete']) {
      approvalMethod = `calendar-${method}`
      const request = await app.receive({ ...input, clientMsgNo: approvalMethod, attachmentClientMsgNos: [] })
      assert.equal(await app.runNext(), true)
      const identity = { runId: request.id, tenantId: 't', agentId: 'a', sessionId: 's' }
      const waiting = await app.readOutcome(identity)
      assert.ok(waiting?.status === 'awaiting_approval')
      assert.equal(await app.readMessage(identity), null)
      const decision = { companyId: 't', userId: 'u', approvalId: waiting.approvalId }
      const review = await app.inspectApproval(decision)
      assert.equal(review.action.action, `calendar.${method}`)
      const callsBeforeRecovery: number = calls
      await database.query("UPDATE lingxios.agent_work_items SET status='queued',goal_outcome=NULL,finished_at=NULL WHERE id=$1", [request.id])
      await database.query("UPDATE lingxios.agent_os_sessions SET history=history-(jsonb_array_length(history)-1) WHERE request_snapshot->>'workId'=$1", [request.id])
      assert.equal(await app.runNext(), false)
      assert.equal(calls, callsBeforeRecovery)
      assert.deepEqual(await app.readOutcome(identity), waiting)
      assert.equal((await database.query('SELECT id FROM approvals WHERE work_id=$1', [request.id])).rows.length, 1)
      assert.equal(await app.runNext(), false)
      assert.deepEqual(await app.approveCalendar(decision), { status: 'resumed', workId: request.id })
      assert.deepEqual(await app.approveCalendar(decision), { status: 'already_resumed', workId: request.id })
      const result = (await app.inspectApproval(decision)).result as { eventId: string; notification: string }
      assert.equal(result.notification, 'queued')
      approvedCalendarId = result.eventId
      assert.equal((await database.query('SELECT id FROM calendar_events WHERE id=$1', [approvedCalendarId])).rows.length, method === 'create' ? 1 : 0)
      approvedReply = true
      assert.equal(await app.runNext(), true)
      approvedReply = false
      assert.equal((await app.readMessage(identity))?.body, 'Approved change recorded.')
      assert.equal(await app.runNext(), true) // Committed response memory synthesis.
      assert.equal(await app.runNext(), false)
    }
    inputMode = 'ask'
    approvalMethod = 'input-ask'
    const waitingInput = await app.receive({ ...input, clientMsgNo: approvalMethod, attachmentClientMsgNos: [] })
    assert.equal(await app.runNext(), true)
    const waitingIdentity = { runId: waitingInput.id, tenantId: 't', agentId: 'a', sessionId: 's' }
    assert.equal((await app.readOutcome(waitingIdentity))?.status, 'awaiting_input')
    inputMode = 'reply'
    approvalMethod = 'input-reply'
    attachmentText = 'Reply attachment details'
    const continuation = { ...input, clientMsgNo: approvalMethod, continuation: { runId: waitingInput.id, requestVersion: 1 } }
    assert.deepEqual(await app.receive(continuation), { id: waitingInput.id, deduplicated: false })
    assert.deepEqual(await app.receive(continuation), { id: waitingInput.id, deduplicated: true })
    assert.equal(await app.runNext(), true)
    assert.equal((await app.readMessage(waitingIdentity))?.body, 'Details received.')
    assert.equal((await app.readOutcome(waitingIdentity))?.requestVersion, 2)
    const controlPort = await app.listenControlPlane({ serviceToken: 'test-worker-secret', port: 0 })
    const remoteWorker = await startWorker({ ...process.env, AGENT_OS_CONTROL_PLANE_URL: `http://127.0.0.1:${controlPort}`,
      AGENT_OS_SERVICE_TOKEN: 'test-worker-secret', AGENT_OS_WORKER_ID: 'loop-test', AGENT_OS_WORKER_PORT: '0',
      AGENT_OS_MODEL: 'test', AGENT_OS_MODEL_API_KEY: 'test', AGENT_OS_MODEL_BASE_URL: `http://127.0.0.1:${address.port}`,
      AGENT_OS_HOMES_ROOT: directory, AGENT_OS_POLL_IDLE_MS: '50', AGENT_OS_MAX_CONCURRENT_RUNS: '1' })
    try {
      const deadline = Date.now() + 10_000
      const backgroundState = async () => (await database.query('SELECT status FROM lingxios.agent_work_items WHERE id=$1', [`memory-synthesis:${waitingInput.id}`])).rows[0]?.['status']
      while (await backgroundState() !== 'completed' && Date.now() < deadline) await delay(25)
      assert.equal(await backgroundState(), 'completed')
      assert.deepEqual((await database.query('SELECT status FROM lingxios.agent_memory_evidence WHERE source_run_id=$1', [waitingInput.id])).rows, [{ status: 'processed' }])
    } finally { await remoteWorker.stop() }
    await db.exec("INSERT INTO learning_project_teacher_agents VALUES('t','a')")
    teacherCalls = 0
    approvalMethod = 'teacher-read'
    const teacherWork = await app.receive({ ...input, clientMsgNo: approvalMethod, attachmentClientMsgNos: [] })
    assert.equal(await app.runNext(), true)
    assert.equal(teacherQueries, 1)
    assert.equal(teacherCalls, 2)
    assert.equal((await app.readMessage({ ...waitingIdentity, runId: teacherWork.id }))?.body, 'Five learners observed.')
    const digestRequest = { id: teacherWork.id, tenantId: 't', agentId: 'a', sessionId: 's', principalId: 'u', kind: 'turn', lane: 'interactive' as const, triggerRef: 'schedule-request', fence: 1, homeEpoch: 1 }
    await configureTeacherDigest(database, services, digestRequest, { frequency: 'daily', localTime: '09:00' })
    await database.query("UPDATE lingxios.agent_routines SET next_run_at=NOW()-INTERVAL '1 day'")
    const digestApp = await createLingxiLoop({ database, services, worker: { id: 'loop-test' }, model: { id: 'test', apiKey: 'test', baseUrl: `http://127.0.0.1:${address.port}` }, kernel: { homesRoot: directory } })
    try {
      teacherDigest = true; teacherCalls = 0
      const digestPort = await digestApp.listenControlPlane({ serviceToken: 'digest-worker-secret', port: 0 })
      const digestWorker = await startWorker({ ...process.env, AGENT_OS_CONTROL_PLANE_URL: `http://127.0.0.1:${digestPort}`,
        AGENT_OS_SERVICE_TOKEN: 'digest-worker-secret', AGENT_OS_WORKER_ID: 'loop-test', AGENT_OS_WORKER_PORT: '0',
        AGENT_OS_MODEL: 'test', AGENT_OS_MODEL_API_KEY: 'test', AGENT_OS_MODEL_BASE_URL: `http://127.0.0.1:${address.port}`,
        AGENT_OS_HOMES_ROOT: directory, AGENT_OS_POLL_IDLE_MS: '50', AGENT_OS_MAX_CONCURRENT_RUNS: '1' })
      try {
        const deadline = Date.now() + 10_000
        while ((await database.query("SELECT status FROM lingxios.agent_work_items WHERE kind='teacher_digest'")).rows[0]?.['status'] !== 'completed' && Date.now() < deadline) await delay(25)
      } finally { await digestWorker.stop() }
      const scheduled = (await database.query("SELECT id,status,error FROM lingxios.agent_work_items WHERE kind='teacher_digest'")).rows
      assert.equal(scheduled.length, 1); assert.equal(scheduled[0]!['status'], 'completed', JSON.stringify(scheduled[0]))
      const digestIdentity = { ...waitingIdentity, runId: String(scheduled[0]!['id']) }
      assert.equal((await digestApp.readMessage(digestIdentity))?.body, 'Five learners observed.')
      assert.equal(await digestApp.readDelivery(digestIdentity), 'delivered')
      assert.equal(teacherCalls, 2)
      assert.equal(delivered.at(-1)?.clientMsgNo, 'agent-' + scheduled[0]!['id'])
      // A later digest survives transport failure but cannot send after teacher revocation or schedule pause.
      await database.query("UPDATE lingxios.agent_routines SET next_run_at=NOW()-INTERVAL '2 days'")
      assert.equal(await scheduleTeacherDigests(database, services), 1)
      teacherCalls = 0; sendFailures = 1
      const deliveredBefore = delivered.length
      assert.equal(await digestApp.runNext(), true)
      assert.equal(delivered.length, deliveredBefore)
      teacherDenied = true
      await database.query('UPDATE lingxios.agent_delivery_outbox SET available_at=NOW() WHERE delivered_at IS NULL')
      assert.equal(await digestApp.runNext(), false)
      assert.equal(delivered.length, deliveredBefore)
      teacherDenied = false
      await configureTeacherDigest(database, services, digestRequest, { frequency: 'off' })
      await database.query('UPDATE lingxios.agent_delivery_outbox SET available_at=NOW() WHERE delivered_at IS NULL')
      assert.equal(await digestApp.runNext(), false)
      assert.equal(delivered.length, deliveredBefore)
    } finally { teacherDigest = false; teacherDenied = false; await digestApp.stop() }
    await db.exec("CREATE TABLE teacher_metadata_fixture(id text,company_id text,name text); INSERT INTO teacher_metadata_fixture VALUES('project','t','Original course')")
    teacherWrite = true
    teacherCalls = 0
    approvalMethod = 'teacher-update'
    const teacherUpdate = await app.receive({ ...input, clientMsgNo: approvalMethod, attachmentClientMsgNos: [] })
    assert.equal(await app.runNext(), true)
    assert.equal(teacherCalls, 2)
    assert.equal((await app.readMessage({ ...waitingIdentity, runId: teacherUpdate.id }))?.body, 'Course updated.')
    assert.deepEqual((await db.query('SELECT * FROM teacher_metadata_fixture')).rows, [{ id: 'project', company_id: 't', name: 'Updated course' }])
    assert.deepEqual((await db.query("SELECT name FROM participants WHERE company_id='t' AND id='a'")).rows, [{ name: 'Pulse · Updated course' }])
    const teacherReceipts = (await db.query<Record<string, unknown>>("SELECT ledger.result FROM lingxios.agent_action_ledger ledger JOIN lingxios.agent_action_intents intent USING(idempotency_key) WHERE intent.intent->'action'->>'action' LIKE 'teacher.%' AND intent.intent->>'workId'=$1", [teacherUpdate.id])).rows
    assert.deepEqual(teacherReceipts.map(row => row['result']), [{ ok: true, value: { id: 'project', company_id: 't', name: 'Updated course' } }])
    await db.exec('CREATE TABLE teacher_objectives_fixture(objective jsonb)')
    teacherDraft = true
    teacherCalls = 0
    approvalMethod = 'teacher-draft'
    const drafted = await app.receive({ ...input, clientMsgNo: approvalMethod, attachmentClientMsgNos: [] })
    assert.equal(await app.runNext(), true)
    assert.equal(teacherCalls, 2)
    assert.equal((await app.readMessage({ ...waitingIdentity, runId: drafted.id }))?.body, 'Objective drafted.')
    const objectives = (await db.query<Record<string, unknown>>('SELECT objective FROM teacher_objectives_fixture')).rows.map(row => row['objective'])
    assert.deepEqual(objectives, [{ id: 'objective', courseId: 'course', title: 'Explain fractions', successCriteria: 'Compare two fractions', targetLevel: 3, position: 0, status: 'DRAFT', prerequisiteIds: [] }])
    const draftReceipts = (await db.query<Record<string, unknown>>("SELECT ledger.result FROM lingxios.agent_action_ledger ledger JOIN lingxios.agent_action_intents intent USING(idempotency_key) WHERE intent.intent->'action'->>'action' LIKE 'teacher.%' AND intent.intent->>'workId'=$1", [drafted.id])).rows
    assert.deepEqual(draftReceipts.map(row => row['result']), [{ ok: true, value: objectives }])
    await db.exec('CREATE TABLE teacher_activities_fixture(activity jsonb)')
    teacherActivity = true
    teacherCalls = 0
    approvalMethod = 'teacher-activity'
    const activityWork = await app.receive({ ...input, clientMsgNo: approvalMethod, attachmentClientMsgNos: [] })
    assert.equal(await app.runNext(), true)
    assert.equal(teacherCalls, 2)
    assert.equal((await app.readMessage({ ...waitingIdentity, runId: activityWork.id }))?.body, 'Activity drafted.')
    const activity = { id: 'activity', courseId: 'course', status: 'DRAFT', title: 'Fraction practice', instructions: 'Compare fractions', type: 'PRACTICE', objectiveIds: ['objective'] }
    assert.deepEqual((await db.query('SELECT activity FROM teacher_activities_fixture')).rows, [{ activity }])
    const activityReceipts = (await db.query<Record<string, unknown>>("SELECT ledger.result FROM lingxios.agent_action_ledger ledger JOIN lingxios.agent_action_intents intent USING(idempotency_key) WHERE intent.intent->'action'->>'action' LIKE 'teacher.%' AND intent.intent->>'workId'=$1", [activityWork.id])).rows
    assert.deepEqual(activityReceipts.map(row => row['result']), [{ ok: true, value: activity }])
    await db.exec(`CREATE TABLE courses(id text,company_id text,project_id text);
      CREATE TABLE learning_course_teacher_rooms(company_id text,course_id text,conversation_id text);
      CREATE TABLE learning_knowledge_units(id text,company_id text,project_id text,status text);
      INSERT INTO courses VALUES('course','t','project');
      INSERT INTO learning_course_teacher_rooms VALUES('t','course','s');
      INSERT INTO learning_knowledge_units VALUES('objective','t','project','DRAFT');`)
    teacherApproval = 'publication'
    approvalMethod = 'teacher-publication'
    const publication = await app.receive({ ...input, clientMsgNo: approvalMethod, attachmentClientMsgNos: [] })
    assert.equal(await app.runNext(), true)
    const publicationIdentity = { ...waitingIdentity, runId: publication.id }
    const approval = await app.readOutcome(publicationIdentity)
    assert.equal(approval?.status, 'awaiting_approval')
    assert.ok(approval?.approvalId)
    assert.equal(teacherApprovalWrites, 0)
    assert.deepEqual((await db.query('SELECT status FROM learning_knowledge_units')).rows, [{ status: 'DRAFT' }])
    const teacherDecision = { companyId: 't', userId: 'u', approvalId: approval.approvalId }
    assert.equal((await app.inspectApproval(teacherDecision)).action.action, 'teacher.publish_objective')
    assert.deepEqual(await app.approveTeacher(teacherDecision), { status: 'resumed', workId: publication.id })
    assert.deepEqual(await app.approveTeacher(teacherDecision), { status: 'already_resumed', workId: publication.id })
    assert.equal(teacherApprovalWrites, 1)
    assert.deepEqual((await db.query('SELECT status FROM learning_knowledge_units')).rows, [{ status: 'PUBLISHED' }])
    teacherApprovalResume = true
    assert.equal(await app.runNext(), true)
    assert.equal((await app.readMessage(publicationIdentity))?.body, 'Objective published.')
    await db.exec('CREATE TABLE project_memberships(company_id text,project_id text,user_id text,status text,role text); CREATE TABLE teacher_effect_fixture(kind text)')
    teacherApproval = 'membership'
    teacherApprovalResume = false
    approvalMethod = 'teacher-membership'
    const membershipWork = await app.receive({ ...input, clientMsgNo: approvalMethod, attachmentClientMsgNos: [] })
    assert.equal(await app.runNext(), true)
    const membershipIdentity = { ...waitingIdentity, runId: membershipWork.id }
    const membershipApproval = await app.readOutcome(membershipIdentity)
    assert.equal(membershipApproval?.status, 'awaiting_approval')
    assert.ok(membershipApproval?.approvalId)
    assert.deepEqual((await db.query('SELECT * FROM project_memberships')).rows, [])
    assert.deepEqual(await app.approveTeacher({ companyId: 't', userId: 'u', approvalId: membershipApproval.approvalId }), { status: 'resumed', workId: membershipWork.id })
    assert.deepEqual((await db.query('SELECT user_id,role FROM project_memberships')).rows, [{ user_id: 'new-teacher', role: 'TEACHER' }])
    assert.deepEqual((await db.query('SELECT kind FROM teacher_effect_fixture')).rows, [{ kind: 'teacher_room.sync' }])
    assert.deepEqual((await db.query('SELECT result FROM approvals WHERE id=$1', [membershipApproval.approvalId])).rows, [{ result: { ok: true, enabled: true, channelSync: 'queued' } }])
    teacherApprovalResume = true
    assert.equal(await app.runNext(), true)
    assert.equal((await app.readMessage(membershipIdentity))?.body, 'Teacher added; channel sync queued.')
    await db.exec("CREATE TABLE projects(id text,company_id text,status text); INSERT INTO projects VALUES('project','t','ACTIVE'); ALTER TABLE learning_course_teacher_rooms ADD COLUMN status text DEFAULT 'active'")
    for (const command of ['END', 'ENTER_READ_ONLY'] as const) {
      teacherApproval = command
      teacherApprovalResume = false
      approvalMethod = `teacher-${command}`
      const transitionWork = await app.receive({ ...input, clientMsgNo: approvalMethod, attachmentClientMsgNos: [] })
      const transitionIdentity = { ...waitingIdentity, runId: transitionWork.id }
      const writes: number = teacherApprovalWrites
      assert.equal(await app.runNext(), true)
      assert.equal(teacherApprovalWrites, writes)
      const waiting = await app.readOutcome(transitionIdentity)
      assert.equal(waiting?.status, 'awaiting_approval')
      assert.ok(waiting?.approvalId)
      const decision = { companyId: 't', userId: 'u', approvalId: waiting.approvalId }
      if (command === 'END') {
        assert.deepEqual(await app.approveTeacher(decision), { status: 'resumed', workId: transitionWork.id })
        teacherApprovalResume = true
        assert.equal(await app.runNext(), true)
        assert.equal((await app.readMessage(transitionIdentity))?.body, 'Course ended.')
      } else {
        assert.match(String((await app.inspectApproval(decision)).summary), /closes the teacher room/)
        const stopped = { status: 'continuation_unavailable', workId: transitionWork.id, result: { ok: true, status: 'READ_ONLY', applied: true, teacherRoomStatus: 'closed' } }
        assert.deepEqual(await app.approveTeacher(decision), stopped)
        assert.deepEqual(await app.approveTeacher(decision), stopped)
        assert.equal((await app.readOutcome(transitionIdentity))?.status, 'blocked')
        const events = await app.readEvents(transitionIdentity, 0)
        assert.equal(events.events.filter(event => event.kind === 'approval.continuation_stopped').length, 1)
        assert.deepEqual(events.events.find(event => event.kind === 'approval.continuation_stopped')?.data['goalOutcome'], await app.readOutcome(transitionIdentity))
        assert.deepEqual((await app.inspectApproval(decision)).result, stopped.result)
        assert.equal(await app.readMessage(transitionIdentity), null)
        assert.equal((await db.query<{ status: string }>('SELECT status FROM lingxios.agent_work_items WHERE id=$1', [transitionWork.id])).rows[0]?.status, 'completed')
      }
      assert.equal(teacherApprovalWrites, writes + 1)
    }
    await db.exec('DELETE FROM learning_project_teacher_agents')
    teacherApproval = undefined
    teacherCalls = undefined
    inputMode = 'reply'
    approvalMethod = 'semantic-read'
    const semanticApp = await createLingxiLoop({ ...invalidOptions, worker: { id: 'loop-test' },
      model: { id: 'test', apiKey: 'test', baseUrl: `http://127.0.0.1:${address.port}` }, kernel: { homesRoot: directory },
      embeddings: { id: 'embedding-test', apiKey: 'test', baseUrl: `http://127.0.0.1:${address.port}`, dimensions: 2 } })
    try {
      await semanticApp.receive({ ...input, clientMsgNo: approvalMethod })
      assert.equal(await semanticApp.runNext(), true)
      const indexPort = await semanticApp.listenControlPlane({ serviceToken: 'index-worker-secret', port: 0 })
      const indexWorker = await startWorker({ ...process.env, AGENT_OS_CONTROL_PLANE_URL: `http://127.0.0.1:${indexPort}`,
        AGENT_OS_SERVICE_TOKEN: 'index-worker-secret', AGENT_OS_WORKER_ID: 'loop-test', AGENT_OS_WORKER_PORT: '0',
        AGENT_OS_MODEL: 'test', AGENT_OS_MODEL_API_KEY: 'test', AGENT_OS_MODEL_BASE_URL: `http://127.0.0.1:${address.port}`,
        AGENT_OS_HOMES_ROOT: directory, AGENT_OS_POLL_IDLE_MS: '50', AGENT_OS_MAX_CONCURRENT_RUNS: '1' })
      try {
        const deadline = Date.now() + 10_000
        const unfinished = async () => (await database.query("SELECT id FROM lingxios.agent_work_items WHERE kind IN ('memory_index','memory_synthesis') AND status IN ('queued','leased')")).rows.length
        while (await unfinished() && Date.now() < deadline) await delay(25)
        assert.equal(await unfinished(), 0)
      } finally { await indexWorker.stop() }
      assert.equal(await semanticApp.runNext(), false)
      assert.deepEqual((await database.query("SELECT status FROM lingxios.agent_work_items WHERE kind='memory_index'")).rows, [{ status: 'completed' }])
      approvalMethod = 'semantic-read-2'
      const semanticRequest = await semanticApp.receive({ ...input, clientMsgNo: approvalMethod })
      assert.equal(await semanticApp.runNext(), true)
      const trace = (await database.query("SELECT data->'memorySnapshot' AS snapshot FROM lingxios.agent_run_events WHERE run_id=$1 AND kind='model.started'", [semanticRequest.id])).rows
      assert.match(JSON.stringify(trace), /"retrieval":"semantic"/)
      assert.match(JSON.stringify(trace), /Source saved for later reference/)
      assert.equal(await semanticApp.runNext(), true)
    } finally { await semanticApp.stop() }
  } finally {
    await app.stop()
    await db.close()
    await new Promise<void>((resolve) => { server.close(() => resolve()); server.closeAllConnections() })
    await rm(directory, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 })
  }
})


it('scopes native polls and preserves agent identity and human authorization', async () => {
  const { executePoll } = await import('../src/integrations/lingxiloop/polls.js')
  const work = { id: 'w', tenantId: 't', agentId: 'a', principalId: 'u', sessionId: 's', kind: 'turn' as const, lane: 'interactive' as const, triggerRef: 'm', fence: 1, homeEpoch: 1 }
  const calls: unknown[] = []
  const services = {
    permissionService: { assertCan: async (request: unknown) => { calls.push(request) } },
    pollApplication: {
      conversationId: async (_tenant: string, id: string) => id === 'p' ? 's' : 'other',
      create: async (input: unknown) => { calls.push(input); return { messageId: 'p' } },
      vote: async (input: unknown) => { calls.push(input) },
      close: async (input: unknown) => { calls.push(input) },
      show: async () => ({ messageId: 'p' }),
    },
  }
  const action = { runId: 'w', cellId: 'c', callIndex: 0, action: 'polls.create', args: { question: 'Which?', options: ['A', 'B'] }, idempotencyKey: 'stable' }
  await executePoll(work, action, services)
  assert.deepEqual(calls, [
    { actorUserId: 'u', companyId: 't', action: 'poll:create', resource: { type: 'conversation', id: 's' } },
    { companyId: 't', actorId: 'a', conversationId: 's', question: 'Which?', options: ['A', 'B'], mode: 'single', expiresInMinutes: null, idempotencyKey: 'stable' },
  ])
  for (const method of ['vote', 'close', 'show']) {
    await assert.rejects(executePoll(work, { ...action, action: `polls.${method}`, args: { messageId: 'foreign' } }, services), /outside/)
  }
  await assert.rejects(executePoll(work, { ...action, args: { ...action.args, actorId: 'forged' } }, services), /unknown/)
  await assert.rejects(executePoll(work, { ...action, args: { ...action.args, mode: 'invalid' } }, services), /mode/)
  assert.equal(calls.length, 3)
  calls.length = 0
  await executePoll(work, { ...action, action: 'polls.vote', args: { messageId: 'p', optionIds: ['o'] } }, services)
  await executePoll(work, { ...action, action: 'polls.close', args: { messageId: 'p' } }, services)
  assert.deepEqual(await executePoll(work, { ...action, action: 'polls.show', args: { messageId: 'p' } }, services), { messageId: 'p' })
  assert.deepEqual(calls, [
    { actorUserId: 'u', companyId: 't', action: 'poll:vote', resource: { type: 'poll', id: 'p' } },
    { companyId: 't', actorId: 'a', messageId: 'p', voterKind: 'agent', optionIds: ['o'] },
    { actorUserId: 'u', companyId: 't', action: 'poll:close', resource: { type: 'poll', id: 'p' } },
    { companyId: 't', actorId: 'a', messageId: 'p', reason: 'manual' },
    { actorUserId: 'u', companyId: 't', action: 'poll:read', resource: { type: 'poll', id: 'p' } },
  ])
  const denied = { ...services, permissionService: { assertCan: async () => { throw new Error('permission denied') } } }
  for (const method of ['create', 'vote', 'close', 'show']) {
    await assert.rejects(executePoll(work, { ...action, action: `polls.${method}`, args: method === 'create' ? action.args : { messageId: 'p' } }, denied), /permission denied/)
  }
  assert.equal(calls.length, 5)
})



it('uses live native project and learner scope for learning reads', async () => {
  const { executeLearning } = await import('../src/integrations/lingxiloop/learning.js')
  const work = { id: 'w', tenantId: 't', agentId: 'a', principalId: 'u', sessionId: 's', kind: 'turn', lane: 'interactive' as const, triggerRef: 'm', fence: 1, homeEpoch: 1 }
  const calls: unknown[] = []
  const services = {
    permissionService: { assertCan: async (input: unknown) => { calls.push(input) } },
    learning: { ...unusedMissionServices,
      createKnowledgeUnits: async (input: unknown) => { calls.push(input); return { status: 'DRAFT' } },
      draftActivity: async (input: unknown) => { calls.push(input); return { status: 'DRAFT' } },
      updateMissionStep: async (...args: unknown[]) => { calls.push(args); return 'updated' },
      addMissionSteps: async (...args: unknown[]) => { calls.push(args); return 'steps-added' },
      finishMissionPlanning: async (...args: unknown[]) => { calls.push(args); return 'active' },
      completeMission: async (...args: unknown[]) => { calls.push(args); return 'completed' },
      loadLearningTurnContext: async (_work: unknown, actor?: string) => { assert.equal(actor, 'u'); return { project: { id: 'project' }, learnerId: 'learner', knowledgeUnits: [], due: [] } },
      getMission: async (...args: string[]) => { calls.push(args); return { id: 'mission' } },
      getActivity: async (...args: string[]) => { calls.push(args); return { id: 'activity' } },
    },
  }
  const action = { runId: 'w', cellId: 'c', callIndex: 0, idempotencyKey: 'key', action: 'learning.get_mission', args: { missionId: 'mission' } }
  assert.deepEqual(await executeLearning(work, action, services), { id: 'mission' })
  assert.deepEqual(calls, [{ actorUserId: 'u', companyId: 't', action: 'learning:read', resource: { type: 'conversation', id: 's' } }, ['mission', 't', 'project', 'learner', 's']])
  assert.equal(await executeLearning(work, { ...action, args: {} }, services), null)
  calls.length = 0
  const step = { kind: 'CHECK', description: 'Explain the result', successCriteria: 'Explain the derivation', knowledgeUnitId: 'unit' }
  const add = { ...action, action: 'learning.add_steps', args: { missionId: 'mission', steps: [step] } }
  assert.equal(await executeLearning(work, add, services), 'steps-added')
  assert.equal((calls[0] as { action: string }).action, 'learning:submit')
  assert.deepEqual((calls[2] as unknown[]).slice(1), ['mission', [step]])
  for (const steps of [[], Array(65).fill(step), [null], [{ ...step, kind: 'INVALID' }], [{ ...step, description: '' }], [{ ...step, successCriteria: 3 }], [{ ...step, knowledgeUnitId: false }], [{ ...step, learnerId: 'forged' }]]) {
    const before: number = calls.filter(Array.isArray).filter(call => call[1] === 'mission').length
    await assert.rejects(executeLearning(work, { ...add, args: { ...add.args, steps } }, services))
    assert.equal(calls.filter(Array.isArray).filter(call => call[1] === 'mission').length, before)
  }
  calls.length = 0
  const update = { ...action, action: 'learning.update_step', args: { missionId: 'mission', stepId: 'step', status: 'COMPLETED', outcome: 'derived result', attemptId: 'attempt' } }
  assert.equal(await executeLearning(work, update, services), 'updated')
  assert.equal((calls[0] as { action: string }).action, 'learning:submit')
  assert.deepEqual((calls[2] as unknown[])[1], update.args)
  for (const patch of [{ status: 'INVALID' }, { stepId: '' }, { outcome: 1 }, { attemptId: '' }, { sourceEvidenceId: false }]) {
    const before: number = calls.length
    await assert.rejects(executeLearning(work, { ...update, args: { ...update.args, ...patch } }, services))
    assert.equal(calls.length, before + 2)
  }
  await assert.rejects(executeLearning(work, update, { ...services, learning: { ...services.learning, updateMissionStep: async () => { throw new Error('completion evidence not found') } } }), /completion evidence not found/)
  for (const [method, result] of [['finish_planning', 'active'], ['complete_mission', 'completed']]) {
    calls.length = 0
    const mutation = { ...action, action: `learning.${method}` }
    assert.equal(await executeLearning(work, mutation, services), result)
    assert.equal((calls[0] as { action: string }).action, 'learning:submit')
    assert.deepEqual(calls[1], ['mission', 't', 'project', 'learner', 's'])
    assert.equal(calls.length, 3)
    const denied = { ...services, learning: { ...services.learning, getMission: async () => { throw new Error('mission not found') } } }
    await assert.rejects(executeLearning(work, mutation, denied), /mission not found/)
    assert.equal(calls.length, 4)
    const blocked = { ...services, learning: { ...services.learning, finishMissionPlanning: async () => { throw new Error('planning gate blocked') }, completeMission: async () => { throw new Error('mission has unresolved steps') } } }
    await assert.rejects(executeLearning(work, mutation, blocked), /planning gate blocked|unresolved steps/)
  }
  const unit = { title: 'Fractions', successCriteria: 'Compare fractions', targetLevel: 3, prerequisiteKnowledgeUnitIds: ['prior'] }
  for (const [method, args] of [
    ['draft_knowledge_units', { knowledgeUnits: [unit] }],
    ['draft_activity', { title: 'Practice', instructions: 'Compare the fractions', kind: 'PRACTICE', knowledgeUnitIds: ['unit'], rubric: [], targetLevel: 2 }],
  ] as const) {
    calls.length = 0
    const draft = { ...action, action: `learning.${method}`, args }
    assert.deepEqual(await executeLearning(work, draft, services), { status: 'DRAFT' })
    assert.deepEqual(calls, [
      { actorUserId: 'u', companyId: 't', action: 'learning:submit', resource: { type: 'conversation', id: 's' } },
      { actorUserId: 'u', companyId: 't', action: 'learning:submit', resource: { type: 'project', id: 'project' } },
      { companyId: 't', projectId: 'project', actorId: 'a', actorKind: 'agent', ...args },
    ])
    await assert.rejects(executeLearning(work, draft, { ...services, permissionService: { assertCan: async () => { throw new Error('denied') } } }), /denied/)
    await assert.rejects(executeLearning(work, { ...draft, args: { ...args, projectId: 'foreign' } }, services), /unknown/)
  }
  for (const knowledgeUnits of [[], [null], [{ ...unit, targetLevel: '3' }], [{ ...unit, prerequisiteKnowledgeUnitIds: ['prior', 'prior'] }], [{ ...unit, title: '' }], [{ ...unit, learnerId: 'other' }]]) {
    const before: number = calls.filter(value => (value as { actorKind?: string })?.actorKind === 'agent').length
    await assert.rejects(executeLearning(work, { ...action, action: 'learning.draft_knowledge_units', args: { knowledgeUnits } }, services))
    assert.equal(calls.filter(value => (value as { actorKind?: string })?.actorKind === 'agent').length, before)
  }
  for (const patch of [{ kind: 'unknown' }, { targetLevel: 0 }, { dueAt: 'not-a-date' }, { rubric: {} }, { knowledgeUnitIds: [3] }, { evaluationMode: 'auto' }]) {
    await assert.rejects(executeLearning(work, { ...action, action: 'learning.draft_activity', args: { title: 'Practice', instructions: 'Compare fractions', kind: 'PRACTICE', ...patch } }, services))
  }
  await assert.rejects(executeLearning(work, { ...action, args: { ...action.args, learnerId: 'forged' } }, services), /unknown/)
  await assert.rejects(executeLearning(work, action, { ...services, learning: { ...services.learning, loadLearningTurnContext: async () => undefined } }), /not bound/)
})



it('authorizes canvas reads before the native reader and rejects supplied scope', async () => {
  const work = { id: 'w', tenantId: 't', agentId: 'a', principalId: 'u', sessionId: 's', kind: 'turn', lane: 'interactive' as const, triggerRef: 'm', fence: 1, homeEpoch: 1 }
  const action = { runId: 'w', cellId: 'c', callIndex: 0, idempotencyKey: 'key', action: 'canvas.current', args: {} }
  const calls: unknown[] = []
  const services = {
    canvas: { addCanvasComment: async () => { throw new Error("unused comment") }, canvasCommentRequestSchema: { parse: () => { throw new Error("unused comment schema") } }, listCanvasAvailableAgents: async () => [], deleteCanvasFrame: async () => { throw new Error('unexpected delete') }, appendCanvasFrameContent: async () => { throw new Error('unexpected append') }, updateCanvasFrame: async () => { throw new Error('unexpected update') }, canvasFrameUpdateRequestSchema: { parse: (input: unknown) => input as Record<string, unknown> }, createCanvasFrame: async () => { throw new Error('unexpected write') }, canvasFrameCreateRequestSchema: { parse: (input: unknown) => input as Record<string, unknown> }, getConversationCanvas: async (...args: string[]) => { calls.push(args); return null } },
    permissionService: { assertCan: async (request: unknown) => { calls.push(request) } },
  }
  assert.equal(await executeCanvas(work, action, services), null)
  assert.deepEqual(calls, [{ actorUserId: 'u', companyId: 't', action: 'conversation:read', resource: { type: 'conversation', id: 's' } }, ['t', 's', 'u']])
  calls.length = 0
  for (const args of [{ canvasId: 'foreign' }, { companyId: 'foreign' }, { actorId: 'admin' }]) {
    await assert.rejects(executeCanvas(work, { ...action, args }, services), /no arguments/)
  }
  const { principalId: _principalId, ...anonymousWork } = work
  await assert.rejects(executeCanvas(anonymousWork, action, services), /authorization principal/)
  await assert.rejects(executeCanvas(work, action, { ...services, permissionService: { assertCan: async () => { throw new Error('denied') } } }), /denied/)
  assert.deepEqual(calls, [])
  await assert.rejects(executeCanvas(work, action, { ...services, canvas: { addCanvasComment: async () => { throw new Error("unused comment") }, canvasCommentRequestSchema: { parse: () => { throw new Error("unused comment schema") } }, listCanvasAvailableAgents: async () => [], deleteCanvasFrame: async () => { throw new Error('unexpected delete') }, appendCanvasFrameContent: async () => { throw new Error('unexpected append') }, updateCanvasFrame: async () => { throw new Error('unexpected update') }, canvasFrameUpdateRequestSchema: { parse: (input: unknown) => input as Record<string, unknown> }, createCanvasFrame: async () => { throw new Error('unexpected write') }, canvasFrameCreateRequestSchema: { parse: (input: unknown) => input as Record<string, unknown> }, getConversationCanvas: async () => { throw new Error('native read failed') } } }), /native read failed/)
})

it('creates a frame only in the authorized current canvas with stable action identity', async () => {
  const work = { id: 'w', tenantId: 't', agentId: 'a', principalId: 'u', sessionId: 's', kind: 'turn', lane: 'interactive' as const, triggerRef: 'm', fence: 1, homeEpoch: 1 }
  const action = { runId: 'w', cellId: 'c', callIndex: 0, idempotencyKey: 'stable', action: 'canvas.create_frame', args: { frame: { type: 'markdown', content: 'Answer' } } }
  const writes: unknown[] = [], permissions: unknown[] = []
  const snapshot = { id: 'board', companyId: 't', conversationId: 's' }
  const services = {
    canvas: { addCanvasComment: async () => { throw new Error("unused comment") }, canvasCommentRequestSchema: { parse: () => { throw new Error("unused comment schema") } },
      listCanvasAvailableAgents: async () => [], deleteCanvasFrame: async () => { throw new Error('unexpected delete') }, appendCanvasFrameContent: async () => { throw new Error('unexpected append') }, updateCanvasFrame: async () => { throw new Error('unexpected update') },
      canvasFrameUpdateRequestSchema: { parse: (input: unknown) => input as Record<string, unknown> },
      getConversationCanvas: async () => snapshot,
      canvasFrameCreateRequestSchema: { parse: (input: unknown) => input as Record<string, unknown> },
      createCanvasFrame: async (input: unknown) => { writes.push(input); return { id: 'frame', revision: 1 } },
    },
    permissionService: { assertCan: async (input: unknown) => { permissions.push(input) } },
  }
  assert.deepEqual(await executeCanvas(work, action, services), { id: 'frame', revision: 1 })
  assert.deepEqual(writes, [{ companyId: 't', actorId: 'a', actorKind: 'agent', idempotencyKey: 'stable', canvasId: 'board', frame: { type: 'markdown', content: 'Answer', canvasId: 'board' } }])
  assert.deepEqual(permissions, [
    { actorUserId: 'u', companyId: 't', action: 'conversation:read', resource: { type: 'conversation', id: 's' } },
    { actorUserId: 'u', companyId: 't', action: 'canvas:write', resource: { type: 'canvas', id: 'board' } },
  ])
  writes.length = 0
  for (const value of [null, { ...snapshot, companyId: 'other' }, { ...snapshot, conversationId: 'other' }]) {
    await assert.rejects(executeCanvas(work, action, { ...services, canvas: { ...services.canvas, getConversationCanvas: async () => value } }), /canvas not found/)
  }
  for (const frame of [null, [], 'text', { canvasId: 'other' }]) {
    await assert.rejects(executeCanvas(work, { ...action, args: { frame } }, services), /frame must|supplied by/)
  }
  await assert.rejects(executeCanvas(work, action, { ...services, canvas: { ...services.canvas, canvasFrameCreateRequestSchema: { parse: () => { throw new Error('invalid native frame') } } } }), /invalid native frame/)
  await assert.rejects(executeCanvas(work, action, { ...services, permissionService: { assertCan: async (input) => { if (input.action === 'canvas:write') throw new Error('write denied') } } }), /write denied/)
  assert.deepEqual(writes, [])
  await assert.rejects(executeCanvas(work, action, { ...services, canvas: { ...services.canvas, createCanvasFrame: async () => { throw new Error('native failure') } } }), /native failure/)
  const commentWrites: unknown[] = []
  const comments = { ...services, canvas: { ...services.canvas,
    getConversationCanvas: async () => ({ ...snapshot, frames: [{ id: 'frame' }] }),
    canvasCommentRequestSchema: { parse: (input: unknown) => input as { canvasId: string; frameId?: string; body: string } },
    addCanvasComment: async (input: unknown) => { commentWrites.push(input); return { id: 'comment' } },
  } }
  const commentAction = { ...action, action: 'canvas.add_comment', args: { body: 'Review this reasoning', frameId: 'frame' } }
  permissions.length = 0
  assert.deepEqual(await executeCanvas(work, commentAction, comments), { id: 'comment' })
  assert.deepEqual(commentWrites, [{ companyId: 't', actorId: 'a', actorKind: 'agent', canvasId: 'board', frameId: 'frame', body: 'Review this reasoning' }])
  assert.deepEqual(permissions.at(-1), { actorUserId: 'u', companyId: 't', action: 'canvas:write', resource: { type: 'canvas_frame', id: 'frame' } })
  for (const args of [{ body: 'x', frameId: 'foreign' }, { body: 'x', canvasId: 'foreign' }, { body: 'x', actorId: 'forged' }]) {
    await assert.rejects(executeCanvas(work, { ...commentAction, args }, comments), /outside|unknown/)
  }
  await assert.rejects(executeCanvas(work, commentAction, { ...comments, permissionService: { assertCan: async () => { throw new Error('denied') } } }), /denied/)
  assert.equal(commentWrites.length, 1)
})
