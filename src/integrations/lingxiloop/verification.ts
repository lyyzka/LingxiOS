import { createHash } from 'node:crypto'
import { isDeepStrictEqual } from 'node:util'
import type { SqlPool } from '../../control-plane/pg-store.js'
import type { HostAction, WorkItem } from '../../protocol/types.js'
import type { VerificationRecord } from '../../outcome/verification.js'
import type { LingxiLoopServices } from './service-contracts.js'
import { executeCalendar } from './calendar.js'
import { executeDocument } from './documents.js'
import { executeDocumentContent } from './document-content.js'
import { executeMemory } from './memory.js'
import { executeRoutine } from './routines.js'
import { executeKnowledge } from './actions.js'
import { executePresentation } from './presentations.js'
import { executeHandoff } from './handoffs.js'
import { executePoll } from './polls.js'
import { executeCanvas } from './canvas.js'
import { executeLearning } from './learning.js'
import { readAttempts } from './learning-attempts.js'
import { executeTeacher } from './teacher.js'
import { teacherContext } from './teacher-context.js'
import { TEACHER_APPROVAL_TARGETS } from './approvals.js'
import { executeChat } from './chat.js'
import { executeEmail } from './email.js'

type Check = Omit<VerificationRecord, 'checker'>
const object = (value: unknown): Record<string, unknown> => value && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : {}
const json = (value: unknown): unknown => value === undefined ? null : JSON.parse(JSON.stringify(value))
const digest = (value: unknown) => createHash('sha256').update(JSON.stringify(json(value))).digest('hex')

function compare(resource: string, expected: Record<string, unknown>, observed: unknown): Check {
  const actual = object(observed), fields = Object.keys(expected)
  const unavailable = !fields.length || fields.some(key => expected[key] === undefined || !Object.hasOwn(actual, key))
  const mismatches = fields.filter(key => !isDeepStrictEqual(json(expected[key]), json(actual[key])))
  const selected = Object.fromEntries(fields.map(key => [key, actual[key]]))
  return { status: unavailable ? 'inconclusive' : mismatches.length ? 'failed' : 'passed', evidence: {
    scope: 'current_resource_postconditions', resource, fields, mismatches,
    expectedHash: digest(expected), observedHash: digest(selected),
    ...(unavailable ? { reason: 'The resource did not expose every required postcondition' }
      : mismatches.length ? { reason: `Current resource differs in: ${mismatches.join(', ')}` } : {}),
  } }
}

function select(value: unknown, keys: readonly string[]) {
  const row = object(value)
  return Object.fromEntries(keys.filter(key => Object.hasOwn(row, key)).map(key => [key,row[key]]))
}

/** Reuses native scope and authorization checks; a failed read never proves deletion. */
export async function verifyLingxiLoopResult(database: SqlPool, services: LingxiLoopServices,
  work: Omit<WorkItem, 'leaseToken'>, action: HostAction, value: unknown, channelType: number): Promise<Check> {
  const args = action.args, result = object(value)
  const read = (name: string, args: Record<string, unknown>): HostAction => ({ ...action, action: name, args })
  if (action.action === 'calendar.update' || action.action === 'calendar.create') {
    const expected = object(result['event']), eventId = args['eventId'] ?? result['eventId'] ?? expected['id']
    const observed = await executeCalendar(database, services, work, read('calendar.get', { eventId }))
    return compare(`calendar:${eventId}`, { id: eventId, ...select(expected, Object.keys(action.action === 'calendar.update' ? object(args['patch']) : args)) }, observed)
  }
  if (action.action === 'calendar.delete' || action.action === 'documents.delete') {
    if (!work.principalId || result['deleted'] !== true) throw new Error('deletion receipt is missing')
    const document = action.action === 'documents.delete', id = args[document ? 'documentId' : 'eventId']
    await services.permissionService.assertCan({ actorUserId: work.principalId, companyId: work.tenantId,
      action: 'conversation:read', resource: { type: 'conversation', id: work.sessionId } })
    const room = await database.query('SELECT project_id FROM conversations WHERE company_id=$1 AND id=$2', [work.tenantId,work.sessionId])
    const projectId = room.rows[0]?.['project_id']
    if (typeof projectId !== 'string') throw new Error('project scope is unavailable')
    await services.permissionService.assertCan({ actorUserId: work.principalId, companyId: work.tenantId, projectId,
      action: document ? 'document:read' : 'calendar:read', resource: { type: 'project', id: projectId } })
    const rows = await database.query(document
      ? 'SELECT id FROM documents WHERE company_id=$1 AND project_id=$2 AND id=$3'
      : 'SELECT id FROM calendar_events WHERE company_id=$1 AND project_id=$2 AND id=$3', [work.tenantId,projectId,id])
    return compare(`${document ? 'document' : 'calendar'}:${id}`, { deleted: true }, { deleted: rows.rows.length === 0 })
  }
  if (action.action.startsWith('documents.')) {
    const documentId = args['documentId'] ?? result['documentId']
    const query = read('documents.read', { documentId })
    const observed = services.documents?.writes?.content ? await executeDocumentContent(database, services, work, query)
      : await executeDocument(database, services, work, query)
    if (action.action === 'documents.rename') return compare(`document:${documentId}`, { title: result['title'] }, observed)
    if (action.action === 'documents.create') return compare(`document:${documentId}`,
      { id: documentId, title: args['title'], body: args['body'] }, observed)
    // Native edits return the new revision. Read the content independently and require that exact revision.
    return compare(`document:${documentId}`, { id: documentId, revision: result['revision'], bodyTruncated: false }, observed)
  }
  if (action.action.startsWith('memory.')) {
    const scope = args['scope'] ?? 'course', learnerId = args['learnerId']
    await executeMemory(work, read('memory.list', { scope, ...(learnerId ? { learnerId } : {}), limit: 1 }), services, database)
    const id = args['id'] ?? result['id'], scopeId = scope === 'course' ? work.sessionId : scope === 'agent_role' ? work.agentId : learnerId
    const current = await database.query('SELECT * FROM lingxios.agent_memories WHERE tenant_id=$1 AND id=$2 AND scope_type=$3 AND scope_id=$4',
      [work.tenantId,id,scope,scopeId])
    return action.action === 'memory.delete'
      ? compare(`memory:${id}`, { deleted: true }, { deleted: result['deleted'] === true && current.rows.length === 0 })
      : compare(`memory:${id}`, select(value, ['id','body','kind','origin','pinned','version','valid_until']), current.rows[0])
  }
  if (action.action.startsWith('routines.')) {
    const current = object(await executeRoutine(database, services, work, read('routines.list', {})))
    const rows = current['routines'], id = args['routineId'] ?? result['id']
    return compare(`routine:${id}`, select(value, ['id','principal_id','title','instructions','schedule','timezone','status','version']),
      Array.isArray(rows) ? rows.find(row => object(row)['id'] === id) : undefined)
  }
  if (action.action.startsWith('knowledge.')) {
    const rows = await executeKnowledge(work, read('knowledge.list_sources', {}), services, channelType)
    const id = args['sourceId'] ?? result['id'], source = Array.isArray(rows) ? rows.find(row => object(row)['id'] === id) : undefined
    if (action.action === 'knowledge.delete_source') return { status: 'inconclusive', evidence: {
      resource: `knowledge:${id}`, reason: 'Visible source absence cannot establish deletion of the authoritative source' } }
    const expected = action.action === 'knowledge.set_source_enabled' ? { id, enabled: args['enabled'] }
      : { id, ...(typeof args['title'] === 'string' ? { title: args['title'].slice(0,200) } : {}) }
    return compare(`knowledge:${id}`, expected, source)
  }
  if (action.action.startsWith('presentations.')) {
    const id = args['presentationId'] ?? result['id'] ?? object(result['presentation'])['id']
    const observed = await executePresentation(work, read('presentations.get', { presentationId: id }), services)
    return compare(`presentation:${id}`, select(value, ['id','status','revision']), observed)
  }
  if (action.action.startsWith('handoffs.')) {
    const id = args['handoffId'] ?? result['id'], rows = await executeHandoff(work, read('handoffs.list', {}), services)
    return compare(`handoff:${id}`, select(value, ['id','title','status','toAgentId','note']),
      Array.isArray(rows) ? rows.find(row => object(row)['id'] === id) : undefined)
  }
  if (action.action.startsWith('polls.')) {
    const messageId = args['messageId'] ?? result['messageId'], snapshot = object(await executePoll(work, read('polls.show', { messageId }), services))
    if (action.action === 'polls.create') return compare(`poll:${messageId}`, { question: args['question'], mode: args['mode'] ?? 'single', options: result['poll'] && object(result['poll'])['options'] }, snapshot['poll'])
    if (action.action === 'polls.close') return compare(`poll:${messageId}`, { closed: true }, { closed: Boolean(object(snapshot['poll'])['closedAt']) })
    const votes = await database.query(`SELECT option_id FROM im_poll_votes WHERE company_id=$1 AND poll_client_msg_no=$2
      AND voter_participant_id=$3 AND voter_kind='agent' ORDER BY option_id`, [work.tenantId,messageId,work.agentId])
    return compare(`poll:${messageId}:voter:${work.agentId}`, { optionIds: [...new Set(args['optionIds'] as string[])].sort() },
      { optionIds: votes.rows.map(row => row['option_id']) })
  }
  if (action.action.startsWith('canvas.')) {
    const snapshot = object(await executeCanvas(work, read('canvas.current', {}), services))
    const canvasId = result['canvasId'] ?? snapshot['id']
    if (action.action === 'canvas.add_comment') {
      const rows = snapshot['comments']
      return compare(`canvas-comment:${result['id']}`, select(value, ['id','canvasId','frameId','authorId','body']),
        Array.isArray(rows) ? rows.find(row => object(row)['id'] === result['id']) : undefined)
    }
    if (['canvas.create_frame','canvas.update_frame','canvas.append_content','canvas.delete_frame'].includes(action.action)) {
      const frameId = args['frameId'] ?? result['id'], frames = snapshot['frames']
      if (!Array.isArray(frames) || snapshot['id'] !== canvasId) throw new Error('current Canvas frames are unavailable')
      const frame = frames.find(row => object(row)['id'] === frameId)
      return action.action === 'canvas.delete_frame'
        ? compare(`canvas-frame:${frameId}`, { deleted: true }, { deleted: !frame })
        : compare(`canvas-frame:${frameId}`, select(value, ['id','type','title','content','data','revision']), frame)
    }
    if (action.action === 'canvas.submit_report') {
      const report = await database.query(`SELECT id AS "reportId",assignment_id AS "assignmentId",evidence_id AS "evidenceId",verdict,unresolved
        FROM canvas_assignment_reports WHERE id=$1 AND canvas_id=$2 AND company_id=$3 AND author_agent_id=$4`,
      [result['reportId'],canvasId,work.tenantId,work.agentId])
      return compare(`canvas-report:${result['reportId']}`, select(value, ['reportId','assignmentId','evidenceId','verdict','unresolved']), report.rows[0])
    }
    if (action.action === 'canvas.stop_workspace') return compare(`canvas:${canvasId}`, { id: canvasId, status: 'stopped' }, snapshot)
    if (action.action === 'canvas.steer_assignment' || action.action === 'canvas.stop_assignment') {
      const current = await database.query(`SELECT a.assignment,a.status,w.cancel_requested_at IS NOT NULL AS cancelled
        FROM canvas_agent_assignments a JOIN lingxios.agent_work_items w ON w.id=a.work_id
        WHERE a.id=$1 AND a.canvas_id=$2 AND w.tenant_id=$3`, [result['assignmentId'],canvasId,work.tenantId])
      return compare(`canvas-assignment:${result['assignmentId']}`, action.action === 'canvas.stop_assignment'
        ? { status: 'cancelled', cancelled: true } : { assignment: args['text'] }, current.rows[0])
    }
    const assignments = result['assignments']
    if (Array.isArray(assignments) && assignments.length) {
      const expected = assignments.map(row => select(row, ['id','agentId','assignment','workId']))
      const rows = Array.isArray(snapshot['assignments']) ? snapshot['assignments'] : []
      return compare(`canvas:${canvasId}`, { assignments: expected }, { assignments: expected.map(item =>
        select(rows.find(row => object(row)['id'] === item['id']), Object.keys(item))) })
    }
  }
  if (action.action.startsWith('learning.')) {
    if (action.action === 'learning.record_attempt' || action.action === 'learning.propose_evaluation') {
      const attemptId = args['attemptId'] ?? result['id'], observed = object(await readAttempts(database, services, work, read('learning.get_attempt', { attemptId })))
      if (action.action === 'learning.record_attempt') return compare(`attempt:${attemptId}`,
        select(value, ['id','activityId','missionStepId','assistance','evidenceId']),
        { ...observed, activityId: observed['activity_id'], missionStepId: observed['mission_step_id'], evidenceId: observed['evidence_id'] })
      const evaluations = observed['evaluations'], target = Array.isArray(evaluations) ? evaluations.find(row => object(row)['id'] === result['id']) : undefined
      return compare(`evaluation:${result['id']}`, { id: result['id'], demonstrated_level: args['demonstratedLevel'], confidence: args['confidence'], rubric_results: args['rubricResults'] }, target)
    }
    if (action.action === 'learning.draft_activity') {
      const observed = await executeLearning(work, read('learning.get_activity', { activityId: result['id'] }), services)
      return compare(`activity:${result['id']}`, select(value, ['id','title','instructions','kind','status']), observed)
    }
    if (action.action === 'learning.draft_knowledge_units') {
      const rows = await executeLearning(work, read('learning.list_knowledge_units', {}), services)
      const expected = Array.isArray(value) ? value.map(row => select(row, ['id','title','successCriteria','targetLevel','status'])) : []
      return compare(`learning-units:${work.sessionId}`, { units: expected }, { units: expected.map(item =>
        select(Array.isArray(rows) ? rows.find(row => object(row)['id'] === item['id']) : undefined, Object.keys(item))) })
    }
    const missionId = args['missionId'] ?? result['id'], observed = object(await executeLearning(work, read('learning.get_mission', { missionId }), services))
    if (action.action === 'learning.update_step') {
      const steps = observed['steps']
      return compare(`mission-step:${args['stepId']}`, { id: args['stepId'], ...select(args, ['status','outcome','sourceEvidenceId','attemptId']) },
        Array.isArray(steps) ? steps.find(row => object(row)['id'] === args['stepId']) : undefined)
    }
    return compare(`mission:${missionId}`, select(value, ['id','goal','successCriteria','kind','status','steps']), observed)
  }
  if (action.action.startsWith('teacher.')) {
    const method = action.action.slice('teacher.'.length), context = await teacherContext(work, services, database)
    const target = TEACHER_APPROVAL_TARGETS[method as keyof typeof TEACHER_APPROVAL_TARGETS]
    if (target) {
      const rows = await database.query(`SELECT id,status FROM ${target.table} WHERE company_id=$1 AND project_id=$2 AND id=$3`,
        [work.tenantId,context.agent.projectId,args[target.key]])
      return compare(`teacher-resource:${args[target.key]}`, { id: args[target.key], status: method === 'review_evaluation' && args['decision'] === 'reject' ? 'REJECTED' : target.status }, rows.rows[0])
    }
    if (method === 'configure_digest') return compare(`teacher-digest:${work.sessionId}`,
      select(value, ['frequency','localTime','timezone','weekday','status','version']),
      await executeTeacher(work, read('teacher.get_digest_schedule', {}), services, database))
    if (method === 'update_course') {
      const rows = await database.query('SELECT id,name,description FROM projects WHERE company_id=$1 AND id=$2', [work.tenantId,context.agent.projectId])
      return compare(`course:${context.course.id}`, select(value, ['id','name','description']), rows.rows[0])
    }
    if (method === 'set_teacher_membership' || method === 'set_learner_membership') {
      const rows = await database.query(`SELECT 1 FROM project_memberships WHERE company_id=$1 AND project_id=$2 AND user_id=$3
        AND status='ACTIVE' AND role=ANY($4::text[])`, [work.tenantId,context.agent.projectId,args['userId'],method === 'set_teacher_membership' ? ['OWNER','TEACHER'] : ['LEARNER']])
      return compare(`course-member:${context.course.id}:${args['userId']}`, { enabled: args['enabled'] }, { enabled: rows.rows.length === 1 })
    }
    if (method === 'set_room_binding') {
      const rows = await database.query('SELECT status,purpose FROM learning_course_rooms WHERE company_id=$1 AND course_id=$2 AND conversation_id=$3',
        [work.tenantId,context.course.id,args['conversationId']])
      return compare(`course-room:${args['conversationId']}`, { enabled: args['enabled'], ...(args['enabled'] ? { purpose: args['purpose'] } : {}) },
        { enabled: rows.rows[0]?.['status'] === 'active', ...(args['enabled'] ? { purpose: rows.rows[0]?.['purpose'] } : {}) })
    }
    const rows = await executeTeacher(work, read(method === 'draft_objectives' ? 'teacher.list_objectives' : 'teacher.list_activities', {}), services, database)
    const expected = (Array.isArray(value) ? value : [value]).map(row => select(row, ['id','title','successCriteria','instructions','status']))
    return compare(`teacher-drafts:${context.course.id}:${method}`, { items: expected }, { items: expected.map(item =>
      select(Array.isArray(rows) ? rows.find(row => object(row)['id'] === item['id']) : undefined, Object.keys(item))) })
  }
  if (action.action === 'email.send' || action.action === 'email.reply') {
    const thread = object(await executeEmail(work, read('email.show', { conversationId: result['conversationId'], limit: 50 }), services))
    const messages = thread['messages'], message = Array.isArray(messages) ? messages.find(row => object(row)['id'] === result['messageId']) : undefined
    return compare(`email:${result['messageId']}`, { id: result['messageId'], body: args['body'], direction: 'out', transportStatus: 'sent',
      ...(action.action === 'email.send' ? { subject: args['subject'], toAddresses: args['to'] } : { inReplyTo: args['messageId'] }) }, message)
  }
  if (action.action.startsWith('chat.')) {
    if (!work.principalId) throw new Error('chat principal is unavailable')
    await services.permissionService.assertCan({ actorUserId: work.principalId, companyId: work.tenantId,
      action: 'conversation:read', resource: { type: 'conversation', id: work.sessionId } })
    if (['chat.rename','chat.set_topic','chat.add_member'].includes(action.action)) {
      const metadata = object(await executeChat(work, read('chat.metadata', {}), services, channelType))
      return action.action === 'chat.add_member'
        ? compare(`conversation-member:${work.sessionId}:${args['participantId']}`, { member: true }, { member: Array.isArray(metadata['members']) && metadata['members'].includes(args['participantId']) })
        : compare(`conversation:${work.sessionId}`, select(args, ['title','topic']), metadata)
    }
    if (action.action === 'chat.set_muted') {
      const rows = await executeChat(work, read('chat.list_mutes', {}), services, channelType)
      const mute = Array.isArray(rows) ? rows.find(row => object(row)['id'] === work.sessionId) : undefined
      return compare(`conversation-mute:${work.sessionId}`, { muted: args['muted'], mutedUntil: result['mutedUntil'] },
        { muted: Boolean(mute), mutedUntil: object(mute)['mutedUntil'] ?? null })
    }
    if (action.action === 'chat.history') {
      const sequence = Array.isArray(value) ? Math.max(0,...value.map(row => Number(object(row)['messageSeq'] ?? 0))) : -1
      const rows = await database.query(`SELECT COALESCE(MAX(read_through_seq),0) AS seq FROM im_read_receipt_advances
        WHERE company_id=$1 AND channel_id=$2 AND reader_id=$3`, [work.tenantId,work.sessionId,work.agentId])
      return compare(`read-receipt:${work.sessionId}:${work.agentId}`, { advanced: true }, { advanced: sequence >= 0 && Number(rows.rows[0]?.['seq']) >= sequence })
    }
    const rows = services.messaging ? await services.messaging.getAgentChannelHistory({ companyId: work.tenantId, agentId: work.agentId, channelId: work.sessionId, limit: 200 })
      : await services.wukongClient().syncMessages(work.sessionId, channelType, 100, work.agentId)
    if (action.action === 'chat.send' || action.action === 'chat.ask') {
      const nonce = `${action.action === 'chat.ask' ? 'questionnaire' : 'action'}-${action.idempotencyKey}`
      const message = rows?.find(row => object(row)['clientMsgNo'] === nonce), payload = object(object(message)['payload'])
      return compare(`message:${nonce}`, { fromUid: work.agentId, body: args[action.action === 'chat.ask' ? 'title' : 'body'], kind: action.action === 'chat.ask' ? 'questionnaire' : 'text' },
        { fromUid: object(message)['fromUid'], body: payload['body'], kind: payload['kind'] })
    }
    if (action.action === 'chat.react') {
      const message = rows?.find(row => object(row)['messageId'] === args['messageId']), reactions = object(object(object(message)['payload'])['data'])['reactions']
      const active = (items: unknown) => Array.isArray(items) && items.some(item => object(item)['emoji'] === args['emoji']
        && Array.isArray(object(item)['users']) && (object(item)['users'] as unknown[]).includes(work.agentId))
      return compare(`reaction:${args['messageId']}:${args['emoji']}:${work.agentId}`, { active: active(result['reactions']) }, { active: active(reactions) })
    }
  }
  return { status: 'inconclusive', evidence: { action: action.action, reason: 'This operation has no authoritative postcondition checker' } }
}
