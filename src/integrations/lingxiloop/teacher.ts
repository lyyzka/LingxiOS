import { lockAction, recordActionResult } from '../../control-plane/action-transaction.js'
import { teacherContext } from './teacher-context.js'
import { withTransaction, type SqlQueryable, type SqlPool } from '../../control-plane/pg-store.js'
import type { HostAction, WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices, NativeQueryable } from './service-contracts.js'
import { assertTeacherDigestWork, configureTeacherDigest, getTeacherDigest, TEACHER_DIGEST_METHODS } from './teacher-digest.js'

export const TEACHER_METHODS = { set_learner_membership: ['userId', 'enabled'], set_room_binding: ['conversationId', 'enabled', 'purpose'], draft_activity: ['title', 'instructions', 'type', 'evaluationMode', 'targetLevel', 'rubric', 'objectiveIds', 'dueAt'], draft_objectives: ['objectives'], update_course: ['title', 'description'], current: [], overview: ['windowDays'], list_learners: ['attentionOnly'], get_learner: ['learnerId'], get_attempt: ['attemptId'], list_objectives: [], list_activities: [], list_reviews: [], list_rooms: [], get_digest_schedule: [], configure_digest: ['frequency', 'localTime', 'timezone', 'weekday'] } as const


export async function executeTeacher(work: Omit<WorkItem, 'leaseToken'>, action: HostAction, services: Pick<LingxiLoopServices, 'teacher' | 'permissionService'>, database?: SqlPool) {
  const method = action.action.slice('teacher.'.length)
  if (!action.action.startsWith('teacher.') || !Object.hasOwn(TEACHER_METHODS, method)) throw new Error('unsupported teacher action')
  if (work.kind === 'teacher_digest' && !(TEACHER_DIGEST_METHODS as readonly string[]).includes(method)) throw new Error('scheduled teacher summaries are read-only')
  const allowed: readonly string[] = TEACHER_METHODS[method as keyof typeof TEACHER_METHODS]
  if (Object.keys(action.args).some(key => !allowed.includes(key))) throw new Error('unknown teacher argument')
  if (method === 'get_learner' || method === 'get_attempt') {
    const id = action.args[method === 'get_learner' ? 'learnerId' : 'attemptId']
    if (typeof id !== 'string' || !id.trim() || id.length > 2000) throw new Error('teacher resource ID is required')
  }
  const days = action.args['windowDays'], attention = action.args['attentionOnly']
  if (days !== undefined && (!Number.isSafeInteger(days) || Number(days) < 1 || Number(days) > 365)) throw new Error('windowDays must be 1-365')
  if (attention !== undefined && typeof attention !== 'boolean') throw new Error('attentionOnly must be boolean')
  if (method === 'set_learner_membership') {
    const { userId, enabled } = action.args
    if (typeof userId !== 'string' || !userId.trim() || userId.length > 2000) throw new Error('userId is required')
    if (typeof enabled !== 'boolean') throw new Error('enabled must be boolean')
  }
  if (method === 'set_room_binding') {
    const { conversationId, enabled, purpose } = action.args
    if (typeof conversationId !== 'string' || !conversationId.trim() || conversationId.length > 2000) throw new Error('conversationId is required')
    if (typeof enabled !== 'boolean') throw new Error('enabled must be boolean')
    if (enabled ? purpose !== 'lab' && purpose !== 'discussion' : purpose !== undefined) throw new Error('purpose is required only when enabling a room binding')
  }
  if (method === 'draft_activity') {
    for (const key of ['title', 'instructions']) {
      const value = action.args[key]
      if (typeof value !== 'string' || !value.trim() || value.length > 10000) throw new Error('activity text must contain 1-10000 characters')
    }
    const { type, evaluationMode, targetLevel, rubric, objectiveIds, dueAt } = action.args
    if (typeof type !== 'string' || !['LESSON', 'PRACTICE', 'ASSESSMENT', 'PROJECT', 'REVIEW'].includes(type)) throw new Error('invalid activity type')
    if (evaluationMode !== undefined && evaluationMode !== 'AGENT_FORMATIVE' && evaluationMode !== 'TEACHER_REQUIRED') throw new Error('invalid evaluationMode')
    if (targetLevel !== undefined && (!Number.isInteger(targetLevel) || Number(targetLevel) < 1 || Number(targetLevel) > 4)) throw new Error('targetLevel must be 1-4')
    if (rubric !== undefined && (!Array.isArray(rubric) || rubric.length > 100)) throw new Error('rubric must contain at most 100 items')
    if (objectiveIds !== undefined && (!Array.isArray(objectiveIds) || objectiveIds.length > 100
      || objectiveIds.some(id => typeof id !== 'string' || !id.trim() || id.length > 2000)
      || new Set(objectiveIds).size !== objectiveIds.length)) throw new Error('invalid objectiveIds')
    if (dueAt !== undefined && (typeof dueAt !== 'string' || dueAt.length > 100 || !Number.isFinite(Date.parse(dueAt)))) throw new Error('invalid dueAt')
  }
  if (method === 'draft_objectives') {
    const objectives = action.args['objectives']
    if (!Array.isArray(objectives) || !objectives.length || objectives.length > 100) throw new Error('objectives must contain 1-100 items')
    for (const item of objectives) {
      if (!item || typeof item !== 'object' || Array.isArray(item)
        || Object.keys(item).some(key => !['title', 'successCriteria', 'targetLevel', 'prerequisiteIds'].includes(key))) throw new Error('invalid objective fields')
      for (const key of ['title', 'successCriteria']) {
        if (typeof item[key] !== 'string' || !item[key].trim() || item[key].length > 10000) throw new Error('objective text must contain 1-10000 characters')
      }
      if (item.targetLevel !== undefined && (!Number.isInteger(item.targetLevel) || item.targetLevel < 1 || item.targetLevel > 4)) throw new Error('targetLevel must be 1-4')
      if (item.prerequisiteIds !== undefined && (!Array.isArray(item.prerequisiteIds) || item.prerequisiteIds.length > 100
        || item.prerequisiteIds.some((id: unknown) => typeof id !== 'string' || !id.trim() || id.length > 2000)
        || new Set(item.prerequisiteIds).size !== item.prerequisiteIds.length)) throw new Error('invalid prerequisiteIds')
    }
  }
  if (method === 'update_course') {
    if (!Object.keys(action.args).length) throw new Error('title or description is required')
    for (const [key, value] of Object.entries(action.args)) {
      if (typeof value !== 'string' || !value.trim() || value.length > (key === 'title' ? 2000 : 10000)) throw new Error('invalid course metadata text')
    }
  }
  if (!database) throw new Error('teacher actions require a database')
  if (method === 'configure_digest') return configureTeacherDigest(database, services, work, action.args)
  if (method === 'update_course' || method === 'draft_objectives' || method === 'draft_activity' || method === 'set_room_binding' || method === 'set_learner_membership') {
    if (!database) throw new Error('teacher updates require a database transaction')
    return withTransaction(database, async client => {
      await lockAction(client, work, action)
      const context = await teacherContext(work, services, client)
      const teacher = services.teacher!
      const transaction = teacherTransaction(client)
      const scope = { companyId: work.tenantId, courseId: context.course.id }
      const args = action.args
      let result: unknown
      if (method === 'update_course') {
        result = await teacher.updateTeacherCourseMetadata(client, { ...scope,
          ...(typeof args['title'] === 'string' ? { title: args['title'].trim() } : {}),
          ...(typeof args['description'] === 'string' ? { description: args['description'].trim() } : {}) })
      } else if (method === 'draft_objectives') {
        result = await teacher.createLearningObjectives(client, transaction, { ...scope, actorId: context.trigger.teacherId, actorKind: 'teacher',
          objectives: (args['objectives'] as Record<string, unknown>[]).map(item => ({ title: String(item['title']).trim(), successCriteria: String(item['successCriteria']).trim(),
            targetLevel: Number(item['targetLevel'] ?? 3), prerequisiteIds: (item['prerequisiteIds'] ?? []) as string[] })) })
      } else if (method === 'draft_activity') {
        result = await teacher.createLearningActivity(client, transaction, { ...scope, actorId: context.trigger.teacherId, actorKind: 'teacher',
          title: String(args['title']).trim(), instructions: String(args['instructions']).trim(), type: args['type'] as 'LESSON' | 'PRACTICE' | 'ASSESSMENT' | 'PROJECT' | 'REVIEW',
          evaluationMode: (args['evaluationMode'] ?? 'TEACHER_REQUIRED') as 'AGENT_FORMATIVE' | 'TEACHER_REQUIRED', targetLevel: Number(args['targetLevel'] ?? 2),
          rubric: (args['rubric'] ?? []) as unknown[], objectiveIds: (args['objectiveIds'] ?? []) as string[],
          ...(typeof args['dueAt'] === 'string' ? { dueAt: args['dueAt'] } : {}) })
      } else if (method === 'set_learner_membership') {
        await teacher.setLearningCourseMembership(client, transaction, { ...scope, managerId: context.trigger.teacherId, userId: String(args['userId']).trim(), role: 'learner', enabled: args['enabled'] as boolean })
        result = { ok: true }
      } else {
        await teacher.bindLearningCourseRoom(client, { ...scope, managerId: context.trigger.teacherId, conversationId: String(args['conversationId']).trim(), enabled: args['enabled'] as boolean,
          ...(args['enabled'] ? { purpose: args['purpose'] as 'lab' | 'discussion' } : {}) })
        result = { ok: true, enabled: args['enabled'] }
      }
      if (method === 'set_learner_membership') {
        if (!result || typeof result !== 'object' || Reflect.get(result, 'ok') !== true) throw new Error('teacher membership change was not acknowledged')
        return recordActionResult(client, action, result)
      }
      if (method === 'set_room_binding') {
        if (!result || typeof result !== 'object' || Reflect.get(result, 'ok') !== true
          || Reflect.get(result, 'enabled') !== action.args['enabled']) throw new Error('teacher room binding did not acknowledge the requested change')
        return recordActionResult(client, action, result)
      }
      if (method === 'draft_activity') {
        if (!result || typeof result !== 'object' || Reflect.get(result, 'courseId') !== context.course.id
          || typeof Reflect.get(result, 'id') !== 'string' || !Reflect.get(result, 'id') || Reflect.get(result, 'status') !== 'DRAFT') throw new Error('teacher activity creation did not return a scoped draft')
        return recordActionResult(client, action, result)
      }
      if (method === 'draft_objectives') {
        // Native creation returns the whole course inventory, including existing published objectives.
        if (!Array.isArray(result) || result.length < (action.args['objectives'] as unknown[]).length
          || result.some(item => !item || typeof item !== 'object' || item.courseId !== context.course.id
            || typeof item.id !== 'string' || !item.id.trim())
          || new Set(result.map(item => item.id)).size !== result.length) throw new Error('teacher draft creation did not return scoped objectives')
        return recordActionResult(client, action, result)
      }
      if (!result || typeof result !== 'object' || !('id' in result) || result.id !== context.agent.projectId
        || !('company_id' in result) || result.company_id !== work.tenantId) throw new Error('teacher update did not return the scoped project')
      for (const [key, value] of Object.entries(action.args)) {
        if (Reflect.get(result, key === 'title' ? 'name' : 'description') !== (value as string).trim()) throw new Error('teacher update did not return the requested metadata')
      }
      return recordActionResult(client, action, result)
    })
  }

  const context = await teacherContext(work, services, database)
  if (work.kind === 'teacher_digest') await assertTeacherDigestWork(database, work, context)
  const teacher = services.teacher!
  const scope = { companyId: work.tenantId, projectId: context.agent.projectId, courseId: context.course.id, teacherUserId: context.trigger.teacherId }
  switch (method) {
    case 'current': return { ...context, digest: await getTeacherDigest(database, work, context) }
    case 'overview': {
      const windowDays = Number(action.args['windowDays'] ?? 30)
      const rows = await teacher.loadTeacherOverviewRows(database, scope, windowDays)
      teacher.inc('learning.teacher_agent.summary_generated')
      return { generatedAt: new Date().toISOString(), windowDays, course: { id: context.course.id, title: context.course.title },
        stateDistribution: rows.distribution, missions: rows.missions, activity: rows.activity[0] ?? {}, evidenceCoverage: rows.coverage[0] ?? {},
        attention: rows.attention.map(row => ({ ...row, reasons: Array.isArray(row['attention_reasons']) ? row['attention_reasons'] : [] })) }
    }
    case 'list_learners': return (await teacher.listTeacherLearnerRows(database, scope, action.args['attentionOnly'] === true))
      .map(row => ({ ...row, attentionReasons: Array.isArray(row['attention_reasons']) ? row['attention_reasons'] : [] }))
    case 'get_learner': {
      const learnerId = String(action.args['learnerId']).trim()
      const member = await teacher.findTeacherLearner(database, scope, learnerId)
      if (!member) throw new Error('learner is outside the current course')
      const detail = await teacher.loadTeacherLearnerDetailRows(database, scope, learnerId)
      teacher.inc('learning.teacher_agent.learner_drilldown')
      return { learner: { id: learnerId, ...member }, ...detail }
    }
    case 'get_attempt': {
      const attemptId = String(action.args['attemptId']).trim()
      const attempt = await teacher.findTeacherAttemptDetail(database, scope, attemptId)
      if (!attempt) throw new Error('attempt is outside the current course')
      await teacher.auditInTransaction(database, { kind: 'teacher_agent_attempt_access', userId: context.trigger.teacherId,
        companyId: work.tenantId, detail: { courseId: context.course.id, attemptId, agentId: work.agentId } })
      teacher.inc('learning.teacher_agent.evidence_accessed')
      return attempt
    }
    case 'list_objectives': return teacher.listTeacherObjectives(database, scope)
    case 'list_activities': return teacher.listTeacherActivities(database, scope)
    case 'list_reviews': return teacher.listTeacherReviews(database, scope)
    case 'list_rooms': return teacher.listTeacherBindableRooms(database, scope)
    case 'get_digest_schedule': return getTeacherDigest(database, work, context)
    default: throw new Error('unsupported teacher action')
  }
}

/** Keep native domain transactions on the already-open package transaction. */
export function teacherTransaction(client: SqlQueryable) {
  const database: NativeQueryable = { query: async <T extends Record<string, unknown>>(text: string, params?: readonly unknown[]) => {
    const result = await client.query(text, params ? [...params] : undefined)
    const command: unknown = Reflect.get(result, 'command'), oid: unknown = Reflect.get(result, 'oid'), fields: unknown = Reflect.get(result, 'fields')
    if (typeof command !== 'string' || typeof oid !== 'number' || !Array.isArray(fields)
      || fields.some(field => !field || typeof field.name !== 'string' || typeof field.format !== 'string'
        || ['tableID', 'columnID', 'dataTypeID', 'dataTypeSize', 'dataTypeModifier'].some(key => typeof field[key] !== 'number'))) {
      throw new Error('native teacher domain transactions require complete pg query results')
    }
    return { rows: result.rows as T[], rowCount: result.rowCount, command, oid, fields }
  } }
  return <T>(run: (db: NativeQueryable) => Promise<T>) => run(database)
}
