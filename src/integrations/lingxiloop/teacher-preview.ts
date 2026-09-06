import type { SqlQueryable } from '../../control-plane/pg-store.js'
import type { HostAction } from '../../protocol/types.js'
import type { LingxiLoopServices } from './service-contracts.js'
import type { teacherContext } from './teacher-context.js'

export async function teacherPreview(database: SqlQueryable, teacher: NonNullable<LingxiLoopServices['teacher']>,
  companyId: string, context: Awaited<ReturnType<typeof teacherContext>>, action: HostAction) {
  const method = action.action.slice('teacher.'.length), args = action.args
  const courseId = context.course.id
  let entityId: string, entityLabel: string | null, currentState: string | boolean, currentVersion: unknown
  if (method === 'set_teacher_membership') {
    entityId = String(args['userId']).trim()
    const target = await teacher.findTeacherMembershipApprovalTarget(database, companyId, courseId, entityId)
    entityLabel = target.label; currentState = currentVersion = target.enabled
  } else if (method === 'review_evaluation') {
    entityId = String(args['evaluationId']).trim()
    const target = await teacher.findTeacherEvaluationApprovalTarget(database, companyId, courseId, entityId)
    if (!target) throw new Error('evaluation is outside the current course')
    entityLabel = target.label; currentState = currentVersion = target.status
  } else {
    entityId = method === 'transition_course' ? courseId : String(args[method.includes('objective') ? 'objectiveId' : 'activityId']).trim()
    const target = method === 'transition_course' ? await teacher.findTeacherCourseApprovalTarget(database, companyId, courseId)
      : method.includes('objective') ? await teacher.findTeacherObjectiveApprovalTarget(database, companyId, courseId, entityId)
      : await teacher.findTeacherActivityApprovalTarget(database, companyId, courseId, entityId)
    if (!target) throw new Error('teacher approval target is outside the current course')
    entityLabel = target.label; currentState = target.status
    const version = target.updatedAt
    currentVersion = version instanceof Date ? version.toISOString()
      : typeof version === 'string' && /[T ]/.test(version) && Number.isFinite(Date.parse(version)) ? new Date(version).toISOString() : version
  }
  const labels: Record<string, string> = { publish_objective: '发布学习目标', archive_objective: '归档学习目标', publish_activity: '发布学习活动', close_activity: '关闭学习活动',
    transition_course: ({ END: '结束课程', ENTER_READ_ONLY: '进入只读', ARCHIVE: '归档课程' } as Record<string, string>)[String(args['command'])] ?? '推进课程生命周期',
    set_teacher_membership: args['enabled'] === false ? '移除教师身份' : '授予教师身份', review_evaluation: args['decision'] === 'reject' ? '退回学习评价' : '采纳学习评价' }
  if (!Object.hasOwn(labels, method)) throw new Error('unsupported teacher approval')
  return { requestedBy: context.trigger.teacherId, summary: `${labels[method]}“${entityLabel ?? '当前对象'}”`,
    scope: { projectId: context.agent.projectId, courseId, roomId: context.room.id, risk: method === 'review_evaluation' ? 'learning_evaluation' : 'course_management' },
    preview: { method, entityId, entityLabel, currentState, currentVersion, args } }
}
