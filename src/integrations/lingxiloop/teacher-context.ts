import type { SqlQueryable } from '../../control-plane/pg-store.js'
import type { WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices } from './service-contracts.js'

export class TeacherScopeError extends Error {}

export async function teacherContext(work: Pick<WorkItem, 'tenantId' | 'agentId' | 'sessionId' | 'principalId' | 'kind'>, services: Pick<LingxiLoopServices, 'teacher' | 'permissionService'>, database: SqlQueryable) {
  if (!work.principalId) throw new Error('teacher principal is required')
  if (!services.teacher) throw new Error('native teacher services are required')
  await services.permissionService.assertCan({ actorUserId: work.principalId, companyId: work.tenantId,
    action: 'conversation:read', resource: { type: 'conversation', id: work.sessionId } })
  const scope = await services.teacher.findTeacherScopeBinding(database, work.tenantId, work.agentId, work.sessionId)
  if (!scope || scope.company_id !== work.tenantId || scope.agent_id !== work.agentId || scope.room_id !== work.sessionId
    || scope.room_status !== 'active' || scope.course_status === 'ARCHIVED' || !scope.has_teacher
    || !scope.course_id || !scope.project_id) throw new TeacherScopeError('teacher scope does not match the persisted request principal')
  await services.teacher.requireLearningCourseRole(database, { companyId: work.tenantId, courseId: scope.course_id, userId: work.principalId, role: 'teacher' })
  const counts = await services.teacher.findTeacherTurnCounts(database, work.tenantId, scope.project_id)
  return {
    agent: { id: scope.agent_id, name: scope.agent_name, projectId: scope.project_id },
    course: { id: scope.course_id, projectId: scope.project_id, title: scope.course_title, status: scope.course_status },
    room: { id: scope.room_id, status: scope.room_status },
    trigger: { mode: work.kind === 'teacher_digest' ? 'routine' : work.kind === 'resume' ? 'approval' : 'teacher', teacherId: work.principalId },
    counts: { learners: Number(counts.learners), objectives: Number(counts.objectives), activities: Number(counts.activities), pendingReviews: Number(counts.pending_reviews) },
  }
}
