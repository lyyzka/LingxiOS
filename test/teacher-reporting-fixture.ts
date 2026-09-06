import type { LingxiLoopServices } from '../src/integrations/lingxiloop/service-contracts.js'

const unexpected = async (): Promise<never> => { throw new Error('unexpected teacher reporting call') }
export const teacherReportingFixture = {
  setLearningObjectiveStatus: unexpected, publishLearningActivity: unexpected, closeLearningActivity: unexpected, reviewLearningEvaluation: unexpected,
  inc: () => {}, projectLifecycleProjection: unexpected, ProjectLifecycleApplication: class { executeInTransaction = unexpected },
  createLearningObjectives: unexpected, createLearningActivity: unexpected, updateTeacherCourseMetadata: unexpected,
  setLearningCourseMembership: unexpected, bindLearningCourseRoom: unexpected,
  findTeacherObjectiveApprovalTarget: unexpected, findTeacherActivityApprovalTarget: unexpected, findTeacherCourseApprovalTarget: unexpected,
  findTeacherMembershipApprovalTarget: unexpected, findTeacherEvaluationApprovalTarget: unexpected,
  loadTeacherOverviewRows: unexpected, listTeacherLearnerRows: unexpected, findTeacherLearner: unexpected,
  loadTeacherLearnerDetailRows: unexpected, findTeacherAttemptDetail: unexpected,
  listTeacherObjectives: unexpected, listTeacherActivities: unexpected, listTeacherReviews: unexpected,
  listTeacherBindableRooms: unexpected, auditInTransaction: unexpected,
} satisfies Partial<NonNullable<LingxiLoopServices['teacher']>>
