import type { SqlQueryable } from '../../control-plane/pg-store.js'

/** Native domain callbacks declare pg's complete result shape; the injected resource is pg-compatible. */
export interface NativeQueryable {
  query<T extends Record<string, unknown> = Record<string, unknown>>(text: string, params?: readonly unknown[]): Promise<{
    rows: T[]; rowCount: number | null; command: string; oid: number;
    fields: { name: string; tableID: number; columnID: number; dataTypeID: number; dataTypeSize: number; dataTypeModifier: number; format: string }[]
  }>
}
type NativeTransaction = <T>(work: (db: NativeQueryable) => Promise<T>) => Promise<T>

export interface NativeCanvasAssignmentRow {
  id: string; canvas_id: string; agent_id: string; assignment: string; color: string
  status: 'queued' | 'blocked' | 'working' | 'waiting' | 'completed' | 'failed' | 'cancelled'
  work_x: number | string; work_y: number | string; work_width: number | string; work_height: number | string
  active_frame_id: string | null; cursor_x: number | string | null; cursor_y: number | string | null; work_id: string | null
  result: string | null; error: string | null; started_at: string | null; completed_at: string | null; updated_at: string
  execution_role: 'specialist' | 'verifier'; verifies_assignment_id: string | null
  progress_fingerprint: string | null; no_progress_count: number | string | null
}

export interface NativeCanvasChanged {
  type: 'canvas.changed'; kind: 'assignment.updated' | 'workspace.updated' | 'activity.created'; companyId: string; canvasId: string
  workspaceId: string; conversationId: string; timestamp: string; assignment?: unknown; workspace?: unknown; activity?: unknown
}

type NativeProjectStatus = 'CREATED' | 'DRAFT' | 'ACTIVE' | 'COURSE_ENDED' | 'READ_ONLY' | 'TRANSFER_PENDING' | 'RETENTION' | 'ARCHIVED' | 'DELETED'
interface NativeLifecycleInfrastructure {
  transaction: NativeTransaction
  auditInTransaction(db: NativeQueryable, input: { kind: string; userId?: string; companyId: string; detail: Record<string, unknown> }): Promise<void>
  projectLifecycleProjection(db: NativeQueryable, input: { companyId: string; projectId: string; status: NativeProjectStatus }): Promise<void>
}

interface TeacherReportingScope { companyId: string; projectId: string; courseId: string; teacherUserId?: string }

/** Structural signatures of existing LingxiLoop exports, reference 020b9295. */
export interface NativeWork {
  id: string
  fence: number
  homeEpoch?: number
  companyId: string
  authorizationUserId?: string
  agentId: string
  channelId: string
  threadRootClientMsgNo?: string
  triggerClientMsgNo: string
  reason: 'message' | 'resume' | 'routine' | 'handoff' | 'canvas_worker' | 'canvas_summary'
  executionRole: 'coordinator' | 'specialist' | 'verifier' | 'reporter'
  lane: 'learner' | 'approval' | 'collaboration' | 'background'
  leaseToken: string
}

export interface KnowledgeServices {
  listKnowledgeSourcesForAgent(work: NativeWork): Promise<unknown[]>
  addKnowledgeText(work: NativeWork, input: { title: string; text: string; idempotencyKey: string }): Promise<{ id: string; status: string }>
  addKnowledgeUrl(work: NativeWork, input: { title: string; url: string; idempotencyKey: string }): Promise<{ id: string; status: string }>
  addKnowledgeFile(work: NativeWork, input: { title: string; storageKey: string; mime: string; size: number; idempotencyKey: string }): Promise<{ id: string; status: string }>
  retryKnowledgeSourceForAgent(work: NativeWork, sourceId: string): Promise<{ status: string }>
  setKnowledgeSourceEnabled(work: NativeWork, sourceId: string, enabled: boolean): Promise<{ enabled: boolean }>
  deleteKnowledgeSourceForAgent(work: NativeWork, sourceId: string): Promise<{ deleted: boolean }>
}

export interface NativeMessage {
  clientMsgNo: string
  messageSeq: number
  channelId: string
  channelType: number
  fromUid: string
  payload: { version: 1; kind: string; body?: string; replyToClientMsgNo?: string; refs?: { agentId?: unknown; handoffId?: unknown; toAgentId?: unknown }; data?: Record<string, unknown> }
}

export interface NativeTextMessage {
  version: 1
  kind: 'text'
  clientMsgNo: string
  body: string
  replyToClientMsgNo?: string
  refs: Record<string, string | string[]>
  data?: Record<string, unknown>
}

export interface NativeDocument {
  id: string
  title: string
  createdBy: string
  conversationId: string | null
  createdAt: string
  updatedAt: string
}

export type NativeDocumentEdit =
  | { kind: 'append'; text: string }
  | { kind: 'replace'; find: string; replace: string }
  | { kind: 'insertParagraph'; at: 'start' | 'end'; text: string }
  | { kind: 'replaceBlock'; anchorText: string; text: string }
  | { kind: 'image'; src: string; alt: string | null; placement: { mode: 'start' | 'end' } | { mode: 'replace' | 'after' | 'before'; anchorText: string } }
  | { kind: 'imageDelete'; match: { by: 'src'; src: string } | { by: 'src-contains'; substring: string } | { by: 'alt'; alt: string } }

export interface NativeDocumentEditResult { replaced: number; imagePlaced: 'absolute' | 'anchor' | 'anchor-missed' | null; imagesDeleted: number; blocksReplaced: number }
export interface NativeDocumentChanged {
  type: 'doc.changed'; kind: 'document.created' | 'document.updated' | 'document.deleted'
  companyId: string; workspaceId: string; documentId: string; actorId: string
}
export interface NativeDocumentUpdate {
  type: 'doc.update'; companyId: string; documentId: string; updateB64: string; originId: string; authorId: string
}
type DocumentScope = { companyId: string; projectId: string; userId: string }
interface NativeDocumentEditor {
  readText(documentId: string, companyId: string): Promise<string>
  applyEdit(documentId: string, companyId: string, agentId: string, operations: NativeDocumentEdit[]): Promise<NativeDocumentEditResult>
}
export interface NativeDocumentContent {
  DocumentsApplication: new (db: NativeQueryable, events: { publish(event: NativeDocumentChanged): Promise<void> }, editor: NativeDocumentEditor) => {
    createForAgent(scope: DocumentScope, input: { id?: string; title: string; body: string }): Promise<{ document: NativeDocument; replayed: boolean }>
    readForAgent(scope: DocumentScope, documentId: string): Promise<NativeDocument & { body: string }>
    editForAgent(scope: DocumentScope, documentId: string, operations: NativeDocumentEdit[]): Promise<NativeDocumentEditResult>
    deleteForAgent(scope: DocumentScope, documentId: string): Promise<{ ok: true }>
  }
  createDocumentCollaborationApplication(dependencies: {
    transaction: NativeTransaction; instanceId: string
    bus: { publish(event: NativeDocumentUpdate | { type: 'doc.awareness'; companyId: string; documentId: string; updateB64: string; originId: string }): Promise<void>; subscribe(listener: (event: unknown) => void): Promise<void> }
    imageStorage: { normalizeKey(value: string | null | undefined): string | null; keyFromPublicUrl(value: string | null | undefined): string | null;
      signedUrlExpiresSoon(url: string, withinSeconds?: number): boolean; publicUrl(key: string): Promise<string> }
  }): { readDocumentText: NativeDocumentEditor['readText']; applyAgentEdit: NativeDocumentEditor['applyEdit'] }
  normalizeStorageKey(value: string): string | null
  storageKeyFromPublicUrl(value: string): string | null
  signedUrlExpiresSoon(url: string, withinSeconds?: number): boolean
  storage: { publicUrl(key: string): Promise<string> }
  CH_DOC_UPDATE: string
  publish(channel: string, event: NativeDocumentUpdate): Promise<void>
}

export interface NativeCalendarPatch {
  title?: string
  kind?: 'personal' | 'agent_task'
  description?: string | null
  assigneeId?: string | null
  targetConversationId?: string | null
  agentPrompt?: string | null
  startAt?: Date
  endAt?: Date | null
  allDay?: boolean
  recurrence?: { freq: 'daily' | 'weekly' | 'monthly' | 'yearly'; interval: number; byweekday?: number[]; until?: string | null; count?: number | null } | null
  status?: 'active' | 'paused' | 'done' | 'cancelled'
  reminderMinutesBefore?: number | null
  reminderChannel?: 'toast' | 'email' | 'both' | null
  isPrivate?: boolean
}

export interface NativeCalendarChanged {
  type: 'calendar.changed'
  kind: 'event.created' | 'event.updated' | 'event.deleted' | 'event.dispatched'
  eventId: string
  companyId: string
  workspaceId: string
  actorId: string | null
}

export type NativeCalendarCreate = NativeCalendarPatch & {
  title: string
  kind: 'personal' | 'agent_task'
  startAt: Date
  allDay: boolean
  status: 'active' | 'paused' | 'done' | 'cancelled'
  isPrivate: boolean
}

export interface LingxiLoopServices {
  calendar?: {
    calendarApplication: {
      list(scope: { companyId: string; projectId: string; userId: string }, range: { from?: Date; to?: Date }): Promise<{ id: string; title: string; startAt: string }[]>
      get(scope: { companyId: string; projectId: string; userId: string }, eventId: string): Promise<{
        id: string; createdBy: string; kind: 'personal' | 'agent_task'; title: string; description: string | null
        assigneeId: string | null; targetConversationId: string | null; agentPrompt: string | null; startAt: string
      }>
      dispatches(scope: { companyId: string; projectId: string; userId: string }, eventId: string): Promise<{
        eventId: string; scheduledFor: string; status: string; conversationId: string | null; messageId: string | null
      }[]>
    }
    listCalendarEventsQuerySchema: { parse(value: unknown): { from?: Date; to?: Date } }
    writes?: {
      CalendarApplication: new (db: NativeQueryable, events: { publish(event: NativeCalendarChanged): Promise<void> }, dispatcher: { dispatch(): Promise<never> }) => {
        get(scope: { companyId: string; projectId: string; userId: string }, eventId: string): Promise<{ id: string; title: string; startAt: string }>
        update(scope: { companyId: string; projectId: string; userId: string }, eventId: string, patch: NativeCalendarPatch): Promise<{ id: string; title: string; startAt: string }>
        create(scope: { companyId: string; projectId: string; userId: string }, input: NativeCalendarCreate, options?: { eventId?: string; replayExisting?: boolean }): Promise<{ id: string; title: string; startAt: string }>
        delete(scope: { companyId: string; projectId: string; userId: string }, eventId: string): Promise<{ ok: true }>
      }
      updateCalendarEventRequestSchema: { parse(value: unknown): NativeCalendarPatch }
      createCalendarEventRequestSchema: { parse(value: unknown): NativeCalendarCreate }
      createPermissionService(db: SqlQueryable, options?: { lockDependencies?: boolean }): LingxiLoopServices['permissionService']
      CH_CALENDAR_EVENTS: string
      publish(channel: string, event: NativeCalendarChanged): Promise<void>
    }
  }
  documents?: {
    listAgentDocuments(scope: { companyId: string; projectId: string }): Promise<NativeDocument[]>
    listRecentAgentDocumentCreations(scope: { companyId: string; projectId: string; userId: string }, sinceMinutes: number): Promise<Array<{ id: string; title: string; createdBy: string; createdAt: unknown }>>
    readAgentDocument(scope: { companyId: string; projectId: string; userId: string }, documentId: string): Promise<NativeDocument & { body: string }>
    writes?: {
      content?: NativeDocumentContent
      createPermissionService(db: SqlQueryable, options?: { lockDependencies?: boolean }): LingxiLoopServices['permissionService']
      renameDocument(db: SqlQueryable, companyId: string, projectId: string, documentId: string, title: string): Promise<boolean>
      renameDocumentRequestSchema: { parse(input: unknown): { title: string } }
      CH_DOCS: string
      publish(channel: string, event: NativeDocumentChanged): Promise<void>
    }
  }
  teacher?: {
    requireLearningCourseRole(db: import('../../control-plane/pg-store.js').SqlQueryable, input: { companyId: string; courseId: string; userId: string; role: 'teacher' }): Promise<void>
    setLearningCourseMembershipRecord?(db: import('../../control-plane/pg-store.js').SqlQueryable, input: { companyId: string; courseId: string; userId: string; role: 'teacher'; enabled: boolean }): Promise<string>
    enqueueLearningEffect?(db: import('../../control-plane/pg-store.js').SqlQueryable, input: { companyId: string; courseId: string; kind: 'teacher_room.sync' }): Promise<void>
    setLearningObjectiveStatus(db: SqlQueryable, input: { companyId: string; courseId: string; objectiveId: string; teacherId: string; status: 'PUBLISHED' | 'ARCHIVED' }): Promise<void>
    publishLearningActivity(transaction: NativeTransaction, input: { companyId: string; courseId: string; activityId: string; teacherId: string }): Promise<void>
    closeLearningActivity(db: SqlQueryable, input: { companyId: string; courseId: string; activityId: string; teacherId: string }): Promise<void>
    reviewLearningEvaluation(db: SqlQueryable, transaction: NativeTransaction, metric: (name: 'learning.state.changed' | 'learning.evaluation.proposed', labels?: Record<string, string>) => void, input: { companyId: string; courseId: string; evaluationId: string; teacherId: string; decision: 'accept' | 'reject'; reason: string }): Promise<void>
    inc(name: 'learning.state.changed' | 'learning.evaluation.proposed' | 'learning.teacher_agent.summary_generated' | 'learning.teacher_agent.learner_drilldown' | 'learning.teacher_agent.evidence_accessed' | 'learning.teacher_agent.digest_configured', labels?: Record<string, string>): void
    projectLifecycleProjection(db: SqlQueryable, input: { companyId: string; projectId: string; status: NativeProjectStatus }): Promise<void>
    ProjectLifecycleApplication: new (infrastructure: NativeLifecycleInfrastructure) => { executeInTransaction(db: SqlQueryable, input: { actorUserId: string; companyId: string; projectId: string; command: 'END' | 'ENTER_READ_ONLY' | 'ARCHIVE' }): Promise<{ ok: true; status: string; applied: boolean }> }
    createLearningObjectives(db: SqlQueryable, transaction: NativeTransaction, input: { companyId: string; courseId: string; actorId: string; actorKind: 'teacher'; objectives: { title: string; successCriteria: string; targetLevel: number; prerequisiteIds: string[] }[] }): Promise<unknown>
    createLearningActivity(db: SqlQueryable, transaction: NativeTransaction, input: { companyId: string; courseId: string; actorId: string; actorKind: 'teacher'; title: string; instructions: string; type: 'LESSON' | 'PRACTICE' | 'ASSESSMENT' | 'PROJECT' | 'REVIEW'; evaluationMode: 'AGENT_FORMATIVE' | 'TEACHER_REQUIRED'; targetLevel: number; rubric: unknown[]; objectiveIds: string[]; dueAt?: string }): Promise<unknown>
    updateTeacherCourseMetadata(db: SqlQueryable, input: { companyId: string; courseId: string; title?: string; description?: string }): Promise<unknown>
    setLearningCourseMembership(db: SqlQueryable, transaction: NativeTransaction, input: { companyId: string; courseId: string; managerId: string; userId: string; role: 'learner'; enabled: boolean }): Promise<void>
    bindLearningCourseRoom(db: SqlQueryable, input: { companyId: string; courseId: string; managerId: string; conversationId: string; enabled: boolean; purpose?: 'lab' | 'discussion' }): Promise<void>
    findTeacherObjectiveApprovalTarget(db: SqlQueryable, companyId: string, courseId: string, id: string): Promise<{ status: string; updatedAt: unknown; label: string | null } | undefined>
    findTeacherActivityApprovalTarget(db: SqlQueryable, companyId: string, courseId: string, id: string): Promise<{ status: string; updatedAt: unknown; label: string | null } | undefined>
    findTeacherCourseApprovalTarget(db: SqlQueryable, companyId: string, courseId: string): Promise<{ status: string; updatedAt: unknown; label: string | null } | undefined>
    findTeacherMembershipApprovalTarget(db: SqlQueryable, companyId: string, courseId: string, id: string): Promise<{ enabled: boolean; label: string | null }>
    findTeacherEvaluationApprovalTarget(db: SqlQueryable, companyId: string, courseId: string, id: string): Promise<{ status: string; label: string | null } | undefined>
    assertTeacherApprovalFresh?(input: { channelId: string; companyId: string; action: string; preview: Record<string, unknown> }, db?: import('../../control-plane/pg-store.js').SqlQueryable): Promise<void>
    loadTeacherOverviewRows(db: SqlQueryable, scope: TeacherReportingScope, windowDays: number): Promise<{ distribution: Record<string, unknown>[]; missions: Record<string, unknown>[]; activity: Record<string, unknown>[]; attention: Record<string, unknown>[]; coverage: Record<string, unknown>[] }>
    listTeacherLearnerRows(db: SqlQueryable, scope: TeacherReportingScope, attentionOnly: boolean): Promise<Record<string, unknown>[]>
    findTeacherLearner(db: SqlQueryable, scope: TeacherReportingScope, learnerId: string): Promise<Record<string, unknown> | undefined>
    loadTeacherLearnerDetailRows(db: SqlQueryable, scope: TeacherReportingScope, learnerId: string): Promise<{ states: Record<string, unknown>[]; missions: Record<string, unknown>[]; attempts: Record<string, unknown>[] }>
    findTeacherAttemptDetail(db: SqlQueryable, scope: TeacherReportingScope, attemptId: string): Promise<Record<string, unknown> | undefined>
    listTeacherObjectives(db: SqlQueryable, scope: TeacherReportingScope): Promise<Record<string, unknown>[]>
    listTeacherActivities(db: SqlQueryable, scope: TeacherReportingScope): Promise<Record<string, unknown>[]>
    listTeacherReviews(db: SqlQueryable, scope: TeacherReportingScope): Promise<Record<string, unknown>[]>
    listTeacherBindableRooms(db: SqlQueryable, scope: TeacherReportingScope): Promise<Record<string, unknown>[]>
    auditInTransaction(db: SqlQueryable, input: { kind: string; userId?: string; companyId: string; detail: Record<string, unknown> }): Promise<void>
    findTeacherScopeBinding(db: import('../../control-plane/pg-store.js').SqlQueryable, companyId: string, agentId: string, channelId: string): Promise<{ company_id: string; project_id: string; course_id: string; course_title: string; course_status: string; room_id: string; room_status: string; agent_id: string; agent_name: string; has_teacher: boolean } | undefined>
    findTeacherTurnCounts(db: import('../../control-plane/pg-store.js').SqlQueryable, companyId: string, projectId: string): Promise<{ learners: number; objectives: number; activities: number; pending_reviews: number }>
  }
  canvas?: {
    orchestration?: {
      CH_CANVAS: string
      publish(channel: string, event: NativeCanvasChanged): Promise<void>
      toAssignment(row: NativeCanvasAssignmentRow, dependencies?: string[]): unknown
      insertAgentWorkspace(db: SqlQueryable, input: { id: string; companyId: string; title: string; conversationId: string; triggerClientMsgNo: string;
        goal: string; initiatorAgentId: string; authorizationUserId: string | null }): Promise<{ id: string; project_id: string | null; status: string } | null>
      createPermissionService(db: SqlQueryable, options?: { lockDependencies?: boolean }): { assertCan(input: Parameters<LingxiLoopServices['permissionService']['assertCan']>[0]): Promise<unknown> }
      assertCanvasDependencyDAG(members: Array<{ agentId: string; assignment: string; dependsOnAgentIds?: string[] }>, existingAgentIds?: Set<string>): void
      canvasAgentColor(agentId: string, used: ReadonlySet<string>): string
      canvasWorkArea(index: number): { x: number; y: number; width: number; height: number }
      createEvidenceRecordInTransaction(db: NativeQueryable, input: {
        id: string; companyId: string; projectId: string; level: 'L1' | 'L2'; derivation: 'OBSERVED'; kind: string;
        data: { [key: string]: string }; createdBy: { type: 'SYSTEM' } | { type: 'AGENT'; id: string }
      }): Promise<unknown>
      createEvidenceWithLinksInTransaction(db: NativeQueryable, input: {
        id: string; companyId: string; projectId: string; level: 'L2'; derivation: 'OBSERVED'; kind: string;
        data: { [key: string]: string }; createdBy: { type: 'AGENT'; id: string }
      }, links: Array<{ relation: 'DERIVED_FROM'; targetLevel: 'L1'; targetKind: 'EVIDENCE_RECORD'; targetId: string }>): Promise<unknown>
    }
    addCanvasComment(input: { companyId: string; actorId: string; actorKind: 'agent'; canvasId: string; frameId?: string; body: string }): Promise<unknown>
    canvasCommentRequestSchema: { parse(input: unknown): { canvasId: string; frameId?: string | null; body: string } }
    listCanvasAvailableAgents(companyId: string): Promise<Array<{ id: string; name: string; role: string; status: string }>>
    getConversationCanvas(companyId: string, conversationId: string, actorId: string): Promise<unknown>
    createCanvasFrame(input: { companyId: string; actorId: string; actorKind: 'agent'; idempotencyKey: string; canvasId: string; frame: Record<string, unknown> }): Promise<unknown>
    deleteCanvasFrame(input: { companyId: string; actorId: string; actorKind: 'agent'; frameId: string }): Promise<unknown>
    appendCanvasFrameContent(input: { companyId: string; actorId: string; actorKind: 'agent'; frameId: string; content: string }): Promise<unknown>
    updateCanvasFrame(input: { companyId: string; actorId: string; actorKind: 'agent'; frameId: string; patch: Record<string, unknown> }): Promise<unknown>
    canvasFrameUpdateRequestSchema: { parse(input: unknown): Record<string, unknown> }
    canvasFrameCreateRequestSchema: { parse(input: unknown): Record<string, unknown> }
  }
  learning?: {
    createPermissionService(db: SqlQueryable, options?: { lockDependencies?: boolean }): { assertCan(input: Parameters<LingxiLoopServices['permissionService']['assertCan']>[0]): Promise<unknown> }
    learningScoreBreakdownSchema: { parse(input: unknown): { label: string; score: number; weight: number; note?: string }[] }
    proposeLearningEvaluation(db: SqlQueryable, transaction: NativeTransaction, metric: (name: 'learning.state.changed' | 'learning.evaluation.proposed', labels?: Record<string, string>) => void,
      input: { companyId: string; channelId: string; agentId: string; attemptId: string; demonstratedLevel: number; confidence: number; rubricResults: { label: string; score: number; weight: number; note?: string }[]; feedback?: string; sourceEvidenceId?: string; verifierEvidenceId?: string }): Promise<{ evaluationId: string; status: 'ACCEPTED' | 'PENDING'; decisions: unknown[] }>
    findLearningDocumentEvidence(db: SqlQueryable, input: { companyId: string; projectId: string; documentId: string }): Promise<{ id: string; revision: number; authorId: string } | null>
    findLearningCanvasEvidence(db: SqlQueryable, input: { companyId: string; projectId: string; frameId: string }): Promise<{ id: string; revision: number; authorId: string } | null>
    recordLearningAttempt(db: SqlQueryable, transaction: NativeTransaction, infrastructure: {
      syncMessages(input: { channelId: string; channelType: number; limit: number; loginUid: string }): Promise<{ clientMsgNo: string; fromUid: string; authoredByAgent: boolean }[]>
      metric(name: string, labels?: Record<string, string>): void
    }, input: { companyId: string; channelId: string; agentId: string; activityId?: string; missionStepId?: string; evidenceClientMsgNos?: string[]; documentIds?: string[]; canvasFrameIds?: string[]; assistance?: 'NONE' | 'HINT' | 'GUIDED' }): Promise<{ id: string; learnerId: string }>
    createKnowledgeUnits(input: { companyId: string; projectId: string; actorId: string; actorKind: 'agent'; knowledgeUnits: { title: string; successCriteria: string; targetLevel?: number; prerequisiteKnowledgeUnitIds?: string[] }[] }): Promise<unknown>
    draftActivity(input: { companyId: string; projectId: string; actorId: string; actorKind: 'agent'; title: string; instructions: string; kind: 'LESSON' | 'PRACTICE' | 'ASSESSMENT' | 'PROJECT' | 'REVIEW'; evaluationMode?: 'AGENT_FORMATIVE' | 'TEACHER_REQUIRED'; targetLevel?: number; rubric?: unknown[]; knowledgeUnitIds?: string[]; dueAt?: string }): Promise<unknown>
    findLearningRoomState(db: SqlQueryable, scope: { companyId: string; channelId: string }): Promise<{ companyId: string; projectId: string; purpose: 'study' | 'lab' | 'discussion'; courseId?: string } | null>
    findEligibleLearningMissionCoordinator(db: SqlQueryable, input: { companyId: string; projectId: string; channelId: string; preferredPreset: string; currentAgentId: string }): Promise<string | null>
    upsertLearningMission(db: SqlQueryable, input: { id: string; companyId: string; projectId: string; learnerId: string; channelId: string; triggerClientMsgNo: string; goal: string; successCriteria: string; kind: 'STUDY' | 'RESEARCH' | 'PROJECT'; coordinatorAgentId: string; createdBy: string }): Promise<{ id: string; inserted: boolean }>
    findLearningMission(db: SqlQueryable, companyId: string, projectId: string, missionId: string): Promise<{ id: string; projectId: string; learnerId: string; conversationId: string; triggerClientMsgNo: string; goal: string; successCriteria: string; kind: 'STUDY' | 'RESEARCH' | 'PROJECT'; coordinatorAgentId: string; status: string } | null>
    inc(name: 'learning.mission.created' | 'learning.mission.deduplicated' | 'learning.attempt.accepted' | 'learning.state.changed' | 'learning.evaluation.proposed', labels?: Record<string, string>): void
    updateMissionStep(work: { companyId: string; channelId: string }, input: { missionId: string; stepId: string; status: 'OPEN' | 'IN_PROGRESS' | 'COMPLETED' | 'CANCELLED'; outcome?: string; sourceEvidenceId?: string; attemptId?: string }): Promise<unknown>
    addMissionSteps(work: { companyId: string; channelId: string }, missionId: string, steps: { kind: 'LEARN' | 'PRACTICE' | 'CHECK' | 'REFLECT'; description: string; successCriteria: string; knowledgeUnitId?: string }[]): Promise<unknown>
    finishMissionPlanning(work: { companyId: string; channelId: string }, missionId: string): Promise<unknown>
    completeMission(work: { companyId: string; channelId: string }, missionId: string): Promise<unknown>
    loadLearningTurnContext(work: NativeWork, actorId?: string): Promise<{ project: { id: string }; learnerId?: string; activeMission?: unknown; knowledgeUnits: unknown[]; due: unknown[] } | null | undefined>
    getMission(missionId: string, companyId: string, projectId: string, learnerId: string, conversationId: string): Promise<unknown>
    getActivity(activityId: string, companyId: string, projectId: string): Promise<unknown>
  }
  advanceAgentReadReceipt?(input: { companyId: string; channelId: string; agentId: string; readThroughSeq: number }): Promise<unknown>
  directory?: {
    getAgentCliIdentity(id: string): Promise<unknown>
    listAgentCliParticipants(actorId: string, kind: string | null): Promise<unknown[]>
    listAgentCliStatuses(actorId: string): Promise<unknown[]>
  }
  conversations?: {
    getAgentConversationMetadata(agentId: string, conversationId: string): Promise<unknown>
    addAgentConversationMember(agentId: string, conversationId: string, participantId: string): Promise<unknown>
    setAgentConversationTopic(agentId: string, conversationId: string, topic: string | null): Promise<unknown>
    setAgentConversationTitle(agentId: string, conversationId: string, title: string, expectedTitle?: string): Promise<unknown>
    listAgentConversationMutes(agentId: string): Promise<unknown[]>
    setAgentConversationMuted(agentId: string, conversationId: string, mute: boolean, until: Date | null): Promise<unknown>
  }
  messaging?: {
    missingAgentChannelMessageIds(input: { companyId: string; agentId: string; channelId: string; messageIds: string[] }): Promise<string[]>
    getAgentChannelHistory(input: { companyId: string; agentId: string; channelId: string; limit?: number; beforeSequence?: number }): Promise<Array<{ channelId: string; messageSeq: number }> | null>
    sendAgentChannelMessage(input: { companyId: string; agentId: string; channelId: string; clientNonce: string; payload: NativeTextMessage | (Omit<NativeTextMessage, 'kind'> & { kind: 'questionnaire' }) }): Promise<{ kind: 'accepted'; duplicate: boolean; messageId: string; sequence: number } | { kind: 'channel_not_found' | 'nonce_conflict' | 'verbatim_peer' }>
    getAgentInbox(input: { companyId: string; agentId: string; limit?: number }): Promise<Array<{ channelId: string }>>
    clearAgentChannelUnread(input: { companyId: string; agentId: string; channelId: string }): Promise<boolean>
    searchAgentMessages(input: { companyId: string; agentId: string; query: string; channelId?: string; limit?: number }): Promise<unknown[]>
    toggleAgentChannelReaction(input: { companyId: string; agentId: string; channelId: string; messageId: string; emoji: string }): Promise<{ kind: 'channel_not_found' | 'message_not_found' } | { kind: 'updated'; reactions: Array<{ emoji: string; count: number; users: string[] }> }>
  }
  handoffs?: {
    createHandoff(input: { companyId: string; conversationId: string; fromAgentId: string; toAgentId: string; title: string;
      contextMessageIds?: string[]; note?: string | null; idempotencyKey?: string | null }): Promise<{ id: string; sourceMessageId: string }>
    updateHandoff(input: { companyId: string; handoffId: string; actorAgentId: string; status: 'accepted' | 'working' | 'completed' | 'blocked'; note?: string | null }): Promise<unknown>
    listHandoffs(companyId: string, conversationId?: string): Promise<unknown[]>
  }
  email?: {
    getAgentEmailIdentity(scope: { userId: string; companyId: string }): Promise<{ email: string; displayName: string } | null>
    listAgentEmailContacts(scope: { userId: string; companyId: string }, query: string): Promise<unknown[]>
    listAgentEmailInbox(scope: { userId: string; companyId: string }, input: { unreadOnly: boolean; limit: number }): Promise<unknown[]>
    getAgentEmailThread(scope: { userId: string; companyId: string }, conversationId: string, limit: number): Promise<unknown>
    sendAgentEmail(scope: { userId: string; companyId: string }, input: { to: string[]; cc: string[]; subject: string; body: string; attachments: NativeEmailAttachment[] }, identity: { idempotencyKey?: string; projectId?: string }): Promise<unknown>
    replyToAgentEmail(scope: { userId: string; companyId: string }, messageId: string, input: { body: string; cc: string[]; attachments: NativeEmailAttachment[] }, identity: { idempotencyKey?: string; projectId?: string }): Promise<unknown>
  }
  presentations?: {
    createPresentationForAgent(work: NativeWork, input: { idempotencyKey: string; requirements: string; title?: string | undefined; sourceIds?: string[] | undefined; targetSlideCount?: number | undefined; language?: string | undefined }): Promise<unknown>
    getPresentationForAgent(work: NativeWork, id: string): Promise<unknown>
    cancelPresentationForAgent(work: NativeWork, id: string, input: { idempotencyKey: string }): Promise<unknown>
    retryPresentationForAgent(work: NativeWork, id: string, input: { idempotencyKey: string }): Promise<unknown>
    approvePresentationOutlineForAgent(work: NativeWork, id: string, input: { idempotencyKey?: string; expectedRevision: number }): Promise<unknown>
    revisePresentationOutlineForAgent(work: NativeWork, id: string, input: { idempotencyKey: string; expectedRevision: number; feedback?: string | undefined; targetSlideCount?: number | undefined }): Promise<unknown>
    revisePresentationForAgent(work: NativeWork, id: string, input: { idempotencyKey: string; instruction: string; scope: 'page' | 'section' | 'deck'; pageIds?: string[] | undefined; sectionIds?: string[] | undefined }): Promise<unknown>
    createPresentationRequestSchema: { parse(input: unknown): Parameters<NonNullable<LingxiLoopServices['presentations']>['createPresentationForAgent']>[1] }
    approvePresentationOutlineRequestSchema: { parse(input: unknown): Parameters<NonNullable<LingxiLoopServices['presentations']>['approvePresentationOutlineForAgent']>[2] }
    revisePresentationOutlineRequestSchema: { parse(input: unknown): Parameters<NonNullable<LingxiLoopServices['presentations']>['revisePresentationOutlineForAgent']>[2] }
    revisePresentationRequestSchema: { parse(input: unknown): Parameters<NonNullable<LingxiLoopServices['presentations']>['revisePresentationForAgent']>[2] }
  }
  pollApplication?: {
    conversationId(companyId: string, messageId: string): Promise<string | null>
    create(input: { companyId: string; actorId: string; conversationId: string; question: string; mode: 'single' | 'multi'; options: string[]; expiresInMinutes?: number | null; idempotencyKey?: string }): Promise<unknown>
    vote(input: { companyId: string; actorId: string; messageId: string; voterKind: 'agent'; optionIds: string[] }): Promise<unknown>
    close(input: { companyId: string; actorId: string; messageId: string; reason: 'manual' }): Promise<unknown>
    show(companyId: string, messageId: string): Promise<unknown>
  }
  knowledge: KnowledgeServices
  storage?: { readObjectBounded(key: string, maxBytes: number): Promise<Uint8Array> }
  wukongClient(): {
    syncMessages(channelId: string, channelType: number, limit: number, loginUid: string): Promise<NativeMessage[]>
    sendMessage(channelId: string, channelType: number, fromUid: string, payload: NativeTextMessage | (Omit<NativeTextMessage, 'kind'> & { kind: 'questionnaire' | 'learning_mission' })): Promise<{ messageId: string; messageSeq: number }>
  }
  permissionService: {
    assertCan(request: {
      actorUserId: string
      companyId: string
      projectId?: string
      action: 'calendar:write' | 'calendar:read' | 'document:write' | 'document:read' | 'document:delete' | 'agent_run:control' | 'agent_memory:read' | 'agent_memory:write' | 'canvas:write' | 'agent_approval:resolve' | 'learning:manage' | 'learning:read' | 'learning:submit' | 'agent:read' | 'conversation:read' | 'conversation:write' | 'conversation:manage' | 'email:read' | 'email:write' | 'knowledge:read' | 'knowledge:write' | 'knowledge:manage' | 'poll:read' | 'poll:create' | 'poll:vote' | 'poll:close'
      resource: { type: 'company' | 'agent' | 'message' | 'routine' | 'calendar_event' | 'document' | 'canvas_frame' | 'canvas' | 'conversation' | 'knowledge_source' | 'poll' | 'approval' | 'project'; id: string }
    }): Promise<unknown>
  }
}

export interface NativeEmailAttachment { key: string; filename: string; mimeType: string; sizeBytes: number }
