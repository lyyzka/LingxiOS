import assert from 'node:assert/strict'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import ts from 'typescript'

const root = fileURLToPath(new URL('../', import.meta.url))
const source = resolve(process.argv[2] ?? process.env.LINGXILOOP_SOURCE ?? join(root, '../LingxiLoop/server/src')).replaceAll('\\', '/')
const contracts = join(root, 'src/integrations/lingxiloop/service-contracts.js').replaceAll('\\', '/')
const directory = await mkdtemp(join(tmpdir(), 'lingxios-contracts-'))
try {
  const fixture = join(directory, 'contracts.mts')
  await writeFile(fixture, `
import type * as calendar from ${JSON.stringify(`${source}/modules/calendar/index.js`)}
import type * as calendarApplication from ${JSON.stringify(`${source}/modules/calendar/application.js`)}
import type * as calendarSchemas from ${JSON.stringify(`${source}/modules/calendar/contracts.js`)}
import type * as canvas from ${JSON.stringify(`${source}/modules/canvas/index.js`)}
import type * as canvasOrchestration from ${JSON.stringify(`${source}/canvas/orchestration.js`)}
import type * as canvasAssignments from ${JSON.stringify(`${source}/modules/canvas/assignments-application.js`)}
import type * as canvasAssignmentRepository from ${JSON.stringify(`${source}/modules/canvas/assignments-repository.js`)}
import type * as canvasEvidence from ${JSON.stringify(`${source}/modules/evidence/public.js`)}
import type * as documents from ${JSON.stringify(`${source}/modules/documents/public.js`)}
import type * as documentRepository from ${JSON.stringify(`${source}/modules/documents/repository.js`)}
import type * as documentApplication from ${JSON.stringify(`${source}/modules/documents/application.js`)}
import type * as documentCollaboration from ${JSON.stringify(`${source}/modules/documents/collaboration-application.js`)}
import type * as documentStorage from ${JSON.stringify(`${source}/storage.js`)}
import type * as documentSchemas from ${JSON.stringify(`${source}/modules/documents/contracts.js`)}
import type * as documentBus from ${JSON.stringify(`${source}/redis.js`)}
import type * as learning from ${JSON.stringify(`${source}/modules/learning/runtime.js`)}
import type * as missionsApplication from ${JSON.stringify(`${source}/modules/learning/missions-application.js`)}
import type * as learningSchemas from ${JSON.stringify(`${source}/modules/learning/contracts.js`)}
import type * as learningEvidence from ${JSON.stringify(`${source}/modules/learning/evidence-repository.js`)}
import type * as missionRepository from ${JSON.stringify(`${source}/modules/learning/missions-repository.js`)}
import type * as evaluation from ${JSON.stringify(`${source}/modules/learning/evaluation-application.js`)}
import type * as metrics from ${JSON.stringify(`${source}/metrics.js`)}
import type * as projects from ${JSON.stringify(`${source}/modules/projects/public.js`)}
import type * as projection from ${JSON.stringify(`${source}/modules/learning/project-lifecycle-projection.js`)}
import type * as curriculum from ${JSON.stringify(`${source}/modules/learning/curriculum-application.js`)}
import type * as teacherManagement from ${JSON.stringify(`${source}/modules/learning/teacher-management-repository.js`)}
import type * as teacherApproval from ${JSON.stringify(`${source}/modules/learning/teacher-approval-repository.js`)}
import type * as teacherReporting from ${JSON.stringify(`${source}/modules/learning/teacher-reporting-repository.js`)}
import type * as identity from ${JSON.stringify(`${source}/modules/identity/public.js`)}
import type * as teacherRepository from ${JSON.stringify(`${source}/modules/learning/teacher-runtime-repository.js`)}
import type * as membership from ${JSON.stringify(`${source}/modules/learning/membership-application.js`)}
import type * as rooms from ${JSON.stringify(`${source}/modules/learning/rooms-repository.js`)}
import type * as effects from ${JSON.stringify(`${source}/modules/learning/effects-repository.js`)}
import type * as presentations from ${JSON.stringify(`${source}/modules/presentations/public.js`)}
import type * as email from ${JSON.stringify(`${source}/modules/email/index.js`)}
import type * as directory from ${JSON.stringify(`${source}/modules/agents/index.js`)}
import type * as conversations from ${JSON.stringify(`${source}/modules/conversations/public.js`)}
import type * as knowledge from ${JSON.stringify(`${source}/modules/knowledge/public.js`)}
import type { pollApplication } from ${JSON.stringify(`${source}/modules/polls/index.js`)}
import type * as access from ${JSON.stringify(`${source}/modules/access/public.js`)}
import type { permissionService } from ${JSON.stringify(`${source}/modules/access/public.js`)}
import type { advanceAgentReadReceipt } from ${JSON.stringify(`${source}/im/read-receipts.js`)}
import type { wukongClient } from ${JSON.stringify(`${source}/im/wukong.js`)}
import type * as messaging from ${JSON.stringify(`${source}/im/public.js`)}
import type * as handoffs from ${JSON.stringify(`${source}/agents/coworker.js`)}
import type { storage } from ${JSON.stringify(`${source}/storage.js`)}
import type { LingxiLoopServices } from ${JSON.stringify(contracts)}
declare const native: { calendar: typeof calendar & typeof calendarSchemas; teacher: typeof evaluation & typeof metrics & typeof projects & typeof projection & typeof curriculum & typeof teacherManagement & typeof teacherApproval & typeof teacherReporting & typeof identity & typeof teacherRepository & typeof learning & typeof membership & typeof rooms & typeof effects; canvas: typeof canvas; storage: typeof storage; learning: typeof learning & typeof missionRepository & typeof metrics & typeof missionsApplication & typeof learningEvidence & typeof evaluation & typeof learningSchemas & typeof access; advanceAgentReadReceipt: typeof advanceAgentReadReceipt; messaging: typeof messaging; handoffs: typeof handoffs; directory: typeof directory; conversations: typeof conversations; email: typeof email; presentations: typeof presentations; pollApplication: typeof pollApplication; knowledge: typeof knowledge; permissionService: typeof permissionService; wukongClient: typeof wukongClient }
const binding: LingxiLoopServices = native
declare const nativeCanvasOrchestration: typeof canvasOrchestration & typeof canvasAssignments & typeof canvasAssignmentRepository & typeof canvasEvidence & typeof access & typeof documentBus
const packagedCanvasOrchestration: NonNullable<NonNullable<LingxiLoopServices['canvas']>['orchestration']> = nativeCanvasOrchestration
declare const nativeCalendarWrites: typeof calendarApplication & typeof calendarSchemas & typeof documentBus & typeof access
const packagedCalendarWrites: NonNullable<NonNullable<LingxiLoopServices['calendar']>['writes']> = nativeCalendarWrites
declare const nativeDocuments: typeof documents
const packagedDocuments: NonNullable<LingxiLoopServices['documents']> = nativeDocuments
declare const nativeDocumentWrites: typeof documentRepository & typeof documentSchemas & typeof documentBus & typeof access
const packagedDocumentWrites: NonNullable<NonNullable<LingxiLoopServices['documents']>['writes']> = nativeDocumentWrites
declare const nativeDocumentContent: typeof documentApplication & typeof documentCollaboration & typeof documentStorage & typeof documentBus
const packagedDocumentContent: NonNullable<NonNullable<NonNullable<LingxiLoopServices['documents']>['writes']>['content']> = nativeDocumentContent
const documentBoundary: {
  listAgentDocuments(scope: { companyId: string; projectId: string }): Promise<unknown[]>
  listRecentAgentDocumentCreations(scope: { companyId: string; projectId: string; userId: string }, sinceMinutes: number): Promise<unknown[]>
  getAgentDocument(scope: { companyId: string; projectId: string }, documentId: string): Promise<unknown>
  readAgentDocument(scope: { companyId: string; projectId: string; userId: string }, documentId: string): Promise<{ id: string; body: string }>
  createAgentDocument(scope: { companyId: string; projectId: string; userId: string }, input: { id?: string; title: string; body: string }): Promise<{ document: { id: string }; replayed: boolean }>
  renameAgentDocument(scope: { companyId: string; projectId: string; userId: string }, documentId: string, title: string): Promise<{ ok: true; title: string }>
  deleteAgentDocument(scope: { companyId: string; projectId: string; userId: string }, documentId: string): Promise<{ ok: true }>
} = nativeDocuments
`)
  const program = ts.createProgram([fixture], { target: ts.ScriptTarget.ES2023, module: ts.ModuleKind.NodeNext, moduleResolution: ts.ModuleResolutionKind.NodeNext, strict: true, skipLibCheck: true, noEmit: true })
  const file = program.getSourceFile(fixture)
  assert.ok(file)
  const diagnostics = program.getSemanticDiagnostics(file)
  if (diagnostics.length) throw new Error(ts.formatDiagnosticsWithColorAndContext(diagnostics, {
    getCurrentDirectory: () => root, getCanonicalFileName: (file) => file, getNewLine: () => '\n',
  }))
  const checker = program.getTypeChecker()
  let native, nativeDocuments, nativeDocumentWrites, nativeCalendarWrites
  function visit(node) {
    if (ts.isVariableDeclaration(node) && node.name.getText() === 'native') native = checker.getTypeAtLocation(node)
    if (ts.isVariableDeclaration(node) && node.name.getText() === 'nativeCalendarWrites') nativeCalendarWrites = checker.getTypeAtLocation(node)
    if (ts.isVariableDeclaration(node) && node.name.getText() === 'nativeDocuments') nativeDocuments = checker.getTypeAtLocation(node)
    if (ts.isVariableDeclaration(node) && node.name.getText() === 'nativeDocumentWrites') nativeDocumentWrites = checker.getTypeAtLocation(node)
    ts.forEachChild(node, visit)
  }
  visit(file)
  assert.ok(native && !(native.flags & ts.TypeFlags.Any))
  assert.ok(nativeCalendarWrites && !(nativeCalendarWrites.flags & ts.TypeFlags.Any))
  const calendarConstructor = checker.getTypeOfSymbolAtLocation(nativeCalendarWrites.getProperty('CalendarApplication'), file)
  assert.ok(!(calendarConstructor.flags & ts.TypeFlags.Any) && calendarConstructor.getConstructSignatures().length)
  assert.ok(nativeDocuments && !(nativeDocuments.flags & ts.TypeFlags.Any))
  assert.ok(nativeDocumentWrites && !(nativeDocumentWrites.flags & ts.TypeFlags.Any))
  for (const name of ['listAgentDocuments', 'listRecentAgentDocumentCreations', 'getAgentDocument', 'readAgentDocument', 'createAgentDocument', 'renameAgentDocument', 'deleteAgentDocument']) {
    const symbol = nativeDocuments.getProperty(name)
    assert.ok(symbol, `missing actual document export ${name}`)
    const type = checker.getTypeOfSymbolAtLocation(symbol, file)
    assert.ok(!(type.flags & ts.TypeFlags.Any) && type.getCallSignatures().length, `${name} must resolve to a typed callable`)
  }
  for (const [resource, names] of Object.entries({
    calendar: [],
    canvas: ['addCanvasComment', 'listCanvasAvailableAgents', 'getConversationCanvas', 'createCanvasFrame', 'updateCanvasFrame', 'appendCanvasFrameContent', 'deleteCanvasFrame'],
    knowledge: ['listKnowledgeSourcesForAgent', 'addKnowledgeText', 'addKnowledgeUrl', 'addKnowledgeFile', 'retryKnowledgeSourceForAgent', 'setKnowledgeSourceEnabled', 'deleteKnowledgeSourceForAgent'],
    teacher: ['setLearningObjectiveStatus', 'publishLearningActivity', 'closeLearningActivity', 'reviewLearningEvaluation', 'inc', 'projectLifecycleProjection', 'ProjectLifecycleApplication', 'createLearningObjectives', 'createLearningActivity', 'updateTeacherCourseMetadata', 'setLearningCourseMembership', 'bindLearningCourseRoom', 'loadTeacherOverviewRows', 'listTeacherLearnerRows', 'findTeacherLearner', 'loadTeacherLearnerDetailRows', 'findTeacherAttemptDetail', 'listTeacherObjectives', 'listTeacherActivities', 'listTeacherReviews', 'listTeacherBindableRooms', 'auditInTransaction', 'findTeacherScopeBinding', 'findTeacherTurnCounts', 'findTeacherObjectiveApprovalTarget', 'findTeacherActivityApprovalTarget', 'findTeacherCourseApprovalTarget', 'findTeacherMembershipApprovalTarget', 'findTeacherEvaluationApprovalTarget', 'assertTeacherApprovalFresh', 'requireLearningCourseRole', 'setLearningCourseMembershipRecord', 'enqueueLearningEffect'],
    learning: ['createPermissionService', 'proposeLearningEvaluation', 'recordLearningAttempt', 'findLearningDocumentEvidence', 'findLearningCanvasEvidence', 'createKnowledgeUnits', 'draftActivity', 'findLearningRoomState', 'findEligibleLearningMissionCoordinator', 'upsertLearningMission', 'findLearningMission', 'inc', 'loadLearningTurnContext', 'getMission', 'getActivity'],
    presentations: ['createPresentationForAgent', 'getPresentationForAgent', 'approvePresentationOutlineForAgent', 'revisePresentationOutlineForAgent', 'revisePresentationForAgent', 'cancelPresentationForAgent', 'retryPresentationForAgent'],
    email: ['getAgentEmailIdentity', 'listAgentEmailContacts', 'listAgentEmailInbox', 'getAgentEmailThread', 'sendAgentEmail', 'replyToAgentEmail'],
    directory: ['getAgentCliIdentity', 'listAgentCliParticipants', 'listAgentCliStatuses'],
    conversations: ['getAgentConversationMetadata', 'addAgentConversationMember', 'setAgentConversationTopic', 'setAgentConversationTitle', 'listAgentConversationMutes', 'setAgentConversationMuted'],
    messaging: ['missingAgentChannelMessageIds', 'getAgentChannelHistory', 'sendAgentChannelMessage', 'getAgentInbox', 'clearAgentChannelUnread', 'searchAgentMessages', 'toggleAgentChannelReaction'],
    handoffs: ['createHandoff', 'updateHandoff', 'listHandoffs'],
    pollApplication: ['conversationId', 'create', 'vote', 'close', 'show'],
    permissionService: ['assertCan'],
    storage: ['readObjectBounded'],
  })) {
    const resourceSymbol = native.getProperty(resource)
    assert.ok(resourceSymbol, `missing native resource ${resource}`)
    const resourceType = checker.getTypeOfSymbolAtLocation(resourceSymbol, file)
    assert.ok(!(resourceType.flags & ts.TypeFlags.Any), `${resource} resolved to any`)
    for (const name of names) {
    const symbol = resourceType.getProperty(name)
    assert.ok(symbol, `missing actual export ${name}`)
    const type = checker.getTypeOfSymbolAtLocation(symbol, file)
    assert.ok(!(type.flags & ts.TypeFlags.Any) && (type.getCallSignatures().length || type.getConstructSignatures().length), `${name} resolved to an unknown/any callable`)
    for (const parameter of (type.getCallSignatures()[0] ?? type.getConstructSignatures()[0]).parameters) {
      assert.ok(!(checker.getTypeOfSymbolAtLocation(parameter, file).flags & ts.TypeFlags.Any), `${name} parameter resolved to any`)
    }
    }
  }
  const calendarType = checker.getTypeOfSymbolAtLocation(native.getProperty('calendar'), file)
  for (const [object, methods] of [['calendarApplication', ['list', 'get', 'dispatches']], ['listCalendarEventsQuerySchema', ['parse']]]) {
    const objectType = checker.getTypeOfSymbolAtLocation(calendarType.getProperty(object), file)
    assert.ok(!(objectType.flags & ts.TypeFlags.Any))
    for (const method of methods) {
      const member = objectType.getProperty(method)
      assert.ok(member, 'missing native calendar method ' + method)
      const type = checker.getTypeOfSymbolAtLocation(member, file)
      assert.ok(!(type.flags & ts.TypeFlags.Any) && type.getCallSignatures().length)
    }
  }
  console.log('All configured LingxiLoop native service signatures accept direct resource binding.')
  console.log('This checks real source signatures only; domain execution and production integration remain separate gates.')
} finally {
  assert.equal(dirname(directory), resolve(tmpdir()))
  await rm(directory, { recursive: true, force: true })
}
