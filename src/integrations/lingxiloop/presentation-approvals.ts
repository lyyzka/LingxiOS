import { isDeepStrictEqual } from 'node:util'
import { withTransaction, type SqlPool } from '../../control-plane/pg-store.js'
import type { HostAction, WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices } from './service-contracts.js'
import { nativeWork } from './actions.js'
import { recordExecutedApproval, claimApprovalExecution, inspectApproval, persistApproval, resumeApproved } from './approvals.js'
import { createLectureDeckApp } from '../../lecture-deck/app.js'
import type { LectureDeckService } from '../../lecture-deck/service.js'

type Services = Pick<LingxiLoopServices, 'presentations' | 'permissionService'>

export async function preparePresentationApproval(services: Services, work: Omit<WorkItem, 'leaseToken'>,
  action: HostAction, approverId?: string, recovering = false) {
  const api = services.presentations
  if (!api || action.action !== 'presentations.approve_outline'
    || Object.keys(action.args).some(key => !['presentationId', 'expectedRevision'].includes(key))) throw new Error('unsupported presentation approval')
  if (!work.principalId) throw new Error('persisted human authorization principal is required')
  const presentationId = action.args['presentationId']
  if (typeof presentationId !== 'string' || !presentationId.trim()) throw new Error('presentationId is required')
  for (const userId of new Set([work.principalId, ...(approverId ? [approverId] : [])])) {
    await services.permissionService.assertCan({ actorUserId: userId, companyId: work.tenantId,
      action: 'knowledge:write', resource: { type: 'conversation', id: work.sessionId } })
  }
  const input = api.approvePresentationOutlineRequestSchema.parse({ expectedRevision: action.args['expectedRevision'], idempotencyKey: action.idempotencyKey })
  const current = recovering ? { id: presentationId, outlineRevision: input.expectedRevision }
    : await api.getPresentationForAgent(nativeWork(work), presentationId) as Record<string, unknown>
  if (current['id'] !== presentationId || current['outlineRevision'] !== input.expectedRevision
    || !recovering && (current['status'] !== 'awaitingOutlineApproval' || !current['outline'])) throw new Error('presentation outline changed or is not awaiting approval')
  const preview = { presentationId, expectedRevision: input.expectedRevision, title: current['title'], outline: current['outline'] }
  if (JSON.stringify(preview).length > 128_000) throw new Error('presentation outline is too large to approve safely')
  return { preview, execute: () => api.approvePresentationOutlineForAgent(nativeWork(work), presentationId, input) }
}

export async function requestPresentationApproval(database: SqlPool, services: Services,
  work: Omit<WorkItem, 'leaseToken'>, action: HostAction) {
  const { preview } = await preparePresentationApproval(services, work, action)
  return withTransaction(database, db => persistApproval(db, work, action, {
    summary: 'Approve the reviewed presentation outline', scope: { conversationId: work.sessionId, presentationId: preview.presentationId }, preview,
  }))
}

export async function approvePresentation(database: SqlPool, services: Services,
  input: { companyId: string; userId: string; approvalId: string }, lecture: LectureDeckService) {
  const reviewed = await inspectApproval(database, services, input)
  if (reviewed.action.action !== 'presentations.approve_outline') throw new Error('unsupported presentation approval')
  if (reviewed.status === 'EXECUTED') return resumeApproved(database, input, reviewed)
  await withTransaction(database, async db => {
  const { intent, recovering } = await claimApprovalExecution(db, input, reviewed)
  const work = { id: intent.workId, fence: 0, homeEpoch: 0, tenantId: intent.tenantId, agentId: intent.agentId,
    sessionId: intent.sessionId, triggerRef: '', kind: 'resume' as const, lane: 'approval' as const,
    ...(intent.principalId ? { principalId: intent.principalId } : {}) }
  const prepared = await preparePresentationApproval(services, work, reviewed.action, input.userId, recovering)
  if (!recovering && !isDeepStrictEqual(prepared.preview, reviewed.preview)) throw new Error('presentation approval preview is stale')
  const value = await createLectureDeckApp(database, lecture).enqueueLectureOperation({
    tenantId: intent.tenantId, principalId: intent.principalId!, agentId: intent.agentId, sessionId: intent.sessionId,
    ...(intent.threadId ? { threadId: intent.threadId } : {}), deckId: String(reviewed.action.args['presentationId']),
    operation: 'approve_outline', idempotencyKey: reviewed.action.idempotencyKey,
    request: { expectedRevision: reviewed.action.args['expectedRevision'] },
  }, db)
    await recordExecutedApproval(db, input, reviewed, value, 'EXECUTING')
  })
  return resumeApproved(database, input, reviewed)
}
