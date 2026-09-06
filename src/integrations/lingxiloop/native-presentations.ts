import type { LectureOperationInput, LectureRequestInput } from '../../lecture-deck/app.js'
import { validateCreateRequest, validateRevisionRequest } from '../../lecture-deck/contracts.js'
import type { LectureDeckService } from '../../lecture-deck/service.js'
import type { LingxiLoopServices, NativeWork } from './service-contracts.js'

interface LectureApp {
  enqueueLecture(input: LectureRequestInput): Promise<{ id: string; deckId: string; revision: number; status: string }>
  enqueueLectureOperation(input: LectureOperationInput): Promise<{ id: string; deckId: string; revision: number; status: string }>
  cancelLecture(input: { tenantId: string; principalId: string; deckId: string }): Promise<unknown>
}
const object = (value: unknown) => {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('presentation request is invalid')
  return value as Record<string, unknown>
}
const idempotency = (input: Record<string, unknown>) => {
  if (typeof input['idempotencyKey'] !== 'string' || !input['idempotencyKey'].trim()) throw new Error('idempotencyKey is required')
  return input['idempotencyKey']
}
const scope = (work: NativeWork) => {
  if (!work.authorizationUserId) throw new Error('presentation requires an authenticated principal')
  return { tenantId: work.companyId, principalId: work.authorizationUserId }
}
const appInput = (work: NativeWork) => ({ tenantId: work.companyId, principalId: work.authorizationUserId!, agentId: work.agentId, sessionId: work.channelId })

/** Package-owned presentation workflow; native services supply only product resources. */
export function createNativePresentationBridge(app: LectureApp, service: LectureDeckService): NonNullable<LingxiLoopServices['presentations']> {
  const createSchema = { parse(value: unknown) {
    const input = object(value), key = idempotency(input), { idempotencyKey: _key, ...request } = input
    return { ...validateCreateRequest(request), idempotencyKey: key }
  } }
  const revision = (value: unknown, fields: string[]) => {
    const input = object(value); idempotency(input)
    if (Object.keys(input).some(key => !fields.includes(key))) throw new Error('unknown presentation revision field')
    return input
  }
  const approveSchema = { parse(value: unknown) { const input = revision(value, ['idempotencyKey', 'expectedRevision']); if (!Number.isSafeInteger(input['expectedRevision']) || Number(input['expectedRevision']) < 1) throw new Error('expectedRevision is invalid'); return { idempotencyKey: idempotency(input), expectedRevision: Number(input['expectedRevision']) } } }
  const reviseSchema = { parse(value: unknown) {
    const input = revision(value, ['idempotencyKey', 'instruction', 'scope', 'pageIds', 'sectionIds']), key = idempotency(input)
    const parsed = validateRevisionRequest({ instruction: input['instruction'], scope: input['scope'], expectedRevision: 1, ...(input['pageIds'] === undefined ? {} : { pageIds: input['pageIds'] }), ...(input['sectionIds'] === undefined ? {} : { sectionIds: input['sectionIds'] }) })
    const { expectedRevision: _revision, ...request } = parsed
    return { ...request, idempotencyKey: key }
  } }
  return {
    createPresentationRequestSchema: createSchema,
    approvePresentationOutlineRequestSchema: approveSchema,
    revisePresentationOutlineRequestSchema: { parse: value => revision(value, ['idempotencyKey', 'expectedRevision', 'feedback', 'targetSlideCount']) as { idempotencyKey: string; expectedRevision: number; feedback?: string; targetSlideCount?: number } },
    revisePresentationRequestSchema: reviseSchema,
    async createPresentationForAgent(work, input) {
      const { idempotencyKey, ...request } = input
      const queued = await app.enqueueLecture({ ...appInput(work), id: idempotencyKey, sourceRef: work.triggerClientMsgNo, request })
      return { id: queued.deckId, status: queued.status, workId: queued.id, revision: queued.revision }
    },
    async getPresentationForAgent(work, id) {
      const deck = await service.get(scope(work), id)
      return { ...deck, title: deck.outline?.title ?? deck.request.title, outlineRevision: deck.revision,
        status: deck.status === 'awaiting_outline_approval' ? 'awaitingOutlineApproval' : deck.status }
    },
    cancelPresentationForAgent: (work, id) => app.cancelLecture({ ...scope(work), deckId: id }),
    async retryPresentationForAgent(work, id, input) {
      return app.enqueueLectureOperation({ ...appInput(work), deckId: id, operation: 'retry', idempotencyKey: input.idempotencyKey })
    },
    async approvePresentationOutlineForAgent(work, id, input) {
      const deck = await service.get(scope(work), id)
      if (deck.revision !== input.expectedRevision) throw new Error('presentation outline changed')
      return app.enqueueLectureOperation({ ...appInput(work), deckId: id, operation: 'approve_outline', idempotencyKey: input.idempotencyKey!, request: { expectedRevision: input.expectedRevision } })
    },
    async revisePresentationOutlineForAgent(work, id, input) {
      const deck = await service.get(scope(work), id)
      if (deck.revision !== input.expectedRevision) throw new Error('presentation outline changed')
      const instruction = [input.feedback, input.targetSlideCount === undefined ? '' : `Use ${input.targetSlideCount} slides.`].filter(Boolean).join(' ')
      if (!instruction) throw new Error('outline feedback or targetSlideCount is required')
      return app.enqueueLectureOperation({ ...appInput(work), deckId: id, operation: 'revise_outline', idempotencyKey: input.idempotencyKey!, request: { expectedRevision: deck.revision, ...(input.feedback ? { feedback: input.feedback } : {}), ...(input.targetSlideCount === undefined ? {} : { targetSlideCount: input.targetSlideCount }) } })
    },
    async revisePresentationForAgent(work, id, input) {
      const deck = await service.get(scope(work), id)
      return app.enqueueLectureOperation({ ...appInput(work), deckId: id, operation: 'revise', idempotencyKey: input.idempotencyKey,
        request: { instruction: input.instruction, scope: input.scope, expectedRevision: deck.revision, ...(input.pageIds ? { pageIds: input.pageIds } : {}), ...(input.sectionIds ? { sectionIds: input.sectionIds } : {}) } })
    },
  }
}
