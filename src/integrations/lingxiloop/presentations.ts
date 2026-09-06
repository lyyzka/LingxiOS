import type { HostAction, WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices } from './service-contracts.js'
import { nativeWork } from './actions.js'

export const PRESENTATION_METHODS = {
  create: ['requirements', 'title', 'sourceIds', 'targetSlideCount', 'language'], get: ['presentationId'],
  cancel: ['presentationId'], retry: ['presentationId'],
  approve_outline: ['presentationId', 'expectedRevision'],
  revise_outline: ['presentationId', 'expectedRevision', 'feedback', 'targetSlideCount'],
  revise: ['presentationId', 'instruction', 'scope', 'pageIds', 'sectionIds'],
} as const

export async function executePresentation(work: Omit<WorkItem, 'leaseToken'>, action: HostAction, services: LingxiLoopServices) {
  const api = services.presentations
  const method = action.action.slice('presentations.'.length)
  if (!api || !action.action.startsWith('presentations.') || !Object.hasOwn(PRESENTATION_METHODS, method)) throw new Error('unsupported or approval-required presentation action')
  const allowed: readonly string[] = PRESENTATION_METHODS[method as keyof typeof PRESENTATION_METHODS]
  if (Object.keys(action.args).some(key => !allowed.includes(key))) throw new Error('unknown presentation argument')
  const native = nativeWork(work)
  await services.permissionService.assertCan({ actorUserId: native.authorizationUserId!, companyId: native.companyId,
    action: method === 'get' ? 'knowledge:read' : 'knowledge:write', resource: { type: 'conversation', id: native.channelId },
  })
  if (method === 'create') return api.createPresentationForAgent(native, api.createPresentationRequestSchema.parse({ ...action.args, idempotencyKey: action.idempotencyKey }))
  const { presentationId, ...args } = action.args
  if (typeof presentationId !== 'string' || !presentationId.trim()) throw new Error('presentationId is required')
  const input = { ...args, idempotencyKey: action.idempotencyKey }
  switch (method) {
    case 'get': return api.getPresentationForAgent(native, presentationId)
    case 'cancel': return api.cancelPresentationForAgent(native, presentationId, input)
    case 'retry': return api.retryPresentationForAgent(native, presentationId, input)
    case 'approve_outline': return api.approvePresentationOutlineForAgent(native, presentationId, api.approvePresentationOutlineRequestSchema.parse(input))
    case 'revise_outline': return api.revisePresentationOutlineForAgent(native, presentationId, api.revisePresentationOutlineRequestSchema.parse(input))
    case 'revise': return api.revisePresentationForAgent(native, presentationId, api.revisePresentationRequestSchema.parse(input))
    default: throw new Error('unsupported presentation action')
  }
}
