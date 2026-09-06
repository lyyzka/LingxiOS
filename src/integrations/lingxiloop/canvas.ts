import type { HostAction, WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices } from './service-contracts.js'
import { nativeWork } from './actions.js'

export const CANVAS_METHODS = { current: [], available_agents: [], add_comment: ['body', 'frameId'], create_frame: ['frame'], update_frame: ['frameId', 'patch'], append_content: ['frameId', 'content'], delete_frame: ['frameId'] } as const

export async function executeCanvas(work: Omit<WorkItem, 'leaseToken'>, action: HostAction, services: Pick<LingxiLoopServices, 'canvas' | 'permissionService'>) {
  const api = services.canvas
  const method = action.action.slice('canvas.'.length)
  if (!api || !action.action.startsWith('canvas.') || !Object.hasOwn(CANVAS_METHODS, method)) throw new Error('unsupported canvas action')
  const allowed: readonly string[] = CANVAS_METHODS[method as keyof typeof CANVAS_METHODS]
  if (Object.keys(action.args).some(key => !allowed.includes(key))) throw new Error(method === 'current' ? 'canvas.current accepts no arguments' : 'unknown canvas argument')
  const native = nativeWork(work)
  // The native snapshot reader does not authorize its actor argument.
  await services.permissionService.assertCan({ actorUserId: native.authorizationUserId!, companyId: native.companyId,
    action: 'conversation:read', resource: { type: 'conversation', id: native.channelId } })
  if (method === 'available_agents') {
    await services.permissionService.assertCan({ actorUserId: native.authorizationUserId!, companyId: native.companyId,
      action: 'agent:read', resource: { type: 'conversation', id: native.channelId } })
    return api.listCanvasAvailableAgents(native.companyId)
  }
  const snapshot = await api.getConversationCanvas(native.companyId, native.channelId, native.authorizationUserId!)
  if (method === 'current') return snapshot
  if (!snapshot || typeof snapshot !== 'object' || !('id' in snapshot) || typeof snapshot.id !== 'string'
    || !snapshot.id.trim() || !('companyId' in snapshot) || snapshot.companyId !== native.companyId
    || !('conversationId' in snapshot) || snapshot.conversationId !== native.channelId) throw new Error('current conversation canvas not found')
  if (method === 'add_comment') {
    const comment = api.canvasCommentRequestSchema.parse({ ...action.args, canvasId: snapshot.id })
    if (comment.frameId && (!('frames' in snapshot) || !Array.isArray(snapshot.frames)
      || !snapshot.frames.some(frame => frame && typeof frame === 'object' && frame.id === comment.frameId))) throw new Error('frame is outside the current conversation canvas')
    await services.permissionService.assertCan({ actorUserId: native.authorizationUserId!, companyId: native.companyId,
      action: 'canvas:write', resource: comment.frameId ? { type: 'canvas_frame', id: comment.frameId } : { type: 'canvas', id: snapshot.id } })
    return api.addCanvasComment({ companyId: native.companyId, actorId: native.agentId, actorKind: 'agent',
      canvasId: snapshot.id, body: comment.body, ...(comment.frameId ? { frameId: comment.frameId } : {}) })
  }
  if (method === 'update_frame' || method === 'append_content' || method === 'delete_frame') {
    const frameId = action.args['frameId'], patch = action.args['patch']
    if (typeof frameId !== 'string' || !frameId.trim() || frameId.length > 2000) throw new Error('frameId must be non-empty')
    if (!('frames' in snapshot) || !Array.isArray(snapshot.frames)
      || !snapshot.frames.some(frame => frame && typeof frame === 'object' && frame.id === frameId)) throw new Error('frame is outside the current conversation canvas')
    if (method === 'delete_frame') {
      await services.permissionService.assertCan({ actorUserId: native.authorizationUserId!, companyId: native.companyId,
        action: 'canvas:write', resource: { type: 'canvas_frame', id: frameId } })
      return api.deleteCanvasFrame({ companyId: native.companyId, actorId: native.agentId, actorKind: 'agent', frameId })
    }
    if (method === 'append_content') {
      const content = action.args['content']
      if (typeof content !== 'string' || !content.length || Buffer.byteLength(content, 'utf8') > 64 * 1024) throw new Error('append content must contain 1 to 65536 UTF-8 bytes')
      await services.permissionService.assertCan({ actorUserId: native.authorizationUserId!, companyId: native.companyId,
        action: 'canvas:write', resource: { type: 'canvas_frame', id: frameId } })
      return api.appendCanvasFrameContent({ companyId: native.companyId, actorId: native.agentId, actorKind: 'agent', frameId, content })
    }
    if (!patch || typeof patch !== 'object' || Array.isArray(patch)) throw new Error('patch must be an object')
    const checked = api.canvasFrameUpdateRequestSchema.parse(patch)
    if (!Number.isSafeInteger(checked['baseRevision']) || Number(checked['baseRevision']) < 0) throw new Error('baseRevision is required for frame updates')
    if (!Object.keys(checked).some(key => key !== 'baseRevision')) throw new Error('frame update requires changed fields')
    await services.permissionService.assertCan({ actorUserId: native.authorizationUserId!, companyId: native.companyId,
      action: 'canvas:write', resource: { type: 'canvas_frame', id: frameId } })
    return api.updateCanvasFrame({ companyId: native.companyId, actorId: native.agentId, actorKind: 'agent', frameId, patch: checked })
  }
  const frame = action.args['frame']
  if (!frame || typeof frame !== 'object' || Array.isArray(frame)) throw new Error('frame must be an object')
  if (Object.hasOwn(frame, 'canvasId')) throw new Error('canvasId is supplied by the package')
  const checked = api.canvasFrameCreateRequestSchema.parse({ ...frame, canvasId: snapshot.id })
  await services.permissionService.assertCan({ actorUserId: native.authorizationUserId!, companyId: native.companyId,
    action: 'canvas:write', resource: { type: 'canvas', id: snapshot.id } })
  return api.createCanvasFrame({ companyId: native.companyId, actorId: native.agentId, actorKind: 'agent',
    idempotencyKey: action.idempotencyKey, canvasId: snapshot.id, frame: checked })
}
