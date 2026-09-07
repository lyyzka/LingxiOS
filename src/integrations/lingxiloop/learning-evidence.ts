import { lockAction, recordActionResult } from '../../control-plane/action-transaction.js'
import { withTransaction, type SqlPool } from '../../control-plane/pg-store.js'
import type { HostAction, WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices } from './service-contracts.js'
import { teacherTransaction } from './teacher.js'

export async function recordAttempt(database: SqlPool, services: Pick<LingxiLoopServices, 'learning' | 'permissionService' | 'wukongClient'>,
  work: Omit<WorkItem, 'leaseToken'>, action: HostAction, channelType: number) {
  const api = services.learning
  if (!api || !work.principalId || action.action !== 'learning.record_attempt') throw new Error('learning evidence services and principal are required')
  if (Object.keys(action.args).some(key => !['activityId', 'missionStepId', 'evidenceClientMsgNos', 'documentIds', 'canvasFrameIds', 'assistance'].includes(key))) throw new Error('unknown attempt argument')
  const id = (value: unknown): string => {
    if (typeof value !== 'string' || !value.trim() || value.length > 2000) throw new Error('evidence identifiers must contain 1 to 2000 characters')
    return value.trim()
  }
  const activityId = action.args['activityId'] === undefined ? undefined : id(action.args['activityId'])
  const missionStepId = action.args['missionStepId'] === undefined ? undefined : id(action.args['missionStepId'])
  if (Boolean(activityId) === Boolean(missionStepId)) throw new Error('exactly one activityId or missionStepId is required')
  const refs = (key: string): string[] => {
    const value = action.args[key] === undefined ? [] : action.args[key]
    if (!Array.isArray(value) || value.length > 20) throw new Error('each evidence list is limited to 20 references')
    const result = value.map(id)
    if (new Set(result).size !== result.length) throw new Error('duplicate evidence reference')
    return result
  }
  const evidenceClientMsgNos = refs('evidenceClientMsgNos'), documentIds = refs('documentIds'), canvasFrameIds = refs('canvasFrameIds')
  if (!evidenceClientMsgNos.length && !documentIds.length && !canvasFrameIds.length) throw new Error('at least one evidence reference is required')
  const assistance = action.args['assistance'] === undefined ? 'NONE' : action.args['assistance']
  if (assistance !== 'NONE' && assistance !== 'HINT' && assistance !== 'GUIDED') throw new Error('invalid assistance')
  if (channelType !== 1 && channelType !== 2) throw new Error('invalid evidence channel type')
  await services.permissionService.assertCan({ actorUserId: work.principalId, companyId: work.tenantId,
    action: 'learning:submit', resource: { type: 'conversation', id: work.sessionId } })
  const messages = evidenceClientMsgNos.length ? await services.wukongClient().syncMessages(work.sessionId, channelType, 100, work.agentId) : []
  const scopedMessages = evidenceClientMsgNos.map(ref => {
    const message = messages.find(item => item.clientMsgNo === ref && item.channelId === work.sessionId && item.channelType === channelType)
    if (!message || message.fromUid !== work.principalId || message.payload.refs?.agentId) throw new Error('evidence message must be authored by the persisted human principal')
    return { clientMsgNo: ref, fromUid: work.principalId!, authoredByAgent: false }
  })
  let metric: { labels?: Record<string, string> } | undefined
  const result = await withTransaction(database, async client => {
    // Author/revision checks and native evidence reads must observe the same versions.
    await client.query('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
    await client.query("SET LOCAL statement_timeout='10s'")
    await lockAction(client, work, action)
    const room = await api.findLearningRoomState(client, { companyId: work.tenantId, channelId: work.sessionId })
    if (!room || room.companyId !== work.tenantId) throw new Error('conversation is not bound to a learning project')
    await api.createPermissionService(client, { lockDependencies: true }).assertCan({ actorUserId: work.principalId!, companyId: work.tenantId,
      action: 'learning:submit', resource: { type: 'conversation', id: work.sessionId } })
    const human = await client.query("SELECT 1 FROM participants WHERE company_id=$1 AND id=$2 AND kind='human' AND departed_at IS NULL", [work.tenantId, work.principalId])
    if (human.rows.length !== 1) throw new Error('evidence principal is not an active human')
    for (const documentId of documentIds) {
      const evidence = await api.findLearningDocumentEvidence(client, { companyId: work.tenantId, projectId: room.projectId, documentId })
      if (!evidence || evidence.authorId !== work.principalId) throw new Error('document evidence must belong to the persisted human principal in this project')
    }
    for (const frameId of canvasFrameIds) {
      const evidence = await api.findLearningCanvasEvidence(client, { companyId: work.tenantId, projectId: room.projectId, frameId })
      if (!evidence || evidence.authorId !== work.principalId) throw new Error('Canvas evidence must belong to the persisted human principal in this project')
    }
    const recorded = await api.recordLearningAttempt(client, teacherTransaction(client), {
      syncMessages: async input => {
        if (input.channelId !== work.sessionId || input.channelType !== channelType || input.loginUid !== work.agentId) throw new Error('native evidence scope changed')
        return scopedMessages
      },
      metric: (name, labels) => {
        if (name !== 'learning.attempt.accepted') throw new Error('unexpected native evidence metric')
        metric = labels ? { labels } : {}
      },
    }, { companyId: work.tenantId, channelId: work.sessionId, agentId: work.agentId,
      ...(activityId ? { activityId } : { missionStepId: missionStepId! }), evidenceClientMsgNos, documentIds, canvasFrameIds, assistance })
    if (recorded.learnerId !== work.principalId || !recorded.id) throw new Error('recorded attempt does not match the authorized learner')
    return recordActionResult(client, action, recorded)
  })
  if (metric) api.inc('learning.attempt.accepted', metric.labels)
  return result
}
