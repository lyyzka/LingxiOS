import { createHash, randomUUID } from 'node:crypto'
import { withTransaction, type SqlPool, type SqlQueryable } from '../../control-plane/pg-store.js'
import type { HostAction, WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices } from './service-contracts.js'

type Services = Pick<LingxiLoopServices, 'learning' | 'permissionService' | 'wukongClient'>

export async function startMission(database: SqlPool, services: Services, work: Omit<WorkItem, 'leaseToken'>, action: HostAction, channelType: number) {
  const api = services.learning
  if (!api || !work.principalId || action.action !== 'learning.start_mission') throw new Error('learning Mission services and principal are required')
  if (Object.keys(action.args).some(key => !['goal', 'successCriteria', 'missionKind', 'sourceClientMsgNo', 'explicit'].includes(key))) throw new Error('unknown Mission argument')
  const required = (key: string) => {
    const value = action.args[key]
    if (typeof value !== 'string' || !value.trim() || value.length > 10_000) throw new Error(`${key} must contain 1 to 10000 characters`)
    return value.trim()
  }
  const goal = required('goal'), successCriteria = required('successCriteria')
  const explicit = action.args['explicit'], requestedKind = action.args['missionKind']
  if (explicit !== undefined && typeof explicit !== 'boolean') throw new Error('explicit must be boolean')
  if (requestedKind !== undefined && requestedKind !== 'STUDY' && requestedKind !== 'RESEARCH' && requestedKind !== 'PROJECT') throw new Error('invalid Mission kind')
  const source = action.args['sourceClientMsgNo'] === undefined ? work.triggerRef : required('sourceClientMsgNo')
  await services.permissionService.assertCan({ actorUserId: work.principalId, companyId: work.tenantId,
    action: 'learning:submit', resource: { type: 'conversation', id: work.sessionId } })
  const room = await api.findLearningRoomState(database, { companyId: work.tenantId, channelId: work.sessionId })
  if (!room || room.companyId !== work.tenantId) throw new Error('conversation is not bound to a learning project')
  if (room.purpose !== 'study' && explicit !== true) throw new Error('Mission creation outside a study room requires an explicit learner request')
  if (channelType !== 1 && channelType !== 2) throw new Error('invalid Mission channel type')
  const messages = await services.wukongClient().syncMessages(work.sessionId, channelType, 100, work.agentId)
  const trigger = messages.find(message => message.clientMsgNo === source && message.channelId === work.sessionId && message.channelType === channelType)
  if (!trigger || trigger.fromUid !== work.principalId || trigger.payload.refs?.agentId || trigger.payload.kind !== 'text' || typeof trigger.payload.body !== 'string') throw new Error('Mission source must be the persisted principal’s committed text message in this conversation')
  const { rows: humans } = await database.query("SELECT name FROM participants WHERE company_id=$1 AND id=$2 AND kind='human' AND departed_at IS NULL", [work.tenantId, trigger.fromUid])
  if (humans.length !== 1) throw new Error('Mission source author is not an active human')
  await services.permissionService.assertCan({ actorUserId: trigger.fromUid, companyId: work.tenantId,
    action: 'learning:submit', resource: { type: 'project', id: room.projectId } })
  const kind = requestedKind ?? (room.purpose === 'lab' ? 'PROJECT' : 'STUDY')
  const result = await withTransaction(database, async client => {
    const current = await api.findLearningRoomState(client, { companyId: work.tenantId, channelId: work.sessionId })
    if (!current || current.projectId !== room.projectId || current.courseId !== room.courseId || current.purpose !== room.purpose) throw new Error('Mission room binding changed')
    const coordinatorAgentId = await api.findEligibleLearningMissionCoordinator(client, {
      companyId: room.companyId, projectId: room.projectId, channelId: work.sessionId,
      preferredPreset: kind === 'PROJECT' ? 'forge' : kind === 'RESEARCH' ? 'scout' : 'nova', currentAgentId: work.agentId,
    })
    if (!coordinatorAgentId) throw new Error('no eligible Mission coordinator is available in this conversation')
    const stored = await api.upsertLearningMission(client, { id: randomUUID(), companyId: room.companyId, projectId: room.projectId,
      learnerId: trigger.fromUid, channelId: work.sessionId, triggerClientMsgNo: source, goal, successCriteria, kind, coordinatorAgentId, createdBy: work.agentId })
    const mission = await api.findLearningMission(client, room.companyId, room.projectId, stored.id)
    if (!mission || mission.projectId !== room.projectId || mission.learnerId !== trigger.fromUid || mission.conversationId !== work.sessionId
      || mission.triggerClientMsgNo !== source || stored.inserted && mission.coordinatorAgentId !== coordinatorAgentId) throw new Error('stored Mission does not match its authorized source')
    if (stored.inserted && coordinatorAgentId !== work.agentId) {
      const id = 'mission-coordinator-' + createHash('sha256').update(JSON.stringify([work.tenantId, mission.id])).digest('hex')
      await client.query(`INSERT INTO lingxios.agent_work_items(id,tenant_id,agent_id,session_id,principal_id,thread_id,kind,lane,trigger_ref,priority,meta)
        VALUES($1,$2,$3,$4,$5,$6,'mission_coordinator','collaboration',$7,190,$8::jsonb)`,
        [id, work.tenantId, coordinatorAgentId, work.sessionId, trigger.fromUid, work.threadId ?? source, source,
          JSON.stringify({ text: trigger.payload.body, authorName: String(humans[0]!['name'] ?? 'Learner'), missionId: mission.id })])
    }
    return { mission, inserted: stored.inserted }
  })
  api.inc(result.inserted ? 'learning.mission.created' : 'learning.mission.deduplicated', result.inserted ? { mode: 'agent' } : undefined)
  const mission = result.mission
  await services.wukongClient().sendMessage(work.sessionId, channelType, work.agentId, {
    version: 1, kind: 'learning_mission', clientMsgNo: `learning-mission-${mission.id}`, body: mission.goal, refs: { agentId: work.agentId },
    data: { missionId: mission.id, projectId: room.projectId, ...(room.courseId ? { courseId: room.courseId } : {}),
      goal: mission.goal, successCriteria: mission.successCriteria, kind: mission.kind, coordinatorAgentId: mission.coordinatorAgentId,
      status: mission.status, suppressAgentWake: true },
  })
  return mission
}

export async function assertMissionCoordinatorWork(database: SqlQueryable, services: Pick<LingxiLoopServices, 'learning' | 'permissionService'>, work: Omit<WorkItem, 'leaseToken'>) {
  if (!services.learning || !work.principalId || typeof work.meta?.['missionId'] !== 'string') throw new Error('Mission coordinator identity is missing')
  await services.permissionService.assertCan({ actorUserId: work.principalId, companyId: work.tenantId,
    action: 'learning:submit', resource: { type: 'conversation', id: work.sessionId } })
  const room = await services.learning.findLearningRoomState(database, { companyId: work.tenantId, channelId: work.sessionId })
  const mission = room && await services.learning.findLearningMission(database, work.tenantId, room.projectId, work.meta['missionId'])
  if (!mission || mission.learnerId !== work.principalId || mission.conversationId !== work.sessionId
    || mission.coordinatorAgentId !== work.agentId || mission.triggerClientMsgNo !== work.triggerRef) throw new Error('Mission coordinator assignment or scope changed')
  if (!['PLANNING', 'ACTIVE', 'COMPLETED'].includes(mission.status)) throw new Error('Mission is no longer runnable')
  const eligible = await database.query(`SELECT 1 FROM participants agent JOIN conversations conversation
    ON conversation.company_id=agent.company_id AND conversation.id=$3 AND conversation.project_id=$4
    WHERE agent.company_id=$1 AND agent.id=$2 AND agent.kind='agent' AND agent.departed_at IS NULL
      AND agent.capabilities @> '["canvas","learning"]'::jsonb AND conversation.members ? agent.id`,
    [work.tenantId, work.agentId, work.sessionId, mission.projectId])
  if (eligible.rows.length !== 1) throw new Error('Mission coordinator is no longer eligible')
  return mission.id
}
