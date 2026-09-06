import type { HostAction, WorkItem } from '../../protocol/types.js'
import type { SqlQueryable } from '../../control-plane/pg-store.js'
import type { LingxiLoopServices } from './service-contracts.js'
import type { RequestSnapshot } from '../../context/request.js'

export const HANDOFF_METHODS = { list: [], create: ['toAgentId', 'title', 'contextMessageIds', 'note'],
  update: ['handoffId', 'status', 'note'] } as const

function strings(value: unknown, name: string, limit: number) {
  if (value === undefined) return []
  if (!Array.isArray(value) || value.length > limit || value.some(item => typeof item !== 'string' || !item.trim())) {
    throw new Error(`${name} must contain at most ${limit} non-empty strings`)
  }
  return value as string[]
}

export async function executeHandoff(work: Omit<WorkItem, 'leaseToken'>, action: HostAction, services: LingxiLoopServices) {
  const api = services.handoffs, method = action.action.slice('handoffs.'.length) as keyof typeof HANDOFF_METHODS
  if (!api || !work.principalId || !Object.hasOwn(HANDOFF_METHODS, method)
    || Object.keys(action.args).some(key => !HANDOFF_METHODS[method].includes(key as never))) throw new Error('unsupported handoff action')
  await services.permissionService.assertCan({ actorUserId: work.principalId, companyId: work.tenantId,
    action: method === 'list' ? 'conversation:read' : 'conversation:write', resource: { type: 'conversation', id: work.sessionId } })
  if (method === 'list') return api.listHandoffs(work.tenantId, work.sessionId)
  if (method === 'create') {
    const toAgentId = action.args['toAgentId'], title = action.args['title'], note = action.args['note']
    if (typeof toAgentId !== 'string' || !toAgentId.trim() || toAgentId === work.agentId || typeof title !== 'string' || !title.trim() || title.length > 500
      || note !== undefined && note !== null && (typeof note !== 'string' || note.length > 4_000)) throw new Error('handoff requires another target agent, title and optional bounded note')
    const contextMessageIds = strings(action.args['contextMessageIds'], 'contextMessageIds', 50)
    if (contextMessageIds.length && services.messaging) {
      const missing = await services.messaging.missingAgentChannelMessageIds({ companyId: work.tenantId, agentId: work.agentId,
        channelId: work.sessionId, messageIds: contextMessageIds })
      if (missing.length) throw new Error(`handoff context messages are unavailable: ${missing.join(', ')}`)
    } else if (contextMessageIds.length) throw new Error('native message validation is required for handoff context')
    return api.createHandoff({ companyId: work.tenantId, conversationId: work.sessionId, fromAgentId: work.agentId,
      toAgentId, title, contextMessageIds, note: typeof note === 'string' ? note : null, idempotencyKey: action.idempotencyKey })
  }
  const handoffId = action.args['handoffId'], status = action.args['status'], note = action.args['note']
  if (typeof handoffId !== 'string' || !handoffId.trim() || !['accepted', 'working', 'completed', 'blocked'].includes(String(status))
    || note !== undefined && note !== null && (typeof note !== 'string' || note.length > 4_000)) throw new Error('handoff update requires id, valid status and optional bounded note')
  return api.updateHandoff({ companyId: work.tenantId, handoffId, actorAgentId: work.agentId,
    status: status as 'accepted' | 'working' | 'completed' | 'blocked', note: typeof note === 'string' ? note : null })
}

export async function resolveHandoffIngress(database: SqlQueryable,
  services: Pick<LingxiLoopServices, 'wukongClient' | 'permissionService'>,
  input: { companyId: string; agentId: string; channelId: string; clientMsgNo: string }, channelType: number) {
  const identity = /^handoff:([^:]+):(created|completed|blocked)(?::([a-f0-9]{64}))?$/.exec(input.clientMsgNo)
  const created = identity?.[2] === 'created'
  if (!identity || (created ? input.clientMsgNo !== `handoff:${identity[1]}:created` : !identity[3])) throw new Error('invalid handoff ingress')
  const messages = await services.wukongClient().syncMessages(input.channelId, channelType, 100, input.agentId)
  const message = messages.find(item => item.clientMsgNo === input.clientMsgNo && item.channelId === input.channelId && item.channelType === channelType)
  const handoffId = message?.payload.refs?.handoffId
  if (!message || message.payload.kind !== 'handoff' || handoffId !== identity[1] || message.payload.refs?.toAgentId !== input.agentId) {
    throw new Error('committed handoff message not found')
  }
  const { rows } = await database.query(`SELECT handoff.from_agent_id,handoff.to_agent_id,handoff.title,handoff.note,handoff.context_message_ids,
      handoff.status,work.id AS parent_work_id,work.principal_id,human.name,intent.intent->>'requestVersion' AS request_version,
      work.meta->>'rootWorkId' AS root_work_id,snapshot.request_snapshot
    FROM agent_handoffs handoff
    JOIN lingxios.agent_action_intents intent ON intent.idempotency_key=handoff.idempotency_key
      AND intent.intent->'action'->>'action'='handoffs.create'
    JOIN lingxios.agent_work_items work ON work.id=intent.intent->>'workId' AND work.tenant_id=handoff.company_id
      AND work.agent_id=handoff.from_agent_id AND work.session_id=handoff.conversation_id
    JOIN lingxios.agent_request_snapshots snapshot ON snapshot.work_id=work.id
    JOIN participants human ON human.company_id=work.tenant_id AND human.id=work.principal_id
      AND human.kind='human' AND human.departed_at IS NULL
    WHERE handoff.id=$1 AND handoff.company_id=$2 AND handoff.conversation_id=$3
      AND (($5='created' AND handoff.to_agent_id=$4) OR ($5 IN ('completed','blocked') AND handoff.from_agent_id=$4 AND handoff.status=$5))`,
  [handoffId, input.companyId, input.channelId, input.agentId, identity[2]])
  const row = rows[0], principalId = row?.['principal_id']
  const parentRequest = row?.['request_snapshot'] as RequestSnapshot | undefined
  const parentRequestVersion = Number(row?.['request_version'])
  if (!row || typeof principalId !== 'string' || !parentRequest || parentRequest.workId !== row['parent_work_id']
    || parentRequest.revisions.length + 1 !== parentRequestVersion
    || message.fromUid !== row[created ? 'from_agent_id' : 'to_agent_id']) throw new Error('handoff has no valid source work')
  await services.permissionService.assertCan({ actorUserId: principalId, companyId: input.companyId,
    action: 'conversation:read', resource: { type: 'conversation', id: input.channelId } })
  const contextIds = Array.isArray(row['context_message_ids']) ? row['context_message_ids'].filter(id => typeof id === 'string') : []
  const text = [created ? `Handoff: ${String(row['title'])}` : `${identity[2] === 'completed' ? 'Completed' : 'Blocked'} handoff: ${String(row['title'])}`,
    row['note'] ? `Note: ${String(row['note'])}` : '', created && contextIds.length ? `Context message IDs: ${contextIds.join(', ')}` : ''].filter(Boolean).join('\n')
  return { handoffId, principalId, authorName: String(row['name']), text,
    parentWorkId: parentRequest.workId, rootWorkId: String(row['root_work_id'] ?? parentRequest.rootWorkId ?? parentRequest.workId),
    parentRequestVersion, instructionAuthorId: message.fromUid, parentRequest: structuredClone(parentRequest) }
}
