import type { HostAction, WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices, NativeWork } from './service-contracts.js'

export const KNOWLEDGE_METHODS = {
  list_sources: [], check_source: ['sourceId', 'expected'], add_text: ['title', 'text'], add_url: ['title', 'url'],
  add_file: ['title', 'clientMsgNo'], retry_ingestion: ['sourceId'],
  set_source_enabled: ['sourceId', 'enabled'], delete_source: ['sourceId'],
} as const

export function nativeWork(work: Omit<WorkItem, 'leaseToken'>): NativeWork {
  if (!work.principalId) throw new Error('persisted human authorization principal is required')
  return { id: work.id, fence: work.fence, homeEpoch: work.homeEpoch,
    companyId: work.tenantId, authorizationUserId: work.principalId, agentId: work.agentId, channelId: work.sessionId,
    ...(work.threadId !== undefined ? { threadRootClientMsgNo: work.threadId } : {}), triggerClientMsgNo: work.triggerRef,
    reason: work.kind === 'resume' ? 'resume' : work.kind === 'routine' ? 'routine' : work.kind === 'mission_coordinator' ? 'handoff' : 'message', executionRole: 'coordinator',
    lane: work.lane === 'interactive' ? 'learner' : work.lane, leaseToken: '',
  }
}

function text(args: Record<string, unknown>, name: string, optional = false): string {
  const value = args[name]
  if (optional && value === undefined) return ''
  if (typeof value !== 'string' || !value.trim()) throw new Error(`${name} must be a non-empty string`)
  return value
}

export async function executeKnowledge(work: Omit<WorkItem, 'leaseToken'>, action: HostAction, services: LingxiLoopServices, channelType: number) {
  const method = action.action.slice('knowledge.'.length)
  if (method === 'set_source_enabled' || method === 'delete_source') throw new Error('approval-required knowledge action must use the approval path')
  if (!action.action.startsWith('knowledge.') || !Object.hasOwn(KNOWLEDGE_METHODS, method)) throw new Error('unsupported or approval-required knowledge action')
  const allowed: readonly string[] = KNOWLEDGE_METHODS[method as keyof typeof KNOWLEDGE_METHODS]
  if (Object.keys(action.args).some((key) => !allowed.includes(key))) throw new Error('unknown knowledge argument; identity and idempotency are supplied by the package')
  const native = nativeWork(work)
  const permission = method === 'list_sources' || method === 'check_source' ? 'knowledge:read' : method === 'retry_ingestion' ? 'knowledge:manage' : 'knowledge:write'
  await services.permissionService.assertCan({ actorUserId: native.authorizationUserId!, companyId: native.companyId, action: permission,
    resource: method === 'retry_ingestion' ? { type: 'knowledge_source', id: text(action.args, 'sourceId') } : { type: 'conversation', id: native.channelId },
  })
  switch (method) {
    case 'list_sources': return services.knowledge.listKnowledgeSourcesForAgent(native)
    case 'check_source': {
      const sourceId = text(action.args, 'sourceId')
      const expected = action.args['expected']
      if (!expected || typeof expected !== 'object' || Array.isArray(expected)) throw new Error('expected source fields are required')
      const fields = Object.entries(expected)
      if (!fields.length || fields.some(([key, value]) => key === 'enabled' ? typeof value !== 'boolean'
        : !['status', 'title'].includes(key) || typeof value !== 'string' || !value.trim() || value.length > 2000)) throw new Error('expected must contain enabled, status or title')
      const sources = await services.knowledge.listKnowledgeSourcesForAgent(native)
      const matches = sources.filter((source): source is Record<string, unknown> => !!source && typeof source === 'object' && 'id' in source && source.id === sourceId)
      const source = matches.length === 1 ? matches[0] : undefined
      const observed = source && fields.every(([key]) => Object.hasOwn(source, key))
        ? Object.fromEntries(fields.map(([key]) => [key, source[key]])) : undefined
      return { scope: 'knowledge_source_fields', sourceId, observedAt: new Date().toISOString(),
        status: observed === undefined ? 'not_observed' : fields.every(([key, value]) => observed[key] === value) ? 'pass' : 'fail',
        ...(observed ? { observed } : {}),
        limitation: 'Only the requested visible source fields were checked; absence does not prove deletion, and this does not verify the whole goal.' }
    }
    case 'add_text': return services.knowledge.addKnowledgeText(native, { title: text(action.args, 'title'), text: text(action.args, 'text'), idempotencyKey: action.idempotencyKey })
    case 'add_url': return services.knowledge.addKnowledgeUrl(native, { title: text(action.args, 'title', true) || text(action.args, 'url'), url: text(action.args, 'url'), idempotencyKey: action.idempotencyKey })
    case 'retry_ingestion': return services.knowledge.retryKnowledgeSourceForAgent(native, text(action.args, 'sourceId'))
    case 'add_file': {
      const clientMsgNo = text(action.args, 'clientMsgNo')
      const messages = await services.wukongClient().syncMessages(native.channelId, channelType, 100, native.agentId)
      const message = messages.find((item) => item.clientMsgNo === clientMsgNo && item.channelId === native.channelId && item.channelType === channelType && item.payload.kind === 'attachment')
      if (!message) throw new Error('committed attachment not found in the authorized conversation')
      const data = message.payload.data
      if (!data || typeof data['key'] !== 'string' || !data['key'].startsWith(`attachments/${native.companyId}/`)
        || typeof data['mime'] !== 'string' || !Number.isSafeInteger(data['size']) || Number(data['size']) < 0) throw new Error('invalid committed attachment')
      return services.knowledge.addKnowledgeFile(native, { title: text(action.args, 'title', true) || String(data['name'] ?? 'Attachment'),
        storageKey: data['key'], mime: data['mime'], size: Number(data['size']), idempotencyKey: action.idempotencyKey })
    }
    default: throw new Error('unsupported knowledge action')
  }
}
