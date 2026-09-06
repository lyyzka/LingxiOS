import type { ContextProvider } from '../../control-plane/stores.js'
import type { SqlPool } from '../../control-plane/pg-store.js'
import type { WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices, NativeMessage, NativeWork } from './service-contracts.js'

type BaseContext = Awaited<ReturnType<ContextProvider['loadContext']>>

function messageTime(message: NativeMessage): string {
  if (!Number.isFinite(message.timestamp)) return ''
  return new Date(message.timestamp! > 10_000_000_000 ? message.timestamp! : message.timestamp! * 1_000).toISOString()
}

function nativeWork(work: Omit<WorkItem, 'leaseToken'>): NativeWork {
  return { id: work.id, fence: work.fence, homeEpoch: work.homeEpoch, companyId: work.tenantId,
    ...(work.principalId ? { authorizationUserId: work.principalId } : {}), agentId: work.agentId, channelId: work.sessionId,
    ...(work.threadId ? { threadRootClientMsgNo: work.threadId } : {}), triggerClientMsgNo: work.triggerRef,
    reason: work.kind === 'resume' ? 'resume' : work.kind === 'canvas_worker' ? 'canvas_worker' : work.kind === 'canvas_summary' ? 'canvas_summary' : work.kind === 'routine' || work.kind === 'mission_coordinator' ? 'routine' : 'message',
    executionRole: work.kind === 'canvas_summary' ? 'reporter' : work.meta?.['executionRole'] === 'verifier' ? 'verifier' : work.kind === 'canvas_worker' ? 'specialist' : 'coordinator',
    lane: work.lane === 'interactive' ? 'learner' : work.lane, leaseToken: '' }
}

export async function enrichLingxiLoopContext(
  database: SqlPool, work: Omit<WorkItem, 'leaseToken'>, services: LingxiLoopServices, channelType: number, base: BaseContext,
): Promise<BaseContext> {
  const started = Date.now()
  const history = await services.wukongClient().syncMessages(work.sessionId, channelType, 80, work.agentId)
  const readThroughSeq = history.reduce((max, message) => Math.max(max, message.messageSeq), 0)
  if (readThroughSeq && services.advanceAgentReadReceipt) await services.advanceAgentReadReceipt({
    companyId: work.tenantId, channelId: work.sessionId, agentId: work.agentId, readThroughSeq,
  })
  const messages = history.map(message => ({
    ref: message.clientMsgNo, authorId: message.fromUid,
    authorName: String(message.payload.data?.['authorName'] ?? message.fromUid),
    authorKind: (message.payload.refs?.agentId ? 'agent' : message.payload.kind === 'system' ? 'system' : 'human') as 'agent' | 'human' | 'system',
    body: message.payload.body ?? JSON.stringify(message.payload.data ?? {}), createdAt: messageTime(message),
    ...(message.payload.replyToClientMsgNo ? { replyToRef: message.payload.replyToClientMsgNo } : {}),
  }))
  const trigger = messages.find(message => message.ref === work.triggerRef)
  const learner = trigger?.authorKind === 'human' ? trigger : [...messages].reverse().find(message => message.authorKind === 'human')
  const queryMessages = messages.slice(Math.max(0, messages.indexOf(trigger ?? learner!) - 7), messages.indexOf(trigger ?? learner!) + 1)
  const retrieval = services.retrieveKnowledge && learner && base.capabilities.includes('knowledge')
    ? await services.retrieveKnowledge({ companyId: work.tenantId, conversationId: work.sessionId,
      authorizationUserId: work.principalId!, query: trigger?.body ?? learner.body,
      contextQuery: queryMessages.map(message => `${message.authorName}: ${message.body}`).join('\n').slice(-8_000), limit: 8 }) : []
  const versions = retrieval.length ? await database.query('SELECT id,updated_at FROM knowledge_sources WHERE company_id=$1 AND id=ANY($2::text[])',
    [work.tenantId, [...new Set(retrieval.map(item => item.sourceId))]]) : { rows: [] }
  const versionBySource = new Map(versions.rows.map(row => [String(row['id']), String(row['updated_at'])]))
  const evidence = retrieval.map(item => ({ marker: item.marker, sourceId: item.sourceId,
    sourceVersion: item.sourceVersion ?? versionBySource.get(item.sourceId) ?? item.sourceId, chunkId: item.chunkId, title: item.sourceTitle,
    excerpt: item.excerpt, ...(item.sourceUrl ? { url: item.sourceUrl } : {}) }))
  const learningContext = services.learning && learner ? await services.learning.loadLearningTurnContext(nativeWork(work), learner.authorId) : undefined
  const approvalId = work.kind === 'resume' && work.triggerRef.startsWith('approval:') ? work.triggerRef.slice(9) : null
  const approval = approvalId ? (await database.query(`SELECT id,status,result,error FROM approvals
    WHERE id=$1 AND agent_id=$2 AND channel_id=$3 AND source='AGENT_OS' AND status IN('EXECUTED','REJECTED') LIMIT 1`,
  [approvalId, work.agentId, work.sessionId])).rows[0] : undefined
  const pendingApproval = approval ? { approvalId: String(approval['id']), approved: approval['status'] === 'EXECUTED',
    ...(approval['result'] === undefined ? {} : { result: approval['result'] }), ...(approval['error'] ? { error: String(approval['error']) } : {}) } : undefined
  const canvas = services.canvas ? await services.canvas.getConversationCanvas(work.tenantId, work.sessionId, work.principalId!) : undefined
  const canvasRoster = services.canvas ? await services.canvas.listCanvasAvailableAgents(work.tenantId) : []
  const contextMessages = messages.some(message => message.ref === work.triggerRef) ? messages
    : [...messages, ...base.messages.filter(message => message.ref === work.triggerRef)]
  return { ...base, messages: contextMessages.length ? contextMessages : base.messages, ...(evidence.length ? { evidence } : {}),
    ...(pendingApproval ? { pendingApproval } : {}), dynamic: { ...base.dynamic, product: {
      knowledgeContext: retrieval, ...(learner ? { learnerId: learner.authorId } : {}),
      ...(learningContext ? { learningContext } : {}), ...(canvas ? { canvas } : {}), canvasRoster,
      contextDurationMs: Math.max(0, Date.now() - started),
    } } }
}
