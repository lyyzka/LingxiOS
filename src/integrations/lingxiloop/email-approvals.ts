import { isDeepStrictEqual } from 'node:util'
import { withTransaction, type SqlPool } from '../../control-plane/pg-store.js'
import type { HostAction, WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices, NativeEmailAttachment, NativeMessage } from './service-contracts.js'
import { EMAIL_APPROVAL_METHODS } from './email.js'
import { claimApprovalExecution, inspectApproval, persistApproval, resumeApproved } from './approvals.js'

type Services = Pick<LingxiLoopServices, 'email' | 'permissionService' | 'wukongClient'>

function strings(value: unknown, name: string, required = false) {
  if (value === undefined && !required) return []
  if (!Array.isArray(value) || required && !value.length || value.some(item => typeof item !== 'string' || !item.trim())) throw new Error(`${name} must be a non-empty string array`)
  return value as string[]
}

export async function prepareEmailApproval(services: Services, work: Omit<WorkItem, 'leaseToken'>, action: HostAction, channelType: number, approverId?: string) {
  const api = services.email, method = action.action.slice('email.'.length) as keyof typeof EMAIL_APPROVAL_METHODS
  if (!api || !Object.hasOwn(EMAIL_APPROVAL_METHODS, method) || Object.keys(action.args).some(key => !EMAIL_APPROVAL_METHODS[method].includes(key as never))) throw new Error('unsupported email approval action')
  const principalId = approverId ?? work.principalId
  if (!principalId) throw new Error('persisted human authorization principal is required')
  const targetConversationId = method === 'reply' ? action.args['conversationId'] : work.sessionId
  if (typeof targetConversationId !== 'string' || !targetConversationId.trim()) throw new Error('email conversationId is required')
  await services.permissionService.assertCan({ actorUserId: principalId, companyId: work.tenantId, action: 'email:write', resource: { type: 'conversation', id: targetConversationId } })
  const body = action.args['body']
  if (typeof body !== 'string' || !body.trim() || body.length > 50_000) throw new Error('email body must contain 1..50000 characters')
  const refs = strings(action.args['attachmentClientMsgNos'], 'attachmentClientMsgNos')
  if (refs.length > 16 || new Set(refs).size !== refs.length) throw new Error('email accepts at most 16 unique attachments')
  const messages = refs.length ? await services.wukongClient().syncMessages(work.sessionId, channelType, 100, work.agentId) : []
  const attachments = refs.map(ref => attachment(messages, work.tenantId, work.sessionId, channelType, ref))
  if (attachments.reduce((sum, item) => sum + item.sizeBytes, 0) > 25 * 1024 * 1024) throw new Error('email attachments exceed 25 MiB')
  const cc = strings(action.args['cc'], 'cc')
  if (method === 'send') {
    const to = strings(action.args['to'], 'to', true), subject = action.args['subject']
    if (typeof subject !== 'string' || !subject.trim() || subject.length > 998) throw new Error('email subject must contain 1..998 characters')
    return { preview: { method, to, cc, subject, body, attachments }, execute: () => api.sendAgentEmail({ companyId: work.tenantId, userId: work.agentId }, { to, cc, subject, body, attachments }, { idempotencyKey: action.idempotencyKey }) }
  }
  const messageId = action.args['messageId']
  if (typeof messageId !== 'string' || !messageId.trim()) throw new Error('email reply messageId is required')
  const thread = await api.getAgentEmailThread({ companyId: work.tenantId, userId: work.agentId }, targetConversationId, 50) as { messages?: Array<{ id?: unknown }> }
  if (!Array.isArray(thread.messages) || !thread.messages.some(message => message.id === messageId)) throw new Error('email reply message is not in the authorized thread')
  return { preview: { method, conversationId: targetConversationId, messageId, cc, body, attachments }, execute: () => api.replyToAgentEmail({ companyId: work.tenantId, userId: work.agentId }, messageId, { cc, body, attachments }, { idempotencyKey: action.idempotencyKey }) }
}

function attachment(messages: NativeMessage[], companyId: string, sessionId: string, channelType: number, ref: string): NativeEmailAttachment {
  const message = messages.find(item => item.clientMsgNo === ref && item.channelId === sessionId && item.channelType === channelType && item.payload.kind === 'attachment')
  const data = message?.payload.data
  if (!data || typeof data['key'] !== 'string' || !data['key'].startsWith(`attachments/${companyId}/`) || typeof data['name'] !== 'string'
    || typeof data['mime'] !== 'string' || !Number.isSafeInteger(data['size']) || Number(data['size']) < 0) throw new Error(`committed attachment not found: ${ref}`)
  return { key: data['key'], filename: data['name'], mimeType: data['mime'], sizeBytes: Number(data['size']) }
}

export async function requestEmailApproval(database: SqlPool, services: Services, work: Omit<WorkItem, 'leaseToken'>, action: HostAction, channelType: number) {
  return withTransaction(database, async db => {
    const { preview } = await prepareEmailApproval(services, work, action, channelType)
    return persistApproval(db, work, action, { summary: action.action === 'email.send' ? 'Send the reviewed external email' : 'Send the reviewed external email reply', scope: { conversationId: work.sessionId }, preview })
  })
}

export async function approveEmail(database: SqlPool, services: Services, input: { companyId: string; userId: string; approvalId: string }) {
  const reviewed = await inspectApproval(database, services, input)
  if (!['email.send', 'email.reply'].includes(reviewed.action.action)) throw new Error('unsupported email approval')
  if (reviewed.status === 'EXECUTED') return resumeApproved(database, input, reviewed)
  const { intent, recovering } = await claimApprovalExecution(database, input, reviewed)
  const binding = await database.query(`SELECT profile FROM im_channel_bindings
    WHERE company_id=$1 AND channel_id=$2`, [intent.tenantId, intent.sessionId])
  const profile = binding.rows[0]?.['profile'] as Record<string, unknown> | undefined, channelType = Number(profile?.['channelType'])
  if (channelType !== 1 && channelType !== 2) throw new Error('email approval channel is unavailable')
  const work = { id: intent.workId, fence: 0, homeEpoch: 0, tenantId: intent.tenantId, agentId: intent.agentId, sessionId: intent.sessionId,
    triggerRef: '', kind: 'resume' as const, lane: 'approval' as const, ...(intent.principalId ? { principalId: intent.principalId } : {}) }
  let value: unknown
  if (!recovering) {
    const prepared = await prepareEmailApproval(services, work, reviewed.action, channelType, input.userId)
    if (!isDeepStrictEqual(prepared.preview, reviewed.preview)) throw new Error('email approval preview is stale')
    value = await prepared.execute()
  } else value = await replayEmail(services, work, reviewed.action, reviewed.preview, input.userId)
  await withTransaction(database, async db => {
    const updated = await db.query(`UPDATE approvals SET status='EXECUTED',resolved_at=NOW(),resolved_by=$2,executed_at=NOW(),result=$3::jsonb,error=NULL
      WHERE id=$1 AND company_id=$4 AND status='EXECUTING' RETURNING id`, [input.approvalId, input.userId, JSON.stringify(value), input.companyId])
    if (updated.rows.length !== 1) throw new Error('email approval changed while executing')
    const receipt = await db.query(`UPDATE lingxios.agent_action_ledger SET result=$2::jsonb WHERE idempotency_key=$1 AND result->'approval'->>'id'=$3 RETURNING idempotency_key`,
      [reviewed.action.idempotencyKey, JSON.stringify({ ok: true, value }), input.approvalId])
    if (receipt.rows.length !== 1) throw new Error('email approval receipt is missing')
  })
  return resumeApproved(database, input, reviewed)
}

async function replayEmail(services: Services, work: Omit<WorkItem, 'leaseToken'>, action: HostAction,
  preview: unknown, approverId: string) {
  const api = services.email, saved = preview as Record<string, unknown>
  if (!api || !saved || typeof saved !== 'object') throw new Error('email recovery data is unavailable')
  const method = action.action.slice('email.'.length)
  const conversationId = method === 'reply' ? saved['conversationId'] : work.sessionId
  if (typeof conversationId !== 'string') throw new Error('email recovery conversation is invalid')
  await services.permissionService.assertCan({ actorUserId: approverId, companyId: work.tenantId,
    action: 'email:write', resource: { type: 'conversation', id: conversationId } })
  const attachments = saved['attachments'] as NativeEmailAttachment[]
  if (!Array.isArray(attachments) || typeof saved['body'] !== 'string') throw new Error('email recovery payload is invalid')
  if (method === 'send') {
    if (!Array.isArray(saved['to']) || !Array.isArray(saved['cc']) || typeof saved['subject'] !== 'string') throw new Error('email recovery payload is invalid')
    return api.sendAgentEmail({ companyId: work.tenantId, userId: work.agentId },
      { to: saved['to'] as string[], cc: saved['cc'] as string[], subject: saved['subject'], body: saved['body'], attachments },
      { idempotencyKey: action.idempotencyKey })
  }
  if (method !== 'reply' || typeof saved['messageId'] !== 'string' || !Array.isArray(saved['cc'])) throw new Error('email recovery payload is invalid')
  return api.replyToAgentEmail({ companyId: work.tenantId, userId: work.agentId }, saved['messageId'],
    { cc: saved['cc'] as string[], body: saved['body'], attachments }, { idempotencyKey: action.idempotencyKey })
}
