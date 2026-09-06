import { isDeepStrictEqual } from 'node:util'
import { withTransaction, type SqlPool } from '../../control-plane/pg-store.js'
import type { ActionIntent } from '../../control-plane/stores.js'
import type { HostAction, WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices, NativeEmailAttachment, NativeMessage } from './service-contracts.js'
import { EMAIL_APPROVAL_METHODS } from './email.js'
import { inspectApproval, persistApproval, resumeApproved } from './approvals.js'

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
  if (reviewed.status !== 'PENDING') throw new Error('email approval is no longer pending')
  const pending = await database.query(`SELECT intent.intent, binding.profile FROM approvals approval
    JOIN lingxios.agent_work_items work ON work.id=approval.work_id AND work.tenant_id=approval.company_id
    JOIN lingxios.agent_action_intents intent ON intent.idempotency_key=approval.idempotency_key
    JOIN im_channel_bindings binding ON binding.company_id=work.tenant_id AND binding.channel_id=work.session_id
    WHERE approval.id=$1 AND approval.company_id=$2 AND approval.status='PENDING' AND approval.expires_at>NOW()
      AND approval.idempotency_key=$3 AND approval.args=$4::jsonb AND approval.preview=$5::jsonb
      AND work.status='completed' AND work.cancel_requested_at IS NULL
      AND work.goal_outcome->>'status'='awaiting_approval' AND work.goal_outcome->>'approvalId'=$1
      AND (work.goal_outcome->>'requestVersion')::integer=$6 AND jsonb_array_length(work.steer_inputs)+1=$6`,
  [input.approvalId, input.companyId, reviewed.action.idempotencyKey, JSON.stringify(reviewed.action.args), JSON.stringify(reviewed.preview), reviewed.requestVersion])
  const intent = pending.rows[0]?.['intent'] as ActionIntent | undefined
  const profile = pending.rows[0]?.['profile'] as Record<string, unknown> | undefined, channelType = Number(profile?.['channelType'])
  if (!intent || channelType !== 1 && channelType !== 2) throw new Error('email approval expired or changed before execution')
  const work = { id: intent.workId, fence: 0, homeEpoch: 0, tenantId: intent.tenantId, agentId: intent.agentId, sessionId: intent.sessionId,
    triggerRef: '', kind: 'resume' as const, lane: 'approval' as const, ...(intent.principalId ? { principalId: intent.principalId } : {}) }
  const prepared = await prepareEmailApproval(services, work, reviewed.action, channelType, input.userId)
  if (!isDeepStrictEqual(prepared.preview, reviewed.preview)) throw new Error('email approval preview is stale')
  const value = await prepared.execute()
  await withTransaction(database, async db => {
    const updated = await db.query(`UPDATE approvals SET status='EXECUTED',resolved_at=NOW(),resolved_by=$2,executed_at=NOW(),result=$3::jsonb,error=NULL
      WHERE id=$1 AND company_id=$4 AND status='PENDING' RETURNING id`, [input.approvalId, input.userId, JSON.stringify(value), input.companyId])
    if (updated.rows.length !== 1) throw new Error('email approval changed while executing')
    const receipt = await db.query(`UPDATE lingxios.agent_action_ledger SET result=$2::jsonb WHERE idempotency_key=$1 AND result->'approval'->>'id'=$3 RETURNING idempotency_key`,
      [reviewed.action.idempotencyKey, JSON.stringify({ ok: true, value }), input.approvalId])
    if (receipt.rows.length !== 1) throw new Error('email approval receipt is missing')
  })
  return resumeApproved(database, input, reviewed)
}
