import { createHash } from 'node:crypto'
import { canonicalJson } from '../context/compiler.js'
import type { SqlQueryable } from '../control-plane/pg-store.js'
import type { WorkItem } from '../protocol/types.js'
import type { Audience, AudienceInput, ConversationCapability, ConversationIdentity, ConversationPolicy } from './types.js'

export const digest = (value: unknown) => createHash('sha256').update(canonicalJson(value)).digest('hex')
export function identifier(value: unknown): asserts value is string {
  if (typeof value !== 'string' || !value.trim() || value.length > 1000) throw new Error('invalid collaboration identifier')
}
export function positiveVersion(value: unknown): asserts value is number {
  if (!Number.isSafeInteger(value) || Number(value) < 1) throw new Error('invalid collaboration version')
}
export function identity(input: ConversationIdentity) {
  identifier(input.tenantId); identifier(input.conversationId)
  if (input.threadId !== undefined) identifier(input.threadId)
}
export async function conversationPolicy(database: SqlQueryable, scope: ConversationIdentity, lock = false): Promise<ConversationPolicy> {
  identity(scope)
  const { rows } = await database.query(`SELECT policy FROM lingxios.agent_conversations WHERE tenant_id=$1 AND id=$2${lock ? ' FOR SHARE' : ''}`,
    [scope.tenantId, scope.conversationId])
  if (!rows[0]) throw new Error('conversation is unavailable')
  if (scope.threadId !== undefined) {
    const thread = await database.query(`SELECT 1 FROM lingxios.agent_conversation_threads
      WHERE tenant_id=$1 AND conversation_id=$2 AND id=$3`, [scope.tenantId, scope.conversationId, scope.threadId])
    if (!thread.rows.length) throw new Error('thread is outside this conversation')
  }
  return rows[0]['policy'] as ConversationPolicy
}
export function participant(policy: ConversationPolicy, id: string, capability: ConversationCapability, kind?: 'human' | 'agent') {
  const entry = policy.participants.find(item => item.id === id && (!kind || item.kind === kind))
  if (!entry?.capabilities.includes(capability)) throw new Error(`conversation ${capability} capability is unavailable`)
  return entry
}
export function audienceOf(policy: ConversationPolicy, input: AudienceInput = { visibility: 'conversation' }): Audience {
  if (!input || !['conversation', 'participants'].includes(input.visibility)) throw new Error('invalid audience')
  const ids = input.visibility === 'conversation' ? policy.participants.filter(item => item.capabilities.includes('read')).map(item => item.id) : input.participantIds
  if (!Array.isArray(ids) || !ids.length || ids.length > 256 || new Set(ids).size !== ids.length) throw new Error('invalid audience participants')
  for (const id of ids) { identifier(id); participant(policy, id, 'read') }
  return { visibility: input.visibility, participantIds: [...ids].sort() }
}
export function requireAudience(policy: ConversationPolicy, audience: Audience, ids: string[]) {
  for (const id of ids) {
    participant(policy, id, 'read')
    if (!audience.participantIds.includes(id)) throw new Error('participant is outside the audience')
  }
}
/** A resource may feed a result only when all result recipients may see that resource. */
export function containsAudience(resource: Audience, output: Audience) {
  return output.participantIds.every(id => resource.participantIds.includes(id))
}
export async function authorizeConversationWork(database: SqlQueryable, work: Omit<WorkItem, 'leaseToken'>,
  operation: 'read' | 'execute' | 'speak' = 'execute', lock = false) {
  const scope = work.conversation
  if (!scope) return
  const policy = await conversationPolicy(database, { tenantId: work.tenantId, conversationId: scope.conversationId,
    ...(work.threadId === undefined ? {} : { threadId: work.threadId }) }, lock)
  requireAudience(policy, scope.audience, [work.principalId!, work.agentId])
  participant(policy, work.principalId!, operation === 'read' ? 'read' : 'execute', 'human')
  participant(policy, work.agentId, operation === 'read' ? 'read' : 'execute', 'agent')
  // Revoked recipients invalidate old outputs rather than silently changing their audience.
  for (const id of scope.audience.participantIds) participant(policy, id, 'read')
  if (operation === 'speak') {
    if (scope.internal || scope.audience.visibility === 'internal') throw new Error('internal work cannot speak')
    participant(policy, work.agentId, 'speak', 'agent')
    const slots = await database.query(`SELECT 1 FROM lingxios.agent_reply_slots WHERE work_id=$1
      AND tenant_id=$2 AND conversation_id=$3 AND message_id=$4 AND message_version=$5 AND agent_id=$6`,
    [work.id, work.tenantId, scope.conversationId, scope.source.messageId, scope.source.version, work.agentId])
    if (!slots.rows.length) throw new Error('work has no reply slot')
  }
  return policy
}
