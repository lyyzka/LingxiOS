import type { RequestAttachment } from '../context/attachments.js'
import type { RunIdentity } from '../app/jobs.js'

export interface ConversationIdentity { tenantId: string; conversationId: string; threadId?: string }
export interface ThreadIdentity extends ConversationIdentity { threadId: string }
export type ConversationCapability = 'read' | 'execute' | 'speak' | 'control'
export interface Participant {
  id: string
  kind: 'human' | 'agent'
  capabilities: ConversationCapability[]
}
/** A versioned assertion from authenticated IM administration, never from a model. */
export interface ConversationPolicy extends Omit<ConversationIdentity, 'threadId'> {
  version: number
  kind: 'direct' | 'group'
  owner: { kind: 'participant' | 'organization'; id: string }
  participants: Participant[]
  defaultAgentId?: string
}
export type Visibility = 'conversation' | 'participants' | 'internal'
export type AudienceInput = { visibility: 'conversation' } | { visibility: 'participants'; participantIds: string[] }
/** Even conversation-wide audiences freeze their recipients; membership changes cannot widen them. */
export interface Audience { visibility: Visibility; participantIds: string[] }
export interface MessageReference { messageId: string; version: number }
export interface WorkConversation {
  conversationId: string
  policyVersion: number
  source: MessageReference
  audience: Audience
  /** Internal delegates retain their parent's audience without acquiring a reply slot. */
  internal: boolean
}
export interface IMMessageInput extends ConversationIdentity, MessageReference {
  policyVersion: number
  author: { id: string; kind: 'human' | 'agent' | 'system' }
  text: string
  mentions?: string[]
  audience?: AudienceInput
  replyTo?: MessageReference
  causedBy?: { resultId: string }
  attachments?: RequestAttachment[]
}
export interface IMIngressResult {
  runs: RunIdentity[]
  deduplicated: boolean
  reason?: 'agent_message' | 'system_message' | 'outbox_echo' | 'no_speaker'
}
export interface IMDeliveryContext extends ConversationIdentity {
  policyVersion: number
  source: MessageReference
  audience: Audience
  /** One logical reply per source message version and addressed Agent. */
  replyKey: string
  /** Pass unchanged to the IM transport's idempotency facility on every retry. */
  messageKey: string
}
export interface IMDeliveryReceipt { messageId: string }
export interface GraphNode { id: string; agentId: string; text: string; dependsOn?: string[] }
export interface GraphInput { id: string; nodes: GraphNode[] }
export interface SharedStateIdentity extends ConversationIdentity { stateId: string }
export interface SharedStateSnapshot {
  version: number
  audience: Audience
  fields: Record<string, { version: number; value?: unknown; deleted: boolean }>
}
export type SharedStateChange = { field: string; expectedVersion: number } & ({ value: unknown } | { delete: true })
export interface SharedStateUpdate {
  operationId: string
  changes: SharedStateChange[]
}
export type SharedStateResult = { ok: true; state: SharedStateSnapshot; deduplicated: boolean }
  | { ok: false; conflicts: string[]; state: SharedStateSnapshot; deduplicated: boolean }
