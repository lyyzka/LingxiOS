import { isDeepStrictEqual } from 'node:util'
import { snapshotAttachments } from '../context/attachments.js'
import { cancelRun, enqueueWork, type RunIdentity } from '../app/jobs.js'
import { withTransaction, workItemFromRow, type SqlPool, type SqlQueryable } from '../control-plane/pg-store.js'
import type { WorkItem } from '../protocol/types.js'
import type { ContextProvider } from '../control-plane/stores.js'
import { audienceOf, authorizeConversationWork, containsAudience, conversationPolicy, digest, identifier, identity, participant, positiveVersion, requireAudience } from './access.js'
import type { Audience, ConversationIdentity, ConversationPolicy, IMDeliveryContext, IMIngressResult, IMMessageInput, WorkConversation } from './types.js'

export async function syncConversation(database: SqlPool, policy: ConversationPolicy) {
  identity(policy); positiveVersion(policy.version); identifier(policy.owner?.id)
  if (!['participant', 'organization'].includes(policy.owner.kind) || !['direct', 'group'].includes(policy.kind)
    || !Array.isArray(policy.participants) || !policy.participants.length || policy.participants.length > 256
    || new Set(policy.participants.map(item => item.id)).size !== policy.participants.length) throw new Error('invalid conversation policy')
  for (const member of policy.participants) {
    identifier(member.id)
    if (!['human', 'agent'].includes(member.kind) || !Array.isArray(member.capabilities)
      || new Set(member.capabilities).size !== member.capabilities.length
      || member.capabilities.some(capability => !['read', 'execute', 'speak', 'control'].includes(capability))) throw new Error('invalid participant capabilities')
  }
  if (policy.owner.kind === 'participant' && !policy.participants.some(item => item.id === policy.owner.id)) throw new Error('conversation owner must be a participant')
  if (policy.defaultAgentId !== undefined) participant(policy, policy.defaultAgentId, 'read', 'agent')
  const snapshot = structuredClone(policy)
  snapshot.participants.sort((a, b) => a.id.localeCompare(b.id))
  for (const member of snapshot.participants) member.capabilities.sort()
  return withTransaction(database, async db => {
    const { rows } = await db.query(`INSERT INTO lingxios.agent_conversations(tenant_id,id,version,policy) VALUES($1,$2,$3,$4::jsonb)
      ON CONFLICT(tenant_id,id) DO UPDATE SET version=EXCLUDED.version,policy=EXCLUDED.policy,updated_at=NOW()
      WHERE lingxios.agent_conversations.version<EXCLUDED.version RETURNING version`,
    [policy.tenantId, policy.conversationId, policy.version, JSON.stringify(snapshot)])
    if (!rows.length && !isDeepStrictEqual(await conversationPolicy(db, policy, true), snapshot)) throw new Error('conversation policy version is stale or reused')
    return { version: policy.version, deduplicated: !rows.length }
  })
}

export async function registerThread(database: SqlPool, scope: ConversationIdentity & { threadId: string; policyVersion: number }) {
  identity(scope); identifier(scope.threadId); positiveVersion(scope.policyVersion)
  return withTransaction(database, async db => {
    const policy = await conversationPolicy(db, { tenantId: scope.tenantId, conversationId: scope.conversationId }, true)
    if (policy.version !== scope.policyVersion) throw new Error('conversation policy version changed')
    await db.query(`INSERT INTO lingxios.agent_conversation_threads(tenant_id,conversation_id,id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`,
      [scope.tenantId, scope.conversationId, scope.threadId])
  })
}

export async function ingestMessage(database: SqlPool, input: IMMessageInput, policyMeta: Record<string, unknown>): Promise<IMIngressResult> {
  identity(input); identifier(input.messageId); identifier(input.author?.id); positiveVersion(input.version); positiveVersion(input.policyVersion)
  if (!['human', 'agent', 'system'].includes(input.author.kind) || typeof input.text !== 'string' || !input.text.trim()
    || input.text.length > 100_000 || input.mentions !== undefined && (!Array.isArray(input.mentions) || input.mentions.length > 64)) throw new Error('invalid IM message')
  for (const id of input.mentions ?? []) identifier(id)
  if (input.replyTo) { identifier(input.replyTo.messageId); positiveVersion(input.replyTo.version) }
  if (input.causedBy) identifier(input.causedBy.resultId)
  const message = { ...structuredClone(input), mentions: [...new Set(input.mentions ?? [])].sort(), attachments: snapshotAttachments(input.attachments ?? []) }
  const fingerprint = digest(message)
  return withTransaction(database, async db => {
    // One short ingress transaction per conversation orders edits and reply-slot acquisition.
    await db.query('SELECT id FROM lingxios.agent_conversations WHERE tenant_id=$1 AND id=$2 FOR UPDATE', [input.tenantId, input.conversationId])
    const policy = await conversationPolicy(db, input)
    if (input.author.kind !== 'system') participant(policy, input.author.id, 'read', input.author.kind)
    const prior = await db.query(`SELECT fingerprint,outcome FROM lingxios.agent_im_messages
      WHERE tenant_id=$1 AND conversation_id=$2 AND message_id=$3 AND version=$4`, [input.tenantId, input.conversationId, input.messageId, input.version])
    if (prior.rows[0]) {
      if (prior.rows[0]['fingerprint'] !== fingerprint) throw new Error('message identity reused with different content')
      return { ...prior.rows[0]['outcome'] as IMIngressResult, deduplicated: true }
    }
    if (policy.version !== input.policyVersion) throw new Error('conversation policy version changed')
    const audience = audienceOf(policy, input.audience)
    if (input.author.kind !== 'system') requireAudience(policy, audience, [input.author.id])
    const previous = await db.query(`SELECT version,input,thread_id FROM lingxios.agent_im_messages
      WHERE tenant_id=$1 AND conversation_id=$2 AND message_id=$3 ORDER BY version DESC LIMIT 1`, [input.tenantId, input.conversationId, input.messageId])
    if (previous.rows[0] && (Number(previous.rows[0]['version']) >= input.version
      || !isDeepStrictEqual((previous.rows[0]['input'] as IMMessageInput).author, input.author)
      || previous.rows[0]['thread_id'] !== (input.threadId ?? null))) throw new Error('message edit is stale or changes its author or thread')
    if (input.replyTo) {
      const parent = await db.query(`SELECT audience FROM lingxios.agent_im_messages WHERE tenant_id=$1 AND conversation_id=$2
        AND message_id=$3 AND version=$4 AND thread_id IS NOT DISTINCT FROM $5`,
      [input.tenantId, input.conversationId, input.replyTo.messageId, input.replyTo.version, input.threadId ?? null])
      if (!parent.rows[0] || !containsAudience(parent.rows[0]['audience'] as Audience, audience)) throw new Error('reply cause is unavailable to this audience')
    }
    if (input.causedBy) {
      const cause = await db.query(`SELECT work.conversation FROM lingxios.agent_results result
        JOIN lingxios.agent_work_items work ON work.id=result.work_id WHERE result.id=$1 AND work.tenant_id=$2
        AND work.conversation->>'conversationId'=$3 AND work.thread_id IS NOT DISTINCT FROM $4`,
      [input.causedBy.resultId, input.tenantId, input.conversationId, input.threadId ?? null])
      if (!cause.rows[0] || !containsAudience((cause.rows[0]['conversation'] as WorkConversation).audience, audience)) throw new Error('result cause is outside this conversation audience')
    }
    const outcome: IMIngressResult = { runs: [], deduplicated: false }
    if (input.causedBy) outcome.reason = 'outbox_echo'
    else if (input.author.kind !== 'human') outcome.reason = input.author.kind === 'agent' ? 'agent_message' : 'system_message'
    const targets = outcome.reason ? [] : (message.mentions.length ? message.mentions : policy.defaultAgentId ? [policy.defaultAgentId] : [])
      .filter(id => audience.participantIds.includes(id) && policy.participants.some(item => item.id === id && item.kind === 'agent'
        && ['read', 'execute', 'speak'].every(capability => item.capabilities.includes(capability as 'read' | 'execute' | 'speak'))))
    if (!outcome.reason && !targets.length) outcome.reason = 'no_speaker'
    if (targets.length) participant(policy, input.author.id, 'execute', 'human')
    await db.query(`INSERT INTO lingxios.agent_im_messages(tenant_id,conversation_id,message_id,version,thread_id,fingerprint,input,audience,outcome)
      VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,$9::jsonb)`,
    [input.tenantId, input.conversationId, input.messageId, input.version, input.threadId ?? null, fingerprint, JSON.stringify(message), JSON.stringify(audience), JSON.stringify(outcome)])
    if (previous.rows.length) {
      const old = await db.query(`SELECT work.* FROM lingxios.agent_reply_slots slot JOIN lingxios.agent_work_items work ON work.id=slot.work_id
        WHERE slot.tenant_id=$1 AND slot.conversation_id=$2 AND slot.message_id=$3 AND slot.message_version<$4 ORDER BY work.id FOR UPDATE OF work`,
      [input.tenantId, input.conversationId, input.messageId, input.version])
      for (const row of old.rows) {
        const work = workItemFromRow(row, '', 1)
        await cancelRun(db, { runId: work.id, tenantId: work.tenantId, agentId: work.agentId, sessionId: work.sessionId,
          principalId: work.principalId!, ...(work.threadId === undefined ? {} : { threadId: work.threadId }) })
        // Finished older replies also lose delivery eligibility after an edit.
        await db.query('UPDATE lingxios.agent_work_items SET cancel_requested_at=COALESCE(cancel_requested_at,NOW()) WHERE id=$1', [work.id])
      }
    }
    for (const agentId of targets) {
      const runId = 'im:' + digest([input.tenantId, input.conversationId, input.messageId, input.version, agentId])
      const sessionId = 'im-session:' + digest([input.conversationId, input.threadId ?? null, input.author.id, audience, policy.version])
      const run: RunIdentity = { runId, tenantId: input.tenantId, agentId, sessionId, principalId: input.author.id,
        ...(input.threadId === undefined ? {} : { threadId: input.threadId }) }
      await enqueueWork(db, { ...run, id: runId, kind: 'turn', lane: 'interactive', triggerRef: input.messageId,
        conversation: { conversationId: input.conversationId, policyVersion: policy.version,
          source: { messageId: input.messageId, version: input.version }, audience, internal: false },
        meta: { ...policyMeta, text: input.text, authorName: input.author.id, attachments: message.attachments } })
      await db.query(`INSERT INTO lingxios.agent_reply_slots(tenant_id,conversation_id,message_id,message_version,agent_id,work_id) VALUES($1,$2,$3,$4,$5,$6)`,
        [input.tenantId, input.conversationId, input.messageId, input.version, agentId, runId])
      outcome.runs.push(run)
    }
    await db.query(`UPDATE lingxios.agent_im_messages SET outcome=$5::jsonb WHERE tenant_id=$1 AND conversation_id=$2 AND message_id=$3 AND version=$4`,
      [input.tenantId, input.conversationId, input.messageId, input.version, JSON.stringify(outcome)])
    return outcome
  })
}

export async function conversationMessages(database: SqlQueryable, work: Omit<WorkItem, 'leaseToken'>): Promise<Awaited<ReturnType<ContextProvider['loadContext']>>['messages']> {
  if (!work.conversation) throw new Error('IM work is required')
  await authorizeConversationWork(database, work)
  const { rows } = await database.query(`SELECT input,audience,recorded_at FROM (SELECT DISTINCT ON(message_id) message_id,input,audience,recorded_at FROM lingxios.agent_im_messages
    WHERE tenant_id=$1 AND conversation_id=$2 AND thread_id IS NOT DISTINCT FROM $3
      AND audience->'participantIds' @> $4::jsonb ORDER BY message_id,version DESC) latest ORDER BY recorded_at DESC,message_id LIMIT 100`,
  [work.tenantId, work.conversation.conversationId, work.threadId ?? null, JSON.stringify(work.conversation.audience.participantIds)])
  const messages = rows.filter(row => (row['input'] as IMMessageInput).author.kind !== 'system').map(row => {
    const message = row['input'] as IMMessageInput
    return { ref: message.messageId, authorId: message.author.id, authorName: message.author.id, authorKind: message.author.kind,
      body: message.text, createdAt: new Date(row['recorded_at'] as string | Date).toISOString() }
  }).sort((a, b) => a.createdAt.localeCompare(b.createdAt) || a.ref.localeCompare(b.ref))
  // The trigger is fetched independently of the bounded history page.
  const trigger = await database.query(`SELECT input,recorded_at FROM lingxios.agent_im_messages
    WHERE tenant_id=$1 AND conversation_id=$2 AND message_id=$3 AND version=$4`,
  [work.tenantId, work.conversation.conversationId, work.conversation.source.messageId, work.conversation.source.version])
  if (!trigger.rows[0]) throw new Error('IM trigger is unavailable')
  const source = trigger.rows[0]['input'] as IMMessageInput
  return [...messages.filter(message => message.ref !== work.triggerRef), { ref: work.triggerRef, authorId: work.principalId!,
    authorName: source.author.id, authorKind: 'human', body: source.text, createdAt: new Date(trigger.rows[0]['recorded_at'] as string | Date).toISOString() }]
}

export async function imDeliveryContext(database: SqlQueryable, work: Omit<WorkItem, 'leaseToken'>, resultId: string): Promise<IMDeliveryContext | undefined> {
  if (!work.conversation) return
  const current = await database.query(`SELECT 1 FROM lingxios.agent_work_items work JOIN lingxios.agent_results result ON result.id=work.result_id
    WHERE work.id=$1 AND result.id=$2 AND work.cancel_requested_at IS NULL AND work.fence=result.fence
      AND result.request_version=jsonb_array_length(work.steer_inputs)+1`, [work.id, resultId])
  if (!current.rows.length) throw new Error('IM reply was cancelled, revised or superseded')
  const policy = (await authorizeConversationWork(database, work, 'speak'))!
  const scope = work.conversation
  const replyKey = 'im-reply:' + digest([work.tenantId, scope.conversationId, scope.source, work.agentId])
  return { tenantId: work.tenantId, conversationId: scope.conversationId, ...(work.threadId === undefined ? {} : { threadId: work.threadId }),
    policyVersion: policy.version, source: scope.source, audience: scope.audience, replyKey,
    messageKey: 'im-message:' + digest([replyKey, resultId]) }
}
