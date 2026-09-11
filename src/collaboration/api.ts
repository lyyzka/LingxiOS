import { cancelRun, requestSnapshot, type RunIdentity } from '../app/jobs.js'
import { withTransaction, workItemFromRow, type SqlPool, type SqlQueryable } from '../control-plane/pg-store.js'
import type { WorkItem } from '../protocol/types.js'
import { authorizeConversationWork, conversationPolicy, digest, identifier, participant, requireAudience } from './access.js'
import { enqueueGraph, readGraph, waitForChildren } from './graphs.js'
import { createSharedState, readSharedState, sharedStateHistory, updateSharedState } from './state.js'
import type { Audience, AudienceInput, ConversationIdentity, GraphInput, IMIngressResult, MessageReference, SharedStateIdentity, SharedStateUpdate } from './types.js'

async function parentWork(database: SqlQueryable, identity: RunIdentity, write = false) {
  const { rows } = await database.query(`SELECT * FROM lingxios.agent_work_items WHERE id=$1 AND tenant_id=$2 AND agent_id=$3
    AND session_id=$4 AND principal_id=$5 AND thread_id IS NOT DISTINCT FROM $6${write ? ' FOR UPDATE' : ''}`,
  [identity.runId, identity.tenantId, identity.agentId, identity.sessionId, identity.principalId, identity.threadId ?? null])
  if (!rows[0]) throw new Error('run is outside this identity')
  if (write && (rows[0]['cancel_requested_at'] || !['queued', 'leased', 'waiting'].includes(String(rows[0]['status'])))) throw new Error('parent run is no longer active')
  const work = workItemFromRow(rows[0], '', 1), version = (rows[0]['steer_inputs'] as unknown[]).length + 1
  await authorizeConversationWork(database, work, write ? 'execute' : 'read', write)
  return { work, version }
}

/** IM audience authorization; ordinary reads enforce principal/thread in their SQL predicates. */
export async function authorizeRunRead(database: SqlQueryable, identity: { runId: string; tenantId: string; agentId: string; sessionId: string; principalId?: string; threadId?: string }, operation: 'read' | 'execute' = 'read') {
  const { rows } = await database.query(`SELECT * FROM lingxios.agent_work_items WHERE id=$1 AND tenant_id=$2 AND agent_id=$3 AND session_id=$4`,
    [identity.runId, identity.tenantId, identity.agentId, identity.sessionId])
  if (!rows[0]?.['conversation']) return
  const work = workItemFromRow(rows[0], '', 1)
  if (!identity.principalId || identity.threadId !== work.threadId) throw new Error('IM content requires an authenticated reader and exact thread')
  const policy = await conversationPolicy(database, { tenantId: work.tenantId, conversationId: work.conversation!.conversationId,
    ...(work.threadId === undefined ? {} : { threadId: work.threadId }) })
  requireAudience(policy, work.conversation!.audience, [identity.principalId])
  if (work.conversation!.internal && identity.principalId !== work.principalId) throw new Error('internal results belong to their original principal')
  if (operation === 'execute') {
    if (identity.principalId !== work.principalId) throw new Error('only the original principal may resume or revise a run')
    await authorizeConversationWork(database, work, 'execute', true)
  }
}

export function createCollaborationAPI(database: SqlPool) {
  return {
    graphs: {
      enqueue: (identity: RunIdentity, input: GraphInput) => withTransaction(database, async db => {
        const { work, version } = await parentWork(db, identity, true)
        return enqueueGraph(db, work, version, input)
      }),
      read: async (identity: RunIdentity, id: string) => { const { work, version } = await parentWork(database, identity); return readGraph(database, work, version, id) },
      waitForChildren: (identity: RunIdentity, ids: string[]) => withTransaction(database, async db => {
        const { work, version } = await parentWork(db, identity, true)
        return waitForChildren(db, work, version, ids)
      }),
    },
    sharedState: {
      create: (scope: SharedStateIdentity & { principalId: string }, audience?: AudienceInput) => withTransaction(database, db => createSharedState(db, scope, { principalId: scope.principalId }, audience)),
      read: (scope: SharedStateIdentity & { principalId: string }) => readSharedState(database, scope, { principalId: scope.principalId }),
      apply: (scope: SharedStateIdentity & { principalId: string }, update: SharedStateUpdate, source?: MessageReference) =>
        withTransaction(database, db => updateSharedState(db, scope, { principalId: scope.principalId, ...(source ? { source } : {}) }, update)),
      history: (scope: SharedStateIdentity & { principalId: string }, afterSeq = 0) => sharedStateHistory(database, scope, { principalId: scope.principalId }, afterSeq),
    },
    /** Authenticated IM control events may cancel another principal's work only with explicit control capability. */
    cancelConversationRun: (scope: ConversationIdentity & { principalId: string; runId: string; commandId: string }) => withTransaction(database, async db => {
      identifier(scope.commandId); identifier(scope.runId)
      const policy = await conversationPolicy(db, scope, true)
      participant(policy, scope.principalId, 'read', 'human')
      const row = (await db.query(`SELECT * FROM lingxios.agent_work_items WHERE id=$1 AND tenant_id=$2
        AND conversation->>'conversationId'=$3 AND thread_id IS NOT DISTINCT FROM $4 FOR UPDATE`,
      [scope.runId, scope.tenantId, scope.conversationId, scope.threadId ?? null])).rows[0]
      if (!row) throw new Error('run is outside this conversation')
      const work = workItemFromRow(row, '', 1)
      requireAudience(policy, work.conversation!.audience, [scope.principalId])
      if (scope.principalId !== work.principalId) participant(policy, scope.principalId, 'control', 'human')
      const fingerprint = digest(scope)
      const prior = await db.query(`SELECT fingerprint,result FROM lingxios.agent_conversation_controls WHERE tenant_id=$1 AND conversation_id=$2 AND command_id=$3`,
        [scope.tenantId, scope.conversationId, scope.commandId])
      if (prior.rows[0]) {
        if (prior.rows[0]['fingerprint'] !== fingerprint) throw new Error('control command identity reused')
        return Boolean(prior.rows[0]['result'])
      }
      const result = await cancelRun(db, { runId: work.id, tenantId: work.tenantId, agentId: work.agentId, sessionId: work.sessionId,
        principalId: work.principalId!, ...(work.threadId === undefined ? {} : { threadId: work.threadId }) })
      await db.query(`INSERT INTO lingxios.agent_conversation_controls(tenant_id,conversation_id,command_id,work_id,actor_id,fingerprint,result)
        VALUES($1,$2,$3,$4,$5,$6,$7)`, [scope.tenantId, scope.conversationId, scope.commandId, work.id, scope.principalId, fingerprint, result])
      return result
    }),
    /** Bounded references only: native tool payloads and private intermediate text are not a public activity feed. */
    readConversationTrace: async (scope: ConversationIdentity & MessageReference & { principalId: string }) => {
      const policy = await conversationPolicy(database, scope)
      participant(policy, scope.principalId, 'read', 'human')
      const source = (await database.query(`SELECT audience,outcome FROM lingxios.agent_im_messages WHERE tenant_id=$1 AND conversation_id=$2
        AND thread_id IS NOT DISTINCT FROM $3 AND message_id=$4 AND version=$5`,
      [scope.tenantId, scope.conversationId, scope.threadId ?? null, scope.messageId, scope.version])).rows[0]
      if (!source) return null
      requireAudience(policy, source['audience'] as Audience, [scope.principalId])
      const roots = (source['outcome'] as IMIngressResult).runs.map(run => run.runId)
      const runs = await database.query(`WITH RECURSIVE tree AS (
        SELECT id,tenant_id,principal_id,ARRAY[id] AS path FROM lingxios.agent_work_items WHERE id=ANY($1::text[]) AND tenant_id=$2
        UNION ALL SELECT child.id,child.tenant_id,child.principal_id,parent.path||child.id FROM lingxios.agent_work_items child
          JOIN tree parent ON child.meta->>'parentWorkId'=parent.id AND child.tenant_id=parent.tenant_id
            AND child.principal_id IS NOT DISTINCT FROM parent.principal_id
          WHERE NOT child.id=ANY(parent.path) AND cardinality(parent.path)<64
      ) SELECT work.id,work.agent_id,work.status,work.meta->>'parentWorkId' AS parent_id,work.meta->>'graphId' AS graph_id,
          work.result_id FROM tree JOIN lingxios.agent_work_items work ON work.id=tree.id ORDER BY work.created_at,work.id LIMIT 257`, [roots, scope.tenantId])
      const ids = runs.rows.slice(0, 256).map(row => row['id'])
      const actions = await database.query(`SELECT idempotency_key,intent->>'workId' AS work_id,intent->'action'->>'action' AS action
        FROM lingxios.agent_action_intents WHERE intent->>'workId'=ANY($1::text[]) ORDER BY recorded_at,idempotency_key LIMIT 257`, [ids])
      const states = await database.query(`SELECT seq,state_id,operation_id,origin FROM lingxios.agent_shared_operations
        WHERE tenant_id=$1 AND conversation_id=$2 AND thread_key=$3 AND origin->>'workId'=ANY($4::text[]) ORDER BY seq LIMIT 257`,
      [scope.tenantId, scope.conversationId, JSON.stringify(scope.threadId ?? null), ids])
      const deliveries = await database.query(`SELECT outbox.result_id,outbox.receipt,outbox.delivered_at,outbox.failed_at FROM lingxios.agent_delivery_outbox outbox
        JOIN lingxios.agent_results result ON result.id=outbox.result_id WHERE result.work_id=ANY($1::text[]) ORDER BY result.committed_at,result.id LIMIT 257`, [ids])
      return { source: { messageId: scope.messageId, version: scope.version }, runs: runs.rows.slice(0, 256), actions: actions.rows.slice(0, 256),
        states: states.rows.slice(0, 256), deliveries: deliveries.rows.slice(0, 256), truncated: [runs, actions, states, deliveries].some(result => result.rows.length > 256) }
    },
  }
}
