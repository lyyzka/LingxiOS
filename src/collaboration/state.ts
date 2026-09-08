import type { SqlQueryable } from '../control-plane/pg-store.js'
import type { WorkItem } from '../protocol/types.js'
import { audienceOf, authorizeConversationWork, containsAudience, conversationPolicy, digest, identifier, participant, requireAudience } from './access.js'
import type { Audience, AudienceInput, MessageReference, SharedStateIdentity, SharedStateResult, SharedStateSnapshot, SharedStateUpdate } from './types.js'

export interface StateActor { principalId: string; work?: Omit<WorkItem, 'leaseToken'>; actionKey?: string; source?: MessageReference }
const scopeParams = (scope: SharedStateIdentity) => [scope.tenantId, scope.conversationId, JSON.stringify(scope.threadId ?? null), scope.stateId]
function snapshot(row: Record<string, unknown>): SharedStateSnapshot {
  return { version: Number(row['version']), audience: row['audience'] as Audience, fields: row['fields'] as SharedStateSnapshot['fields'] }
}
async function authorize(database: SqlQueryable, scope: SharedStateIdentity, actor: StateActor, write: boolean, audience?: Audience) {
  identifier(scope.stateId); identifier(actor.principalId)
  const policy = await conversationPolicy(database, scope, write)
  participant(policy, actor.principalId, write ? 'execute' : 'read', 'human')
  if (audience) requireAudience(policy, audience, [actor.principalId])
  if (actor.work) {
    const work = actor.work
    if (!work.conversation || work.tenantId !== scope.tenantId || work.conversation.conversationId !== scope.conversationId
      || work.threadId !== scope.threadId || work.principalId !== actor.principalId) throw new Error('shared state is outside this work conversation')
    await authorizeConversationWork(database, work, write ? 'execute' : 'read', write)
    if (audience && !containsAudience(audience, work.conversation.audience)) throw new Error('state cannot be disclosed to the work audience')
  }
  return policy
}

/** All mutations use the caller's transaction, including native action receipts. */
export async function createSharedState(database: SqlQueryable, scope: SharedStateIdentity, actor: StateActor, input: AudienceInput = { visibility: 'conversation' }) {
  const policy = await authorize(database, scope, actor, true), audience = audienceOf(policy, input)
  await authorize(database, scope, actor, true, audience)
  await database.query(`INSERT INTO lingxios.agent_shared_states(tenant_id,conversation_id,thread_key,id,audience)
    VALUES($1,$2,$3,$4,$5::jsonb) ON CONFLICT DO NOTHING`, [...scopeParams(scope), JSON.stringify(audience)])
  const state = (await readSharedState(database, scope, actor))!
  if (digest(state.audience) !== digest(audience)) throw new Error('state identity reused with a different audience')
  return state
}

export async function readSharedState(database: SqlQueryable, scope: SharedStateIdentity, actor: StateActor): Promise<SharedStateSnapshot | null> {
  await authorize(database, scope, actor, false)
  const { rows } = await database.query(`SELECT audience,version,fields FROM lingxios.agent_shared_states
    WHERE tenant_id=$1 AND conversation_id=$2 AND thread_key=$3 AND id=$4`, scopeParams(scope))
  if (!rows[0]) return null
  const state = snapshot(rows[0])
  await authorize(database, scope, actor, false, state.audience)
  return state
}

export function validateStateUpdate(update: SharedStateUpdate) {
  identifier(update.operationId)
  if (!Array.isArray(update.changes) || !update.changes.length || update.changes.length > 64
    || new Set(update.changes.map(change => change.field)).size !== update.changes.length) throw new Error('state update requires 1-64 distinct fields')
  for (const change of update.changes) {
    if (!change || typeof change.field !== 'string' || !change.field || change.field.length > 256
      || ['__proto__', 'constructor', 'prototype'].includes(change.field)
      || !Number.isSafeInteger(change.expectedVersion) || change.expectedVersion < 0
      || ('delete' in change ? change.delete !== true || Object.hasOwn(change, 'value') : !Object.hasOwn(change, 'value') || change.value === undefined)
      || Object.keys(change).some(key => !['field', 'expectedVersion', 'value', 'delete'].includes(key))) throw new Error('invalid state change')
    if ('value' in change) jsonValue(change.value)
  }
  if (Buffer.byteLength(JSON.stringify(update)) > 64_000) throw new Error('state update exceeds 64 KB')
}

function jsonValue(value: unknown, depth = 0): void {
  if (depth > 32) throw new Error('state values exceed 32 nesting levels')
  if (value === null || typeof value === 'string' || typeof value === 'boolean' || typeof value === 'number' && Number.isFinite(value)) return
  if (Array.isArray(value)) { for (const item of value) jsonValue(item, depth + 1); return }
  if (value && typeof value === 'object' && [Object.prototype, null].includes(Object.getPrototypeOf(value))) {
    for (const item of Object.values(value)) jsonValue(item, depth + 1)
    return
  }
  throw new Error('state values must be finite JSON values')
}

export async function updateSharedState(database: SqlQueryable, scope: SharedStateIdentity, actor: StateActor, update: SharedStateUpdate): Promise<SharedStateResult> {
  validateStateUpdate(update)
  await authorize(database, scope, actor, true)
  if (actor.source) {
    const source = await database.query(`SELECT 1 FROM lingxios.agent_im_messages WHERE tenant_id=$1 AND conversation_id=$2
      AND thread_id IS NOT DISTINCT FROM $3 AND message_id=$4 AND version=$5 AND input->'author'->>'id'=$6`,
    [scope.tenantId, scope.conversationId, scope.threadId ?? null, actor.source.messageId, actor.source.version, actor.principalId])
    if (!source.rows.length) throw new Error('state update source is outside this actor and conversation')
  }
  const { rows } = await database.query(`SELECT audience,version,fields FROM lingxios.agent_shared_states
    WHERE tenant_id=$1 AND conversation_id=$2 AND thread_key=$3 AND id=$4 FOR UPDATE`, scopeParams(scope))
  if (!rows[0]) throw new Error('shared state is unavailable')
  const state = snapshot(rows[0])
  await authorize(database, scope, actor, true, state.audience)
  const origin = { principalId: actor.principalId, ...(actor.work ? { agentId: actor.work.agentId, workId: actor.work.id,
    graphId: actor.work.meta?.['graphId'] ?? null, source: actor.work.conversation!.source, actionKey: actor.actionKey } : { source: actor.source ?? null }) }
  const fingerprint = digest({ update, origin })
  const prior = await database.query(`SELECT fingerprint,result FROM lingxios.agent_shared_operations
    WHERE tenant_id=$1 AND conversation_id=$2 AND thread_key=$3 AND state_id=$4 AND operation_id=$5`, [...scopeParams(scope), update.operationId])
  if (prior.rows[0]) {
    if (prior.rows[0]['fingerprint'] !== fingerprint) throw new Error('state operation identity reused with different content or actor')
    return { ...prior.rows[0]['result'] as SharedStateResult, deduplicated: true }
  }
  const conflicts = update.changes.filter(change => (state.fields[change.field]?.version ?? 0) !== change.expectedVersion).map(change => change.field)
  let result: SharedStateResult
  if (conflicts.length) result = { ok: false, conflicts, state, deduplicated: false }
  else {
    if (!Number.isSafeInteger(state.version + 1)) throw new Error('state version limit reached')
    for (const change of update.changes) {
      const version = change.expectedVersion + 1
      if (!Number.isSafeInteger(version)) throw new Error('field version limit reached')
      state.fields[change.field] = 'delete' in change ? { version, deleted: true } : { version, deleted: false, value: structuredClone(change.value) }
    }
    if (Object.keys(state.fields).length > 256 || Buffer.byteLength(JSON.stringify(state.fields)) > 256_000) throw new Error('shared state exceeds 256 fields or 256 KB')
    state.version++
    await database.query(`UPDATE lingxios.agent_shared_states SET fields=$5::jsonb,version=$6
      WHERE tenant_id=$1 AND conversation_id=$2 AND thread_key=$3 AND id=$4`, [...scopeParams(scope), JSON.stringify(state.fields), state.version])
    result = { ok: true, state, deduplicated: false }
  }
  await database.query(`INSERT INTO lingxios.agent_shared_operations(tenant_id,conversation_id,thread_key,state_id,operation_id,fingerprint,origin,changes,result)
    VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,$9::jsonb)`, [...scopeParams(scope), update.operationId, fingerprint, JSON.stringify(origin), JSON.stringify(update.changes), JSON.stringify(result)])
  return result
}

export async function sharedStateHistory(database: SqlQueryable, scope: SharedStateIdentity, actor: StateActor, afterSeq = 0) {
  if (!Number.isSafeInteger(afterSeq) || afterSeq < 0) throw new Error('invalid state history cursor')
  if (!await readSharedState(database, scope, actor)) return { items: [], nextSeq: afterSeq }
  const { rows } = await database.query(`SELECT seq,operation_id,origin,changes,result,recorded_at FROM lingxios.agent_shared_operations
    WHERE tenant_id=$1 AND conversation_id=$2 AND thread_key=$3 AND state_id=$4 AND seq>$5 ORDER BY seq LIMIT 64`, [...scopeParams(scope), afterSeq])
  return { items: rows, nextSeq: rows.length ? Number(rows.at(-1)!['seq']) : afterSeq }
}
