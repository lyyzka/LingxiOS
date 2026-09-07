import { isDeepStrictEqual } from 'node:util'
import type { HostAction, WorkItem } from '../protocol/types.js'
import type { SqlQueryable } from './pg-store.js'

/** Call inside the native write transaction, before reading or changing domain state. */
export async function lockAction(database: SqlQueryable, work: Omit<WorkItem, 'leaseToken'>, action: HostAction) {
  const { rows } = await database.query(`SELECT work.id FROM lingxios.agent_work_items work
    JOIN lingxios.agent_action_intents intent ON intent.idempotency_key=$3
    WHERE work.id=$1 AND work.fence=$2 AND work.status='leased' AND work.lease_expires_at>NOW()
      AND work.cancel_requested_at IS NULL AND work.tenant_id=$4 AND work.principal_id IS NOT DISTINCT FROM $5
      AND work.agent_id=$6 AND work.session_id=$7 AND work.thread_id IS NOT DISTINCT FROM $8
      AND intent.intent->>'workId'=work.id AND intent.intent->'action'=$9::jsonb
      AND (intent.intent->>'requestVersion')::integer=jsonb_array_length(work.steer_inputs)+1
    FOR UPDATE OF work,intent`, [work.id, work.fence, action.idempotencyKey, work.tenantId, work.principalId ?? null,
    work.agentId, work.sessionId, work.threadId ?? null, JSON.stringify(action)])
  if (rows.length !== 1) throw new Error('write requires the current live action intent')
}

/** The effect and its authoritative receipt either commit together or roll back together. */
export async function recordActionResult<T>(database: SqlQueryable, action: HostAction, value: T): Promise<T> {
  const result = { ok: true, value }
  const { rows } = await database.query(`INSERT INTO lingxios.agent_action_ledger(idempotency_key,result)
    VALUES($1,$2::jsonb) ON CONFLICT DO NOTHING RETURNING idempotency_key`, [action.idempotencyKey, JSON.stringify(result)])
  if (!rows.length) {
    const prior = await database.query('SELECT result FROM lingxios.agent_action_ledger WHERE idempotency_key=$1', [action.idempotencyKey])
    if (!isDeepStrictEqual(prior.rows[0]?.['result'], JSON.parse(JSON.stringify(result)))) throw new Error('action receipt changed during transaction')
  }
  return value
}
