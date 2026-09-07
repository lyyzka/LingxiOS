import type { SqlQueryable } from '../control-plane/pg-store.js'
import type { MetricsRegistry } from '../metrics.js'
import { readRun, runSnapshot, type RunIdentity, type RunSnapshot } from './jobs.js'

export interface RunListQuery {
  tenantId?: string
  sessionId?: string
  agentId?: string
  principalId?: string
  id?: string
  status?: RunSnapshot['status']
  search?: string
  cursor?: string
  offset?: number
  order?: 'newest' | 'oldest' | 'id'
  limit?: number
}
export interface RunRecord extends RunSnapshot {
  identity: RunIdentity
  executionMs: number
  model: string | null
  tokens: number
  costMicros: number
  unmeasuredCalls: number
}

/** Trusted server read API. The product authorizes tenant, conversation or administrator scope. */
export async function listRuns(database: SqlQueryable, query: RunListQuery = {}): Promise<{ items: RunRecord[]; nextCursor: string | null }> {
  const limit = query.limit ?? 50
  const order = query.order ?? 'newest', offset = query.offset ?? 0
  if (!['newest','oldest','id'].includes(order) || !Number.isSafeInteger(offset) || offset < 0 || offset > 10000
    || query.cursor && query.offset !== undefined) throw new Error('invalid run pagination')
  const comparison = order === 'newest' ? '<' : '>', direction = order === 'newest' ? 'DESC' : 'ASC'
  if ((query.cursor?.length ?? 0) > 5000) throw new Error('invalid run cursor')
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100 || (query.search?.length ?? 0) > 200
    || query.status && !['queued','leased','waiting','succeeded','partial','blocked','failed','cancelled'].includes(query.status)) throw new Error('invalid run query')
  let cursor: [string,string] | undefined
  if (query.cursor) {
    try { cursor = JSON.parse(Buffer.from(query.cursor,'base64url').toString()) as [string,string] } catch { throw new Error('invalid run cursor') }
    if (!Array.isArray(cursor) || cursor.length !== 2 || typeof cursor[0] !== 'string' || !Number.isFinite(Date.parse(cursor[0]))
      || typeof cursor[1] !== 'string' || !cursor[1] || cursor[1].length > 2000) throw new Error('invalid run cursor')
  }
  const { rows } = await database.query(`SELECT work.id,work.tenant_id,work.agent_id,work.principal_id,work.session_id,work.thread_id,
    work.fence,work.result_id,work.status,work.kind,work.attempts,work.created_at,work.available_at,work.heartbeat_at,work.last_progress_at,
    work.goal_outcome,work.error,jsonb_array_length(work.steer_inputs)+1 AS request_version,work.created_at::text AS cursor_time,
    (SELECT fence FROM lingxios.agent_results WHERE id=work.result_id) AS result_fence,
    (SELECT COALESCE(SUM(EXTRACT(EPOCH FROM (COALESCE(ended_at,NOW())-started_at))*1000),0) FROM lingxios.agent_attempts WHERE work_id=work.id) AS execution_ms,
    usage.model,usage.tokens,usage.cost_micros,usage.unmeasured_calls
    FROM lingxios.agent_work_items work LEFT JOIN LATERAL (SELECT MAX(observation->>'model') AS model,
      COALESCE(SUM(COALESCE(input_tokens+output_tokens,reserved_tokens)),0) AS tokens,
      COALESCE(SUM(COALESCE(cost_micros,reserved_cost_micros)),0) AS cost_micros,
      COUNT(*) FILTER(WHERE observation IS NULL OR observation->'cost'->>'usage' IS DISTINCT FROM 'measured') AS unmeasured_calls
      FROM lingxios.agent_model_budget_calls WHERE work_id=work.id) usage ON TRUE
    WHERE ($1::text IS NULL OR work.tenant_id=$1) AND ($2::text IS NULL OR work.session_id=$2)
      AND ($3::text IS NULL OR work.agent_id=$3) AND ($4::text IS NULL OR work.principal_id=$4)
      AND ($5::text IS NULL OR work.id=$5) AND ($6::text IS NULL OR work.status=$6)
      AND ($7::text IS NULL OR work.id ILIKE $7 OR work.agent_id ILIKE $7 OR work.session_id ILIKE $7)
      AND ($8::timestamptz IS NULL OR ${order === 'id' ? 'work.id>$9::text' : `(work.created_at,work.id)${comparison}($8,$9::text)`})
    ORDER BY ${order === 'id' ? 'work.id' : `work.created_at ${direction},work.id ${direction}`} LIMIT $10 OFFSET $11`,
  [query.tenantId ?? null,query.sessionId ?? null,query.agentId ?? null,query.principalId ?? null,query.id ?? null,query.status ?? null,
    query.search ? `%${query.search}%` : null,cursor?.[0] ?? null,cursor?.[1] ?? null,limit+1,offset])
  const selected = rows.slice(0,limit), last = selected.at(-1)
  return { items: selected.map(row => ({ ...runSnapshot(row), identity: { runId: String(row['id']), tenantId: String(row['tenant_id']),
    agentId: String(row['agent_id']), sessionId: String(row['session_id']), principalId: String(row['principal_id']),
    ...(row['thread_id'] ? { threadId: String(row['thread_id']) } : {}) }, executionMs: Number(row['execution_ms']),
    model: row['model'] as string | null, tokens: Number(row['tokens']), costMicros: Number(row['cost_micros']), unmeasuredCalls: Number(row['unmeasured_calls']) })),
    nextCursor: rows.length > limit && last ? Buffer.from(JSON.stringify([last['cursor_time'],last['id']])).toString('base64url') : null }
}

/** Read within the caller's product transaction when attaching a runtime evidence reference. */
export async function readRunReference(database: SqlQueryable, tenantId: string, runId: string): Promise<RunIdentity | null> {
  const { rows } = await database.query('SELECT agent_id,session_id,principal_id,thread_id FROM lingxios.agent_work_items WHERE tenant_id=$1 AND id=$2',[tenantId,runId])
  const row = rows[0]
  return row ? { tenantId,runId,agentId: String(row['agent_id']),sessionId: String(row['session_id']),principalId: String(row['principal_id']),
    ...(row['thread_id'] ? { threadId: String(row['thread_id']) } : {}) } : null
}

export async function countPendingApprovals(database: SqlQueryable, tenantId: string, sessionId: string): Promise<number> {
  const { rows } = await database.query(`SELECT COUNT(*) AS count FROM lingxios.agent_approvals approval
    JOIN lingxios.agent_action_intents intent ON intent.idempotency_key=approval.action_key
    JOIN lingxios.agent_work_items work ON work.id=intent.intent->>'workId'
    WHERE work.tenant_id=$1 AND work.session_id=$2 AND work.cancel_requested_at IS NULL AND approval.decision IS NULL
      AND work.status IN ('queued','leased','waiting') AND jsonb_array_length(work.steer_inputs)+1=(intent.intent->>'requestVersion')::integer`,[tenantId,sessionId])
  return Number(rows[0]?.['count'] ?? 0)
}

export async function readOperations(database: SqlQueryable) {
  const [summary,trend,models] = await Promise.all([
    database.query(`SELECT COUNT(*) AS runs,COUNT(*) FILTER(WHERE status='succeeded') AS successes,
      COUNT(*) FILTER(WHERE status IN ('succeeded','partial','blocked','failed')) AS finished,
      COUNT(*) FILTER(WHERE status='failed') AS failures,COUNT(*) FILTER(WHERE status='cancelled') AS cancelled,
      COUNT(*) FILTER(WHERE status='leased') AS active,COUNT(*) FILTER(WHERE status='waiting') AS waiting,
      COUNT(*) FILTER(WHERE status='queued') AS queued,
      COALESCE(AVG((SELECT SUM(EXTRACT(EPOCH FROM (COALESCE(ended_at,NOW())-started_at))*1000)
        FROM lingxios.agent_attempts WHERE work_id=work.id)),0) AS average_execution_ms,
      COALESCE(SUM((SELECT SUM(COALESCE(input_tokens+output_tokens,reserved_tokens)) FROM lingxios.agent_model_budget_calls WHERE work_id=work.id)),0) AS tokens,
      COALESCE(SUM((SELECT SUM(COALESCE(cost_micros,reserved_cost_micros)) FROM lingxios.agent_model_budget_calls WHERE work_id=work.id)),0) AS cost_micros,
      (SELECT COUNT(*) FROM lingxios.agent_delivery_outbox WHERE delivered_at IS NULL AND failed_at IS NOT NULL) AS failed_deliveries,
      (SELECT COUNT(*) FROM lingxios.agent_model_budget_calls WHERE failed_at IS NOT NULL) AS failed_usage_deliveries
      FROM lingxios.agent_work_items work WHERE created_at>=NOW()-INTERVAL '24 hours'`),
    database.query(`WITH hours AS (SELECT generate_series(date_trunc('hour',NOW())-INTERVAL '23 hours',date_trunc('hour',NOW()),INTERVAL '1 hour') AS time)
      SELECT hours.time,COUNT(work.id) AS runs,COUNT(work.id) FILTER(WHERE status='failed') AS failures FROM hours
      LEFT JOIN lingxios.agent_work_items work ON work.created_at>=hours.time AND work.created_at<hours.time+INTERVAL '1 hour'
      GROUP BY hours.time ORDER BY hours.time`),
    database.query(`SELECT observation->>'model' AS model,COUNT(*) AS calls,SUM(COALESCE(input_tokens+output_tokens,reserved_tokens)) AS tokens,
      COUNT(*) FILTER(WHERE observation IS NULL OR observation->'cost'->>'usage' IS DISTINCT FROM 'measured') AS unmeasured_calls,
      SUM(COALESCE(call.cost_micros,call.reserved_cost_micros)) AS cost_micros FROM lingxios.agent_model_budget_calls call
      JOIN lingxios.agent_work_items work ON work.id=call.work_id WHERE work.created_at>=NOW()-INTERVAL '24 hours'
      GROUP BY 1 ORDER BY tokens DESC LIMIT 8`),
  ])
  const value = summary.rows[0]!
  return { periodHours: 24, runs: Number(value['runs']), successes: Number(value['successes']), finished: Number(value['finished']),
    failures: Number(value['failures']), cancelled: Number(value['cancelled']), active: Number(value['active']), waiting: Number(value['waiting']), queued: Number(value['queued']),
    averageExecutionMs: Number(value['average_execution_ms']), tokens: Number(value['tokens']), costMicros: Number(value['cost_micros']),
    failedDeliveries: Number(value['failed_deliveries']), failedUsageDeliveries: Number(value['failed_usage_deliveries']),
    trend: trend.rows.map(row => ({ time: new Date(row['time'] as Date | string).toISOString(), runs: Number(row['runs']), failures: Number(row['failures']) })),
    models: models.rows.map(row => ({ model: row['model'] as string | null, calls: Number(row['calls']), tokens: Number(row['tokens']),
      costMicros: Number(row['cost_micros']), unmeasuredCalls: Number(row['unmeasured_calls']) })) }
}

export async function readDiagnostics(database: SqlQueryable, identity: RunIdentity) {
  const run = await readRun(database, identity)
  if (!run) return null
  const [actions, delivery, execution] = await Promise.all([
    database.query(`SELECT intent.idempotency_key,intent.intent->'action'->>'action' AS action,
      COALESCE(resolved.result,receipt.result) AS result FROM lingxios.agent_action_intents intent
      LEFT JOIN lingxios.agent_action_ledger receipt USING(idempotency_key)
      LEFT JOIN LATERAL (SELECT resolution->'result' AS result FROM lingxios.agent_action_resolutions
        WHERE idempotency_key=intent.idempotency_key ORDER BY resolution_seq DESC LIMIT 1) resolved ON TRUE
      WHERE intent.intent->>'workId'=$1 AND (COALESCE(resolved.result,receipt.result) IS NULL
        OR COALESCE(resolved.result,receipt.result)->>'ok'='false') ORDER BY intent.recorded_at DESC LIMIT 65`, [run.id]),
    database.query(`SELECT result_id,attempts,last_error,failed_at,delivered_at,available_at FROM lingxios.agent_delivery_outbox
      WHERE result_id=$1`, [run.resultId]),
    database.query(`SELECT
      (SELECT COUNT(*) FROM lingxios.agent_steps WHERE work_id=$1 AND completed_at IS NOT NULL) AS completed_steps,
      (SELECT COUNT(*) FROM lingxios.agent_run_events WHERE run_id=$1 AND failed_at IS NOT NULL) AS failed_events,
      (SELECT COUNT(*) FROM lingxios.agent_model_budget_calls WHERE work_id=$1 AND failed_at IS NOT NULL) AS failed_usage,
      (SELECT COUNT(*) FROM lingxios.agent_verifications WHERE work_id=$1 AND request_version=$2 AND status='failed') AS failed_checks`, [run.id,run.requestVersion]),
  ])
  const row = execution.rows[0]!, now = Date.now()
  const age = (date: string | null) => date ? Math.max(0,(now-Date.parse(date))/1000) : null
  return { run, queueSeconds: run.status === 'queued' ? age(run.availableAt) : null,
    heartbeatAgeSeconds: age(run.heartbeatAt), progressAgeSeconds: age(run.lastProgressAt),
    completedSteps: Number(row['completed_steps']), failedChecks: Number(row['failed_checks']),
    failedEvents: Number(row['failed_events']), failedUsageDeliveries: Number(row['failed_usage']),
    actions: actions.rows.slice(0,64).map(row => ({ actionKey: String(row['idempotency_key']), action: String(row['action']),
      result: row['result'] as import('../protocol/types.js').HostActionResult | null })), actionsTruncated: actions.rows.length > 64,
    delivery: delivery.rows[0] ?? null }
}

export async function retryDelivery(database: SqlQueryable, identity: RunIdentity, channel: 'message' | 'events' | 'usage') {
  const sources = {
    message: ['agent_delivery_outbox','outbox.result_id=work.result_id AND work.cancel_requested_at IS NULL'],
    events: ['agent_run_events','outbox.run_id=work.id'],
    usage: ['agent_model_budget_calls','outbox.work_id=work.id'],
  } as const
  const source = sources[channel]
  if (!source) throw new Error('invalid delivery channel')
  const { rows } = await database.query(`UPDATE lingxios.${source[0]} outbox
    SET failed_at=NULL,last_error=NULL,attempts=0,available_at=NOW(),claim_token=NULL
    FROM lingxios.agent_work_items work WHERE ${source[1]} AND work.id=$1 AND work.tenant_id=$2
      AND work.agent_id=$3 AND work.session_id=$4 AND work.principal_id=$5 AND work.thread_id IS NOT DISTINCT FROM $6
      AND outbox.delivered_at IS NULL AND outbox.failed_at IS NOT NULL RETURNING work.id`,
  [identity.runId,identity.tenantId,identity.agentId,identity.sessionId,identity.principalId,identity.threadId ?? null])
  return rows.length > 0
}

/** Durable gauges survive restarts; execution counters remain local to each process. */
export async function refreshMetrics(database: SqlQueryable, metrics: MetricsRegistry) {
  const { rows } = await database.query(`SELECT
    (SELECT COUNT(*) FROM lingxios.agent_work_items WHERE status='queued' AND cancel_requested_at IS NULL) AS queued,
    (SELECT COALESCE(MAX(EXTRACT(EPOCH FROM NOW()-available_at)),0) FROM lingxios.agent_work_items
      WHERE status='queued' AND available_at<=NOW() AND cancel_requested_at IS NULL) AS queue_age,
    (SELECT COUNT(*) FROM lingxios.agent_work_items WHERE status='leased' AND lease_expires_at>NOW()
      AND last_progress_at<NOW()-INTERVAL '2 minutes') AS stalled,
    (SELECT COUNT(*) FROM lingxios.agent_delivery_outbox outbox JOIN lingxios.agent_work_items work ON work.result_id=outbox.result_id
      WHERE outbox.delivered_at IS NULL AND outbox.failed_at IS NULL AND work.cancel_requested_at IS NULL) AS delivery_pending,
    (SELECT COUNT(*) FROM lingxios.agent_delivery_outbox WHERE failed_at IS NOT NULL) AS delivery_failed,
    (SELECT COUNT(*) FROM lingxios.agent_model_budget_calls WHERE failed_at IS NOT NULL) AS usage_failed,
    (SELECT COALESCE(SUM(cost_micros),0) FROM lingxios.agent_model_budget_calls) AS cost_micros`)
  const row = rows[0]!
  for (const [field,name,help] of [
    ['queued','agentos_queue_pending','Queued work'], ['queue_age','agentos_queue_oldest_seconds','Oldest eligible queued work'],
    ['stalled','agentos_runs_stalled','Leased work without new resource facts for two minutes'],
    ['delivery_pending','agentos_delivery_pending','Current results awaiting delivery'],
    ['delivery_failed','agentos_delivery_failed','Deliveries requiring intervention'],
    ['usage_failed','agentos_usage_delivery_failed','Model observations requiring ledger redelivery'],
    ['cost_micros','agentos_recorded_cost_micros','Durable settled model cost in USD millionths'],
  ]) metrics.gauge(name!,help!).set(Number(row[field!]))
}
