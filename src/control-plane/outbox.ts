import { randomUUID } from 'node:crypto'
import type { SqlQueryable } from './pg-store.js'
import { createLogger } from '../logging.js'
import { abortable } from '../deadline.js'
import { errorMessage } from '../errors.js'
import { LATENCY_BUCKETS, type MetricsRegistry } from '../metrics.js'

const definitions = {
  agent_run_events: { keys: ['run_id','seq'], where: `AND outbox.delivery_work IS NOT NULL
    AND NOT EXISTS(SELECT 1 FROM lingxios.agent_run_events prior WHERE prior.run_id=outbox.run_id
      AND prior.seq<outbox.seq AND prior.delivery_work IS NOT NULL AND prior.delivered_at IS NULL)` },
  agent_delivery_outbox: { keys: ['result_id'],
    join: `JOIN lingxios.agent_results result ON result.id=outbox.result_id
      JOIN lingxios.agent_work_items work ON work.id=result.work_id AND work.result_id=result.id`,
    where: `AND work.cancel_requested_at IS NULL AND result.request_version=jsonb_array_length(work.steer_inputs)+1
      AND result.fence=work.fence AND work.status IN ('succeeded','partial','blocked','waiting')
      AND NOT EXISTS(SELECT 1 FROM lingxios.agent_delivery_outbox prior JOIN lingxios.agent_results r ON r.id=prior.result_id
        WHERE r.work_id=work.id AND r.id<>result.id AND prior.claim_token IS NOT NULL
          AND prior.delivered_at IS NULL AND prior.available_at>NOW())`,
    select: ',result.message,result.fence AS result_fence,result.home_epoch,result.committed_at,to_jsonb(work) AS delivery_work',
    returning: ',candidate.message,candidate.result_fence,candidate.home_epoch,candidate.committed_at,candidate.delivery_work' },
  agent_model_budget_calls: { keys: ['root_work_id','call_id'], where: 'AND outbox.observation IS NOT NULL' },
} satisfies Record<string, { keys: string[]; join?: string; where?: string; select?: string; returning?: string }>
type Table = keyof typeof definitions
const pending = new WeakMap<SqlQueryable, Map<Table, Set<Promise<void>>>>()
export interface DeliveryContext {
  signal: AbortSignal
  deadlineAt: string
  commit?: { resultId: string; fence: number }
  im?: import('../collaboration/types.js').IMDeliveryContext
}
type Deliver = (row: Record<string, unknown>, context: DeliveryContext) => Promise<void>
export interface OutboxOptions {
  signal?: AbortSignal
  timeoutMs?: number
  maxAttempts?: number
  concurrency?: number
  budgetMs?: number
  maxRows?: number
  metrics?: MetricsRegistry
}

/** Independent lanes claim just before sending. Slow lanes never hold unstarted rows or healthy lanes. */
export async function flushOutbox(database: SqlQueryable, table: Table, deliver: Deliver,
  options: OutboxOptions = {}) {
  const concurrency = options.concurrency ?? 4, budgetMs = options.budgetMs ?? 1000, maxRows = options.maxRows ?? 256
  const timeoutMs = options.timeoutMs ?? 10_000, maxAttempts = options.maxAttempts ?? 12
  if (!Object.hasOwn(definitions, table)) throw new Error('unsupported outbox')
  if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > 32
    || !Number.isInteger(budgetMs) || budgetMs < 1 || budgetMs > 30_000
    || !Number.isInteger(maxRows) || maxRows < 1 || maxRows > 4096
    || !Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 30_000
    || !Number.isInteger(maxAttempts) || maxAttempts < 1 || maxAttempts > 30) throw new Error('invalid outbox limits')
  let commands = pending.get(database)
  if (!commands) { commands = new Map(); pending.set(database, commands) }
  let lanes = commands.get(table)
  if (!lanes) { lanes = new Set(); commands.set(table, lanes) }
  const free = concurrency - lanes.size
  if (free <= 0 || options.signal?.aborted) return
  const until = performance.now() + budgetMs
  let admitted = 0
  const started = Array.from({ length: free }, () => {
    const operation = (async () => {
      await database.query(`UPDATE lingxios.${table} SET failed_at=NOW(),claim_token=NULL,
        last_error=COALESCE(last_error,'delivery lease expired at the attempt limit')
        WHERE delivered_at IS NULL AND failed_at IS NULL AND available_at<=NOW() AND attempts>=$1`, [maxAttempts])
      while (!options.signal?.aborted && performance.now() < until && admitted++ < maxRows) {
        if (!await deliverBatch(database, table, deliver, options)) return
      }
    })().catch(() => {
      createLogger().warn('outbox acknowledgement failed; durable rows retain the delivery intent', { table })
    }).finally(() => lanes!.delete(operation))
    lanes!.add(operation)
    return operation
  })
  await Promise.all(started)
}

export async function drainOutboxes(database: SqlQueryable): Promise<void> {
  await Promise.allSettled([...(pending.get(database)?.values() ?? [])].flatMap(lanes => [...lanes]))
}

async function deliverBatch(database: SqlQueryable, table: Table, deliver: Deliver,
  options: OutboxOptions): Promise<boolean> {
  const timeoutMs = options.timeoutMs ?? 10_000, maxAttempts = options.maxAttempts ?? 12
  const spec: { keys: string[]; join?: string; where?: string; select?: string; returning?: string } = definitions[table]
  const token = randomUUID()
  // Identifiers are from this package-owned table, never input from a model or resource.
  const { rows } = await database.query(`WITH candidate AS (
    SELECT ${spec.keys.map(key => `outbox.${key}`).join(',')}${spec.select ?? ''} FROM lingxios.${table} outbox ${spec.join ?? ''}
    WHERE outbox.delivered_at IS NULL AND outbox.failed_at IS NULL AND outbox.available_at<=NOW() AND outbox.attempts<$2 ${spec.where ?? ''}
    ORDER BY outbox.available_at,${spec.keys.map(key => `outbox.${key}`).join(',')} LIMIT 1 FOR UPDATE OF outbox SKIP LOCKED)
    UPDATE lingxios.${table} outbox SET claim_token=$1,available_at=NOW()+INTERVAL '60 seconds',attempts=LEAST(attempts+1,30)
    FROM candidate WHERE ${spec.keys.map(key => `outbox.${key}=candidate.${key}`).join(' AND ')}
    RETURNING outbox.*${spec.returning ?? ''}`, [token, maxAttempts])
  for (const row of rows) {
    const params = [...spec.keys.map(key => row[key]), token]
    const where = `${spec.keys.map((key, index) => `${key}=$${index + 1}`).join(' AND ')} AND claim_token=$${params.length}`
    const timeout = new AbortController()
    const timer = setTimeout(() => timeout.abort(new Error('delivery deadline exceeded')), timeoutMs)
    const signal = AbortSignal.any([timeout.signal, ...(options.signal ? [options.signal] : [])])
    const began = performance.now()
    try {
      signal.throwIfAborted()
      await abortable(deliver(row, { signal, deadlineAt: new Date(Date.now() + timeoutMs).toISOString() }), signal)
      await database.query(`UPDATE lingxios.${table} SET delivered_at=NOW(),claim_token=NULL,last_error=NULL WHERE ${where}`, params)
      if (table === 'agent_delivery_outbox' && row['committed_at']) {
        options.metrics?.histogram('agentos_commit_delivery_seconds', 'Commit to acknowledged final delivery; requires synchronized clocks', LATENCY_BUCKETS)
          .observe(Math.max(0, (Date.now() - new Date(row['committed_at'] as string | Date).getTime()) / 1000))
      }
    } catch (error) {
      await database.query(`UPDATE lingxios.${table} SET claim_token=NULL,
        last_error=$${params.length + 1},failed_at=CASE WHEN attempts>=$${params.length + 2} THEN NOW() ELSE NULL END,
        available_at=NOW()+LEAST(300,5*power(2,LEAST(attempts-1,6)))*INTERVAL '1 second' WHERE ${where}`,
      [...params, errorMessage(error).slice(0, 1000), maxAttempts])
        .catch(() => { throw new Error('outbox failure could not be persisted') })
    } finally {
      clearTimeout(timer)
      options.metrics?.histogram('agentos_outbox_delivery_seconds', 'One delivery attempt including acknowledgement', LATENCY_BUCKETS)
        .observe((performance.now() - began) / 1000, { channel: table })
    }
  }
  return rows.length > 0
}
