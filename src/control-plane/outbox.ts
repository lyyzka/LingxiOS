import { randomUUID } from 'node:crypto'
import type { SqlQueryable } from './pg-store.js'
import { createLogger } from '../logging.js'
import { abortable } from '../deadline.js'
import { errorMessage } from '../errors.js'

const definitions = {
  agent_run_events: { keys: ['run_id','seq'], where: `AND outbox.delivery_work IS NOT NULL
    AND NOT EXISTS(SELECT 1 FROM lingxios.agent_run_events prior WHERE prior.run_id=outbox.run_id
      AND prior.seq<outbox.seq AND prior.delivery_work IS NOT NULL AND prior.delivered_at IS NULL)` },
  agent_delivery_outbox: { keys: ['result_id'],
    join: `JOIN lingxios.agent_results result ON result.id=outbox.result_id
      JOIN lingxios.agent_work_items work ON work.id=result.work_id AND work.result_id=result.id`,
    where: `AND work.cancel_requested_at IS NULL AND result.request_version=jsonb_array_length(work.steer_inputs)+1
      AND result.fence=work.fence AND work.status IN ('succeeded','partial','blocked','waiting')`,
    select: ',result.message,result.fence AS result_fence,result.home_epoch,to_jsonb(work) AS delivery_work',
    returning: ',candidate.message,candidate.result_fence,candidate.home_epoch,candidate.delivery_work' },
  agent_model_budget_calls: { keys: ['root_work_id','call_id'], where: 'AND outbox.observation IS NOT NULL' },
} satisfies Record<string, { keys: string[]; join?: string; where?: string; select?: string; returning?: string }>
type Table = keyof typeof definitions
const pending = new WeakMap<SqlQueryable, Map<Table, Promise<void>>>()
export interface DeliveryContext {
  signal: AbortSignal
  deadlineAt: string
  commit?: { resultId: string; fence: number }
  im?: import('../collaboration/types.js').IMDeliveryContext
}
type Deliver = (row: Record<string, unknown>, context: DeliveryContext) => Promise<void>

/** One outstanding batch per channel. Non-abortable native transports cannot block scheduling or pile up promises. */
export async function flushOutbox(database: SqlQueryable, table: Table, deliver: Deliver,
  options: { signal?: AbortSignal; timeoutMs?: number; maxAttempts?: number } = {}) {
  let commands = pending.get(database)
  if (!commands) { commands = new Map(); pending.set(database, commands) }
  if (commands.has(table)) return
  const operation = deliverBatch(database, table, deliver, options).catch(() => {
    createLogger().warn('outbox acknowledgement failed; durable rows retain the delivery intent', { table })
  }).finally(() => commands.delete(table))
  commands.set(table, operation)
  await operation
}

async function deliverBatch(database: SqlQueryable, table: Table, deliver: Deliver,
  options: { signal?: AbortSignal; timeoutMs?: number; maxAttempts?: number }) {
  const timeoutMs = options.timeoutMs ?? 10_000, maxAttempts = options.maxAttempts ?? 12
  if (!Number.isInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 30_000
    || !Number.isInteger(maxAttempts) || maxAttempts < 1 || maxAttempts > 30) throw new Error('invalid outbox limits')
  const spec: { keys: string[]; join?: string; where?: string; select?: string; returning?: string } = definitions[table]
  if (!spec) throw new Error('unsupported outbox')
  const token = randomUUID()
  await database.query(`UPDATE lingxios.${table} SET failed_at=NOW(),claim_token=NULL,
    last_error=COALESCE(last_error,'delivery lease expired at the attempt limit')
    WHERE delivered_at IS NULL AND failed_at IS NULL AND available_at<=NOW() AND attempts>=$1`, [maxAttempts])
  // Identifiers are from this package-owned table, never input from a model or resource.
  const { rows } = await database.query(`WITH candidate AS (
    SELECT ${spec.keys.map(key => `outbox.${key}`).join(',')}${spec.select ?? ''} FROM lingxios.${table} outbox ${spec.join ?? ''}
    WHERE outbox.delivered_at IS NULL AND outbox.failed_at IS NULL AND outbox.available_at<=NOW() ${spec.where ?? ''}
    ORDER BY outbox.available_at,${spec.keys.map(key => `outbox.${key}`).join(',')} LIMIT ${table === 'agent_run_events' ? 64 : 8} FOR UPDATE OF outbox SKIP LOCKED)
    UPDATE lingxios.${table} outbox SET claim_token=$1,available_at=NOW()+INTERVAL '60 seconds',attempts=LEAST(attempts+1,30)
    FROM candidate WHERE ${spec.keys.map(key => `outbox.${key}=candidate.${key}`).join(' AND ')}
    RETURNING outbox.*${spec.returning ?? ''}`, [token])
  if (table === 'agent_run_events') rows.sort((a,b) => String(a['run_id']).localeCompare(String(b['run_id'])) || Number(a['seq'])-Number(b['seq']))
  for (const row of rows) {
    const params = [...spec.keys.map(key => row[key]), token]
    const where = `${spec.keys.map((key, index) => `${key}=$${index + 1}`).join(' AND ')} AND claim_token=$${params.length}`
    const timeout = new AbortController()
    const timer = setTimeout(() => timeout.abort(new Error('delivery deadline exceeded')), timeoutMs)
    const signal = AbortSignal.any([timeout.signal, ...(options.signal ? [options.signal] : [])])
    try {
      const owned = await database.query(`UPDATE lingxios.${table} SET available_at=NOW()+INTERVAL '60 seconds' WHERE ${where} RETURNING claim_token`, params)
      if (!owned.rows.length) continue
      signal.throwIfAborted()
      await abortable(deliver(row, { signal, deadlineAt: new Date(Date.now() + timeoutMs).toISOString() }), signal)
      await database.query(`UPDATE lingxios.${table} SET delivered_at=NOW(),claim_token=NULL,last_error=NULL WHERE ${where}`, params)
    } catch (error) {
      await database.query(`UPDATE lingxios.${table} SET claim_token=NULL,
        last_error=$${params.length + 1},failed_at=CASE WHEN attempts>=$${params.length + 2} THEN NOW() ELSE NULL END,
        available_at=NOW()+LEAST(300,5*power(2,LEAST(attempts-1,6)))*INTERVAL '1 second' WHERE ${where}`,
      [...params, errorMessage(error).slice(0, 1000), maxAttempts])
        .catch(() => { throw new Error('outbox failure could not be persisted') })
    } finally { clearTimeout(timer) }
  }
}
