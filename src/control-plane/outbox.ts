import { randomUUID } from 'node:crypto'
import type { SqlQueryable } from './pg-store.js'
import { createLogger } from '../logging.js'

const definitions = {
  agent_run_events: { keys: ['run_id','seq'], where: `AND outbox.delivery_work IS NOT NULL
    AND NOT EXISTS(SELECT 1 FROM lingxios.agent_run_events prior WHERE prior.run_id=outbox.run_id
      AND prior.seq<outbox.seq AND prior.delivery_work IS NOT NULL AND prior.delivered_at IS NULL AND prior.available_at>NOW())` },
  agent_calendar_outbox: { keys: ['id'] },
  agent_canvas_outbox: { keys: ['id'] },
  agent_document_outbox: { keys: ['id'] },
  agent_delivery_outbox: { keys: ['run_id'],
    join: `JOIN lingxios.agent_messages messages ON messages.run_id=outbox.run_id
      JOIN lingxios.agent_work_items work ON work.id=outbox.run_id`,
    where: `AND work.cancel_requested_at IS NULL AND (messages.message->'envelope'->>'requestVersion')::integer=jsonb_array_length(work.steer_inputs)+1`,
    select: ',messages.message', returning: ',candidate.message' },
  agent_model_budget_calls: { keys: ['root_work_id','call_id'], where: 'AND outbox.observation IS NOT NULL' },
} satisfies Record<string, { keys: string[]; join?: string; where?: string; select?: string; returning?: string }>
type Table = keyof typeof definitions
const pending = new WeakMap<SqlQueryable, Map<Table, Promise<void>>>()

/** One outstanding batch per channel. Non-abortable native transports cannot block scheduling or pile up promises. */
export async function flushOutbox(database: SqlQueryable, table: Table, deliver: (row: Record<string, unknown>) => Promise<void>) {
  let commands = pending.get(database)
  if (!commands) { commands = new Map(); pending.set(database, commands) }
  if (commands.has(table)) return
  const operation = deliverBatch(database, table, deliver).catch(() => {
    createLogger().warn('outbox acknowledgement failed; durable rows retain the delivery intent', { table })
  }).finally(() => commands.delete(table))
  commands.set(table, operation)
  let timer: ReturnType<typeof setTimeout> | undefined
  try { await Promise.race([operation, new Promise<void>(resolve => { timer = setTimeout(resolve, 1000); timer.unref() })]) }
  finally { if (timer) clearTimeout(timer) }
}

async function deliverBatch(database: SqlQueryable, table: Table, deliver: (row: Record<string, unknown>) => Promise<void>) {
  const spec: { keys: string[]; join?: string; where?: string; select?: string; returning?: string } = definitions[table]
  if (!spec) throw new Error('unsupported outbox')
  const token = randomUUID()
  // Identifiers are from this package-owned table, never input from a model or resource.
  const { rows } = await database.query(`WITH candidate AS (
    SELECT ${spec.keys.map(key => `outbox.${key}`).join(',')}${spec.select ?? ''} FROM lingxios.${table} outbox ${spec.join ?? ''}
    WHERE outbox.delivered_at IS NULL AND outbox.available_at<=NOW() ${spec.where ?? ''}
    ORDER BY outbox.available_at,${spec.keys.map(key => `outbox.${key}`).join(',')} LIMIT ${table === 'agent_run_events' ? 64 : 8} FOR UPDATE OF outbox SKIP LOCKED)
    UPDATE lingxios.${table} outbox SET claim_token=$1,available_at=NOW()+INTERVAL '60 seconds',attempts=LEAST(attempts+1,30)
    FROM candidate WHERE ${spec.keys.map(key => `outbox.${key}=candidate.${key}`).join(' AND ')}
    RETURNING outbox.*${spec.returning ?? ''}`, [token])
  if (table === 'agent_run_events') rows.sort((a,b) => String(a['run_id']).localeCompare(String(b['run_id'])) || Number(a['seq'])-Number(b['seq']))
  for (const row of rows) {
    const params = [...spec.keys.map(key => row[key]), token]
    const where = `${spec.keys.map((key, index) => `${key}=$${index + 1}`).join(' AND ')} AND claim_token=$${params.length}`
    try {
      await deliver(row)
      await database.query(`UPDATE lingxios.${table} SET delivered_at=NOW(),claim_token=NULL WHERE ${where}`, params)
    } catch {
      await database.query(`UPDATE lingxios.${table} SET claim_token=NULL,
        available_at=NOW()+LEAST(300,5*power(2,LEAST(attempts-1,6)))*INTERVAL '1 second' WHERE ${where}`, params)
      if (table === 'agent_run_events') break // Preserve stream order until the failed batch can be retried.
    }
  }
}
