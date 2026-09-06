import { randomUUID } from 'node:crypto'
import type { SqlQueryable } from '../../control-plane/pg-store.js'
import { createLogger } from '../../logging.js'

type Table = 'agent_calendar_outbox' | 'agent_canvas_outbox' | 'agent_document_outbox'
const pending = new WeakMap<SqlQueryable, Map<Table, Promise<void>>>()

/** Native Redis publishing is not abortable. Bound outstanding commands while durable rows own retries. */
export async function flushEventOutbox(database: SqlQueryable, table: Table, publish: (event: unknown) => Promise<void>) {
  let commands = pending.get(database)
  if (!commands) { commands = new Map(); pending.set(database, commands) }
  if (commands.has(table)) return
  const operation = publishNext(database, table, publish).catch(() => {
    createLogger().warn('native notification acknowledgement failed; the durable outbox will retry', { table })
  }).finally(() => commands.delete(table))
  commands.set(table, operation)
  let timer: ReturnType<typeof setTimeout> | undefined
  try { await Promise.race([operation, new Promise<void>(resolve => { timer = setTimeout(resolve, 1000); timer.unref() })]) }
  finally { if (timer) clearTimeout(timer) }
}

async function publishNext(database: SqlQueryable, table: Table, publish: (event: unknown) => Promise<void>) {
  const token = randomUUID()
  // table is a package-owned union, never a resource or model-supplied identifier.
  const { rows } = await database.query(`WITH candidate AS (
    SELECT id FROM lingxios.${table} WHERE delivered_at IS NULL AND available_at<=NOW()
    ORDER BY available_at,id LIMIT 1 FOR UPDATE SKIP LOCKED)
    UPDATE lingxios.${table} outbox SET claim_token=$1,available_at=NOW()+INTERVAL '60 seconds',attempts=LEAST(attempts+1,30)
    FROM candidate WHERE outbox.id=candidate.id RETURNING outbox.id,outbox.event`, [token])
  const row = rows[0]
  if (!row) return
  try {
    await publish(row['event'])
    await database.query(`UPDATE lingxios.${table} SET delivered_at=NOW(),claim_token=NULL WHERE id=$1 AND claim_token=$2`, [row['id'], token])
  } catch {
    await database.query(`UPDATE lingxios.${table} SET claim_token=NULL,
      available_at=NOW()+LEAST(300,5*power(2,LEAST(attempts-1,6)))*INTERVAL '1 second'
      WHERE id=$1 AND claim_token=$2`, [row['id'], token])
  }
}
