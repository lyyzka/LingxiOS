import type { SqlQueryable } from '../../control-plane/pg-store.js'
import { flushOutbox } from '../../control-plane/outbox.js'

export function flushEventOutbox(database: SqlQueryable,
  table: 'agent_calendar_outbox' | 'agent_canvas_outbox' | 'agent_document_outbox', publish: (event: unknown) => Promise<void>) {
  return flushOutbox(database, table, row => publish(row['event']))
}
