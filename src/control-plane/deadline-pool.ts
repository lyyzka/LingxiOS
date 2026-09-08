import { abortable } from '../deadline.js'
import type { SqlClient, SqlPool } from './pg-store.js'
import { LATENCY_BUCKETS, type MetricsRegistry } from '../metrics.js'

/** Package SQL only; SELECT ... FOR UPDATE is a read, including its OF/SKIP LOCKED clauses. */
export function sqlOperation(sql: string): 'write' | 'read' | 'transaction' {
  return /\b(?:INSERT\s+INTO|DELETE\s+FROM|UPDATE\s+[\w."]+(?:\s+(?:AS\s+)?(?!SET\b)\w+)?\s+SET)\b/i.test(sql)
    ? 'write' : /^\s*(SELECT|WITH)\b/i.test(sql) ? 'read' : 'transaction'
}

/** Destroy timed-out connections: an unfinished query/transaction must never return to the pool. */
export function deadlinePool(pool: SqlPool, signal?: AbortSignal, timeoutMs = 30_000, metrics?: MetricsRegistry): SqlPool {
  return {
    async connect() {
      const bounded = AbortSignal.any([AbortSignal.timeout(timeoutMs),...signal ? [signal] : []])
      bounded.throwIfAborted()
      const pending = pool.connect()
      let client: SqlClient
      try { client = await abortable(pending,bounded) }
      catch (error) {
        void pending.then(late => late.release(error instanceof Error ? error : new Error('connection acquisition cancelled')), () => {})
        throw error
      }
      let released = false
      const release = (error?: Error) => {
        if (released) return
        released = true
        bounded.removeEventListener('abort',cancel)
        client.release(error)
      }
      const cancel = () => release(bounded.reason instanceof Error ? bounded.reason : new Error('database deadline exceeded'))
      bounded.addEventListener('abort',cancel,{ once: true })
      if (bounded.aborted) cancel()
      return { async query(sql, params) {
        bounded.throwIfAborted()
        if (released) throw new Error('database connection is closed')
        const began = performance.now()
        const operation = sqlOperation(sql)
        metrics?.counter('agentos_database_queries_total', 'Database statements sent').inc({ operation })
        metrics?.counter('agentos_database_parameter_bytes_total', 'String and binary SQL parameter bytes').inc({ operation },
          (params ?? []).reduce<number>((sum, value) => sum + (typeof value === 'string' ? Buffer.byteLength(value) : value instanceof Uint8Array ? value.byteLength : 0), 0))
        try { return await abortable(client.query(sql,params),bounded) }
        finally { metrics?.histogram('agentos_database_query_seconds', 'Database query latency', LATENCY_BUCKETS)
          .observe((performance.now() - began) / 1000, { operation }) }
      }, release }
    },
    async query(sql, params) {
      const client = await this.connect()
      try { return await client.query(sql,params) }
      finally { client.release() }
    },
  }
}
