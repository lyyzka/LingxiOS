import { abortable } from '../deadline.js'
import type { SqlClient, SqlPool } from './pg-store.js'

/** Destroy timed-out connections: an unfinished query/transaction must never return to the pool. */
export function deadlinePool(pool: SqlPool, signal?: AbortSignal, timeoutMs = 30_000): SqlPool {
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
        return abortable(client.query(sql,params),bounded)
      }, release }
    },
    async query(sql, params) {
      const client = await this.connect()
      try { return await client.query(sql,params) }
      finally { client.release() }
    },
  }
}
