import { randomUUID } from 'node:crypto'
import { withTransaction, workItemFromRow, type SqlPool } from '../control-plane/pg-store.js'
import { deadlinePool } from '../control-plane/deadline-pool.js'
import { authorizedScopes, identityOf, sameScope, type MemoryOptions } from './access.js'
import { captureMemoryEvidence, prepareMemoryEvidence } from './evidence.js'
import type { AssistantMessage } from '../protocol/types.js'

/** The queue contains only committed result references. Privacy preparation never holds a transaction. */
export async function drainMemoryCapture(pool: SqlPool, options: MemoryOptions, external?: AbortSignal) {
  for (let count = 0; count < 16 && !external?.aborted; count++) {
    const token = randomUUID()
    const claim = (await pool.query(`WITH ready AS (
      SELECT result_id FROM lingxios.agent_memory_capture WHERE completed_at IS NULL AND attempts<5
        AND available_at<=NOW() AND (claim_until IS NULL OR claim_until<=NOW())
      ORDER BY available_at,result_id LIMIT 1 FOR UPDATE SKIP LOCKED)
      UPDATE lingxios.agent_memory_capture q SET claim_token=$1,claim_until=NOW()+INTERVAL '60 seconds',attempts=attempts+1
      FROM ready WHERE q.result_id=ready.result_id RETURNING q.result_id`, [token])).rows[0]
    if (!claim) return
    const resultId = String(claim['result_id'])
    const signal = AbortSignal.any([AbortSignal.timeout(30_000), ...external ? [external] : []])
    const db = deadlinePool(pool, signal)
    try {
      const source = (await db.query(`SELECT w.*,r.message FROM lingxios.agent_work_items w
        JOIN lingxios.agent_results r ON r.id=w.result_id WHERE r.id=$1
          AND w.status IN ('succeeded','partial') AND w.cancel_requested_at IS NULL
          AND r.request_version=jsonb_array_length(w.steer_inputs)+1`, [resultId])).rows[0]
      if (source) {
        const work = workItemFromRow(source, '', 1), message = source['message'] as AssistantMessage
        const scopes = await authorizedScopes(options, identityOf(work), db, signal)
        const prepared = await prepareMemoryEvidence(db, work, message, scopes, options.writePolicy, signal)
        if (prepared) {
          const current = await authorizedScopes(options, identityOf(work), db, signal)
          if (scopes.some(scope => !current.some(item => sameScope(scope, item)))) throw new Error('memory authorization changed')
          await withTransaction(db, async client => {
            // All extension callbacks have finished. Fence the source and queue claim, then filter forgetting epochs.
            const valid = await client.query(`SELECT w.id FROM lingxios.agent_work_items w
              JOIN lingxios.agent_memory_capture q ON q.result_id=w.result_id
              WHERE q.result_id=$1 AND q.claim_token=$2 AND q.claim_until>NOW()
                AND w.status IN ('succeeded','partial') AND w.cancel_requested_at IS NULL
                AND jsonb_array_length(w.steer_inputs)+1=$3 FOR UPDATE OF w,q`, [resultId, token, message.envelope.requestVersion])
            if (!valid.rows.length) return
            await captureMemoryEvidence(client, work, message, scopes, undefined, prepared)
            await client.query('UPDATE lingxios.agent_memory_capture SET completed_at=NOW(),claim_token=NULL,claim_until=NULL WHERE result_id=$1 AND claim_token=$2', [resultId, token])
          })
        }
      }
      await db.query('UPDATE lingxios.agent_memory_capture SET completed_at=NOW(),claim_token=NULL,claim_until=NULL WHERE result_id=$1 AND claim_token=$2', [resultId, token])
    } catch (error) {
      // No raw text or policy error is written to the queue. Exhaustion remains visible and operator-retryable.
      await pool.query(`UPDATE lingxios.agent_memory_capture SET claim_token=NULL,claim_until=NULL,
        available_at=NOW()+INTERVAL '5 seconds'*power(2,attempts) WHERE result_id=$1 AND claim_token=$2`, [resultId, token])
      if (external?.aborted) return
      throw error
    }
  }
}
