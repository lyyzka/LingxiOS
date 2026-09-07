import type { SqlPool } from '../../control-plane/pg-store.js'

const laneRank = `(CASE lane WHEN 'interactive' THEN 4 WHEN 'approval' THEN 3 WHEN 'collaboration' THEN 2 ELSE 1 END)`

export async function sweepLingxiLoopWatchdog(database: SqlPool, now = new Date(), watchdogMs = 120_000, graceMs = 30_000) {
  watchdogMs = Math.max(1_000, watchdogMs)
  graceMs = Math.max(1_000, graceMs)
  const tripped = await database.query(`UPDATE lingxios.agent_work_items active SET preempt_requested_at=$1,updated_at=NOW()
    WHERE active.status='leased' AND active.preempt_requested_at IS NULL AND EXISTS(
      SELECT 1 FROM lingxios.agent_work_items waiting WHERE waiting.status='queued' AND waiting.cancel_requested_at IS NULL
        AND waiting.available_at<=$1 AND waiting.tenant_id=active.tenant_id AND waiting.agent_id=active.agent_id
        AND waiting.session_id=active.session_id AND waiting.thread_id IS NOT DISTINCT FROM active.thread_id
        AND ${laneRank.replaceAll('lane', 'waiting.lane')} > ${laneRank.replaceAll('lane', 'active.lane')}
        AND GREATEST(waiting.created_at,waiting.available_at)<=$1::timestamptz-($2::text||' milliseconds')::interval
    ) RETURNING active.id`, [now, watchdogMs])
  const client = await database.connect()
  let fenced = 0
  try {
    await client.query('BEGIN')
    const forced = await client.query(`UPDATE lingxios.agent_work_items SET status='queued',fence=fence+1,
      lease_token_hash=NULL,leased_by=NULL,lease_expires_at=NULL,preempt_requested_at=NULL,preemptions=preemptions+1,
      available_at=NOW()+INTERVAL '1 second',updated_at=NOW()
      WHERE status='leased' AND preempt_requested_at IS NOT NULL
        AND preempt_requested_at<=$1::timestamptz-($2::text||' milliseconds')::interval RETURNING id`, [now, graceMs])
    fenced = forced.rows.length
    if (fenced) await client.query('DELETE FROM lingxios.agent_os_session_leases WHERE work_id=ANY($1::text[])', [forced.rows.map(row => row['id'])])
    await client.query('COMMIT')
  } catch (error) {
    await client.query('ROLLBACK').catch(() => undefined)
    throw error
  } finally { client.release() }
  return { tripped: tripped.rows.length, fenced }
}
