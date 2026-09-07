import { createHash } from 'node:crypto'
import { withTransaction, type SqlPool, type SqlQueryable } from '../../control-plane/pg-store.js'

export async function cancelRoutineRuns(database: SqlQueryable, id: string) {
  await database.query(`UPDATE lingxios.agent_work_items work SET cancel_requested_at=COALESCE(cancel_requested_at,NOW()),
    status=CASE WHEN status IN ('queued','waiting') THEN 'cancelled' ELSE status END,updated_at=NOW()
    FROM lingxios.agent_routine_runs run WHERE run.routine_id=$1 AND run.work_id=work.id
      AND (work.status IN ('queued','leased','waiting') OR EXISTS(SELECT 1 FROM lingxios.agent_delivery_outbox outbox WHERE outbox.run_id=work.id AND outbox.delivered_at IS NULL))`, [id])
}

/** Shared scheduling transaction; each domain still authorizes its live scope and validates its own schedule. */
export function scheduleRoutineKind(database: SqlPool, kind: 'routine' | 'teacher_digest',
  prepare: (client: SqlQueryable, row: Record<string, unknown>) => Promise<{ next: string; text: string; authorName: string } | { pauseReason: string }>) {
  return withTransaction(database, async client => {
    await client.query("SET LOCAL lock_timeout='2s'")
    await client.query("SET LOCAL statement_timeout='5s'")
    const { rows } = await client.query(`SELECT routine.*,NOW() AS clock,
      EXISTS(SELECT 1 FROM lingxios.agent_routine_runs run JOIN lingxios.agent_work_items work ON work.id=run.work_id
        WHERE run.routine_id=routine.id AND work.cancel_requested_at IS NULL AND (work.status IN ('queued','leased','waiting')
          OR EXISTS(SELECT 1 FROM lingxios.agent_delivery_outbox outbox WHERE outbox.run_id=work.id AND outbox.delivered_at IS NULL))) AS pending
      FROM lingxios.agent_routines routine WHERE status='active' AND ${kind === 'teacher_digest' ? "kind='teacher_digest'" : "kind<>'teacher_digest'"} AND next_run_at<=NOW()
      ORDER BY next_run_at,id LIMIT 8 FOR UPDATE OF routine SKIP LOCKED`)
    let enqueued = 0
    for (const row of rows) {
      const decision = await prepare(client, row)
      if ('pauseReason' in decision) {
        await client.query("UPDATE lingxios.agent_routines SET status='paused',next_run_at=NULL,version=version+1,pause_reason=$2,updated_at=NOW() WHERE id=$1", [row['id'],decision.pauseReason])
        await cancelRoutineRuns(client, String(row['id'])); continue
      }
      const scheduledAt = new Date(String(row['next_run_at'])).toISOString()
      const existing = await client.query('SELECT work_id FROM lingxios.agent_routine_runs WHERE routine_id=$1 AND routine_version=$2 AND scheduled_at=$3', [row['id'],row['version'],scheduledAt])
      if (!row['pending'] && !existing.rows.length) {
        const id = (kind === 'teacher_digest' ? 'digest-run-' : 'routine-run-') + createHash('sha256').update(JSON.stringify([row['id'],row['version'],scheduledAt])).digest('hex')
        await client.query(`INSERT INTO lingxios.agent_work_items(id,tenant_id,agent_id,session_id,principal_id,thread_id,kind,lane,trigger_ref,priority,meta)
          VALUES($1,$2,$3,$4,$5,$6,$7,'background',$1,$8,$9::jsonb)`,
          [id,row['tenant_id'],row['agent_id'],row['session_id'],row['principal_id'],row['thread_id'],kind,kind === 'teacher_digest' ? -10 : 0,
          JSON.stringify({ text: decision.text, authorName: decision.authorName, routineId: row['id'],routineVersion: row['version'],scheduledAt })])
        await client.query('INSERT INTO lingxios.agent_routine_runs(routine_id,routine_version,scheduled_at,work_id) VALUES($1,$2,$3,$4)', [row['id'],row['version'],scheduledAt,id])
        enqueued++
      }
      await client.query('UPDATE lingxios.agent_routines SET next_run_at=$2,updated_at=NOW() WHERE id=$1', [row['id'],decision.next])
    }
    return enqueued
  })
}
