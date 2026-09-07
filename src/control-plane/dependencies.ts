import type { SqlQueryable } from './pg-store.js'

/** Complements child completion: catches the race where the parent parks after its child finishes. */
export async function resumeDependents(database: SqlQueryable) {
  return database.query(`WITH ready AS (
    SELECT parent.id FROM lingxios.agent_work_items parent JOIN lingxios.agent_work_items child
      ON child.id=parent.goal_outcome->>'taskRef' AND child.meta->>'parentWorkId'=parent.id
      AND child.tenant_id=parent.tenant_id AND child.principal_id IS NOT DISTINCT FROM parent.principal_id
      AND child.meta->'parentRequestVersion'=parent.goal_outcome->'requestVersion'
    WHERE parent.status='waiting' AND parent.goal_outcome->>'status'='delegated' AND parent.cancel_requested_at IS NULL
      AND child.status IN ('succeeded','partial','blocked','failed','cancelled')
    ORDER BY parent.created_at LIMIT 128 FOR UPDATE OF parent SKIP LOCKED
  ) UPDATE lingxios.agent_work_items parent SET status='queued',available_at=NOW(),finished_at=NULL,
    goal_outcome=NULL,updated_at=NOW() FROM ready WHERE parent.id=ready.id`)
}
