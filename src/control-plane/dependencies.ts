import type { SqlQueryable } from './pg-store.js'

/** Complements child completion: catches the race where the parent parks after its child finishes. */
export async function resumeDependents(database: SqlQueryable) {
  await database.query(`WITH RECURSIVE blocked AS (
    SELECT dependency.work_id FROM lingxios.agent_work_dependencies dependency
      JOIN lingxios.agent_work_items prerequisite ON prerequisite.id=dependency.dependency_id
      LEFT JOIN lingxios.agent_results result ON result.id=prerequisite.result_id
      WHERE prerequisite.status IN ('partial','blocked','failed','cancelled') OR prerequisite.cancel_requested_at IS NOT NULL
        OR (prerequisite.status='succeeded' AND (result.id IS NULL
          OR result.request_version<>jsonb_array_length(prerequisite.steer_inputs)+1
          OR result.message->'envelope'->'goalOutcome'->>'status' IS DISTINCT FROM 'satisfied'))
    UNION SELECT dependency.work_id FROM lingxios.agent_work_dependencies dependency JOIN blocked ON blocked.work_id=dependency.dependency_id
  ) UPDATE lingxios.agent_work_items work SET status='blocked',finished_at=NOW(),updated_at=NOW(),
    error='A required dependency did not produce a current satisfied result',
    goal_outcome=jsonb_build_object('status','blocked','verification','not_run','requestVersion',jsonb_array_length(work.steer_inputs)+1,
      'gaps',jsonb_build_array('A required dependency did not produce a current satisfied result'))
    WHERE work.status='queued' AND work.id IN(SELECT work_id FROM blocked)`)
  return database.query(`WITH ready AS (
    SELECT parent.id FROM lingxios.agent_work_items parent JOIN lingxios.agent_work_items child
      ON child.id=parent.goal_outcome->>'taskRef' AND child.meta->>'parentWorkId'=parent.id
      AND child.tenant_id=parent.tenant_id AND child.principal_id IS NOT DISTINCT FROM parent.principal_id
      AND child.meta->'parentRequestVersion'=parent.goal_outcome->'requestVersion'
    LEFT JOIN lingxios.agent_work_waits wait ON wait.parent_work_id=parent.id
      AND wait.request_version=(parent.goal_outcome->>'requestVersion')::integer AND wait.task_ref=child.id
    WHERE parent.status='waiting' AND parent.goal_outcome->>'status'='delegated' AND parent.cancel_requested_at IS NULL
      AND child.status IN ('succeeded','partial','blocked','failed','cancelled')
      AND NOT EXISTS (SELECT 1 FROM lingxios.agent_work_items sibling WHERE sibling.meta->>'parentWorkId'=parent.id
        AND sibling.tenant_id=parent.tenant_id AND sibling.principal_id IS NOT DISTINCT FROM parent.principal_id
        AND sibling.meta->'parentRequestVersion'=parent.goal_outcome->'requestVersion'
        AND (wait.children IS NULL OR wait.children ? sibling.id)
        AND sibling.status IN ('queued','leased','waiting'))
    ORDER BY parent.created_at LIMIT 128 FOR UPDATE OF parent SKIP LOCKED
  ) UPDATE lingxios.agent_work_items parent SET status='queued',available_at=NOW(),finished_at=NULL,
    goal_outcome=NULL,updated_at=NOW() FROM ready WHERE parent.id=ready.id`)
}
