import { createHash } from 'node:crypto'
import type { SqlQueryable } from '../../control-plane/pg-store.js'
import type { WorkItem } from '../../protocol/types.js'
import { queueCanvasWorkspace } from './canvas-events.js'

export async function assertCanvasSummary(database: SqlQueryable, work: Omit<WorkItem, 'leaseToken'>) {
  const { rows } = await database.query(`SELECT c.id FROM canvases c JOIN lingxios.agent_work_items w ON w.id=$1
    WHERE c.id=$2 AND c.company_id=$3 AND c.conversation_id=$4 AND c.status='summarizing'
      AND w.kind='canvas_summary' AND w.meta->>'executionRole'='reporter' AND w.agent_id=$5 AND w.principal_id=$6
      AND w.meta->>'canvasId'=c.id AND NOT EXISTS(SELECT 1 FROM canvas_agent_assignments a
        WHERE a.canvas_id=c.id AND a.status NOT IN ('completed','failed','cancelled'))`,
  [work.id, work.meta?.['canvasId'], work.tenantId, work.sessionId, work.agentId, work.principalId])
  if (rows.length !== 1) throw new Error('Canvas reporter work is no longer runnable')
}

/** Called under the current Canvas row lock. Summary tasks retain the originating human request. */
export async function reconcileCanvasSummary(database: SqlQueryable, canvasId: string) {
  const canvas = (await database.query('SELECT * FROM canvases WHERE id=$1', [canvasId])).rows[0]!
  if (canvas['status'] === 'summarizing') {
    const summary = (await database.query(`SELECT w.id,w.status,w.goal_outcome,w.result_text,w.error,
      EXISTS(SELECT 1 FROM canvas_assignment_reports r JOIN evidence_records e ON e.id=r.evidence_id
        WHERE r.canvas_id=$1 AND r.company_id=w.tenant_id AND r.execution_role='reporter' AND r.author_agent_id=w.agent_id
          AND e.data->>'workId'=w.id AND e.data->>'requestVersion'=(jsonb_array_length(w.steer_inputs)+1)::text) AS report_ready
      FROM lingxios.agent_work_items w WHERE w.kind='canvas_summary' AND w.tenant_id=$2 AND w.meta->>'canvasId'=$1
      ORDER BY w.created_at DESC LIMIT 1 FOR UPDATE OF w`, [canvasId, canvas['company_id']])).rows[0]
    if (!summary || !['succeeded', 'partial', 'blocked', 'failed', 'cancelled'].includes(String(summary['status']))) return
    const outcome = summary['goal_outcome'] as { status?: string } | null
    if (outcome?.status === 'awaiting_input' || outcome?.status === 'awaiting_approval') return
    const status = ['succeeded','partial','blocked'].includes(String(summary['status'])) && summary['report_ready'] ? 'completed' : summary['status'] === 'cancelled' ? 'stopped' : 'failed'
    await database.query('UPDATE canvases SET status=$2,summary=$3,completed_at=NOW(),updated_at=NOW() WHERE id=$1',
      [canvasId, status, summary['result_text'] ?? summary['error'] ?? 'Canvas reporter ended without a persisted report'])
    await queueCanvasWorkspace(database, canvasId)
    return
  }
  if (canvas['status'] !== 'active') return
  const unfinished = await database.query("SELECT 1 FROM canvas_agent_assignments WHERE canvas_id=$1 AND status NOT IN ('completed','failed','cancelled') LIMIT 1", [canvasId])
  if (unfinished.rows.length) return
  const origin = (await database.query(`SELECT parent.* FROM canvas_agent_assignments a
    JOIN lingxios.agent_work_items child ON child.id=a.work_id AND child.kind='canvas_worker' AND child.meta->>'assignmentId'=a.id
    JOIN lingxios.agent_work_items parent ON parent.id=child.meta->>'parentWorkId'
      AND parent.tenant_id=child.tenant_id AND parent.principal_id=child.principal_id AND parent.session_id=child.session_id
    WHERE a.canvas_id=$1 AND child.tenant_id=$2 ORDER BY child.created_at,child.id LIMIT 1`, [canvasId, canvas['company_id']])).rows[0]
  if (!origin || typeof (origin['meta'] as Record<string, unknown> | null)?.['text'] !== 'string') return
  const id = 'canvas-summary-' + createHash('sha256').update(canvasId).digest('hex').slice(0, 24)
  const meta = origin['meta'] as Record<string, unknown>
  await database.query(`INSERT INTO lingxios.agent_work_items(id,tenant_id,agent_id,session_id,principal_id,thread_id,kind,lane,trigger_ref,priority,meta,steer_inputs)
    VALUES($1,$2,$3,$4,$5,$6,'canvas_summary','collaboration',$7,200,$8::jsonb,$9::jsonb)`,
  [id, origin['tenant_id'], origin['agent_id'], origin['session_id'], origin['principal_id'], origin['thread_id'], origin['trigger_ref'],
    JSON.stringify({ text: meta['text'], authorName: meta['authorName'], attachments: meta['attachments'], canvasId, executionRole: 'reporter', parentWorkId: origin['id'], rootWorkId: meta['rootWorkId'] ?? origin['id'], parentRequestVersion: (origin['steer_inputs'] as unknown[]).length + 1 }), JSON.stringify(origin['steer_inputs'])])
  await database.query("UPDATE canvases SET status='summarizing',updated_at=NOW() WHERE id=$1", [canvasId])
  await queueCanvasWorkspace(database, canvasId)
}
