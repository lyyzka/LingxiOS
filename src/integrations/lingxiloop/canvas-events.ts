import { randomUUID } from 'node:crypto'
import type { SqlQueryable } from '../../control-plane/pg-store.js'
import type { LingxiLoopServices, NativeCanvasAssignmentRow, NativeCanvasChanged } from './service-contracts.js'
import { flushEventOutbox } from './event-outbox.js'

export async function queueCanvasAssignments(database: SqlQueryable, services: Pick<LingxiLoopServices, 'canvas'>,
  canvasId: unknown, previous: Record<string, unknown>[]) {
  const api = services.canvas!.orchestration!
  const canvas = (await database.query('SELECT id,company_id,project_id,conversation_id FROM canvases WHERE id=$1', [canvasId])).rows[0]!
  const current = (await database.query('SELECT * FROM canvas_agent_assignments WHERE canvas_id=$1', [canvasId])).rows
  const dependencies = (await database.query(`SELECT d.assignment_id,p.agent_id FROM canvas_assignment_dependencies d
    JOIN canvas_agent_assignments p ON p.id=d.depends_on_assignment_id WHERE p.canvas_id=$1`, [canvasId])).rows
  const state = (row: Record<string, unknown> | undefined) => row && JSON.stringify([row['status'], row['assignment'], row['result'], row['error'], row['work_id']])
  for (const row of current) {
    if (state(row) === state(previous.find(before => before['id'] === row['id']))) continue
    const event: NativeCanvasChanged = { type: 'canvas.changed', kind: 'assignment.updated', companyId: String(canvas['company_id']),
      canvasId: String(canvasId), workspaceId: String(canvas['project_id']), conversationId: String(canvas['conversation_id']), timestamp: new Date().toISOString(),
      assignment: api.toAssignment({ ...row, progress_fingerprint: null, no_progress_count: 0 } as unknown as NativeCanvasAssignmentRow,
        dependencies.filter(item => item['assignment_id'] === row['id']).map(item => String(item['agent_id']))) }
    await database.query('INSERT INTO lingxios.agent_canvas_outbox(id,event) VALUES($1,$2::jsonb)', [randomUUID(), JSON.stringify(event)])
  }
}

export async function queueCanvasReports(database: SqlQueryable, canvasId: string) {
  const canvas = (await database.query('SELECT company_id,project_id,conversation_id FROM canvases WHERE id=$1', [canvasId])).rows[0]!
  const reports = (await database.query('SELECT * FROM canvas_assignment_reports WHERE canvas_id=$1 ORDER BY created_at LIMIT 33', [canvasId])).rows
  if (reports.length > 32) throw new Error('Canvas report notification capacity exceeded')
  const event: NativeCanvasChanged = { type: 'canvas.changed', kind: 'workspace.updated', companyId: String(canvas['company_id']),
    canvasId, workspaceId: String(canvas['project_id']), conversationId: String(canvas['conversation_id']), timestamp: new Date().toISOString(),
    workspace: { reports: reports.map(row => ({ id: row['id'], canvasId, assignmentId: row['assignment_id'], authorAgentId: row['author_agent_id'],
      executionRole: row['execution_role'], schemaVersion: row['schema_version'], finding: row['finding'], evidenceId: row['evidence_id'],
      sourceEvidenceIds: row['source_evidence_ids'], confidence: Number(row['confidence']), unresolved: row['unresolved'], nextStep: row['next_step'],
      verifiesReportId: row['verifies_report_id'], disconfirmingChecks: row['disconfirming_checks'], verdict: row['verdict'],
      consumedReportIds: row['consumed_report_ids'], conflictResolution: row['conflict_resolution'], createdAt: row['created_at'] })) } }
  await database.query('INSERT INTO lingxios.agent_canvas_outbox(id,event) VALUES($1,$2::jsonb)', [randomUUID(), JSON.stringify(event)])
}

export async function flushCanvasEvents(database: SqlQueryable, services: Pick<LingxiLoopServices, 'canvas'>) {
  const api = services.canvas?.orchestration
  if (api) await flushEventOutbox(database, 'agent_canvas_outbox', event => api.publish(api.CH_CANVAS, event as NativeCanvasChanged))
}

export async function queueCanvasActivity(database: SqlQueryable, canvasId: string, activityId: string) {
  const row = (await database.query(`SELECT a.*,c.company_id,c.project_id,c.conversation_id FROM canvas_activity a
    JOIN canvases c ON c.id=a.canvas_id WHERE a.id=$1 AND a.canvas_id=$2`, [activityId, canvasId])).rows[0]!
  const event: NativeCanvasChanged = { type: 'canvas.changed', kind: 'activity.created', companyId: String(row['company_id']), canvasId,
    workspaceId: String(row['project_id']), conversationId: String(row['conversation_id']), timestamp: new Date().toISOString(),
    activity: { id: row['id'], canvasId, frameId: row['frame_id'], actorId: row['actor_id'], actorKind: row['actor_kind'], action: row['action'], detail: row['detail'], createdAt: row['created_at'] } }
  await database.query('INSERT INTO lingxios.agent_canvas_outbox(id,event) VALUES($1,$2::jsonb)', [randomUUID(), JSON.stringify(event)])
}

export async function queueCanvasWorkspace(database: SqlQueryable, canvasId: string) {
  const row = (await database.query('SELECT * FROM canvases WHERE id=$1', [canvasId])).rows[0]!
  const event: NativeCanvasChanged = { type: 'canvas.changed', kind: 'workspace.updated', companyId: String(row['company_id']), canvasId,
    workspaceId: String(row['project_id']), conversationId: String(row['conversation_id']), timestamp: new Date().toISOString(),
    workspace: { id: canvasId, title: row['title'], goal: row['goal'], status: row['status'], summary: row['summary'] } }
  await database.query('INSERT INTO lingxios.agent_canvas_outbox(id,event) VALUES($1,$2::jsonb)', [randomUUID(), JSON.stringify(event)])
}
