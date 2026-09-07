import { createHash } from 'node:crypto'
import { withTransaction, type SqlPool, type SqlQueryable } from '../../control-plane/pg-store.js'
import type { HostAction, HostActionResult, WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices } from './service-contracts.js'
import { submitCanvasWorkReport } from './canvas-work-report.js'
import { queueCanvasAssignments, queueCanvasReports, queueCanvasActivity, queueCanvasWorkspace } from './canvas-events.js'
import { assertCanvasSummary, reconcileCanvasSummary } from './canvas-summary.js'

export const CANVAS_WORK_METHODS = ['start_workspace', 'stop_workspace', 'assign', 'steer_assignment', 'stop_assignment', 'submit_report', 'handoff'] as const
type Services = Pick<LingxiLoopServices, 'canvas'>
type Member = { agentId: string; assignment: string; dependsOnAgentIds: string[]; executionRole: 'specialist' | 'verifier'; verifiesAgentId?: string }

function text(value: unknown, name: string, limit = 240): string {
  if (typeof value !== 'string' || !value.trim() || value.length > limit) throw new Error(`invalid ${name}`)
  return value.trim()
}

function membersOf(value: unknown): Member[] {
  if (!Array.isArray(value) || !value.length || value.length > 32) throw new Error('members requires 1 to 32 assignments')
  const members = value.map(item => {
    if (!item || typeof item !== 'object' || Array.isArray(item)
      || Object.keys(item).some(key => !['agentId', 'assignment', 'dependsOnAgentIds', 'executionRole', 'verifiesAgentId'].includes(key))) throw new Error('invalid Canvas member')
    const dependencies = item.dependsOnAgentIds ?? []
    if (!Array.isArray(dependencies) || dependencies.length > 32) throw new Error('invalid Canvas dependencies')
    const role = item.executionRole ?? 'specialist'
    if (role !== 'specialist' && role !== 'verifier') throw new Error('invalid Canvas execution role')
    return { agentId: text(item.agentId, 'agentId'), assignment: text(item.assignment, 'assignment', 4000),
      dependsOnAgentIds: [...new Set(dependencies.map(id => text(id, 'dependency')))], executionRole: role,
      ...(item.verifiesAgentId !== undefined ? { verifiesAgentId: text(item.verifiesAgentId, 'verifiesAgentId') } : {}) } as Member
  })
  if (new Set(members.map(item => item.agentId)).size !== members.length) throw new Error('Canvas members must be unique')
  for (const member of members) {
    if (member.executionRole === 'verifier') {
      if (!member.verifiesAgentId || member.verifiesAgentId === member.agentId) throw new Error('verifier requires a different builder')
      if (!member.dependsOnAgentIds.includes(member.verifiesAgentId)) member.dependsOnAgentIds.push(member.verifiesAgentId)
    } else if (member.verifiesAgentId) throw new Error('only verifiers may specify a builder')
  }
  return members
}

/** Domain assignments and durable execution belong to one transaction; no old work table is used. */
export async function executeCanvasWork(database: SqlPool, services: Services, work: Omit<WorkItem, 'leaseToken'>, action: HostAction): Promise<HostActionResult> {
  const api = services.canvas?.orchestration
  if (!api || !work.principalId || !CANVAS_WORK_METHODS.some(method => action.action === `canvas.${method}`)) throw new Error('Canvas orchestration is unavailable')
  const handoff = action.action === 'canvas.handoff'
  const start = action.action === 'canvas.start_workspace', stop = action.action === 'canvas.stop_workspace'
  const assign = action.action === 'canvas.assign' || handoff || start
  const report = action.action === 'canvas.submit_report'
  const allowed = start ? ['title', 'goal', 'members'] : stop ? [] : handoff ? ['toAgentId', 'task', 'context', 'frameIds'] : assign ? ['members'] : report ? ['finding', 'evidenceRefs', 'confidence', 'unresolved', 'nextStep', 'verifiesReportId', 'disconfirmingChecks', 'verdict', 'consumedReportIds', 'conflictResolution'] : action.action === 'canvas.steer_assignment' ? ['agentId', 'text'] : ['agentId']
  if (Object.keys(action.args).some(key => !allowed.includes(key))) throw new Error('unknown Canvas work argument')
  const members = assign ? membersOf(handoff ? [{ agentId: action.args['toAgentId'], assignment: action.args['task'] }] : action.args['members']) : []
  const targetId = handoff ? members[0]!.agentId : assign || report || stop ? undefined : text(action.args['agentId'], 'agentId')
  const handoffContext = handoff && action.args['context'] !== undefined ? text(action.args['context'], 'context', 8000) : ''
  const frameInput = handoff ? action.args['frameIds'] ?? [] : []
  if (!Array.isArray(frameInput) || frameInput.length > 32) throw new Error('invalid handoff frameIds')
  const frameIds = [...new Set(frameInput.map(id => text(id, 'frameId')))]
  const handoffInstruction = handoff ? JSON.stringify({ task: members[0]!.assignment, context: handoffContext, frameIds }) : undefined
  const steer = handoffInstruction ?? (action.action === 'canvas.steer_assignment' ? text(action.args['text'], 'text', 4000) : undefined)
  return withTransaction(database, async client => {
    await client.query("SET LOCAL lock_timeout='5s'")
    await client.query("SET LOCAL statement_timeout='10s'")
    const live = await client.query(`SELECT w.meta,w.steer_inputs FROM lingxios.agent_work_items w
      JOIN lingxios.agent_action_intents i ON i.idempotency_key=$3
      WHERE w.id=$1 AND w.fence=$2 AND w.status='leased' AND w.lease_expires_at>NOW()
        AND w.cancel_requested_at IS NULL AND w.preempt_requested_at IS NULL AND w.tenant_id=$4 AND w.principal_id=$5
        AND w.agent_id=$6 AND w.session_id=$7 AND i.intent->>'workId'=w.id
        AND i.intent->'action'->>'action'=$8 AND i.intent->'action'->'args'=$9::jsonb
        AND (i.intent->>'requestVersion')::integer=jsonb_array_length(w.steer_inputs)+1
      FOR UPDATE OF w`, [work.id, work.fence, action.idempotencyKey, work.tenantId, work.principalId,
      work.agentId, work.sessionId, action.action, JSON.stringify(action.args)])
    if (live.rows.length !== 1) throw new Error('Canvas action requires the current live intent')
    const permissions = api.createPermissionService(client, { lockDependencies: true })
    await permissions.assertCan({ actorUserId: work.principalId!, companyId: work.tenantId,
      action: 'conversation:read', resource: { type: 'conversation', id: work.sessionId } })
    let canvas = (await client.query(`SELECT c.id,c.project_id,c.status FROM canvases c
      WHERE c.company_id=$1 AND c.conversation_id=$2 FOR UPDATE`, [work.tenantId, work.sessionId])).rows[0]
    if (start) {
      if (canvas) throw new Error('conversation already has a Canvas; inspect it before assigning work')
      await permissions.assertCan({ actorUserId: work.principalId!, companyId: work.tenantId,
        action: 'conversation:write', resource: { type: 'conversation', id: work.sessionId } })
      const id = 'canvas-' + createHash('sha256').update(action.idempotencyKey).digest('hex').slice(0, 28)
      const created = await api.insertAgentWorkspace(client, { id, companyId: work.tenantId, conversationId: work.sessionId,
        initiatorAgentId: work.agentId, authorizationUserId: work.principalId!, triggerClientMsgNo: work.triggerRef,
        title: text(action.args['title'], 'title', 200), goal: text(action.args['goal'], 'goal', 16_000) })
      if (!created || created.id !== id) throw new Error('Canvas changed concurrently; read it before retrying')
      canvas = created
    }
    const summary = report && work.kind === 'canvas_summary'
    if (!canvas || (stop ? !['active', 'summarizing', 'stopped'].includes(String(canvas['status'])) : canvas['status'] !== (summary ? 'summarizing' : 'active')) || !canvas['project_id']) throw new Error('an active current Canvas with a project is required')
    // Native reports may be written while summarizing; frame-write state checks apply to active canvases.
    // Authorize the same project/member write policy on the locked conversation, then assert the reporter job below.
    await permissions.assertCan({ actorUserId: work.principalId!, companyId: work.tenantId,
      action: 'canvas:write', resource: { type: summary || stop ? 'conversation' : 'canvas', id: summary || stop ? work.sessionId : String(canvas['id']) } })
    const ids = [...new Set([work.agentId, ...members.map(item => item.agentId), ...(targetId ? [targetId] : [])])]
    const eligible = await client.query(`SELECT p.id FROM participants p
      JOIN im_channel_bindings b ON b.company_id=p.company_id AND b.channel_id=$3
      WHERE p.company_id=$1 AND p.id=ANY($2::text[]) AND p.kind='agent' AND p.departed_at IS NULL
        AND p.capabilities @> '["canvas"]'::jsonb AND b.profile->'members' ? p.id
        AND NOT EXISTS(SELECT 1 FROM learning_project_teacher_agents t WHERE t.company_id=p.company_id AND t.agent_id=p.id)
      FOR SHARE OF p,b`, [work.tenantId, ids, work.sessionId])
    if (eligible.rows.length !== ids.length) throw new Error('Canvas agents must be active members with the Canvas capability')
    const existing = (await client.query('SELECT * FROM canvas_agent_assignments WHERE canvas_id=$1 FOR UPDATE', [canvas['id']])).rows
    const sourceAssignment = handoff ? existing.find(row => row['agent_id'] === work.agentId && row['work_id'] === work.id) : undefined
    if (handoff) {
      if (targetId === work.agentId || !sourceAssignment || !['queued', 'working', 'waiting'].includes(String(sourceAssignment['status']))) throw new Error('handoff requires the current Canvas worker and a different target')
      const frames = await client.query('SELECT id FROM canvas_frames WHERE canvas_id=$1 AND id=ANY($2::text[]) FOR SHARE', [canvas['id'], frameIds])
      if (frames.rows.length !== frameIds.length) throw new Error('handoff frames must belong to the current Canvas')
    }
    let value: unknown
    if (stop) {
      await client.query("UPDATE canvases SET status='stopped',completed_at=COALESCE(completed_at,NOW()),updated_at=NOW() WHERE id=$1", [canvas['id']])
      await client.query(`UPDATE lingxios.agent_work_items SET cancel_requested_at=COALESCE(cancel_requested_at,NOW()),
        status=CASE WHEN status='queued' THEN 'cancelled' ELSE status END,updated_at=NOW()
        WHERE tenant_id=$1 AND meta->>'canvasId'=$2 AND kind IN ('canvas_worker','canvas_summary') AND status IN ('queued','leased')`, [work.tenantId, canvas['id']])
      await client.query("UPDATE canvas_agent_assignments SET status='cancelled',error='Workspace stopped by request',completed_at=NOW(),updated_at=NOW() WHERE canvas_id=$1 AND status NOT IN ('completed','failed','cancelled')", [canvas['id']])
      await queueCanvasWorkspace(client, String(canvas['id']))
      value = { canvasId: canvas['id'], status: 'stopped' }
    } else if (report) {
      if (summary) await assertCanvasSummary(client, work)
      else await assertCanvasWorker(client, work)
      value = await submitCanvasWorkReport(client, services, work, action.args, String(canvas['id']), String(canvas['project_id']),
        (live.rows[0]!['steer_inputs'] as unknown[]).length + 1, Number((live.rows[0]!['meta'] as Record<string, unknown>)['assignmentVersion'] ?? 1))
      await queueCanvasReports(client, String(canvas['id']))
    } else if (assign && !(handoff && existing.some(row => row['agent_id'] === targetId))) {
      if (existing.length + members.length > 32) throw new Error('Canvas assignment capacity is 32 agents')
      if (members.some(member => existing.some(row => row['agent_id'] === member.agentId))) throw new Error('agent is already assigned to this Canvas')
      api.assertCanvasDependencyDAG(members, new Set(existing.map(row => String(row['agent_id']))))
      const used = new Set(existing.map(row => String(row['color'])))
      const assignments = new Map(existing.map(row => [String(row['agent_id']), String(row['id'])]))
      for (const member of members) assignments.set(member.agentId, 'assignment-' + createHash('sha256').update(`${canvas['id']}:${member.agentId}`).digest('hex').slice(0, 28))
      const created: Array<{ agentId: string; assignmentId: string; taskRef: string; waitingForDependencies: boolean }> = []
      const meta = live.rows[0]!['meta'] as Record<string, unknown> | undefined
      if (typeof meta?.['text'] !== 'string') throw new Error('Canvas delegation requires the captured original request')
      for (const [offset, member] of members.entries()) {
        const assignmentId = assignments.get(member.agentId)!
        const taskRef = 'canvas-work-' + createHash('sha256').update(assignmentId).digest('hex').slice(0, 28)
        const area = api.canvasWorkArea(existing.length + offset), color = api.canvasAgentColor(member.agentId, used)
        used.add(color)
        const blocked = member.dependsOnAgentIds.length > 0
        await client.query(`INSERT INTO canvas_agent_assignments
          (id,canvas_id,agent_id,assignment,color,status,work_x,work_y,work_width,work_height,work_id,execution_role)
          VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, [assignmentId, canvas['id'], member.agentId,
          member.assignment, color, blocked ? 'blocked' : 'queued', area.x, area.y, area.width, area.height, taskRef, member.executionRole])
        await client.query(`INSERT INTO lingxios.agent_work_items
          (id,tenant_id,agent_id,session_id,thread_id,principal_id,kind,lane,trigger_ref,priority,available_at,meta,steer_inputs)
          VALUES($1,$2,$3,$4,$5,$6,'canvas_worker','collaboration',$7,180,CASE WHEN $8 THEN 'infinity'::timestamptz ELSE NOW() END,$9::jsonb,$10::jsonb)`,
        [taskRef, work.tenantId, member.agentId, work.sessionId, work.threadId ?? work.triggerRef, work.principalId, work.triggerRef, blocked,
          JSON.stringify({ text: meta['text'], authorName: meta['authorName'], attachments: meta['attachments'], canvasId: canvas['id'], assignmentId,
            assignment: member.assignment, assignmentVersion: 1, ...(handoffInstruction ? { assignmentSteers: [{ id: action.idempotencyKey, agentId: work.agentId, text: handoffInstruction }] } : {}),
            executionRole: member.executionRole, parentWorkId: work.id, rootWorkId: meta['rootWorkId'] ?? work.id,
            parentRequestVersion: (live.rows[0]!['steer_inputs'] as unknown[]).length + 1,
            parentActionKey: action.idempotencyKey }), JSON.stringify(live.rows[0]!['steer_inputs'])])
        created.push({ agentId: member.agentId, assignmentId, taskRef, waitingForDependencies: blocked })
      }
      for (const member of members) {
        if (member.verifiesAgentId) {
          const target = assignments.get(member.verifiesAgentId)
          if (!target) throw new Error('verifier target must belong to this Canvas')
          await client.query('UPDATE canvas_agent_assignments SET verifies_assignment_id=$2 WHERE id=$1', [assignments.get(member.agentId), target])
        }
        for (const dependency of member.dependsOnAgentIds) await client.query(`INSERT INTO canvas_assignment_dependencies(assignment_id,depends_on_assignment_id) VALUES($1,$2)`, [assignments.get(member.agentId), assignments.get(dependency)])
      }
      value = { canvasId: canvas['id'], assignments: created }
    } else {
      const assignment = existing.find(row => row['agent_id'] === targetId)
      if (!assignment || !['queued', 'blocked', 'working', 'waiting'].includes(String(assignment['status']))) throw new Error('active Canvas assignment not found')
      const child = await client.query(`SELECT id,meta FROM lingxios.agent_work_items WHERE id=$1 AND tenant_id=$2
        AND session_id=$3 AND principal_id=$4 AND kind='canvas_worker' AND meta->>'assignmentId'=$5
        AND status IN ('queued','leased') FOR UPDATE`, [assignment['work_id'], work.tenantId, work.sessionId, work.principalId, assignment['id']])
      if (child.rows.length !== 1) throw new Error('current recoverable Canvas work was not found')
      if (steer) {
        const meta = child.rows[0]!['meta'] as Record<string, unknown>
        const instructions = meta['assignmentSteers'] ?? []
        if (!Array.isArray(instructions) || instructions.length >= 16) throw new Error('Canvas assignment instruction capacity reached')
        const nextInstructions = [...instructions, { id: action.idempotencyKey, agentId: work.agentId, text: steer }]
        if (Buffer.byteLength(JSON.stringify(nextInstructions)) > 64_000) throw new Error('Canvas collaborator instructions exceed 64000 bytes')
        await client.query(`UPDATE lingxios.agent_work_items SET meta=$2::jsonb,
          preempt_requested_at=CASE WHEN status='leased' THEN NOW() ELSE preempt_requested_at END,updated_at=NOW() WHERE id=$1`,
        [assignment['work_id'], JSON.stringify({ ...meta, assignmentVersion: Number(meta['assignmentVersion'] ?? 1) + 1,
          assignmentSteers: nextInstructions })])
        await client.query('UPDATE canvas_agent_assignments SET assignment=$2,updated_at=NOW() WHERE id=$1', [assignment['id'], steer])
      } else {
        await client.query(`UPDATE lingxios.agent_work_items SET cancel_requested_at=NOW(),
          status=CASE WHEN status='queued' THEN 'cancelled' ELSE status END,updated_at=NOW() WHERE id=$1`, [assignment['work_id']])
        await client.query("UPDATE canvas_agent_assignments SET status='cancelled',error='Stopped by request',completed_at=NOW(),updated_at=NOW() WHERE id=$1", [assignment['id']])
      }
      value = { assignmentId: assignment['id'], taskRef: assignment['work_id'], status: steer ? 'steered' : 'cancel_requested' }
    }
    if (handoff) {
      const activityId = 'activity-' + createHash('sha256').update(action.idempotencyKey).digest('hex').slice(0, 32)
      const detail = { fromAgentId: work.agentId, toAgentId: targetId, sourceAssignmentId: sourceAssignment!['id'], task: members[0]!.assignment, context: handoffContext, frameIds }
      await client.query(`INSERT INTO canvas_activity(id,canvas_id,frame_id,actor_id,actor_kind,action,detail)
        VALUES($1,$2,$3,$4,'agent','handoff',$5::jsonb)`, [activityId, canvas['id'], sourceAssignment!['active_frame_id'], work.agentId, JSON.stringify(detail)])
      await queueCanvasActivity(client, String(canvas['id']), activityId)
      value = { ...value as Record<string, unknown>, activityId }
    }
    await queueCanvasAssignments(client, services, canvas['id'], existing)
    if (start) await queueCanvasWorkspace(client, String(canvas['id']))
    const result: HostActionResult = { ok: true, value }
    const recorded = await client.query('INSERT INTO lingxios.agent_action_ledger(idempotency_key,result) VALUES($1,$2::jsonb) ON CONFLICT DO NOTHING RETURNING idempotency_key', [action.idempotencyKey, JSON.stringify(result)])
    if (recorded.rows.length !== 1) throw new Error('Canvas action receipt already exists')
    return result
  })
}

export async function assertCanvasWorker(database: SqlQueryable, work: Omit<WorkItem, 'leaseToken'>) {
  const { rows } = await database.query(`SELECT a.assignment,a.execution_role FROM canvas_agent_assignments a
    JOIN canvases c ON c.id=a.canvas_id JOIN lingxios.agent_work_items w ON w.id=a.work_id
    WHERE a.id=$1 AND a.agent_id=$2 AND c.id=$3 AND c.company_id=$4 AND c.conversation_id=$5
      AND c.status='active' AND w.id=$6 AND w.principal_id=$7 AND a.status IN ('queued','working','waiting')
      AND NOT EXISTS(SELECT 1 FROM canvas_assignment_dependencies d JOIN canvas_agent_assignments p ON p.id=d.depends_on_assignment_id
        WHERE d.assignment_id=a.id AND p.status<>'completed')`,
  [work.meta?.['assignmentId'], work.agentId, work.meta?.['canvasId'], work.tenantId, work.sessionId, work.id, work.principalId])
  if (rows.length !== 1) throw new Error('Canvas assignment is no longer runnable')
  return rows[0]!
}

/** Restart-safe projection of package work into domain assignment state, followed by dependency release. */
export async function reconcileCanvasWork(database: SqlPool, services: Services) {
  return withTransaction(database, async client => {
    await client.query("SET LOCAL lock_timeout='5s'")
    await client.query("SET LOCAL statement_timeout='10s'")
    const canvases = await client.query(`SELECT c.id FROM canvases c WHERE EXISTS(
      SELECT 1 FROM canvas_agent_assignments a JOIN lingxios.agent_work_items w ON w.id=a.work_id
      WHERE a.canvas_id=c.id AND w.kind='canvas_worker' AND w.tenant_id=c.company_id
        AND w.meta->>'assignmentId'=a.id AND a.status IN ('queued','blocked','working','waiting'))
      OR (c.status='summarizing' AND EXISTS(SELECT 1 FROM lingxios.agent_work_items w WHERE w.kind='canvas_summary' AND w.tenant_id=c.company_id AND w.meta->>'canvasId'=c.id))
      ORDER BY c.updated_at LIMIT 32 FOR UPDATE OF c SKIP LOCKED`)
    for (const canvas of canvases.rows) {
      const id = canvas['id']
      const previous = (await client.query('SELECT * FROM canvas_agent_assignments WHERE canvas_id=$1', [id])).rows
      await client.query(`UPDATE lingxios.agent_work_items w SET cancel_requested_at=COALESCE(w.cancel_requested_at,NOW()),
        status=CASE WHEN w.status='queued' THEN 'cancelled' ELSE w.status END,updated_at=NOW()
        FROM canvas_agent_assignments a,canvases c WHERE a.canvas_id=$1 AND c.id=a.canvas_id
          AND w.id=a.work_id AND w.kind='canvas_worker' AND w.tenant_id=c.company_id AND w.meta->>'assignmentId'=a.id
          AND w.status IN ('queued','leased') AND (c.status<>'active' OR a.status='cancelled')`, [id])
      await client.query(`UPDATE canvas_agent_assignments a SET
        status=CASE WHEN w.status='cancelled' OR w.cancel_requested_at IS NOT NULL THEN 'cancelled'
          WHEN w.status='failed' THEN 'failed'
          WHEN w.status='waiting' THEN 'waiting'
          WHEN w.status IN ('succeeded','partial','blocked') AND r.id IS NOT NULL THEN 'completed'
          WHEN w.status IN ('succeeded','partial','blocked') THEN 'failed'
          WHEN w.status='leased' THEN 'working' ELSE a.status END,
        result=CASE WHEN w.status IN ('succeeded','partial','blocked') THEN w.result_text ELSE a.result END,
        error=CASE WHEN w.status='failed' THEN w.error
          WHEN w.status IN ('succeeded','partial','blocked') AND COALESCE(w.goal_outcome->>'status','') NOT IN ('awaiting_input','awaiting_approval') AND r.id IS NULL
          THEN 'Canvas worker ended without a persisted assignment report' ELSE a.error END,
        completed_at=CASE WHEN w.status IN ('failed','cancelled') OR w.cancel_requested_at IS NOT NULL
          OR (w.status IN ('succeeded','partial','blocked') AND COALESCE(w.goal_outcome->>'status','') NOT IN ('awaiting_input','awaiting_approval'))
          THEN COALESCE(a.completed_at,NOW()) ELSE NULL END,updated_at=NOW()
        FROM lingxios.agent_work_items w LEFT JOIN canvas_assignment_reports r ON r.assignment_id=w.meta->>'assignmentId'
          AND r.author_agent_id=w.agent_id AND r.company_id=w.tenant_id AND EXISTS (
            SELECT 1 FROM evidence_records e WHERE e.id=r.evidence_id AND e.company_id=w.tenant_id
              AND e.data->>'workId'=w.id AND e.data->>'requestVersion'=(jsonb_array_length(w.steer_inputs)+1)::text)
          AND EXISTS (SELECT 1 FROM evidence_records e WHERE e.id=r.evidence_id
            AND e.data->>'assignmentVersion'=COALESCE(w.meta->>'assignmentVersion','1'))
        WHERE a.canvas_id=$1 AND a.work_id=w.id AND w.kind='canvas_worker' AND w.meta->>'assignmentId'=a.id
          AND a.status IN ('queued','blocked','working','waiting')`, [id])
      await client.query(`WITH RECURSIVE stopped(id) AS (
        SELECT a.id FROM canvas_agent_assignments a WHERE a.canvas_id=$1 AND a.status IN ('failed','cancelled')
        UNION SELECT d.assignment_id FROM canvas_assignment_dependencies d JOIN stopped s ON s.id=d.depends_on_assignment_id)
        UPDATE canvas_agent_assignments a SET status='cancelled',error='Canvas dependency failed or was cancelled',completed_at=NOW(),updated_at=NOW()
        WHERE a.canvas_id=$1 AND a.status='blocked' AND a.id IN (SELECT id FROM stopped)`, [id])
      await client.query(`UPDATE lingxios.agent_work_items w SET status='cancelled',cancel_requested_at=NOW(),updated_at=NOW()
        FROM canvas_agent_assignments a WHERE a.canvas_id=$1 AND a.work_id=w.id AND a.status='cancelled'
          AND w.kind='canvas_worker' AND w.meta->>'assignmentId'=a.id AND w.status='queued'`, [id])
      await client.query(`UPDATE canvas_agent_assignments a SET status='queued',updated_at=NOW()
        WHERE a.canvas_id=$1 AND a.status='blocked' AND NOT EXISTS (
          SELECT 1 FROM canvas_assignment_dependencies d JOIN canvas_agent_assignments p ON p.id=d.depends_on_assignment_id
          WHERE d.assignment_id=a.id AND (p.status<>'completed' OR NOT EXISTS(
            SELECT 1 FROM canvas_assignment_reports r WHERE r.assignment_id=p.id AND r.author_agent_id=p.agent_id)))`, [id])
      await client.query(`UPDATE lingxios.agent_work_items w SET available_at=NOW(),updated_at=NOW()
        FROM canvas_agent_assignments a WHERE a.canvas_id=$1 AND a.work_id=w.id AND a.status='queued'
          AND w.kind='canvas_worker' AND w.meta->>'assignmentId'=a.id AND w.status='queued' AND w.available_at='infinity'::timestamptz`, [id])
      await client.query('UPDATE canvases SET updated_at=NOW() WHERE id=$1', [id])
      await queueCanvasAssignments(client, services, id, previous)
      await reconcileCanvasSummary(client, String(id))
    }
  })
}
