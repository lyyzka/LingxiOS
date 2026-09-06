import { createHash } from 'node:crypto'
import type { SqlQueryable } from '../../control-plane/pg-store.js'
import type { WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices, NativeQueryable } from './service-contracts.js'

/** A persisted report records an agent finding, not proof that the user's goal is satisfied. */
export async function submitCanvasWorkReport(db: SqlQueryable, services: Pick<LingxiLoopServices, 'canvas'>,
  work: Omit<WorkItem, 'leaseToken'>, args: Record<string, unknown>, canvasId: string, projectId: string, requestVersion: number, assignmentVersion: number) {
  const api = services.canvas!.orchestration!
  if (Buffer.byteLength(JSON.stringify(args)) > 32_768) throw new Error('Canvas report input exceeds 32768 bytes')
  const bounded = (value: unknown, name: string, max = 4000) => {
    if (typeof value !== 'string' || !value.trim() || value.length > max) throw new Error(`invalid report ${name}`)
    return value.trim()
  }
  const strings = (value: unknown, name: string) => {
    if (!Array.isArray(value) || value.length > 32) throw new Error(`invalid report ${name}`)
    return value.map(item => bounded(item, name))
  }
  const finding = bounded(args['finding'], 'finding', 16_000)
  const confidence = args['confidence']
  if (typeof confidence !== 'number' || !Number.isFinite(confidence) || confidence < 0 || confidence > 1) throw new Error('invalid report confidence')
  const unresolved = strings(args['unresolved'] ?? [], 'unresolved')
  const checks = strings(args['disconfirmingChecks'] ?? [], 'disconfirmingChecks')
  const nextStep = args['nextStep'] === undefined ? null : bounded(args['nextStep'], 'nextStep')
  const inputRefs = args['evidenceRefs']
  if (!Array.isArray(inputRefs) || inputRefs.length > 64) throw new Error('invalid report evidenceRefs')
  let refs = inputRefs
  const reporter = work.kind === 'canvas_summary'
  const assignment = reporter ? { id: null, execution_role: 'reporter', verifies_assignment_id: null } : (await db.query(`SELECT id,execution_role,verifies_assignment_id FROM canvas_agent_assignments
    WHERE id=$1 AND canvas_id=$2 AND agent_id=$3 AND work_id=$4 FOR UPDATE`, [work.meta?.['assignmentId'], canvasId, work.agentId, work.id])).rows[0]
  if (!assignment || !['specialist', 'verifier', 'reporter'].includes(String(assignment['execution_role']))) throw new Error('report requires the current specialist, verifier or reporter work')
  const consumed = args['consumedReportIds'] === undefined ? [] : strings(args['consumedReportIds'], 'consumedReportIds')
  const conflicts = args['conflictResolution'] ?? []
  if (!Array.isArray(conflicts) || conflicts.length > 32) throw new Error('invalid report conflictResolution')
  if (reporter) {
    if (!consumed.length || new Set(consumed).size !== consumed.length) throw new Error('reporter must consume persisted reports')
    const available = (await db.query('SELECT id,assignment_id FROM canvas_assignment_reports WHERE canvas_id=$1 AND company_id=$2 FOR SHARE', [canvasId, work.tenantId])).rows
    if (consumed.some(id => !available.some(row => row['id'] === id))
      || available.some(row => row['assignment_id'] !== null && !consumed.includes(String(row['id'])))) throw new Error('reporter must consume every current assignment report in this Canvas')
    refs = [...refs, ...consumed.map(id => ({ kind: 'report', id }))]
    if (new Set(refs.map(ref => JSON.stringify([ref?.kind, ref?.id]))).size > 64) throw new Error('report evidence reference capacity exceeded')
  } else if (consumed.length || conflicts.length) throw new Error('only reporter work may consume reports or resolve conflicts')
  let verifiesReportId: string | null = null, verdict: string | null = null
  if (assignment['execution_role'] === 'verifier') {
    verifiesReportId = bounded(args['verifiesReportId'], 'verifiesReportId', 240)
    if (!['supported', 'rejected', 'inconclusive'].includes(String(args['verdict']))) throw new Error('invalid verifier verdict')
    verdict = String(args['verdict'])
    if (!checks.length) throw new Error('verifier must record disconfirming checks')
    const source = await db.query(`SELECT id FROM canvas_assignment_reports WHERE id=$1 AND company_id=$2
      AND canvas_id=$3 AND assignment_id=$4 AND author_agent_id<>$5 AND execution_role='specialist' FOR SHARE`,
    [verifiesReportId, work.tenantId, canvasId, assignment['verifies_assignment_id'], work.agentId])
    if (source.rows.length !== 1) throw new Error('verifier report must target its assigned builder report')
  } else if (args['verifiesReportId'] !== undefined || args['verdict'] !== undefined || checks.length) throw new Error('only verifier reports may set verification fields')
  const permissions = api.createPermissionService(db, { lockDependencies: true })
  const sourceEvidenceIds: string[] = []
  const seen = new Set<string>()
  for (const ref of refs) {
    if (!ref || typeof ref !== 'object' || Array.isArray(ref) || Object.keys(ref).some(key => !['kind', 'id'].includes(key))) throw new Error('invalid report evidence reference')
    const id = bounded(ref.id, 'evidence id', 240), kind = ref.kind
    const key = JSON.stringify([kind, id])
    if (seen.has(key)) continue
    seen.add(key)
    let observed: Record<string, unknown> | undefined
    if (kind === 'frame') {
      observed = (await db.query('SELECT id,revision FROM canvas_frames WHERE id=$1 AND canvas_id=$2 FOR SHARE', [id, canvasId])).rows[0]
    } else if (kind === 'report') {
      observed = (await db.query('SELECT id,evidence_id FROM canvas_assignment_reports WHERE id=$1 AND canvas_id=$2 AND company_id=$3 FOR SHARE', [id, canvasId, work.tenantId])).rows[0]
    } else if (kind === 'document') {
      await permissions.assertCan({ actorUserId: work.principalId!, companyId: work.tenantId, action: 'document:read', resource: { type: 'document', id } })
      observed = (await db.query(`SELECT id,updated_at FROM documents WHERE id=$1 AND company_id=$2 AND project_id=$3
        AND (conversation_id IS NULL OR conversation_id=$4) FOR SHARE`, [id, work.tenantId, projectId, work.sessionId])).rows[0]
    } else if (kind === 'source') {
      await permissions.assertCan({ actorUserId: work.principalId!, companyId: work.tenantId, action: 'knowledge:read', resource: { type: 'knowledge_source', id } })
      observed = (await db.query('SELECT id,updated_at FROM knowledge_sources WHERE id=$1 AND company_id=$2 AND project_id=$3 AND deleted_at IS NULL FOR SHARE', [id, work.tenantId, projectId])).rows[0]
    } else if (kind === 'attempt') {
      await permissions.assertCan({ actorUserId: work.principalId!, companyId: work.tenantId, action: 'learning:read', resource: { type: 'project', id: projectId } })
      observed = (await db.query('SELECT id,created_at FROM learning_attempts WHERE id=$1 AND company_id=$2 AND project_id=$3 AND learner_id=$4 FOR SHARE', [id, work.tenantId, projectId, work.principalId])).rows[0]
    } else throw new Error('report evidence kind must be frame, report, document, source or attempt')
    if (!observed) throw new Error('report evidence is not observable in the authorized Canvas scope')
    const observation = JSON.stringify(observed)
    const evidenceId = 'evidence-' + createHash('sha256').update(JSON.stringify([work.tenantId, projectId, kind, id, observation])).digest('hex')
    await api.createEvidenceRecordInTransaction(db as NativeQueryable, { id: evidenceId, companyId: work.tenantId, projectId,
      level: 'L1', derivation: 'OBSERVED', kind: 'CANVAS_SOURCE_REFERENCE', data: { sourceKind: kind, sourceId: id, observation }, createdBy: { type: 'SYSTEM' } })
    sourceEvidenceIds.push(evidenceId)
  }
  const reportId = 'report-' + createHash('sha256').update(JSON.stringify([work.id, requestVersion, assignmentVersion, 'learning_report_v1'])).digest('hex').slice(0, 28)
  const evidenceId = `evidence-${reportId}`
  await api.createEvidenceWithLinksInTransaction(db as NativeQueryable, { id: evidenceId, companyId: work.tenantId, projectId,
    level: 'L2', derivation: 'OBSERVED', kind: 'CANVAS_REPORT', data: { reportId, canvasId, assignmentId: String(assignment['id']), executionRole: String(assignment['execution_role']), workId: work.id, requestVersion: String(requestVersion), assignmentVersion: String(assignmentVersion) },
    createdBy: { type: 'AGENT', id: work.agentId } }, sourceEvidenceIds.map(targetId => ({ relation: 'DERIVED_FROM', targetLevel: 'L1', targetKind: 'EVIDENCE_RECORD', targetId })))
  // Preserve historical reports and their evidence/verification links; only the current projection owns assignment_id.
  if (assignment['id']) await db.query('UPDATE canvas_assignment_reports SET assignment_id=NULL WHERE assignment_id=$1 AND id<>$2', [assignment['id'], reportId])
  await db.query(`INSERT INTO canvas_assignment_reports(id,company_id,canvas_id,assignment_id,author_agent_id,execution_role,
    finding,evidence_id,source_evidence_ids,confidence,unresolved,next_step,verifies_report_id,disconfirming_checks,verdict,consumed_report_ids,conflict_resolution)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11::jsonb,$12,$13,$14::jsonb,$15,$16::jsonb,$17::jsonb)`,
  [reportId, work.tenantId, canvasId, assignment['id'], work.agentId, assignment['execution_role'], finding, evidenceId,
    JSON.stringify(sourceEvidenceIds), confidence, JSON.stringify(unresolved), nextStep, verifiesReportId, JSON.stringify(checks), verdict, JSON.stringify(consumed), JSON.stringify(conflicts)])
  return { reportId, assignmentId: assignment['id'], evidenceId, sourceEvidenceIds, verdict, unresolved }
}
