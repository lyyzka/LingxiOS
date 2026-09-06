import { withTransaction, type SqlPool } from '../../control-plane/pg-store.js'
import type { HostAction, WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices } from './service-contracts.js'

export async function readAttempts(database: SqlPool, services: Pick<LingxiLoopServices, 'learning' | 'permissionService'>,
  work: Omit<WorkItem, 'leaseToken'>, action: HostAction) {
  const detail = action.action === 'learning.get_attempt'
  if (!services.learning || !work.principalId || (!detail && action.action !== 'learning.list_attempts')) throw new Error('learning attempt services and principal are required')
  const allowed = detail ? ['attemptId'] : ['activityId', 'missionStepId']
  if (Object.keys(action.args).some(key => !allowed.includes(key))) throw new Error('unknown attempt read argument')
  const id = (value: unknown): string | undefined => {
    if (value === undefined) return undefined
    if (typeof value !== 'string' || !value.trim() || value.length > 200) throw new Error('attempt read identifiers must contain 1 to 200 characters')
    return value.trim()
  }
  const attemptId = id(action.args['attemptId']), activityId = id(action.args['activityId']), missionStepId = id(action.args['missionStepId'])
  if (detail && !attemptId) throw new Error('attemptId is required')
  if (activityId && missionStepId) throw new Error('filter by activityId or missionStepId, not both')
  await services.permissionService.assertCan({ actorUserId: work.principalId, companyId: work.tenantId,
    action: 'learning:read', resource: { type: 'conversation', id: work.sessionId } })
  const api = services.learning
  return withTransaction(database, async client => {
    await client.query('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
    await client.query("SET LOCAL statement_timeout='10s'")
    const room = await api.findLearningRoomState(client, { companyId: work.tenantId, channelId: work.sessionId })
    if (!room || room.companyId !== work.tenantId) throw new Error('conversation is not bound to a learning project')
    await api.createPermissionService(client).assertCan({ actorUserId: work.principalId!, companyId: work.tenantId,
      action: 'learning:read', resource: { type: 'conversation', id: work.sessionId } })
    const { rows } = await client.query(`SELECT attempt.id,attempt.activity_id,attempt.mission_step_id,attempt.assistance,
        attempt.status,attempt.submitted_at,attempt.evidence_id
      FROM learning_attempts attempt JOIN participants learner
        ON learner.id=attempt.learner_id AND learner.company_id=attempt.company_id
      WHERE attempt.company_id=$1 AND attempt.project_id=$2 AND attempt.learner_id=$3
        AND learner.kind='human' AND learner.departed_at IS NULL
        AND ($4::text IS NULL OR attempt.id=$4)
        AND ($5::text IS NULL OR attempt.activity_id=$5)
        AND ($6::text IS NULL OR attempt.mission_step_id=$6)
      ORDER BY attempt.submitted_at DESC,attempt.id DESC LIMIT 101`,
    [work.tenantId, room.projectId, work.principalId, attemptId ?? null, activityId ?? null, missionStepId ?? null])
    if (!detail) return { attempts: rows.slice(0, 100), truncated: rows.length > 100 }
    const attempt = rows[0]
    if (!attempt) throw new Error('attempt not found for this principal in this project')
    const evidence = await client.query(`SELECT id,data,created_by_type,created_by_id,created_at FROM evidence_records
      WHERE id=$1 AND company_id=$2 AND project_id=$3`, [attempt['evidence_id'], work.tenantId, room.projectId])
    if (evidence.rows.length !== 1) throw new Error('attempt evidence not found in this project')
    const evaluations = await client.query(`SELECT id,demonstrated_level,confidence,rubric_results,feedback,evaluator_id,
        evaluator_kind,status,source_evidence_id,verifier_evidence_id,created_at
      FROM learning_evaluations WHERE attempt_id=$1 AND company_id=$2 AND project_id=$3
      ORDER BY created_at DESC,id DESC LIMIT 101`, [attemptId, work.tenantId, room.projectId])
    return { ...attempt, evidence: evidence.rows[0], evaluations: evaluations.rows.slice(0, 100), evaluationsTruncated: evaluations.rows.length > 100 }
  })
}
