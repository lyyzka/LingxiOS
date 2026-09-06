import { withTransaction, type SqlPool } from '../../control-plane/pg-store.js'
import type { HostAction, WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices } from './service-contracts.js'
import { teacherTransaction } from './teacher.js'

export async function proposeEvaluation(database: SqlPool, services: Pick<LingxiLoopServices, 'learning' | 'permissionService'>,
  work: Omit<WorkItem, 'leaseToken'>, action: HostAction) {
  const api = services.learning
  if (!api || !work.principalId || action.action !== 'learning.propose_evaluation') throw new Error('learning evaluation services and principal are required')
  if (Object.keys(action.args).some(key => !['attemptId', 'demonstratedLevel', 'confidence', 'rubricResults', 'feedback', 'sourceEvidenceId', 'verifierEvidenceId'].includes(key))) throw new Error('unknown evaluation argument')
  const text = (value: unknown, max: number): string => {
    if (typeof value !== 'string' || !value.trim() || value.length > max) throw new Error(`evaluation text must contain 1 to ${max} characters`)
    return value.trim()
  }
  const attemptId = text(action.args['attemptId'], 200)
  const demonstratedLevel = action.args['demonstratedLevel'], confidence = action.args['confidence']
  if (typeof demonstratedLevel !== 'number' || !Number.isInteger(demonstratedLevel) || demonstratedLevel < 0 || demonstratedLevel > 4) throw new Error('demonstratedLevel must be an integer from 0 to 4')
  if (typeof confidence !== 'number' || !Number.isFinite(confidence) || confidence < 0 || confidence > 1) throw new Error('confidence must be from 0 to 1')
  const rubricResults = api.learningScoreBreakdownSchema.parse(action.args['rubricResults'])
  const feedback = action.args['feedback'] === undefined ? undefined : text(action.args['feedback'], 10000)
  const sourceEvidenceId = action.args['sourceEvidenceId'] === undefined ? undefined : text(action.args['sourceEvidenceId'], 200)
  const verifierEvidenceId = action.args['verifierEvidenceId'] === undefined ? undefined : text(action.args['verifierEvidenceId'], 200)
  if (verifierEvidenceId && !sourceEvidenceId) throw new Error('verifier evidence requires source evidence')
  await services.permissionService.assertCan({ actorUserId: work.principalId, companyId: work.tenantId,
    action: 'learning:submit', resource: { type: 'conversation', id: work.sessionId } })
  const metrics: { name: 'learning.state.changed' | 'learning.evaluation.proposed'; labels?: Record<string, string> }[] = []
  const result = await withTransaction(database, async client => {
    // Native evaluation reads state before taking its state locks; concurrent changes must abort this snapshot.
    await client.query('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE')
    await client.query("SET LOCAL statement_timeout='10s'")
    const room = await api.findLearningRoomState(client, { companyId: work.tenantId, channelId: work.sessionId })
    if (!room || room.companyId !== work.tenantId) throw new Error('conversation is not bound to a learning project')
    await api.createPermissionService(client, { lockDependencies: true }).assertCan({ actorUserId: work.principalId!, companyId: work.tenantId,
      action: 'learning:submit', resource: { type: 'conversation', id: work.sessionId } })
    const attempt = await client.query(`SELECT attempt.id FROM learning_attempts attempt
      JOIN participants learner ON learner.id=attempt.learner_id AND learner.company_id=attempt.company_id
      WHERE attempt.id=$1 AND attempt.company_id=$2 AND attempt.project_id=$3 AND attempt.learner_id=$4
        AND learner.kind='human' AND learner.departed_at IS NULL FOR UPDATE OF attempt`,
    [attemptId, work.tenantId, room.projectId, work.principalId])
    if (attempt.rows.length !== 1) throw new Error('attempt must belong to the persisted human principal in this project')
    for (const evidenceId of [sourceEvidenceId, verifierEvidenceId]) {
      if (!evidenceId) continue
      const evidence = await client.query('SELECT id FROM evidence_records WHERE id=$1 AND company_id=$2 AND project_id=$3', [evidenceId, work.tenantId, room.projectId])
      if (evidence.rows.length !== 1) throw new Error('evaluation evidence is outside this project')
    }
    return api.proposeLearningEvaluation(client, teacherTransaction(client), (name, labels) => {
      if (name !== 'learning.state.changed' && name !== 'learning.evaluation.proposed') throw new Error('unexpected evaluation metric')
      if (metrics.length >= 1000) throw new Error('evaluation exceeds metric limit')
      metrics.push({ name, ...(labels ? { labels } : {}) })
    }, { companyId: work.tenantId, channelId: work.sessionId, agentId: work.agentId, attemptId, demonstratedLevel, confidence, rubricResults,
      ...(feedback === undefined ? {} : { feedback }), ...(sourceEvidenceId ? { sourceEvidenceId } : {}), ...(verifierEvidenceId ? { verifierEvidenceId } : {}) })
  })
  for (const metric of metrics) api.inc(metric.name, metric.labels)
  return result
}
