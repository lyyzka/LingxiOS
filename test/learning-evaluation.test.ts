import assert from 'node:assert/strict'
import { it } from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import type { SqlPool } from '../src/control-plane/pg-store.js'
import type { LingxiLoopServices } from '../src/integrations/lingxiloop/service-contracts.js'
import { proposeEvaluation } from '../src/integrations/lingxiloop/learning-evaluation.js'

it('scopes evaluation to the human and emits native metrics only after commit', async () => {
  const db = new PGlite()
  const database: SqlPool = { query: async (sql, params) => {
    const result = await db.query<Record<string, unknown>>(sql, params)
    return { ...result, rowCount: result.affectedRows ?? result.rows.length, command: 'TEST', oid: 0, fields: [] }
  }, connect: async () => ({ query: database.query, release() {} }) }
  const work = { id: 'work', tenantId: 'tenant', agentId: 'agent', principalId: 'human', sessionId: 'room', kind: 'turn', lane: 'interactive' as const, triggerRef: 'message', fence: 1, homeEpoch: 1 }
  const action = { runId: 'work', cellId: 'cell', callIndex: 0, idempotencyKey: 'key', action: 'learning.propose_evaluation', args: { attemptId: 'attempt', demonstratedLevel: 2, confidence: 0.9, rubricResults: [{ label: 'reasoning', score: 2, weight: 1 }] } }
  let fail = false, denied = false, writes = 0
  const metrics: string[] = []
  const unused = async (): Promise<never> => { throw new Error('unexpected action') }
  const services: Pick<LingxiLoopServices, 'learning' | 'permissionService'> = {
    permissionService: { assertCan: async request => { assert.equal(request.actorUserId, 'human'); if (denied) throw new Error('denied') } },
    learning: { createPermissionService: (_client, options) => { assert.deepEqual(options, { lockDependencies: true }); return { assertCan: async input => { assert.equal(input.actorUserId, "human") } } },
      recordLearningAttempt: unused, findLearningDocumentEvidence: unused, findLearningCanvasEvidence: unused,
      createKnowledgeUnits: unused, draftActivity: unused, findEligibleLearningMissionCoordinator: unused, upsertLearningMission: unused, findLearningMission: unused,
      updateMissionStep: unused, addMissionSteps: unused, finishMissionPlanning: unused, completeMission: unused, loadLearningTurnContext: unused, getMission: unused, getActivity: unused,
      findLearningRoomState: async () => ({ companyId: 'tenant', projectId: 'project', purpose: 'study' }),
      learningScoreBreakdownSchema: { parse: value => { assert.deepEqual(value, action.args.rubricResults); return action.args.rubricResults } },
      inc: name => metrics.push(name),
      proposeLearningEvaluation: async (_db, transaction, metric, input) => {
        writes++
        assert.deepEqual(input, { companyId: 'tenant', channelId: 'room', agentId: 'agent', ...action.args })
        await transaction(async client => {
          assert.deepEqual((await client.query('SHOW transaction_isolation')).rows, [{ transaction_isolation: 'serializable' }])
          await client.query('INSERT INTO evaluations DEFAULT VALUES')
          metric('learning.state.changed', { status: 'LEARNING' })
          if (fail) throw new Error('native failure')
        })
        metric('learning.evaluation.proposed', { status: 'PENDING' })
        return { evaluationId: 'evaluation', status: 'PENDING', decisions: [] }
      },
    },
  }
  try {
    await db.exec(`CREATE TABLE participants(id text,company_id text,kind text,departed_at timestamptz);
      CREATE TABLE learning_attempts(id text,company_id text,project_id text,learner_id text);
      CREATE TABLE evidence_records(id text,company_id text,project_id text);
      CREATE TABLE evaluations(id serial);
      INSERT INTO participants VALUES('human','tenant','human',NULL),('other','tenant','human',NULL);
      INSERT INTO learning_attempts VALUES('attempt','tenant','project','human'),('foreign','tenant','project','other'),('wrong-project','tenant','elsewhere','human');`)
    for (const args of [{ ...action.args, attemptId: 'foreign' }, { ...action.args, attemptId: 'wrong-project' }, { ...action.args, sourceEvidenceId: 'missing' }, { ...action.args, confidence: '0.9' }, { ...action.args, principalId: 'other' }]) {
      await assert.rejects(proposeEvaluation(database, services, work, { ...action, args }))
    }
    denied = true
    await assert.rejects(proposeEvaluation(database, services, work, action), /denied/)
    denied = false
    assert.equal(writes, 0)
    fail = true
    await assert.rejects(proposeEvaluation(database, services, work, action), /native failure/)
    assert.deepEqual((await db.query('SELECT * FROM evaluations')).rows, [])
    assert.deepEqual(metrics, [])
    fail = false
    assert.deepEqual(await proposeEvaluation(database, services, work, action), { evaluationId: 'evaluation', status: 'PENDING', decisions: [] })
    assert.deepEqual((await db.query('SELECT COUNT(*)::int AS count FROM evaluations')).rows, [{ count: 1 }])
    assert.deepEqual(metrics, ['learning.state.changed', 'learning.evaluation.proposed'])
  } finally { await db.close() }
})
