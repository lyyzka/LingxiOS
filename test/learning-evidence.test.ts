import assert from 'node:assert/strict'
import { it } from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import type { SqlPool } from '../src/control-plane/pg-store.js'
import type { LingxiLoopServices, NativeMessage } from '../src/integrations/lingxiloop/service-contracts.js'
import { recordAttempt } from '../src/integrations/lingxiloop/learning-evidence.js'

it('pins attempt evidence to the original human and rolls back mismatched native recording', async () => {
  const db = new PGlite()
  const database: SqlPool = { query: async (sql, params) => {
    const result = await db.query<Record<string, unknown>>(sql, params)
    return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length, command: 'TEST', oid: 0, fields: [] }
  }, connect: async () => ({ query: database.query, release() {} }) }
  const work = { id: 'w', tenantId: 'tenant', agentId: 'agent', principalId: 'human', sessionId: 'room', kind: 'turn', lane: 'interactive' as const, triggerRef: 'message', fence: 1, homeEpoch: 1 }
  const action = { runId: 'w', cellId: 'cell', callIndex: 0, idempotencyKey: 'key', action: 'learning.record_attempt', args: { activityId: 'activity', evidenceClientMsgNos: ['message'], documentIds: ['document'], canvasFrameIds: ['frame'], assistance: 'HINT' } }
  const message: NativeMessage = { clientMsgNo: 'message', messageSeq: 1, fromUid: 'human', channelId: 'room', channelType: 2, payload: { version: 1, kind: 'text', body: 'My work' } }
  let author = 'human', canvasAuthor = 'human', wrongLearner = false, failNative = false, denied = false, writes = 0, metrics = 0
  const unused = async (): Promise<never> => { throw new Error('unexpected unrelated action') }
  const services: Pick<LingxiLoopServices, 'learning' | 'permissionService' | 'wukongClient'> = {
    permissionService: { assertCan: async input => { assert.equal(input.actorUserId, 'human'); if (denied) throw new Error('denied') } },
    wukongClient: () => ({ syncMessages: async () => [message], sendMessage: unused }),
    learning: { createPermissionService: (_client, options) => { assert.deepEqual(options, { lockDependencies: true }); return { assertCan: async input => { assert.equal(input.actorUserId, "human") } } }, proposeLearningEvaluation: unused, learningScoreBreakdownSchema: { parse: () => { throw new Error("unused schema") } },
      createKnowledgeUnits: unused, draftActivity: unused, findEligibleLearningMissionCoordinator: unused, upsertLearningMission: unused, findLearningMission: unused,
      updateMissionStep: unused, addMissionSteps: unused, finishMissionPlanning: unused, completeMission: unused, loadLearningTurnContext: unused, getMission: unused, getActivity: unused,
      findLearningRoomState: async () => ({ companyId: 'tenant', projectId: 'project', purpose: 'study' }),
      findLearningDocumentEvidence: async (_db, input) => { assert.deepEqual(input, { companyId: 'tenant', projectId: 'project', documentId: 'document' }); return { id: 'document', revision: 7, authorId: author } },
      findLearningCanvasEvidence: async (_db, input) => { assert.deepEqual(input, { companyId: 'tenant', projectId: 'project', frameId: 'frame' }); return { id: 'frame', revision: 3, authorId: canvasAuthor } },
      inc: name => { assert.equal(name, 'learning.attempt.accepted'); metrics++ },
      recordLearningAttempt: async (_db, transaction, infrastructure, input) => {
        writes++
        assert.deepEqual(input, { companyId: 'tenant', channelId: 'room', agentId: 'agent', ...action.args })
        assert.deepEqual(await infrastructure.syncMessages({ channelId: 'room', channelType: 2, limit: 100, loginUid: 'agent' }), [{ clientMsgNo: 'message', fromUid: 'human', authoredByAgent: false }])
        await transaction(async client => {
          assert.deepEqual((await client.query('SHOW transaction_isolation')).rows, [{ transaction_isolation: 'repeatable read' }])
          await client.query("INSERT INTO recorded VALUES('attempt')")
          if (failNative) throw new Error('native validation failed')
        })
        infrastructure.metric('learning.attempt.accepted', { source: 'message' })
        return { id: 'attempt', learnerId: wrongLearner ? 'other' : 'human' }
      },
    },
  }
  try {
    await db.exec("CREATE TABLE participants(company_id text,id text,kind text,departed_at timestamptz); INSERT INTO participants VALUES('tenant','human','human',NULL); CREATE TABLE recorded(id text)")
    for (const patch of [{ missionStepId: 'also-a-step' }, { assistance: null }, { evidenceClientMsgNos: ['message', 'message'] }, { documentIds: null }, { canvasFrameIds: Array(21).fill('frame') }, { learnerId: 'other' }]) {
      await assert.rejects(recordAttempt(database, services, work, { ...action, args: { ...action.args, ...patch } }, 2))
    }
    message.payload.refs = { agentId: 'agent' }
    await assert.rejects(recordAttempt(database, services, work, action, 2), /authored/)
    delete message.payload.refs
    message.fromUid = 'other'
    await assert.rejects(recordAttempt(database, services, work, action, 2), /authored/)
    message.fromUid = 'human'
    author = 'other'
    await assert.rejects(recordAttempt(database, services, work, action, 2), /persisted human/)
    author = 'human'
    canvasAuthor = 'other'
    await assert.rejects(recordAttempt(database, services, work, action, 2), /Canvas evidence/)
    canvasAuthor = 'human'
    denied = true
    await assert.rejects(recordAttempt(database, services, work, action, 2), /denied/)
    denied = false
    assert.equal(writes, 0)
    for (const failure of ['native', 'learner']) {
      failNative = failure === 'native'; wrongLearner = failure === 'learner'
      await assert.rejects(recordAttempt(database, services, work, action, 2), /native validation|authorized learner/)
      assert.deepEqual((await database.query('SELECT * FROM recorded')).rows, [])
      assert.equal(metrics, 0)
    }
    failNative = false; wrongLearner = false
    assert.deepEqual(await recordAttempt(database, services, work, action, 2), { id: 'attempt', learnerId: 'human' })
    assert.deepEqual((await database.query('SELECT * FROM recorded')).rows, [{ id: 'attempt' }])
    assert.equal(metrics, 1)
  } finally { await db.close() }
})
