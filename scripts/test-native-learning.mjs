import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import ts from 'typescript'
import { PGlite } from '@electric-sql/pglite'

const source = resolve(process.argv[2] ?? process.env.LINGXILOOP_SOURCE ?? fileURLToPath(new URL('../../LingxiLoop/server/src', import.meta.url)))
const input = await readFile(resolve(source, 'modules/learning/missions-repository.ts'), 'utf8')
const output = ts.transpileModule(input, { compilerOptions: { target: ts.ScriptTarget.ES2023, module: ts.ModuleKind.ESNext } }).outputText
const original = await import(`data:text/javascript;base64,${Buffer.from(output).toString('base64')}`)
const repository = original
const db = new PGlite()
try {
  // Minimal relational fixture; execute the unchanged native repository SQL.
  await db.exec(`
    CREATE TABLE learning_missions(id text,company_id text,project_id text,learner_id text,conversation_id text);
    CREATE TABLE learning_mission_steps(id text,company_id text,project_id text,mission_id text,status text,outcome text,completion_evidence_id text,completion_attempt_id text,updated_at timestamptz);
    CREATE TABLE evidence_records(id text,company_id text,project_id text);
    CREATE TABLE learning_attempts(id text,company_id text,project_id text,learner_id text);
    INSERT INTO learning_missions VALUES('mission','tenant','project','learner','channel');
    INSERT INTO learning_mission_steps(id,company_id,project_id,mission_id,status) VALUES('step','tenant','project','mission','OPEN');
    INSERT INTO learning_attempts VALUES('valid','tenant','project','learner'),('foreign-learner','tenant','project','other'),('foreign-project','tenant','other','learner'),('foreign-tenant','other','project','learner');
    INSERT INTO evidence_records VALUES('report','tenant','project'),('foreign-report','other','project');
  `)
  const queryable = { query: async (sql, params) => {
    const result = await db.query(sql, params)
    return { rows: result.rows, rowCount: result.affectedRows }
  } }
  const base = { companyId: 'tenant', projectId: 'project', channelId: 'channel', missionId: 'mission', stepId: 'step', status: 'COMPLETED', outcome: 'observed' }
  for (const patch of [{}, { attemptId: 'missing' }, { attemptId: 'foreign-learner' }, { attemptId: 'foreign-project' }, { attemptId: 'foreign-tenant' }, { sourceEvidenceId: 'foreign-report' }, { attemptId: 'valid', channelId: 'other' }, { attemptId: 'valid', missionId: 'other' }]) {
    assert.equal(await repository.updateLearningMissionStepRecord(queryable, { ...base, ...patch }), false)
    assert.deepEqual((await db.query('SELECT status,completion_attempt_id FROM learning_mission_steps')).rows, [{ status: 'OPEN', completion_attempt_id: null }])
  }
  assert.equal(await repository.updateLearningMissionStepRecord(queryable, { ...base, attemptId: 'valid' }), true)
  assert.deepEqual((await db.query('SELECT status,outcome,completion_attempt_id FROM learning_mission_steps')).rows, [{ status: 'COMPLETED', outcome: 'observed', completion_attempt_id: 'valid' }])
  assert.equal(await repository.updateLearningMissionStepRecord(queryable, { ...base, sourceEvidenceId: 'report' }), true)
  assert.deepEqual((await db.query('SELECT completion_evidence_id FROM learning_mission_steps')).rows, [{ completion_evidence_id: 'report' }])
  await db.exec(`
    ALTER TABLE learning_missions ADD COLUMN status text DEFAULT 'PLANNING', ADD COLUMN updated_at timestamptz, ADD COLUMN completed_at timestamptz;
    ALTER TABLE learning_mission_steps ADD COLUMN kind text DEFAULT 'CHECK';
    INSERT INTO learning_mission_steps(id,company_id,project_id,mission_id,status,kind) VALUES('reflect','tenant','project','mission','OPEN','REFLECT'),('foreign','other','project','mission','OPEN','CHECK');
  `)
  assert.deepEqual(await original.learningMissionPlanningSummary(queryable, base), { total: 2, checks: 1, reflections: 1 })
  assert.deepEqual(await original.learningMissionCompletionSummary(queryable, base), { unresolved: 1, reflections: 0 })
  assert.equal(await original.lockLearningMission(queryable, { ...base, statuses: ['PLANNING'] }), true)
  assert.equal(await original.lockLearningMission(queryable, { ...base, channelId: 'other', statuses: ['PLANNING'] }), false)
  assert.equal(await original.completeLearningMissionRecord(queryable, base), false)
  assert.equal(await original.activateLearningMission(queryable, base), true)
  assert.equal(await original.activateLearningMission(queryable, base), false)
  assert.equal(await original.lockLearningMission(queryable, { ...base, statuses: ['PLANNING'] }), false)
  assert.equal(await repository.updateLearningMissionStepRecord(queryable, { ...base, stepId: 'reflect', attemptId: 'valid' }), true)
  assert.deepEqual(await original.learningMissionCompletionSummary(queryable, base), { unresolved: 0, reflections: 1 })
  assert.equal(await original.completeLearningMissionRecord(queryable, { ...base, companyId: 'other' }), false)
  assert.equal(await original.completeLearningMissionRecord(queryable, base), true)
  assert.equal(await original.completeLearningMissionRecord(queryable, base), false)
  assert.deepEqual((await db.query('SELECT status,completed_at IS NOT NULL AS dated FROM learning_missions')).rows, [{ status: 'COMPLETED', dated: true }])
  console.log('Native planning/completion repository SQL passed scoped statistics and state-transition checks.')
  console.log('Unmodified native SQL passed scope and persistence checks on PostgreSQL WASM; full lifecycle, production schema and service authorization remain separate checks.')
} finally {
  await db.close()
}
