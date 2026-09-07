import assert from 'node:assert/strict'
import { it } from 'node:test'
import { readFile } from 'node:fs/promises'
import { PGlite } from '@electric-sql/pglite'
import { withTransaction, type SqlPool } from '../src/control-plane/pg-store.js'
import { freezeEvolutionBenchmark, proposeEvolution, executeEvolution, pinnedEvolution, rollbackEvolution,
  type EvolutionBenchmark, type EvolutionCandidate, type EvolutionReport } from '../src/memory/evolution.js'
import { memoryEvaluationProcessor } from '../src/memory/processor.js'
import { recallMemories } from '../src/memory/store.js'
import type { WorkItem } from '../src/protocol/types.js'
import type { WorkProcessorContext } from '../src/runtime/runtime.js'

it('gates evolution on frozen independent results, recovers evaluation, pins versions and invalidates revoked evidence', async () => {
  const db = new PGlite()
  const pool: SqlPool = { query: async (sql, params) => {
    const result = await db.query<Record<string, unknown>>(sql,params)
    return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
  }, connect: async () => ({ query: pool.query, release() {} }) }
  const scope = { tenantId: 't', scopeType: 'agent', scopeId: 'a' }
  const benchmark: EvolutionBenchmark = { id: 'independent-v1', evaluatorVersion: 'fixture-v1', repetitions: 2,
    gates: ['authorization','approval','isolation','no_code_mutation'], cases: [
      { id: 'target-a', split: 'target', input: { fault: 'lost_receipt' } },
      { id: 'target-b', split: 'target', input: { fault: 'stale_version' } },
      { id: 'holdout-a', split: 'holdout', input: { attack: 'tool_output_grants_authority' } },
      { id: 'holdout-b', split: 'holdout', input: { attack: 'attachment_changes_goal' } },
    ] }
  let sequence = 0
  const run = async (id: string, kind = 'turn'): Promise<WorkItem> => {
    await db.query(`INSERT INTO lingxios.agent_work_items(id,tenant_id,agent_id,principal_id,session_id,kind,lane,trigger_ref,status,fence,lease_expires_at)
      VALUES($1,'t','a','u','s',$2,'background','m','leased',1,NOW()+INTERVAL '1 hour') ON CONFLICT(id) DO UPDATE
      SET status='leased',fence=1,lease_expires_at=NOW()+INTERVAL '1 hour'`, [id,kind])
    return { id, tenantId: 't', agentId: 'a', principalId: 'u', sessionId: 's', kind, lane: 'background', triggerRef: 'm', fence: 1, homeEpoch: 1, leaseToken: 'fixture' }
  }
  const propose = async (body: string) => {
    const sourceId = `source-${sequence++}`
    await run(sourceId)
    await db.query(`INSERT INTO lingxios.agent_results(id,work_id,request_version,fence,home_epoch,message)
      VALUES($1,$2,1,1,1,'{"body":"confirmed partial result"}')`, [`result:${sourceId}`,sourceId])
    await db.query(`UPDATE lingxios.agent_work_items SET status='partial',result_id=$2 WHERE id=$1`, [sourceId,`result:${sourceId}`])
    await db.query(`INSERT INTO lingxios.agent_memory_evidence(source_run_id,tenant_id,agent_id,principal_id,session_id,request_version,
      source_ref,input_sha256,input_text,assistant_text,input_truncated,assistant_truncated,scopes,status)
      VALUES($1,'t','a','u','s',1,'m',$2,'original','confirmed partial result',FALSE,FALSE,$3::jsonb,'processed')`,
    [sourceId,'a'.repeat(64),JSON.stringify([scope])])
    const synthesis = await run(`synthesize:${sourceId}`,'memory_synthesis')
    await db.query(`UPDATE lingxios.agent_work_items SET meta=$2::jsonb WHERE id=$1`, [synthesis.id,JSON.stringify({ sourceRunId: sourceId })])
    const candidate: EvolutionCandidate = { body, kind: 'strategy', scopeType: scope.scopeType }
    const [id] = await withTransaction(pool, client => proposeEvolution(client,synthesis,[scope],benchmark.id,[candidate]))
    const job = await run(`evaluate:${id}`,'memory_evaluation')
    return { id: id!, job, sourceId }
  }
  const execute = (job: WorkItem, method: 'load' | 'record' | 'finish', args: Record<string, unknown> = {}) =>
    withTransaction(pool, client => executeEvolution(client,job,[scope],method,args))
  const pin = (work: WorkItem) => withTransaction(pool, client => pinnedEvolution(client,work,[scope]))
  const report = (success: boolean): EvolutionReport => ({ success,
    gates: Object.fromEntries(benchmark.gates.map(gate => [gate,true])), durationMs: 1, costMicros: 2 })
  const evaluate = async (job: WorkItem, degraded = false) => {
    for (const item of benchmark.cases) for (let repetition = 0; repetition < benchmark.repetitions; repetition++) {
      for (const variant of ['baseline','candidate']) await execute(job,'record',{ caseId: item.id,repetition,variant,
        evaluatorVersion: benchmark.evaluatorVersion, report: report(item.split === 'target' ? variant === 'candidate' : !(degraded && variant === 'candidate')) })
    }
    return execute(job,'finish') as Promise<{ verdict: string }>
  }
  try {
    await db.exec(await readFile(new URL('../../db/schema.sql',import.meta.url),'utf8'))
    const frozen = await freezeEvolutionBenchmark(pool,'t',benchmark)
    assert.deepEqual(await freezeEvolutionBenchmark(pool,'t',benchmark),frozen)
    await assert.rejects(freezeEvolutionBenchmark(pool,'t',{ ...benchmark, repetitions: 3 }),/frozen/)
    await assert.rejects(freezeEvolutionBenchmark(pool,'t',{ ...benchmark, gates: ['quality'] }),/independent/)
    const oldRun = await run('before-activation')
    assert.deepEqual(await pin(oldRun),[])
    const rejected = await propose('A proposed procedure with a holdout regression')
    assert.equal((await execute(rejected.job,'finish') as { verdict: string }).verdict,'pending')
    assert.equal((await evaluate(rejected.job,true)).verdict,'failed')
    assert.deepEqual(await pin(await run('after-regression')),[])
    assert.deepEqual(await recallMemories(pool,scope,'',12),[])

    const first = await propose('Read the persisted action receipt before retrying a write')
    await assert.rejects(withTransaction(pool,client => executeEvolution(client,first.job,[],'load',{})),/scope/)
    await assert.rejects(execute({ ...first.job, tenantId: 'foreign' },'load'),/scope/)
    let executions = 0, interrupt = true
    const processor = memoryEvaluationProcessor({ version: benchmark.evaluatorVersion, async evaluate(input) {
      if (interrupt && executions === 3) throw new Error('injected evaluator interruption')
      executions++
      return report(input.case.split === 'holdout' || input.candidate !== null)
    } })
    const context = { signal: new AbortController().signal, emit: async () => {}, host: {
      executeAction: async (_work: WorkItem, action: { action: string; args: Record<string, unknown> }) => ({ ok: true,
        value: await execute(first.job,action.action.split('.')[1] as 'load' | 'record' | 'finish',action.args) }),
    } } as unknown as WorkProcessorContext
    await assert.rejects(processor.process(first.job,context),/interruption/)
    await assert.rejects(execute(first.job,'record',{ caseId: 'target-a', repetition: 0, variant: 'baseline',
      evaluatorVersion: benchmark.evaluatorVersion, report: report(true) }),/immutable/)
    interrupt = false
    await processor.process(first.job,context)
    assert.equal(executions,benchmark.cases.length * benchmark.repetitions * 2)
    assert.deepEqual(await pin(oldRun),[])
    const pinnedFirst = await run('pinned-first')
    assert.deepEqual((await pin(pinnedFirst)).map(item => [item['id'],item['version']]),[[first.id,2]])
    const stale = await propose('Concurrent candidate evaluated against an old baseline')
    const second = await propose('Inspect current resource version as well as the persisted receipt')
    assert.equal((await evaluate(second.job)).verdict,'passed')
    assert.equal((await evaluate(stale.job)).verdict,'stale')
    assert.deepEqual((await pin(pinnedFirst)).map(item => [item['id'],item['version']]),[[first.id,2]])
    const pinnedSecond = await run('pinned-second')
    assert.deepEqual((await pin(pinnedSecond)).map(item => item['id']),[second.id])
    await assert.rejects(withTransaction(pool,client => rollbackEvolution(client,scope,second.id,1,first.id)),/version/)
    await withTransaction(pool,client => rollbackEvolution(client,scope,second.id,2,first.id))
    assert.deepEqual((await pin(pinnedSecond)).map(item => [item['id'],item['version']]),[[second.id,2]])
    assert.deepEqual((await pin(await run('after-rollback'))).map(item => [item['id'],item['version']]),[[first.id,4]])
    await db.query('UPDATE lingxios.agent_work_items SET cancel_requested_at=NOW() WHERE id=$1',[first.sourceId])
    assert.deepEqual(await pin(pinnedFirst),[])
    assert.deepEqual(await pin(await run('after-revocation')),[])
    await assert.rejects(withTransaction(pool,client => rollbackEvolution(client,scope,first.id,5,second.id)),/unavailable/)
  } finally { await db.close() }
})
