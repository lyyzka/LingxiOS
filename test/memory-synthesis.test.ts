import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { it } from 'node:test'
import { withTransaction } from '../src/control-plane/pg-store.js'
import { executeMemorySynthesis,parseMemoryChanges,type MemoryBatch } from '../src/memory/synthesis.js'
import { memorySynthesisProcessor } from '../src/memory/processor.js'
import { retryMemorySynthesis,scheduleMemoryReflection } from '../src/memory/evidence.js'
import { memoryFixture,content,identity,scope } from './memory-fixture.js'
import type { WorkItem } from '../src/protocol/types.js'
import type { WorkProcessorContext } from '../src/runtime/runtime.js'

it('batches five committed interactions, keeps per-scope progress and learns with two independent model calls',async()=>{
  const other={...scope,scopeType:'project',scopeId:'p'}
  const f=await memoryFixture({resolveScopes:async()=>[scope,other]})
  try{
    for(let i=0;i<4;i++) await f.source({scopes:[scope,other]})
    assert.deepEqual(await scheduleMemoryReflection(f.pool,f.options),{jobIds:[]})
    const latest=await f.source({text:'我仍然喜欢图表和具体示例。',scopes:[scope,other]})
    const scheduled=await scheduleMemoryReflection(f.pool,f.options)
    assert.equal(scheduled.jobIds.length,2)
    assert.deepEqual(await scheduleMemoryReflection(f.pool,f.options),{jobIds:[]})
    const job=(await f.store.claim('memory-worker'))!
    assert.equal(job.kind,'memory_synthesis')
    assert.notEqual(job.sessionId,latest.work.sessionId)
    const invoke=(method:string,args:Record<string,unknown>)=>withTransaction(f.pool,db=>executeMemorySynthesis(db,job,method,args,f.options))
    const batch=await invoke('load',{}) as MemoryBatch
    assert.equal(batch.evidence.length,5)
    const changes=[{sourceRunIds:batch.evidence.map(item=>item.sourceRunId),change:{action:'create',content:content()}}]
    let calls=0
    const context={signal:new AbortController().signal,emit:async()=>{},host:{executeAction:async(_work:WorkItem,action:{action:string;args:Record<string,unknown>})=>({
      ok:true,executionState:'succeeded',value:await invoke(action.action.split('.')[1]!,action.args),
    })},model:{structured:async(request:{instructions:string;input:unknown})=>{
      calls++
      if(calls===2){assert.match(request.instructions,/Independently audit/);assert.deepEqual((request.input as {changes:unknown}).changes,changes)}
      return{value:calls===1?{changes,conflicts:[]}:{approved:true,confidence:0.95},model:'test',usage:{available:true,inputTokens:500,outputTokens:100}}
    }}} as unknown as WorkProcessorContext
    await memorySynthesisProcessor.process(job,context)
    assert.equal(calls,2)
    assert.equal(await invoke('load',{}),null)
    const learned=await f.api.list(identity,batch.scope)
    assert.equal(learned.items.length,1)
    const doc=await f.api.read(identity,batch.scope,learned.items[0]!.id)
    assert.equal(doc!.origin,'synthesized')
    assert.equal(doc!.sources.length,5)
    const state=(await f.db.query<{status:string;count:number}>('SELECT status,COUNT(*)::int AS count FROM lingxios.agent_memory_evidence_scopes GROUP BY status ORDER BY status')).rows
    assert.deepEqual(state,[{status:'pending',count:5},{status:'processed',count:5}])
    assert.equal(await f.store.complete(job.id,job.fence,createHash('sha256').update(job.leaseToken).digest('hex'),{status:'completed'}),true)
  }finally{await f.close()}
})

it('uses persisted idle timestamps, bounds batches and retries, and never schedules uncommitted sources',async()=>{
  const f=await memoryFixture()
  try{
    const first=await f.source()
    assert.deepEqual(await scheduleMemoryReflection(f.pool,f.options),{jobIds:[]})
    await f.db.exec("UPDATE lingxios.agent_memory_evidence_scopes SET created_at=NOW()-INTERVAL '11 minutes'")
    assert.equal((await scheduleMemoryReflection(f.pool,{...f.options})).jobIds.length,1)
    const job=(await f.store.claim('worker'))!
    await f.db.query("UPDATE lingxios.agent_work_items SET status='failed',updated_at=NOW()-INTERVAL '61 seconds' WHERE id=$1",[job.id])
    await retryMemorySynthesis(f.pool)
    assert.equal((await f.db.query<{status:string}>('SELECT status FROM lingxios.agent_work_items WHERE id=$1',[job.id])).rows[0]!.status,'queued')
    await f.db.query("UPDATE lingxios.agent_work_items SET status='failed',attempts=3 WHERE id=$1",[job.id])
    await retryMemorySynthesis(f.pool)
    assert.equal((await f.db.query<{status:string}>('SELECT status FROM lingxios.agent_memory_evidence_scopes WHERE source_run_id=$1',[first.work.id])).rows[0]!.status,'rejected')
    for(let i=0;i<24;i++) await f.source()
    assert.equal((await scheduleMemoryReflection(f.pool,f.options)).jobIds.length,1)
    assert.equal((await f.db.query<{count:number}>("SELECT COUNT(*)::int AS count FROM lingxios.agent_memory_evidence_scopes WHERE status='pending' AND job_id IS NOT NULL")).rows[0]!.count,20)
    await f.db.exec(`UPDATE lingxios.agent_memory_evidence_scopes SET status='processed' WHERE status='pending' AND job_id IS NOT NULL;
      UPDATE lingxios.agent_work_items SET status='succeeded' WHERE kind='memory_synthesis' AND status='queued'`)
    assert.equal((await scheduleMemoryReflection(f.pool,f.options)).jobIds.length,1,'the four remaining sources continue without waiting for another interaction')
    const uncommitted=await f.source({leased:true})
    await f.db.query("UPDATE lingxios.agent_memory_evidence_scopes SET created_at=NOW()-INTERVAL '1 day' WHERE source_run_id=$1",[uncommitted.work.id])
    assert.ok(!(await scheduleMemoryReflection(f.pool,f.options)).jobIds.includes(`memory-synthesis:${uncommitted.work.id}`))
  }finally{await f.close()}
})

it('protects explicit memory, records conflicts, rejects unknown evidence and stale leases, and fences forgetting',async()=>{
  const f=await memoryFixture()
  try{
    const explicit=(await f.api.initialize(identity,{scope,documents:[content('I prefer prose.')],idempotencyKey:'seed',sourceRef:'settings'})).documents[0]!
    const source=await f.source({text:'I prefer diagrams now.'})
    const {job}=await f.reflection()
    assert.ok(job)
    const invoke=(method:string,args:Record<string,unknown>,work=job)=>withTransaction(f.pool,db=>executeMemorySynthesis(db,work!,method,args,f.options))
    const batch=await invoke('load',{}) as MemoryBatch
    assert.equal(batch.currentMemories[0]!.id,explicit.id)
    const change={sourceRunIds:[source.work.id],change:{action:'update',id:explicit.id,expectedVersion:1,content:content('I prefer diagrams.')}}
    await assert.rejects(invoke('apply',{changes:[change],approved:true,confidence:1}),/protected/)
    assert.equal((await f.api.read(identity,scope,explicit.id))!.body,'I prefer prose.')
    await assert.rejects(invoke('apply',{changes:[{...change,sourceRunIds:['foreign']}],approved:true,confidence:1}),/unknown evidence/)
    await assert.rejects(invoke('load',{}, {...job,fence:job.fence+1}),/lease/)
    const conflict={sourceRunIds:[source.work.id],memoryIds:[explicit.id],reason:'New evidence contradicts the protected preference.'}
    assert.deepEqual(await invoke('apply',{changes:[],conflicts:[conflict],approved:true,confidence:1}),{outcome:'committed',changeCount:0})
    assert.equal((await f.api.doctor(identity,scope)).conflicts.length,1)
    await f.api.forget(identity,scope)
    assert.equal((await f.api.doctor(identity,scope)).conflicts.length,0)
    await assert.rejects(invoke('load',{}),/lease/)
    assert.equal((await f.api.search(identity,scope,{target:'history',query:'diagrams'})).items.length,0)
    assert.throws(()=>parseMemoryChanges([{sourceRunIds:['s'],change:{action:'delete',id:'x',expectedVersion:1}}]),/cannot delete/)
  }finally{await f.close()}
})

it('rechecks source permissions and request revisions between proposal and apply',async()=>{
  let revoked=false
  const f=await memoryFixture({resolveScopes:async()=>revoked?[]:[scope]})
  try{
    const source=await f.source()
    const {job}=await f.reflection()
    assert.ok(job)
    const invoke=(method:string,args:Record<string,unknown>)=>withTransaction(f.pool,db=>executeMemorySynthesis(db,job,method,args,f.options))
    await invoke('load',{})
    revoked=true
    await assert.rejects(invoke('apply',{changes:[],approved:true,confidence:1}),/revoked/)
    revoked=false
    await f.db.query("UPDATE lingxios.agent_work_items SET steer_inputs='[{\"id\":\"new\",\"text\":\"Do not remember this\",\"createdAt\":\"2026-09-08T00:00:00Z\"}]' WHERE id=$1",[source.work.id])
    assert.deepEqual(await invoke('apply',{changes:[],approved:true,confidence:1}),{outcome:'processed',changeCount:0})
    assert.equal((await f.api.list(identity,scope)).items.length,0)
  }finally{await f.close()}
})

it('does not renew expired knowledge from an old request committed after expiry',async()=>{
  const f=await memoryFixture()
  try{
    const saved=(await f.api.initialize(identity,{scope,documents:[content('Temporary preference')],idempotencyKey:'seed',sourceRef:'settings'})).documents[0]!
    const source=await f.source()
    await f.db.query("UPDATE lingxios.agent_memories SET origin='synthesized',valid_until=NOW()-INTERVAL '1 hour' WHERE id=$1",[saved.id])
    await f.db.query("UPDATE lingxios.agent_work_items SET created_at=NOW()-INTERVAL '2 hours' WHERE id=$1",[source.work.id])
    const {job}=await f.reflection()
    assert.ok(job)
    const invoke=(method:string,args:Record<string,unknown>)=>withTransaction(f.pool,db=>executeMemorySynthesis(db,job,method,args,f.options))
    await invoke('load',{})
    await assert.rejects(invoke('apply',{changes:[{sourceRunIds:[source.work.id],change:{action:'update',id:saved.id,expectedVersion:1,
      content:{...content('Renewed'),validUntil:new Date(Date.now()+3600_000).toISOString()}}}],approved:true,confidence:1}),/new evidence/)
  }finally{await f.close()}
})
