import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { it } from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import { createLingxiOS } from '../src/app/index.js'
import { checkStorage } from '../src/app/storage.js'
import { actionKeyOf,type HostAction,type WorkItem } from '../src/protocol/types.js'
import { memoryFixture,identity,scope,content } from './memory-fixture.js'
import { captureMemoryEvidence } from '../src/memory/evidence.js'
import { reviewedMemoryHost } from '../src/memory/worker-review.js'
import type { ModelDriver } from '../src/model/driver.js'

function action(work:WorkItem,index:number,method:string,args:Record<string,unknown>):HostAction {
  const key={runId:work.id,cellId:`memory-${index}`,callIndex:0}
  return{...key,idempotencyKey:actionKeyOf(key),action:`memory.${method}`,args:{scopeType:scope.scopeType,scopeId:scope.scopeId,...args}}
}

it('requires a private reviewer, protects explicit memory, binds reviews to versions and supports native read pagination',async()=>{
  const f=await memoryFixture()
  const app=await createLingxiOS({database:f.pool,memory:f.options})
  try{
    const saved=(await app.memory!.initialize(identity,{scope,documents:[content('Keep the original preference.')],idempotencyKey:'seed',sourceRef:'settings'})).documents[0]!
    const {work}=await f.source({leased:true,capture:false,text:'Please change my saved preference to diagrams.'})
    const host=app.connectWorker({workerId:'test',workKinds:['turn']})
    const changes=[{action:'update',id:saved.id,expectedVersion:1,content:content('Use diagrams.')}]
    const unreviewed=await host.executeAction(work,action(work,0,'apply',{changes}))
    assert.equal(unreviewed.ok,false)
    assert.match(unreviewed.error!,/review/)
    const automatic=action(work,1,'apply',{changes})
    const prepared=(await host.prepareMemoryReview!(work,automatic))!
    assert.equal(prepared.input.request.originalText,'Please change my saved preference to diagrams.')
    await host.recordMemoryReview!(work,automatic,prepared.hash,{approved:true,explicit:false,confidence:1})
    assert.equal((await host.executeAction(work,automatic)).ok,false)
    assert.equal((await app.memory!.read(identity,scope,saved.id))!.version,1)
    const explicit=action(work,2,'apply',{changes})
    const preview=(await host.prepareMemoryReview!(work,explicit))!
    await host.recordMemoryReview!(work,explicit,preview.hash,{approved:true,explicit:true,confidence:1})
    const result=await host.executeAction(work,explicit)
    assert.equal(result.ok,true,result.error)
    assert.deepEqual(await host.executeAction(work,explicit),result)
    assert.equal((await app.memory!.read(identity,scope,saved.id))!.version,2)
    const read=await host.executeAction(work,action(work,3,'read',{id:saved.id,length:4}))
    assert.equal((read.value as {body:string}).body,'Use ')
    assert.equal((read.value as {nextOffset:number}).nextOffset,4)
    const historical=await host.executeAction(work,action(work,30,'read',{id:saved.id,version:1}))
    assert.equal((historical.value as {body:string}).body,'Keep the original preference.')
    await f.source({id:'pending-reflection',sessionId:'committed-source'})
    const reflected=await host.executeAction(work,action(work,31,'reflect',{}))
    const jobs=(reflected.value as {jobIds:string[]}).jobIds
    assert.equal(jobs.length,1)
    assert.deepEqual(await f.api.reflect(identity,scope),{jobIds:jobs})
    const forged=await host.executeAction(work,action(work,4,'apply',{changes,explicit:true,approved:true}))
    assert.equal(forged.ok,false)
    const stale=action(work,5,'apply',{changes:[{...changes[0],expectedVersion:2}]})
    const stalePreview=(await host.prepareMemoryReview!(work,stale))!
    await app.memory!.apply(identity,{scope,changes:[{action:'move',id:saved.id,expectedVersion:2,path:'moved.md'}],idempotencyKey:'move',sourceRef:'settings'})
    await assert.rejects(host.recordMemoryReview!(work,stale,stalePreview.hash,{approved:true,explicit:true,confidence:1}),/stale|superseded/)
    await f.db.query("UPDATE lingxios.agent_work_items SET meta=jsonb_set(meta,'{mode}','\"read\"') WHERE id=$1",[work.id])
    const context=await host.loadContext(work)
    assert.ok(context.tools!.some(tool=>tool.action==='memory.search'))
    assert.ok(context.tools!.every(tool=>!['memory.apply','memory.restore','memory.forget','memory.reflect'].includes(tool.action)))
    await assert.rejects(host.prepareMemoryReview!(work,action(work,6,'forget',{})),/unavailable/)
  }finally{await app.stop();await f.close()}
})

it('applies privacy policy before searchable history capture without rejecting the committed user result',async()=>{
  const f=await memoryFixture({writePolicy:input=>input.body.includes('private-code')?{action:'redact',body:input.body.replaceAll('private-code','redacted')}:{action:'allow'}})
  try{
    await f.source({text:'api_key=unsafe-value'})
    assert.equal((await f.api.search(identity,scope,{target:'history',query:''})).items.length,0)
    await f.source({text:'My private-code is a fictional example.'})
    assert.equal((await f.api.search(identity,scope,{target:'history',query:'private-code'})).items.length,0)
    assert.equal((await f.api.search(identity,scope,{target:'history',query:'redacted'})).items.length,2)
  }finally{await f.close()}
})

it('runs native memory writes through the budgeted independent reviewer and preserves explicit rejection',async()=>{
  const f=await memoryFixture(),app=await createLingxiOS({database:f.pool,memory:f.options})
  try{
    const {work}=await f.source({leased:true,capture:false,text:'Remember that I like diagrams.'})
    let calls=0,approved=true
    const model={structured:async(request:{instructions:string;input:unknown})=>{
      calls++;assert.match(request.instructions,/Independently review/)
      return{value:{approved,explicit:true,confidence:1},model:'review-test',usage:{available:true,inputTokens:100,outputTokens:10}}
    }} as unknown as ModelDriver
    const host=reviewedMemoryHost(app.connectWorker({workerId:'test',workKinds:['turn']}),model)
    const first=await host.executeAction(work,action(work,0,'apply',{changes:[{action:'create',content:content()}]}))
    assert.equal(first.ok,true,first.error)
    assert.equal(calls,1)
    assert.deepEqual(await host.executeAction(work,action(work,0,'apply',{changes:[{action:'create',content:content()}]})),first)
    assert.equal(calls,1)
    const memory=(await app.memory!.list(identity,scope)).items[0]!
    assert.equal(memory.origin,'explicit')
    approved=false
    const denied=await host.executeAction(work,action(work,1,'forget',{}))
    assert.equal(denied.ok,false)
    assert.equal((await app.memory!.list(identity,scope)).items.length,1)
    assert.equal((await f.db.query('SELECT * FROM lingxios.agent_model_budget_calls')).rows.length,2)
    assert.ok((await f.db.query<{review:{approved:boolean}}>('SELECT review FROM lingxios.agent_memory_reviews')).rows.some(row=>!row.review.approved))
  }finally{await app.stop();await f.close()}
})

it('isolates searchable history by principal, rechecks original session permissions and prevents forgotten evidence from returning',async()=>{
  let revokedSession=''
  const f=await memoryFixture({resolveScopes:async actor=>actor.tenantId==='t'&&actor.sessionId!==revokedSession?[scope]:[]})
  try{
    const first=await f.source({text:'My preference is diagrams.',sessionId:'old-session'})
    await f.source({text:'Another person has a secret preference.',principalId:'other',sessionId:'other-session'})
    const history=await f.api.search(identity,scope,{target:'history',query:'preference'})
    assert.equal(history.items.length,2)
    assert.ok(history.items.every(item=>'sourceRunId'in item&&item.sourceRunId===first.work.id))
    const page=await f.api.search(identity,scope,{target:'history',query:'preference',limit:1})
    assert.equal(page.items.length,1)
    assert.equal((page.items[0] as {role:string}).role,'user')
    const next=await f.api.search(identity,scope,{target:'history',query:'preference',limit:1,cursor:page.nextCursor!})
    assert.equal(next.items.length,1)
    assert.equal((next.items[0] as {role:string}).role,'assistant')
    assert.equal(next.nextCursor,null)
    assert.equal((await f.api.search(identity,scope,{target:'history',query:'!!!'})).items.length,0)
    revokedSession='old-session'
    assert.equal((await f.api.search(identity,scope,{target:'history',query:'preference'})).items.length,0)
    revokedSession=''
    const late=await f.source({leased:true,capture:false})
    await f.api.forget(identity,scope)
    await captureMemoryEvidence(f.pool,late.work,late.message,[scope])
    assert.equal((await f.db.query('SELECT source_run_id FROM lingxios.agent_memory_evidence WHERE source_run_id=$1',[late.work.id])).rows.length,0)
    assert.equal((await f.api.search(identity,scope,{target:'history',query:''})).items.length,0)
    assert.ok((await f.db.query<{input_text:string;assistant_text:string;search_text:string}>('SELECT input_text,assistant_text,search_text FROM lingxios.agent_memory_evidence')).rows.every(row=>!row.input_text&&!row.assistant_text&&!row.search_text))
  }finally{await f.close()}
})

it('resets only schema-8 memory, retains product records and frozen benchmarks, and rejects live workers and reapplication',async()=>{
  const db=new PGlite()
  try{
    await db.exec(await readFile(new URL('../../test/fixtures/schema-8.sql',import.meta.url),'utf8'))
    await db.exec(`CREATE TABLE public.product_data(value text); INSERT INTO public.product_data VALUES('untouched');
      INSERT INTO lingxios.agent_evolution_benchmarks(tenant_id,id,hash,definition) VALUES('t','benchmark','hash','{}');
      INSERT INTO lingxios.agent_work_items(id,tenant_id,agent_id,session_id,kind,lane,trigger_ref,status,lease_expires_at)
        VALUES('business','t','a','s','turn','interactive','r','leased',NOW()+INTERVAL '1 hour');
      INSERT INTO lingxios.agent_memories(tenant_id,id,scope_type,scope_id,body,kind,origin,source_refs)
        VALUES('t','old','user','u','Old memory','observation','explicit','[{}]')`)
    const reset=await readFile(new URL('../../db/migrations/009-cognitive-memory-reset.sql',import.meta.url),'utf8')
    await assert.rejects(db.exec(reset),/stop and drain/)
    await db.exec('ROLLBACK')
    assert.equal((await db.query('SELECT id FROM lingxios.agent_memories')).rows.length,1)
    await db.exec("UPDATE lingxios.agent_work_items SET status='succeeded' WHERE id='business'")
    await db.exec(reset)
    assert.deepEqual((await db.query('SELECT value FROM public.product_data')).rows,[{value:'untouched'}])
    assert.deepEqual((await db.query('SELECT id FROM lingxios.agent_work_items')).rows,[{id:'business'}])
    assert.deepEqual((await db.query('SELECT id FROM lingxios.agent_evolution_benchmarks')).rows,[{id:'benchmark'}])
    assert.deepEqual((await db.query('SELECT id FROM lingxios.agent_memories')).rows,[])
    await db.exec(await readFile(new URL('../../db/migrations/010-im-collaboration.sql',import.meta.url),'utf8'))
    await checkStorage({query:async(sql,params)=>({rows:(await db.query<Record<string,unknown>>(sql,params)).rows,rowCount:null})})
    await assert.rejects(db.exec(reset),/requires schema version 8/)
  }finally{await db.close()}
})
