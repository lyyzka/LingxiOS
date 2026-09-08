import assert from 'node:assert/strict'
import { it } from 'node:test'
import { memoryFixture,content,identity,scope } from './memory-fixture.js'
import { parseDocumentChanges,contentOf } from '../src/memory/store.js'
import { snapshotMemories,fitMemorySnapshot } from '../src/memory/context.js'
import { memorySearchText } from '../src/memory/text.js'
import type { MemoryDocument } from '../src/memory/types.js'

it('stores structured documents, searches Chinese/English and browses paths across sessions without a worker lease',async()=>{
  const f=await memoryFixture()
  try{
    const initialized=await f.api.initialize(identity,{scope,documents:[content(),content('Use npm for this project.','project/tooling.md','core')],idempotencyKey:'init',sourceRef:'settings'})
    assert.equal(initialized.documents.length,2)
    assert.deepEqual(await f.api.initialize(identity,{scope,documents:[content(),content('Use npm for this project.','project/tooling.md','core')],idempotencyKey:'init',sourceRef:'settings'}),initialized)
    const reader={...identity,sessionId:'another-session'}
    assert.deepEqual((await f.api.search(reader,scope,{query:'图表'})).items.map(item=>'path'in item?item.path:null),['preferences/learning.md'])
    assert.equal((await f.api.search(reader,scope,{query:'NPM'})).items.length,1)
    assert.equal((await f.api.search(reader,scope,{query:'unfindable-token-xyz'})).items.length,0)
    const first=await f.api.list(reader,scope,{limit:1})
    assert.equal(first.items[0]!.path,'preferences/learning.md')
    assert.equal((await f.api.list(reader,scope,{limit:1,cursor:first.nextCursor!})).items[0]!.path,'project/tooling.md')
    assert.equal((await f.api.list(reader,scope,{prefix:'project/'})).items.length,1)
    assert.equal((await f.api.read(reader,scope,initialized.documents[0]!.id))!.body,content().body)
    await assert.rejects(f.api.list({...identity,principalId:'someone-else'},scope),/revoked/)
    await assert.rejects(f.api.search(identity,{...scope,tenantId:'foreign'},{query:'npm'}),/revoked/)
    await assert.rejects(f.api.apply(identity,{scope,changes:[{action:'create',content:content('different')}],idempotencyKey:'init',sourceRef:'settings'}),/reused/)
    assert.ok(memorySearchText('图表、示例，NPM').includes('图表'))
  }finally{await f.close()}
})

it('atomically updates, moves, merges and restores versions, and diagnoses broken links and conflicts',async()=>{
  const f=await memoryFixture()
  try{
    const initial=await f.api.initialize(identity,{scope,documents:[content('Same useful detail.','notes/a.md'),content('Same useful detail.','notes/b.md')],idempotencyKey:'init',sourceRef:'settings'})
    const [a,b]=initial.documents as [MemoryDocument,MemoryDocument]
    assert.equal((await f.api.doctor(identity,scope)).duplicates.length,1)
    await assert.rejects(f.api.apply(identity,{scope,idempotencyKey:'bad-batch',sourceRef:'edit',changes:[
      {action:'move',id:a.id,expectedVersion:1,path:'notes/moved.md'},
      {action:'update',id:b.id,expectedVersion:9,content:content('bad','notes/b.md')},
    ]}),/stale/)
    assert.equal((await f.api.read(identity,scope,a.id))!.path,'notes/a.md')
    const merged=await f.api.apply(identity,{scope,idempotencyKey:'merge',sourceRef:'edit',changes:[{action:'merge',id:a.id,expectedVersion:1,
      content:content('Merged detail. [missing](missing.md)','notes/merged.md'),from:[{id:b.id,expectedVersion:1}]}]})
    assert.equal(merged.documents[0]!.version,2)
    assert.equal((await f.api.read(identity,scope,b.id))!.status,'retired')
    const doctor=await f.api.doctor(identity,scope)
    assert.deepEqual(doctor.brokenLinks,[{path:'notes/merged.md',target:'missing.md'}])
    const history=await f.api.history(identity,scope,a.id,{limit:1})
    assert.equal(history.items[0]!.version,2)
    assert.equal((await f.api.history(identity,scope,a.id,{cursor:history.nextCursor!})).items[0]!.snapshot.body,'Same useful detail.')
    const restore={scope,id:a.id,expectedVersion:2,version:1,idempotencyKey:'restore',sourceRef:'user-correction'}
    const restored=await f.api.restore(identity,restore)
    assert.equal(restored.documents[0]!.version,3)
    assert.equal(restored.documents[0]!.path,'notes/a.md')
    assert.deepEqual(await f.api.restore(identity,restore),restored)
    await f.api.forget(identity,scope)
    assert.equal((await f.api.history(identity,scope,a.id)).items.length,0)
    await assert.rejects(f.api.restore(identity,{...restore,expectedVersion:3,idempotencyKey:'resurrect'}),/unavailable/)
    for(const table of ['agent_memories','agent_memory_versions','agent_memory_commands','agent_memory_embeddings']) assert.equal((await f.db.query(`SELECT * FROM lingxios.${table}`)).rows.length,0)
  }finally{await f.close()}
})

it('validates paths, byte limits, versioned changes and content policy on body and metadata',async()=>{
  const f=await memoryFixture({writePolicy:input=>input.body.includes('private')?{action:'redact',body:input.body.replaceAll('private','redacted')}:{action:'allow'}})
  try{
    for(const path of ['../secret.md','/root.md','bad\\path.md','a//b.md','a/../b.md','a%2fb.md']) assert.throws(()=>parseDocumentChanges([{action:'create',content:content('body',path)}]),/path/)
    assert.throws(()=>parseDocumentChanges([{action:'create',content:content('字'.repeat(6000))}]),/content/)
    assert.throws(()=>parseDocumentChanges([{action:'create',content:{...content(),authority:'approved'}}]),/unknown/)
    assert.throws(()=>parseDocumentChanges([{action:'update',id:'x',expectedVersion:0,content:content()}]),/expectedVersion/)
    const saved=await f.api.initialize(identity,{scope,documents:[{...content('private '.repeat(300)),title:'private title'}],idempotencyKey:'seed',sourceRef:'settings'})
    assert.equal(saved.documents[0]!.title,'redacted title')
    assert.ok(saved.documents[0]!.body.length>500)
    await assert.rejects(f.api.apply(identity,{scope,changes:[{action:'create',content:content('api_key=very-secret-value','secrets.md')}],idempotencyKey:'secret',sourceRef:'settings'}),/credential/)
    await assert.rejects(f.api.apply(identity,{scope,changes:[{action:'create',content:{...content('plain','metadata.md'),description:'password=unsafe'}}],idempotencyKey:'metadata',sourceRef:'settings'}),/credential/)
  }finally{await f.close()}
})

it('keeps complete core records ahead of reference context and reports all budget omissions deterministically',async()=>{
  const f=await memoryFixture()
  try{
    const docs=(await f.api.initialize(identity,{scope,documents:[content('Stable preference.','user/preference.md','core'),content('detail '.repeat(700),'project/detail.md')],idempotencyKey:'seed',sourceRef:'settings'})).documents
    const snapshot=snapshotMemories({status:'available',core:[docs[0]!],directory:[],recalled:[{...docs[1]!,excerpt:docs[1]!.body,score:1}],strategies:[],
      omitted:{core:0,directory:0,recalled:0,strategies:0},budget:{ratio:0.08,maxTokens:8000},retrieval:['keyword']})
    const fitted=fitMemorySnapshot(snapshot,24_000)
    assert.deepEqual(fitted.core,[docs[0]!])
    assert.equal(fitted.recalled.length,0)
    assert.equal(fitted.omitted.recalled,1)
    assert.ok(Buffer.byteLength(JSON.stringify(fitted))<=1920)
    assert.equal(fitMemorySnapshot(snapshot,24_000).id,fitted.id)
    assert.equal(fitMemorySnapshot(snapshot,24_000,512).omitted.core,1)
    assert.equal(contentOf(docs[0]!).layer,'core')
  }finally{await f.close()}
})
