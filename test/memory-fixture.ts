import { readFile } from 'node:fs/promises'
import { PGlite } from '@electric-sql/pglite'
import { PgWorkStore, type SqlPool } from '../src/control-plane/pg-store.js'
import { snapshotEvidence } from '../src/context/evidence.js'
import { createResponseEnvelope } from '../src/outcome/envelope.js'
import { sessionKeyOf, type WorkItem } from '../src/protocol/types.js'
import type { RequestSnapshot } from '../src/context/request.js'
import { captureMemoryEvidence, scheduleMemoryReflection } from '../src/memory/evidence.js'
import type { MemoryOptions } from '../src/memory/access.js'
import type { MemoryContent, MemoryIdentity, MemoryScope } from '../src/memory/types.js'
import { createMemoryService } from '../src/memory/service.js'

export const scope: MemoryScope={tenantId:'t',scopeType:'user',scopeId:'u'}
export const identity: MemoryIdentity={tenantId:'t',agentId:'a',principalId:'u',sessionId:'s'}
export const content=(body='喜欢使用图表和具体示例。',path='preferences/learning.md',layer:MemoryContent['layer']='reference'):MemoryContent=>({
  path,title:'Learning preference',description:'How the user learns',body,layer,
})
export async function memoryFixture(overrides: Partial<MemoryOptions>={}) {
  const db=new PGlite()
  await db.exec(await readFile(new URL('../../db/schema.sql',import.meta.url),'utf8'))
  const pool: SqlPool={query:async(sql,params)=>{const result=await db.query<Record<string,unknown>>(sql,params);return{rows:result.rows,rowCount:result.affectedRows??result.rows.length}},
    connect:async()=>({query:pool.query,release(){}})}
  const options:MemoryOptions={resolveScopes:async actor=>actor.tenantId==='t'&&actor.principalId==='u'?[scope]:[],...overrides}
  const api=createMemoryService(pool,options).api,store=new PgWorkStore(pool)
  let sequence=0
  async function source(input:{text?:string;assistant?:string;sessionId?:string;scopes?:MemoryScope[];leased?:boolean;principalId?:string;id?:string;capture?:boolean}={}) {
    const id=input.id??`source-${++sequence}`,sessionId=input.sessionId??`session-${sequence}`,principalId=input.principalId??'u',text=input.text??'我喜欢图表和具体示例。'
    await store.enqueue({id,tenantId:'t',agentId:'a',principalId,sessionId,kind:'turn',lane:'interactive',triggerRef:id,meta:{text}})
    let work:WorkItem
    if (input.leased) work=(await store.claim(`worker-${sequence}`))!
    else {
      await db.query("UPDATE lingxios.agent_work_items SET status='succeeded' WHERE id=$1",[id])
      work={id,tenantId:'t',agentId:'a',principalId,sessionId,kind:'turn',lane:'interactive',triggerRef:id,fence:0,homeEpoch:1,leaseToken:''}
    }
    const request:RequestSnapshot={version:1,workId:id,tenantId:'t',sessionId,authorId:principalId,sourceRef:id,originalText:text,revisions:[],attachments:[],evidence:snapshotEvidence('e',[])}
    await db.query(`INSERT INTO lingxios.agent_os_sessions(session_key,tenant_id,agent_id,session_id,request_snapshot)
      VALUES($1,'t','a',$2,$3::jsonb) ON CONFLICT(session_key) DO UPDATE SET request_snapshot=EXCLUDED.request_snapshot`,[sessionKeyOf(work),sessionId,JSON.stringify(request)])
    const body=input.assistant??'Understood.',message={version:2 as const,runId:id,agentId:'a',sessionId,body,
      envelope:createResponseEnvelope(body,{status:'partial',verification:'not_run',requestVersion:1},snapshotEvidence('e',[]))}
    if (input.capture!==false) await captureMemoryEvidence(pool,work,message,input.scopes??[scope],options.writePolicy)
    return{work,request,message}
  }
  async function reflection(manual=true) {
    const scheduled=await scheduleMemoryReflection(pool,options,manual?{identity,scope}:undefined)
    const job=await store.claim('reflection-worker')
    return{scheduled,job}
  }
  return{db,pool,options,api,store,source,reflection,close:()=>db.close()}
}
