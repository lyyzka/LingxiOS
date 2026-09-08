import assert from 'node:assert/strict'
import { mkdir, readFile, writeFile } from 'node:fs/promises'
import { resolve, join } from 'node:path'
import { createHash } from 'node:crypto'
import { PGlite } from '@electric-sql/pglite'
import { createLingxiOS, packageResources, DEFAULT_MODEL, releaseVersions } from '../dist/src/index.js'
import { createWorker } from '../dist/src/worker/index.js'
import { executeRequest } from '../dist/src/eval/index.js'

const args=process.argv.slice(2)
if (![2,3].includes(args.length) || args[0]!=='--output' || args.length===3 && args[2]!=='--live') throw new Error('usage: node scripts/eval-memory.mjs --output NEW_DIRECTORY [--live]')
const live=args.includes('--live'),output=resolve(args[1])
const apiKey=process.env.AGENT_OS_MODEL_API_KEY ?? process.env.OPENAI_API_KEY
if(live && !apiKey?.trim()) throw new Error('configure AGENT_OS_MODEL_API_KEY or OPENAI_API_KEY for live evaluation')
const bytes=await readFile(new URL('../eval/memory-cases.json',import.meta.url)),dataset=JSON.parse(bytes)
assert.equal(dataset.version,1)
assert.equal(new Set(dataset.cases.map(sample=>sample.id)).size,dataset.cases.length)
await mkdir(output,{recursive:false})
await writeFile(join(output,'dataset.json'),bytes,{flag:'wx'})
const reports=[]
for(const sample of dataset.cases) {
  const db=new PGlite(),started=performance.now()
  const database={query:async(sql,params)=>{const result=await db.query(sql,params);return{rows:result.rows,rowCount:result.affectedRows??result.rows.length}},
    connect:async()=>({query:database.query,release(){}})}
  const identity={tenantId:sample.id,agentId:'assistant',principalId:'human',sessionId:'admin'}
  const scope={tenantId:sample.id,scopeType:'private',scopeId:'human'}
  const report={caseId:sample.id,checks:[],turns:[],mode:live?'live_model':'scripted_contract',semanticQuality:live?'requires_rubric_review':'not_assessed'}
  let app,worker,timer
  try {
    await db.exec(await readFile(packageResources().schema,'utf8'))
    app=await createLingxiOS({database,memory:{resolveScopes:async actor=>actor.tenantId===scope.tenantId && actor.principalId==='human'?[scope]:[]}})
    if(live) worker=createWorker({controlPlane:app,model:{id:process.env.AGENT_OS_MODEL ?? process.env.OPENAI_MODEL ?? DEFAULT_MODEL.id,apiKey,
      baseUrl:process.env.AGENT_OS_MODEL_BASE_URL ?? process.env.OPENAI_BASE_URL ?? DEFAULT_MODEL.baseUrl,maxOutputTokens:4096,contextWindowTokens:128000}})
    let document
    for(const [index,turn] of sample.turns.entries()) {
      const actor={...identity,sessionId:`session-${index}`},turnStarted=performance.now()
      if(live) {
        const input={...actor,id:`${sample.id}-${index}`,text:turn.text,codeExecution:'disabled',mode:'execute'}
        timer=setTimeout(()=>{void app.cancel({...actor,runId:input.id}).catch(()=>{})},180000)
        const result=await executeRequest(app,worker,input)
        clearTimeout(timer)
        report.turns.push({body:result.message?.body ?? null,outcome:result.outcome,durationMs:Math.round(performance.now()-turnStarted)})
        const pending=await app.memory.reflect(actor,scope)
        for(let tries=0;tries<8 && pending.jobIds.length;tries++) {
          const states=(await db.query('SELECT status FROM lingxios.agent_work_items WHERE id=ANY($1::text[])',[pending.jobIds])).rows
          if(states.every(item=>['succeeded','partial','failed','cancelled'].includes(item.status))) break
          if(!await worker.runNext()) break
        }
      } else {
        // Fixed expected writes exercise storage/retrieval only; they are not generated learning.
        if(turn.forget) await app.memory.forget(actor,scope)
        else if(turn.fixtureBody) {
          const content={path:'facts.md',title:'Saved facts',description:'Multi-session fixture',body:turn.fixtureBody,layer:'reference',locked:turn.locked??false}
          const changes=[document?{action:'update',id:document.id,expectedVersion:document.version,content}:{action:'create',content}]
          document=(await app.memory.apply(actor,{scope,changes,sourceRef:`turn-${index}`,idempotencyKey:`fixture-${index}`})).documents[0]
        }
        report.turns.push({durationMs:Math.round(performance.now()-turnStarted)})
      }
    }
    const probe={...identity,sessionId:'probe',principalId:sample.probePrincipal??identity.principalId}
    const recallStarted=performance.now()
    let result,denied=false
    try {result=await app.memory.search(probe,scope,{query:sample.query,limit:12})}
    catch(error) {if(!/scope was revoked/.test(String(error))) throw error;denied=true}
    report.recallLatencyMs=Math.round(performance.now()-recallStarted)
    const recalled=result?.items.map(item=>item.excerpt ?? item.text).join('\n')??''
    report.retrieval=result?.retrieval??'denied'
    report.recall={matched:sample.required.filter(term=>recalled.includes(term)).length,expected:sample.required.length,items:result?.items.length??0}
    report.checks.push({id:'required_details',status:sample.required.every(term=>recalled.includes(term))?'pass':'fail'},
      {id:'forbidden_details',status:sample.forbidden.every(term=>!recalled.includes(term))?'pass':'fail'},
      {id:'authorization',status:denied===!!sample.expectDenied?'pass':'fail'})
    if(sample.expectEmpty) report.checks.push({id:'empty_after_forget',status:result?.items.length===0?'pass':'fail'})
    if(sample.expectConflict) report.checks.push({id:'conflict_diagnostic',status:live?(await app.memory.doctor(identity,scope)).conflicts.length?'pass':'fail':'not_assessed'})
    const usage=(await db.query(`SELECT COUNT(*)::int AS calls,COUNT(*) FILTER(WHERE observation IS NULL)::int AS pending,
      COALESCE(SUM(input_tokens),0)::bigint AS input_tokens,COALESCE(SUM(output_tokens),0)::bigint AS output_tokens,
      COUNT(*) FILTER(WHERE observation->'cost'->>'usage'='estimated')::int AS estimated FROM lingxios.agent_model_budget_calls`)).rows[0]
    report.modelCalls=usage.calls;report.pendingModelCalls=usage.pending;report.estimatedCalls=usage.estimated
    report.tokens={input:Number(usage.input_tokens),output:Number(usage.output_tokens)}
  } catch(error) {
    report.error=String(error).replaceAll(apiKey??'\0','[redacted]').slice(0,2000)
    report.checks.push({id:'execution',status:'fail'})
  } finally {
    clearTimeout(timer);await worker?.stop();await app?.stop();await db.close()
    report.durationMs=Math.round(performance.now()-started);reports.push(report)
  }
  await writeFile(join(output,`${sample.id}.json`),JSON.stringify(report,null,2),{flag:'wx'})
}
const summary={mode:live?'live_model':'scripted_contract',versions:releaseVersions,datasetSha256:createHash('sha256').update(bytes).digest('hex'),
  runnerSha256:createHash('sha256').update(await readFile(new URL(import.meta.url))).digest('hex'),
  generatedAt:new Date().toISOString(),passed:reports.filter(item=>item.checks.every(check=>check.status!=='fail')).length,total:reports.length,
  modelCalls:reports.reduce((sum,item)=>sum+(item.modelCalls??0),0),reports}
await writeFile(join(output,'summary.json'),JSON.stringify(summary,null,2),{flag:'wx'})
console.log(JSON.stringify({mode:summary.mode,passed:summary.passed,total:summary.total,modelCalls:summary.modelCalls,report:join(output,'summary.json')}))
if(summary.passed!==summary.total) process.exitCode=1
