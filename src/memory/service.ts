import { withTransaction, type SqlPool, type SqlQueryable } from '../control-plane/pg-store.js'
import type { ActionContext, ToolDefinition } from '../tools/definition.js'
import type { WorkItem } from '../protocol/types.js'
import { requestSnapshot } from '../app/jobs.js'
import { authorizeScope, identityOf, memorySettings, type MemoryOptions } from './access.js'
import { applyMemoryChanges, listMemories, memoryEntry, memoryVersions, readMemory, restoreMemory, scopeParams, searchMemories } from './store.js'
import { lockMemoryScopes, forgetMemoryScope } from './forget.js'
import { searchMemoryHistory, scheduleMemoryReflection, scheduleMemoryReflectionInTransaction } from './evidence.js'
import { diagnoseMemory } from './doctor.js'
import { MEMORY_METHODS, memoryToolSpecification, parseMemoryInput } from './contracts.js'
import { requireMemoryReview } from './review.js'
import { excerpt, memoryDigest } from './text.js'
import type { MemoryApplyInput, MemoryChange, MemoryContent, MemoryIdentity, MemoryListQuery, MemoryRestoreInput, MemoryScope, MemorySearchQuery, MemorySource } from './types.js'
import type { SemanticMemory } from './semantic.js'

export function createMemoryService(database: SqlPool,options: MemoryOptions,semantic?: SemanticMemory) {
  const settings = memorySettings(options)
  const access = (identity: MemoryIdentity,scope: MemoryScope,db: SqlQueryable=database) => authorizeScope(options,identity,db,scope)
  const sources = (identity: MemoryIdentity,input: { sourceRef: string; idempotencyKey: string }): MemorySource[] => {
    if (typeof input.sourceRef!=='string' || !input.sourceRef.trim() || input.sourceRef.length>1000
      || typeof input.idempotencyKey!=='string' || !input.idempotencyKey.trim() || input.idempotencyKey.length>2000) throw new Error('memory administration requires a sourceRef and idempotencyKey')
    return [{ sourceRef:input.sourceRef,authorId:identity.principalId,actionId:input.idempotencyKey,observedAt:new Date().toISOString() }]
  }
  const search = async (identity: MemoryIdentity,scope: MemoryScope,query: MemorySearchQuery,db: SqlQueryable=database,work?: Omit<WorkItem,'leaseToken'>,signal?: AbortSignal) => {
    await access(identity,scope,db)
    if (query.target==='history') return searchMemoryHistory(db,options,identity,scope,query.query,query.limit,query.cursor)
    if (query.target!==undefined && query.target!=='documents') throw new Error('invalid memory search target')
    if (semantic && work && query.cursor===undefined) {
      const result = await semantic.recall(work,scope,query.query,query.limit ?? 12,signal)
      await access(identity,scope,db)
      return result
    }
    return searchMemories(db,scope,query.query,query.limit,query.cursor)
  }
  const resolveConflicts = async (db: SqlQueryable,scope: MemoryScope,changes: MemoryChange[]) => {
    const ids = changes.flatMap(change => change.action==='create'?[]:[change.id,...change.action==='merge'?change.from.map(item => item.id):[]])
    if (ids.length) await db.query(`DELETE FROM lingxios.agent_memory_conflicts WHERE tenant_id=$1 AND scope_type=$2 AND scope_id=$3
      AND memory_ids ?| $4::text[]`,[...scopeParams(scope),ids])
  }
  const api = {
    async list(identity: MemoryIdentity,scope: MemoryScope,query: MemoryListQuery={}) {
      await access(identity,scope)
      const page = await listMemories(database,scope,query)
      return { ...page,items:page.items.map(memoryEntry) }
    },
    async read(identity: MemoryIdentity,scope: MemoryScope,id: string,version?:number) { await access(identity,scope); return readMemory(database,scope,id,version) },
    search: (identity: MemoryIdentity,scope: MemoryScope,query: MemorySearchQuery) => search(identity,scope,query),
    async history(identity: MemoryIdentity,scope: MemoryScope,id: string,query: { limit?: number; cursor?: string }={}) {
      await access(identity,scope); return memoryVersions(database,scope,id,query.limit,query.cursor)
    },
    async apply(identity: MemoryIdentity,input: MemoryApplyInput) {
      return withTransaction(database,async db => {
        await access(identity,input.scope,db)
        const [epoch] = await lockMemoryScopes(db,[input.scope])
        const result = await applyMemoryChanges(db,input.scope,input.changes,{ identity,epoch:epoch!.epoch,actionId:input.idempotencyKey,explicit:true,
          sources:sources(identity,input),resolveScopes:options.resolveScopes,...options.writePolicy?{policy:options.writePolicy}:{} })
        await resolveConflicts(db,input.scope,input.changes)
        return result
      })
    },
    async initialize(identity: MemoryIdentity,input: { scope: MemoryScope; documents: MemoryContent[]; idempotencyKey: string; sourceRef: string }) {
      if (!Array.isArray(input.documents)) throw new Error('initial memory requires documents')
      return api.apply(identity,{ ...input,changes:input.documents.map(content => ({action:'create',content})) })
    },
    async restore(identity: MemoryIdentity,input: MemoryRestoreInput) {
      return withTransaction(database,async db => {
        await access(identity,input.scope,db)
        const [epoch] = await lockMemoryScopes(db,[input.scope])
        return restoreMemory(db,input.scope,input.id,input.expectedVersion,input.version,{ identity,epoch:epoch!.epoch,actionId:input.idempotencyKey,explicit:true,
          sources:sources(identity,input),resolveScopes:options.resolveScopes,...options.writePolicy?{policy:options.writePolicy}:{} })
      })
    },
    async forget(identity: MemoryIdentity,scope: MemoryScope) {
      return withTransaction(database,async db => { await access(identity,scope,db); return forgetMemoryScope(db,scope) })
    },
    reflect: (identity: MemoryIdentity,scope: MemoryScope) => scheduleMemoryReflection(database,options,{identity,scope}),
    async doctor(identity: MemoryIdentity,scope: MemoryScope,cursor?: string) {
      await access(identity,scope); return diagnoseMemory(database,scope,settings.maxTokens,cursor)
    },
  }
  const tools: ToolDefinition[] = MEMORY_METHODS.map(method => ({
    ...memoryToolSpecification(method),parse:value => parseMemoryInput(method,value),
    async authorize(context,input) {
      if (context.work.kind.startsWith('memory_')) throw new Error('foreground memory tools cannot run as maintenance actions')
      await access(identityOf(context.work),{tenantId:context.work.tenantId,scopeType:String(input['scopeType']),scopeId:String(input['scopeId'])},context.database)
    },
    async execute(context,input) {
      const identity = identityOf(context.work),scope = {tenantId:context.work.tenantId,scopeType:String(input['scopeType']),scopeId:String(input['scopeId'])}
      const db = context.database
      let value: unknown
      switch (method) {
        case 'list': {
          const {scopeType:_type,scopeId:_id,...query} = input
          const page = await listMemories(db,scope,query as MemoryListQuery)
          value={...page,items:page.items.map(memoryEntry)}; break
        }
        case 'read': {
          const doc = await readMemory(db,scope,String(input['id']),input['version'] as number|undefined)
          if (!doc) { value=null; break }
          const offset=Number(input['offset'] ?? 0)
          if (offset>0 && /[\uDC00-\uDFFF]/u.test(doc.body.charAt(offset))) throw new Error('memory offset splits a Unicode character')
          const body=excerpt(doc.body.slice(offset),Number(input['length'] ?? 2400))
          value={document:memoryEntry(doc),body,offset,nextOffset:offset+body.length<doc.body.length?offset+body.length:null,
            sources:doc.sources.slice(0,4),omittedSources:Math.max(0,doc.sources.length-4)}; break
        }
        case 'search': value=await search(identity,scope,input as unknown as MemorySearchQuery,db,context.work,context.signal); break
        case 'history': {
          const page = await memoryVersions(db,scope,String(input['id']),input['limit'] as number|undefined,input['cursor'] as string|undefined)
          value={...page,items:page.items.map(item => ({version:item.version,replacedAt:item.replacedAt,snapshot:memoryEntry(item.snapshot)}))}; break
        }
        case 'doctor': value=await diagnoseMemory(db,scope,settings.maxTokens,input['cursor'] as string|undefined); break
        case 'reflect': {
          value=await scheduleMemoryReflectionInTransaction(db,options,{identity,scope}); break
        }
        case 'apply': case 'restore': case 'forget': value=await mutate(context,scope); break
      }
      return {ok:true,executionState:'succeeded',value}
    },
  }))
  async function mutate(context: ActionContext,scope: MemoryScope) {
    const {work,action,database:db}=context
    const reviewed=await requireMemoryReview(db,options,work,action),identity=identityOf(work),request=await requestSnapshot(db,work.id,reviewed.requestVersion)
    const refs: MemorySource[]=[{sourceRef:request.sourceRef,authorId:request.authorId,workId:work.id,requestVersion:reviewed.requestVersion,
      inputSha256:memoryDigest(reviewed.input.request),actionId:action.idempotencyKey,observedAt:new Date().toISOString()}]
    const write={identity,actionId:action.idempotencyKey,epoch:reviewed.epoch,explicit:reviewed.review.explicit,sources:refs,resolveScopes:options.resolveScopes,...options.writePolicy?{policy:options.writePolicy}:{}}
    if (action.action==='memory.forget') return forgetMemoryScope(db,scope)
    if (action.action==='memory.restore') return restoreMemory(db,scope,String(action.args['id']),Number(action.args['expectedVersion']),Number(action.args['version']),write)
    const changes=action.args['changes'] as MemoryChange[]
    const result=await applyMemoryChanges(db,scope,changes,write)
    if (write.explicit) await resolveConflicts(db,scope,changes)
    return result
  }
  return {api,tools,search}
}
export type MemoryAPI = ReturnType<typeof createMemoryService>['api']
