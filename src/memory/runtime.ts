import type { SqlPool, SqlQueryable } from '../control-plane/pg-store.js'
import type { WorkItem } from '../protocol/types.js'
import type { ToolDefinition } from '../tools/definition.js'
import type { RootModelBudgetOptions } from '../model/execution.js'
import { memoryDocument, memoryEntry, scopeParams } from './store.js'
import { createSemanticMemory } from './semantic.js'
import { executeMemorySynthesis, parseMemoryChanges, parseMemoryConflicts } from './synthesis.js'
import { executeEvolution, parseEvolutionCandidates, pinnedEvolution, proposeEvolution } from './evolution.js'
import { withTransaction } from '../control-plane/pg-store.js'
import { authorizedScopes, identityOf, memorySettings, sourceIdentity, type MemoryOptions } from './access.js'
import { createMemoryService } from './service.js'
import { snapshotMemories, fitMemorySnapshot, roundRobin } from './context.js'
import { prepareMemoryReview, recordMemoryReview } from './review.js'
import type { HostAction } from '../protocol/types.js'
import type { MemoryDocument, MemoryEntry, MemoryHit, MemoryReview } from './types.js'
export type { MemoryOptions } from './access.js'

export function createMemoryRuntime(database: SqlPool, options: MemoryOptions, budget?: RootModelBudgetOptions) {
  const semantic = options.embeddings ? createSemanticMemory(database, options.embeddings, budget) : undefined
  const service = createMemoryService(database,options,semantic),settings = memorySettings(options)
  const maintenanceScopes = async (work: Omit<WorkItem,'leaseToken'>,db: SqlQueryable) => {
    const sourceId = work.meta?.['sourceRunId'] ?? work.meta?.['sourceWorkId']
    const source = (await db.query(`SELECT * FROM lingxios.agent_work_items WHERE id=$1 AND tenant_id=$2 AND principal_id=$3`,
      [sourceId,work.tenantId,work.principalId])).rows[0]
    if (!source) throw new Error('memory maintenance source is unavailable')
    return authorizedScopes(options,sourceIdentity(source),db)
  }
  const tools: ToolDefinition[] = ['load','apply'].map(method => ({
    action: `memory_synthesis.${method}`, name: `memory_synthesis__${method}`, description: 'Maintain memory from committed, scoped task evidence.',
    effect: 'transaction', approval: false,
    parameters: { type: 'object', additionalProperties: false, properties: method === 'load' ? {} : {
      changes: { type: 'array', maxItems: 12, items: { type: 'object' } }, approved: { type: 'boolean' }, confidence: { type: 'number', minimum: 0, maximum: 1 },
      conflicts: { type: 'array',maxItems:12,items:{type:'object'} },
      candidates: { type: 'array', maxItems: 3, items: { type: 'object', additionalProperties: false,
        properties: { kind: { type: 'string', enum: ['experience','skill','strategy'] }, scopeType: { type: 'string' }, body: { type: 'string', maxLength: 2000 } }, required: ['kind','scopeType','body'] } },
    }, required: method === 'load' ? [] : ['changes','approved','confidence'] },
    parse(value) {
      if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('invalid memory synthesis input')
      const input = value as Record<string, unknown>
      if (Object.keys(input).some(key => !(method === 'load' ? [] : ['changes','conflicts','approved','confidence','candidates']).includes(key))) throw new Error('invalid memory synthesis input')
      if (method === 'apply') {
        parseMemoryChanges(input['changes'])
        parseMemoryConflicts(input['conflicts'] ?? [])
        parseEvolutionCandidates(input['candidates'] ?? [])
        if (!options.evolution && (input['candidates'] as unknown[] | undefined)?.length) throw new Error('evolution requires a frozen benchmark')
        if (typeof input['approved'] !== 'boolean' || typeof input['confidence'] !== 'number' || !Number.isFinite(input['confidence']) || input['confidence'] < 0 || input['confidence'] > 1) throw new Error('invalid memory verification')
      }
      return input
    },
    async authorize(context) {
      if (context.work.kind !== 'memory_synthesis') throw new Error('memory synthesis requires its background work')
    },
    async execute(context, input) {
      const { candidates, ...memoryInput } = input
      const value = await executeMemorySynthesis(context.database,context.work,method,memoryInput,options)
      if (method === 'apply' && value && 'outcome' in value && value.outcome === 'committed' && options.evolution) {
        const scopes = (await maintenanceScopes(context.work,context.database)).filter(scope => scope.scopeType===context.work.meta?.['scopeType'] && scope.scopeId===context.work.meta?.['scopeId'])
        await proposeEvolution(context.database,context.work,scopes,options.evolution.benchmarkId,parseEvolutionCandidates(candidates ?? []),options.writePolicy)
      }
      return { ok: true, executionState: 'succeeded', value: method === 'load' && value ? { ...value, evolutionEnabled: !!options.evolution } : value }
    },
  }))
  if (options.evolution) for (const method of ['load','record','finish'] as const) tools.push({
    action: `memory_evaluation.${method}`, name: `memory_evaluation__${method}`, description: 'Run the frozen independent evaluation and gate candidate activation.',
    effect: 'transaction', approval: false,
    parameters: { type: 'object', additionalProperties: false, properties: method === 'record' ? {
      caseId: { type: 'string' }, repetition: { type: 'integer', minimum: 0, maximum: 9 }, variant: { type: 'string', enum: ['baseline','candidate'] },
      evaluatorVersion: { type: 'string' }, report: { type: 'object', additionalProperties: false, properties: {
        success: { type: 'boolean' }, gates: { type: 'object', additionalProperties: { type: 'boolean' } },
        durationMs: { type: 'integer', minimum: 0 }, costMicros: { type: 'integer', minimum: 0 },
      }, required: ['success','gates','durationMs','costMicros'] },
    } : {}, required: method === 'record' ? ['caseId','repetition','variant','evaluatorVersion','report'] : [] },
    parse(value) {
      if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('invalid evaluation input')
      const input = value as Record<string, unknown>
      if (Object.keys(input).some(key => !(method === 'record' ? ['caseId','repetition','variant','evaluatorVersion','report'] : []).includes(key))) throw new Error('invalid evaluation input')
      if (method === 'record' && (typeof input['caseId'] !== 'string' || !Number.isSafeInteger(input['repetition'])
        || !['baseline','candidate'].includes(String(input['variant'])) || typeof input['evaluatorVersion'] !== 'string'
        || !input['report'] || typeof input['report'] !== 'object')) throw new Error('invalid evaluation report')
      return input
    },
    async authorize(context) {
      if (context.work.kind !== 'memory_evaluation') throw new Error('evaluation requires its trusted background processor')
      await maintenanceScopes(context.work,context.database)
    },
    async execute(context, input) {
      return { ok: true, executionState: 'succeeded', value: await executeEvolution(context.database,context.work,
        await maintenanceScopes(context.work,context.database),method,input) }
    },
  })
  if (semantic) tools.push({ action: 'memory_index.refresh', name: 'memory_index__refresh', description: 'Refresh one scoped, versioned memory index.',
    effect: 'idempotent', approval: false, parameters: { type: 'object', properties: {}, additionalProperties: false },
    parse(value) { if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).length) throw new Error('invalid memory index input'); return {} },
    async authorize(context) {
      if (context.work.kind !== 'memory_index') throw new Error('memory index requires its background work')
      await maintenanceScopes(context.work,context.database)
    },
    async execute(context) {
      return { ok: true, executionState: 'succeeded', value: await semantic.refresh(context.work,async db => {
        const scopes = await maintenanceScopes(context.work,db)
        if (!scopes.some(scope => scope.tenantId === context.work.tenantId && scope.scopeType === context.work.meta?.['scopeType']
          && scope.scopeId === context.work.meta?.['scopeId'])) throw new Error('memory index scope was revoked')
      },context.signal) }
    },
  })
  return { tools:[...service.tools,...tools],api:service.api,
    prepareReview: (work: Omit<WorkItem,'leaseToken'>,action:HostAction) => withTransaction(database,db => prepareMemoryReview(db,options,work,action)),
    recordReview: (work: Omit<WorkItem,'leaseToken'>,action:HostAction,hash:string,review:MemoryReview) => withTransaction(database,db => recordMemoryReview(db,options,work,action,hash,review)),
    async context(work: Omit<WorkItem, 'leaseToken'>) {
      const identity=identityOf(work),scopes=await authorizedScopes(options,identity,database)
      const request = (await database.query(`SELECT request_snapshot FROM lingxios.agent_request_snapshots WHERE work_id=$1`,[work.id])).rows[0]?.['request_snapshot'] as
        {originalText:string;revisions:Array<{text:string}>;inheritedRevisions?:Array<{text:string}>}|undefined
      const query = [request?.originalText ?? String(work.meta?.['text'] ?? ''),...request?.inheritedRevisions?.map(item => item.text) ?? [],
        ...request?.revisions.map(item => item.text) ?? []].join('\n').slice(-2000)
      const core:MemoryDocument[][]=[],directory:MemoryEntry[][]=[],recalled:MemoryHit[][]=[],retrieval:import('./types.js').MemorySearchResult['retrieval'][]=[]
      let omittedCore=0,omittedDirectory=0
      for (const scope of scopes) {
        const rows=(await database.query(`SELECT *,COUNT(*) OVER() AS total FROM lingxios.agent_memories WHERE tenant_id=$1 AND scope_type=$2 AND scope_id=$3
          AND origin<>'evolved' AND layer='core' AND status='active' AND (valid_until IS NULL OR valid_until>NOW())
          ORDER BY pinned DESC,origin='explicit' DESC,path LIMIT 64`,scopeParams(scope))).rows
        core.push(rows.map(memoryDocument)); omittedCore+=Number(rows[0]?.['total']??0)-rows.length
        const entries=(await database.query(`SELECT *,COUNT(*) OVER() AS total FROM lingxios.agent_memories WHERE tenant_id=$1 AND scope_type=$2 AND scope_id=$3
          AND origin<>'evolved' AND status='active' AND (valid_until IS NULL OR valid_until>NOW()) ORDER BY path LIMIT 64`,scopeParams(scope))).rows
        directory.push(entries.map(row => memoryEntry(memoryDocument(row)))); omittedDirectory+=Number(entries[0]?.['total']??0)-entries.length
        const result=await service.search(identity,scope,{query,limit:12},database,work)
        recalled.push((result.items as MemoryHit[]).filter(item => item.layer!=='core')); retrieval.push(result.retrieval)
      }
      const strategies=options.evolution?await withTransaction(database,client=>pinnedEvolution(client,work,scopes)):[]
      return fitMemorySnapshot(snapshotMemories({status:'available',core:roundRobin(core),directory:roundRobin(directory),recalled:roundRobin(recalled),strategies,
        omitted:{core:omittedCore,directory:omittedDirectory,recalled:0,strategies:0},budget:{ratio:settings.ratio,maxTokens:settings.maxTokens},retrieval}),Infinity)
    },
  }
}
