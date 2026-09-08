import type { SqlPool, SqlQueryable } from '../control-plane/pg-store.js'
import type { WorkItem } from '../protocol/types.js'
import type { ToolDefinition } from '../tools/definition.js'
import type { EmbeddingOptions } from '../model/embeddings.js'
import type { RootModelBudgetOptions } from '../model/execution.js'
import { recallMemories, snapshotMemories, type MemoryScope } from './store.js'
import { createSemanticMemory } from './semantic.js'
import { executeMemorySynthesis, parseMemoryChanges } from './synthesis.js'
import { executeEvolution, parseEvolutionCandidates, pinnedEvolution, proposeEvolution } from './evolution.js'
import { withTransaction } from '../control-plane/pg-store.js'
import type { MemoryWritePolicy } from './policy.js'

export interface MemoryOptions {
  writePolicy?: MemoryWritePolicy
  /** Resolve opaque scopes from the original human's current product permissions. */
  resolveScopes(work: Omit<WorkItem, 'leaseToken'>, database: SqlQueryable): Promise<MemoryScope[]>
  embeddings?: EmbeddingOptions
  /** A preinstalled frozen benchmark. Candidates wait for a worker with its trusted evaluator. */
  evolution?: { benchmarkId: string }
}

export function createMemoryRuntime(database: SqlPool, options: MemoryOptions, budget?: RootModelBudgetOptions) {
  const semantic = options.embeddings ? createSemanticMemory(database, options.embeddings, budget) : undefined
  const recall = async (work: Omit<WorkItem, 'leaseToken'>, scope: MemoryScope, query: string, limit: number, signal?: AbortSignal) => {
    signal?.throwIfAborted()
    const authorized = await options.resolveScopes(work,database)
    if (scope.tenantId !== work.tenantId || !authorized.some(item => item.tenantId === scope.tenantId
      && item.scopeType === scope.scopeType && item.scopeId === scope.scopeId)) throw new Error('memory scope was revoked')
    return semantic ? semantic.recall(work,scope,query,limit,signal) : recallMemories(database,scope,query,limit)
  }
  const tools: ToolDefinition[] = ['load','apply'].map(method => ({
    action: `memory_synthesis.${method}`, name: `memory_synthesis__${method}`, description: 'Maintain memory from committed, scoped task evidence.',
    effect: 'transaction', approval: false,
    parameters: { type: 'object', additionalProperties: false, properties: method === 'load' ? {} : {
      changes: { type: 'array', maxItems: 12, items: { type: 'object' } }, approved: { type: 'boolean' }, confidence: { type: 'number', minimum: 0, maximum: 1 },
      candidates: { type: 'array', maxItems: 3, items: { type: 'object', additionalProperties: false,
        properties: { kind: { type: 'string', enum: ['experience','skill','strategy'] }, scopeType: { type: 'string' }, body: { type: 'string', maxLength: 2000 } }, required: ['kind','scopeType','body'] } },
    }, required: method === 'load' ? [] : ['changes','approved','confidence'] },
    parse(value) {
      if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('invalid memory synthesis input')
      const input = value as Record<string, unknown>
      if (Object.keys(input).some(key => !(method === 'load' ? [] : ['changes','approved','confidence','candidates']).includes(key))) throw new Error('invalid memory synthesis input')
      if (method === 'apply') {
        parseMemoryChanges(input['changes'])
        parseEvolutionCandidates(input['candidates'] ?? [])
        if (!options.evolution && (input['candidates'] as unknown[] | undefined)?.length) throw new Error('evolution requires a frozen benchmark')
        if (typeof input['approved'] !== 'boolean' || typeof input['confidence'] !== 'number' || !Number.isFinite(input['confidence']) || input['confidence'] < 0 || input['confidence'] > 1) throw new Error('invalid memory verification')
      }
      return input
    },
    async authorize(context) {
      if (context.work.kind !== 'memory_synthesis') throw new Error('memory synthesis requires its background work')
      await options.resolveScopes(context.work,context.database)
    },
    async execute(context, input) {
      const scopes = await options.resolveScopes(context.work,context.database)
      const { candidates, ...memoryInput } = input
      const value = await executeMemorySynthesis(context.database,context.work,method,memoryInput,scopes,options.writePolicy)
      if (method === 'apply' && value && 'outcome' in value && value.outcome === 'committed' && options.evolution) {
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
      await options.resolveScopes(context.work,context.database)
    },
    async execute(context, input) {
      return { ok: true, executionState: 'succeeded', value: await executeEvolution(context.database,context.work,
        await options.resolveScopes(context.work,context.database),method,input) }
    },
  })
  if (semantic) tools.push({ action: 'memory_index.refresh', name: 'memory_index__refresh', description: 'Refresh one scoped, versioned memory index.',
    effect: 'idempotent', approval: false, parameters: { type: 'object', properties: {}, additionalProperties: false },
    parse(value) { if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).length) throw new Error('invalid memory index input'); return {} },
    async authorize(context) {
      if (context.work.kind !== 'memory_index') throw new Error('memory index requires its background work')
      await options.resolveScopes(context.work,context.database)
    },
    async execute(context) {
      return { ok: true, executionState: 'succeeded', value: await semantic.refresh(context.work,async db => {
        const scopes = await options.resolveScopes(context.work,db)
        if (!scopes.some(scope => scope.tenantId === context.work.tenantId && scope.scopeType === context.work.meta?.['scopeType']
          && scope.scopeId === context.work.meta?.['scopeId'])) throw new Error('memory index scope was revoked')
      },context.signal) }
    },
  })
  return { tools, recall,
    async context(work: Omit<WorkItem, 'leaseToken'>) {
      const scopes = await options.resolveScopes(work,database)
      if (scopes.length > 12) throw new Error('too many memory scopes')
      const groups = []
      for (const scope of scopes) groups.push({ scope, items: await recall(work,scope,String(work.meta?.['text'] ?? '').slice(0,2000),12) })
      const strategies = await withTransaction(database, client => pinnedEvolution(client,work,scopes))
      for (const scope of scopes) groups.push({ scope, items: strategies.filter(item => item['scope_type'] === scope.scopeType && item['scope_id'] === scope.scopeId) })
      return snapshotMemories(groups)
    },
  }
}
