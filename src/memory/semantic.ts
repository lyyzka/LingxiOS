import { modelExecution, DEFAULT_MODEL_BUDGET, type RootModelBudgetOptions } from '../model/execution.js'
import { PgModelBudgetStore, PgWorkStore } from '../control-plane/pg-store.js'
import { createHash, randomUUID } from 'node:crypto'
import { OpenAIEmbeddingDriver, type EmbeddingOptions } from '../model/embeddings.js'
import { withTransaction, type SqlPool, type SqlQueryable } from '../control-plane/pg-store.js'
import type { WorkItem } from '../protocol/types.js'
import { recallMemories, type MemoryScope } from './store.js'

/** Package-owned semantic index; no extension or embedding callback is required from the consumer. */
export function createSemanticMemory(database: SqlPool, options: EmbeddingOptions, budget: RootModelBudgetOptions = {}) {
  const driver = new OpenAIEmbeddingDriver(options)
  const limits = { ...DEFAULT_MODEL_BUDGET, ...budget,
    inputCostMicrosPerMillion: options.inputCostMicrosPerMillion ?? budget.inputCostMicrosPerMillion ?? 0,
    outputCostMicrosPerMillion: 0 }
  const budgets = new PgModelBudgetStore(database), works = new PgWorkStore(database)
  const embed = async (work: Omit<WorkItem, 'leaseToken'>, input: readonly string[], signal?: AbortSignal) => {
    const { rows } = await database.query(`SELECT lease_token_hash FROM lingxios.agent_work_items WHERE id=$1 AND fence=$2
      AND status='leased' AND lease_expires_at>NOW() AND cancel_requested_at IS NULL`, [work.id, work.fence])
    if (typeof rows[0]?.['lease_token_hash'] !== 'string') throw new Error('embedding requires an active attempt')
    const proof = { workId: work.id, fence: work.fence, leaseTokenHash: rows[0]['lease_token_hash'] }
    const root = typeof work.meta?.['rootWorkId'] === 'string' ? work.meta['rootWorkId'] : work.id
    if (!await works.ownsBudgetRoot(work, root)) throw new Error('embedding root is outside the work lineage')
    const { invoke } = modelExecution({
      reserveModelCall: (_work, callId, reserved) => budgets.reserve(root, callId, reserved, proof),
      recordModelUsage: (_work, callId, usage, observation) => budgets.record(root, callId, usage.inputTokens, usage.outputTokens, usage.costMicros, proof, observation),
    }, { modelId: options.id, maxOutputTokens: 0, toolDefinitionTokens: 0 }, { ...work, leaseToken: '' }, limits,
    undefined, `embedding:${randomUUID()}`)
    return invoke('embedding', { input, signal }, async requestSignal => {
      const result = await driver.embed(input, requestSignal)
      return { ...result, usage: { ...result.usage, outputTokens: 0 } }
    })
  }
  const queries = new Map<string, { expires: number; value: ReturnType<OpenAIEmbeddingDriver['embed']> }>()
  return {
    async recall(work: Omit<WorkItem, 'leaseToken'>, scope: MemoryScope, query: string, limit: number, signal?: AbortSignal): Promise<Array<Record<string, unknown>>> {
      signal?.throwIfAborted()
      if (scope.tenantId !== work.tenantId || query.length > 2000 || !Number.isSafeInteger(limit) || limit < 1 || limit > 12) throw new Error('invalid memory recall scope or query')
      // Backfill every eligible record over successive reads, without blocking the foreground on indexing.
      const enqueue = (model: string | null) => database.query(`INSERT INTO lingxios.agent_work_items
        (id,tenant_id,agent_id,principal_id,session_id,thread_id,kind,lane,trigger_ref,meta)
        SELECT 'memory-index:'||md5(jsonb_build_array($1::text,m.id,m.version,$4::text,$5::text,$6::text,$7::text,$8::text,$11::text)::text),
          $1,$5,$6,$7,$8,'memory_index','background',m.id,
          jsonb_build_object('memoryId',m.id,'version',m.version,'modelKey',$4::text,'expectedModel',$11::text,'scopeType',m.scope_type,'scopeId',m.scope_id)
        FROM lingxios.agent_memories m LEFT JOIN lingxios.agent_memory_embeddings e ON e.tenant_id=m.tenant_id AND e.memory_id=m.id AND e.model_key=$4
        WHERE m.tenant_id=$1 AND m.scope_type=$2 AND m.scope_id=$3 AND m.origin<>'evolved' AND m.status='active' AND (m.valid_until IS NULL OR m.valid_until>NOW())
          AND (e.memory_id IS NULL OR e.version<>m.version OR ($11::text IS NOT NULL AND e.model<>$11))
          AND EXISTS(SELECT 1 FROM lingxios.agent_work_items w WHERE w.id=$9 AND w.fence=$10 AND w.status='leased'
            AND w.lease_expires_at>NOW() AND w.cancel_requested_at IS NULL)
          AND NOT EXISTS(SELECT 1 FROM lingxios.agent_work_items j WHERE j.id='memory-index:'||md5(jsonb_build_array($1::text,m.id,m.version,$4::text,$5::text,$6::text,$7::text,$8::text,$11::text)::text))
        ORDER BY m.pinned DESC,m.updated_at DESC,m.id LIMIT 32 ON CONFLICT(id) DO NOTHING`,
      [scope.tenantId, scope.scopeType, scope.scopeId, driver.cacheKey, work.agentId, work.principalId, work.sessionId, work.threadId ?? null, work.id, work.fence, model])
      if (!query.trim()) { await enqueue(null); return recallMemories(database, scope, '', limit) }
      const key = createHash('sha256').update(JSON.stringify([scope.tenantId, work.principalId, query])).digest('hex')
      let cached = queries.get(key)
      if (!cached || cached.expires <= Date.now()) {
        if (queries.size >= 64) queries.delete(queries.keys().next().value!)
        const value = embed(work, [query], AbortSignal.any([AbortSignal.timeout(3000), ...(signal ? [signal] : [])]))
        cached = { expires: Date.now() + 60_000, value }
        queries.set(key, cached)
      }
      let vector: Awaited<ReturnType<OpenAIEmbeddingDriver['embed']>>
      try { vector = await cached.value }
      catch {
        signal?.throwIfAborted()
        // Keep the rejected promise for the short TTL, preventing one outage from being retried every hop.
        return (await recallMemories(database, scope, '', limit)).map(row => ({ ...row, retrieval: 'recency_embedding_unavailable' }))
      }
      await enqueue(vector.model)
      // ponytail: exact scoped cosine scan; add pgvector indexing if measured scope size warrants it.
      return withTransaction(database, async client => {
        await client.query("SET LOCAL statement_timeout='3s'")
        const { rows } = await client.query(`SELECT m.id,m.body,m.kind,m.origin,m.pinned,m.version,m.source_refs,m.valid_until,m.updated_at,
          CASE WHEN e.memory_id IS NULL THEN 'recency_unindexed' ELSE 'semantic' END AS retrieval,
          (SELECT SUM(a*b) FROM unnest(e.embedding,$6::double precision[]) AS v(a,b)) AS similarity
          FROM lingxios.agent_memories m LEFT JOIN lingxios.agent_memory_embeddings e
            ON e.tenant_id=m.tenant_id AND e.memory_id=m.id AND e.version=m.version AND e.model_key=$4 AND e.model=$5
              AND cardinality(e.embedding)=cardinality($6::double precision[])
          WHERE m.tenant_id=$1 AND m.scope_type=$2 AND m.scope_id=$3 AND m.origin<>'evolved' AND m.status='active' AND (m.valid_until IS NULL OR m.valid_until>NOW())
          ORDER BY m.pinned DESC,similarity DESC NULLS LAST,m.updated_at DESC,m.id LIMIT $7`,
        [scope.tenantId, scope.scopeType, scope.scopeId, driver.cacheKey, vector.model, vector.vectors[0], limit])
        return rows
      }).catch(async () => (await recallMemories(database, scope, '', limit)).map(row => ({ ...row, retrieval: 'recency_index_unavailable' })))
    },
    async refresh(work: Omit<WorkItem, 'leaseToken'>, authorize: (database: SqlQueryable) => Promise<void>, signal?: AbortSignal) {
      signal?.throwIfAborted()
      if (work.kind !== 'memory_index') throw new Error('invalid memory index work')
      if (work.meta?.['modelKey'] !== driver.cacheKey) return { outcome: 'obsolete' }
      const read = async (client: Pick<SqlPool, 'query'>, lock: boolean) => client.query(`SELECT m.id,m.body,m.version,m.scope_type,m.scope_id
        FROM lingxios.agent_work_items w JOIN lingxios.agent_memories m ON m.tenant_id=w.tenant_id AND m.id=w.meta->>'memoryId'
        WHERE w.id=$1 AND w.fence=$2 AND w.status='leased' AND w.lease_expires_at>NOW() AND w.cancel_requested_at IS NULL
          AND w.tenant_id=$3 AND m.version=(w.meta->>'version')::integer AND m.scope_type=w.meta->>'scopeType' AND m.scope_id=w.meta->>'scopeId'
          AND m.status='active' AND (m.valid_until IS NULL OR m.valid_until>NOW()) ${lock ? 'FOR UPDATE OF w,m' : ''}`,
      [work.id, work.fence, work.tenantId])
      await authorize(database)
      const initial = (await read(database, false)).rows[0]
      if (!initial) return { outcome: 'stale' }
      const result = await embed(work, [String(initial['body'])], signal)
      if (typeof work.meta?.['expectedModel'] === 'string' && work.meta['expectedModel'] !== result.model) return { outcome: 'obsolete' }
      return withTransaction(database, async client => {
        signal?.throwIfAborted()
        await authorize(client)
        const current = (await read(client, true)).rows[0]
        if (!current || current['body'] !== initial['body']) return { outcome: 'stale' }
        await client.query(`INSERT INTO lingxios.agent_memory_embeddings(tenant_id,memory_id,version,model_key,model,embedding)
          VALUES($1,$2,$3,$4,$5,$6::double precision[]) ON CONFLICT(tenant_id,memory_id,model_key)
          DO UPDATE SET version=EXCLUDED.version,model_key=EXCLUDED.model_key,model=EXCLUDED.model,embedding=EXCLUDED.embedding`,
        [work.tenantId, current['id'], current['version'], driver.cacheKey, result.model, result.vectors[0]])
        return { outcome: 'indexed', usage: result.usage }
      })
    },
  }
}

export type SemanticMemory = ReturnType<typeof createSemanticMemory>
