import assert from 'node:assert/strict'
import { it } from 'node:test'
import { createMemoryRuntime } from '../src/memory/runtime.js'
import type { SqlPool } from '../src/control-plane/pg-store.js'
import type { WorkItem } from '../src/protocol/types.js'

it('bounds scope concurrency, degrades only optional recall timeouts and rejects revoked scopes', async () => {
  const work: WorkItem = { id: 'w', tenantId: 't', agentId: 'a', principalId: 'u', sessionId: 's', meta: { text: 'query' },
    fence: 1, homeEpoch: 1, kind: 'turn', lane: 'interactive', triggerRef: 'm', leaseToken: 'lease' }
  const scopes = [1, 2, 3].map(n => ({ tenantId: 't', scopeType: 'project', scopeId: String(n) }))
  let running = 0, peak = 0, mode: 'normal' | 'recall_timeout' | 'core_timeout' | 'error' | 'revoked' = 'normal', resolutions = 0
  const query: SqlPool['query'] = async sql => {
    if (sql.includes('jsonb_agg(core)')) {
      peak = Math.max(peak, ++running)
      await new Promise(resolve => setTimeout(resolve, mode === 'core_timeout' ? 120 : 5))
      running--
      return { rows: [{ core: [], directory: [] }], rowCount: 1 }
    }
    if (sql.includes('ts_rank_cd')) {
      if (mode === 'error') throw new Error('storage failure')
      if (mode === 'recall_timeout') await new Promise(resolve => setTimeout(resolve, 40))
    }
    return { rows: [], rowCount: 0 }
  }
  const pool: SqlPool = { query, connect: async () => ({ query, release() {} }) }
  const runtime = createMemoryRuntime(pool, { resolveScopes: async () => ++resolutions > 1 && mode === 'revoked' ? [] : scopes,
    contextBudget: { concurrency: 2, timeoutMs: 100, optionalRecall: true, recallTimeoutMs: 10 } })
  assert.equal((await runtime.context(work)).status, 'available')
  assert.equal(peak, 2)
  mode = 'recall_timeout'
  assert.deepEqual((await runtime.context(work)).retrieval, ['optional_timeout','optional_timeout','optional_timeout'])
  mode = 'error'
  await assert.rejects(runtime.context(work), /storage failure/)
  mode = 'revoked'; resolutions = 0
  await assert.rejects(runtime.context(work), /revoked/)
  mode = 'core_timeout'
  await assert.rejects(runtime.context(work), /timeout|aborted/i)
})
