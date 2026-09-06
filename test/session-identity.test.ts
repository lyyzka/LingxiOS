import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { it } from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import { MemoryWorkStore } from '../src/control-plane/memory-store.js'
import { PgWorkStore, type SqlPool } from '../src/control-plane/pg-store.js'
import { sessionKeyOf, actionKeyOf } from '../src/protocol/types.js'
import { nativeWork } from '../src/integrations/lingxiloop/actions.js'
import { canvasVerifierCapabilities } from '../src/integrations/lingxiloop/app.js'
import type { LingxiLoopServices } from '../src/integrations/lingxiloop/service-contracts.js'

it('preserves absent, empty and populated thread identities in native resource calls', () => {
  const work = { id: 'w', tenantId: 't', agentId: 'a', principalId: 'u', sessionId: 's', kind: 'resume', lane: 'approval' as const, triggerRef: 'm', fence: 2, homeEpoch: 3 }
  assert.equal(Object.hasOwn(nativeWork(work), 'threadRootClientMsgNo'), false)
  for (const threadId of ['', 'thread']) {
    const native = nativeWork({ ...work, threadId })
    assert.equal(Object.hasOwn(native, 'threadRootClientMsgNo'), true)
    assert.equal(native.threadRootClientMsgNo, threadId)
  }
})

it('preserves Canvas execution roles at native product boundaries', () => {
  const base = { id: 'w', tenantId: 't', agentId: 'a', principalId: 'u', sessionId: 's', lane: 'collaboration' as const,
    triggerRef: 'm', fence: 2, homeEpoch: 3 }
  assert.deepEqual(({ reason: nativeWork({ ...base, kind: 'canvas_worker', meta: { executionRole: 'verifier' } }).reason,
    role: nativeWork({ ...base, kind: 'canvas_worker', meta: { executionRole: 'verifier' } }).executionRole }),
  { reason: 'canvas_worker', role: 'verifier' })
  assert.deepEqual(({ reason: nativeWork({ ...base, kind: 'canvas_summary', meta: { executionRole: 'reporter' } }).reason,
    role: nativeWork({ ...base, kind: 'canvas_summary', meta: { executionRole: 'reporter' } }).executionRole }),
  { reason: 'canvas_summary', role: 'reporter' })
})

it('keeps Canvas verifiers on explicit read and report methods', () => {
  const services = { canvas: {}, learning: {}, presentations: {}, email: {}, calendar: {}, documents: {}, handoffs: {} } as LingxiLoopServices
  assert.deepEqual(canvasVerifierCapabilities(services, ['canvas', 'learning', 'knowledge', 'web', 'email', 'calendar', 'documents']), [
    { name: 'canvas', methods: ['current', 'set_status', 'submit_report'] },
    { name: 'learning', methods: ['current', 'get_learner_state', 'list_knowledge_units', 'list_due', 'get_mission', 'get_activity', 'propose_evaluation'] },
    { name: 'knowledge', methods: ['list_sources'] },
    { name: 'presentations', methods: ['get'] },
    { name: 'research', methods: ['search', 'read'] },
  ])
})

it('keeps action identity tuples unambiguous', () => {
  const keys = new Set<string>()
  for (const runId of ['w', 'w:c', '"任务"', 'w\\\n']) {
    for (const cellId of ['c:step', 'step', '"调用"', 'c\\\n']) {
      for (const callIndex of [0, 1, Number.MAX_SAFE_INTEGER]) {
        const key = actionKeyOf({ runId, cellId, callIndex })
        assert.equal(keys.has(key), false)
        keys.add(key)
        assert.deepEqual(JSON.parse(key), [runId, cellId, callIndex])
      }
    }
  }
  assert.notEqual(actionKeyOf({ runId: 'w:c', cellId: 'step', callIndex: 0 }), actionKeyOf({ runId: 'w', cellId: 'c:step', callIndex: 0 }))
})

it('isolates ambiguous identity tuples and uses identical SQL and runtime session keys', async () => {
  const db = new PGlite()
  const pool: SqlPool = { query: async (sql, params) => {
    const result = await db.query<Record<string, unknown>>(sql, params)
    return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
  }, connect: async () => ({ query: pool.query, release() {} }) }
  try {
    await db.exec(await readFile(new URL('../../db/schema.sql', import.meta.url), 'utf8'))
    for (const store of [new MemoryWorkStore(), new PgWorkStore(pool)]) {
      const identities = [
        { tenantId: 't:a', agentId: 'b', sessionId: 's' },
        { tenantId: 't', agentId: 'a:b', sessionId: 's' },
        { tenantId: 't', agentId: 'a', sessionId: 's' },
        { tenantId: 't', agentId: 'a', sessionId: 's', threadId: '-' },
        { tenantId: 't', agentId: 'a', sessionId: 's', threadId: '' },
        { tenantId: '租户"\n', agentId: '助手\\', sessionId: '会话', threadId: '🧵' },
      ]
      for (const [index, identity] of identities.entries()) {
        await store.enqueue({ ...identity, id: String(index), kind: 'turn', lane: 'interactive', triggerRef: 'm' })
      }
      const keys = new Set<string>()
      for (let index = 0; index < identities.length; index++) {
        const work = await store.claim('worker')
        assert.ok(work)
        const key = sessionKeyOf(work)
        assert.equal(keys.has(key), false)
        keys.add(key)
      }
      assert.equal(await store.claim('worker'), null)
      if (store instanceof PgWorkStore) {
        const leases = await db.query<{ session_key: string }>('SELECT session_key FROM lingxios.agent_os_session_leases')
        assert.deepEqual(new Set(leases.rows.map(row => row.session_key)), keys)
        await store.enqueue({ ...identities[0]!, id: 'same-session', kind: 'turn', lane: 'interactive', triggerRef: 'next' })
        assert.equal(await store.claim('other-worker'), null)
      }
    }
  } finally { await db.close() }
})
