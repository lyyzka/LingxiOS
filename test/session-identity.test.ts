import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { it } from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import { MemoryWorkStore } from '../src/control-plane/memory-store.js'
import { PgWorkStore, type SqlPool } from '../src/control-plane/pg-store.js'
import { sessionKeyOf, actionKeyOf } from '../src/protocol/types.js'
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
