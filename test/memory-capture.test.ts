import assert from 'node:assert/strict'
import { it } from 'node:test'
import { memoryFixture, scope } from './memory-fixture.js'
import { drainMemoryCapture } from '../src/memory/capture.js'
import { forgetMemoryScope } from '../src/memory/forget.js'
import { withTransaction, type SqlPool } from '../src/control-plane/pg-store.js'

it('durably captures only filtered committed sources, runs hooks without locks, and fences forgetting and cancellation', async () => {
  let release!: () => void, entered!: () => void
  let gate = Promise.resolve(), ready = Promise.resolve()
  let transaction = false, hookCalls = 0
  const f = await memoryFixture({ writePolicy: async () => {
    hookCalls++
    assert.equal(transaction, false, 'privacy policy must not hold commit or memory locks')
    entered?.(); await gate
    return { action: 'redact', body: 'Filtered.' }
  } })
  const pool: SqlPool = { query: f.pool.query, connect: async () => ({ release() {}, query: async (sql, params) => {
    if (sql.startsWith('BEGIN')) transaction = true
    try { return await f.pool.query(sql, params) }
    finally { if (/^(COMMIT|ROLLBACK)/.test(sql)) transaction = false }
  } }) }
  const enqueue = async (id: string) => {
    const { work, message } = await f.source({ id, capture: false })
    await f.db.query(`INSERT INTO lingxios.agent_results(id,work_id,request_version,fence,home_epoch,message)
      VALUES($1,$2,1,1,1,$3)`, [id, work.id, JSON.stringify(message)])
    await f.db.query('UPDATE lingxios.agent_work_items SET result_id=$1 WHERE id=$1', [id])
    await f.db.query('INSERT INTO lingxios.agent_memory_capture(result_id) VALUES($1)', [id])
  }
  const pause = () => {
    gate = new Promise(resolve => { release = resolve })
    ready = new Promise(resolve => { entered = resolve })
  }
  try {
    await enqueue('committed')
    pause()
    const capture = drainMemoryCapture(pool, f.options)
    await ready
    assert.equal((await f.db.query('SELECT input_text FROM lingxios.agent_memory_evidence')).rows.length, 0)
    const queued = (await f.db.query<Record<string, unknown>>('SELECT * FROM lingxios.agent_memory_capture')).rows[0]!
    assert.equal(queued['result_id'], 'committed')
    assert.ok(!('input_text' in queued) && !('assistant_text' in queued))
    // Another transaction remains available while an extension is stalled.
    await withTransaction(pool, db => db.query('SELECT 1'))
    release(); await capture
    assert.deepEqual((await f.db.query('SELECT input_text,assistant_text FROM lingxios.agent_memory_evidence')).rows,
      [{ input_text: 'Filtered.', assistant_text: 'Filtered.' }])
    const calls = hookCalls
    await drainMemoryCapture(pool, f.options)
    assert.equal(hookCalls, calls)
    for (const id of ['forgotten', 'cancelled']) {
      await enqueue(id); pause()
      const pending = drainMemoryCapture(pool, f.options)
      await ready
      if (id === 'forgotten') await withTransaction(pool, db => forgetMemoryScope(db, scope))
      else await f.db.query("UPDATE lingxios.agent_work_items SET cancel_requested_at=NOW() WHERE id=$1", [id])
      release(); await pending
      assert.equal((await f.db.query('SELECT 1 FROM lingxios.agent_memory_evidence WHERE source_run_id=$1', [id])).rows.length, 0)
    }
  } finally { release?.(); await f.close() }
})
