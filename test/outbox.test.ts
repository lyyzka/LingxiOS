import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { it } from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import { flushOutbox } from '../src/control-plane/outbox.js'

it('aborts hung delivery, persists exhaustion, and continues other streams without reordering one stream', async () => {
  const db = new PGlite()
  await db.exec(await readFile(new URL('../../db/schema.sql', import.meta.url), 'utf8'))
  const database = { async query(sql: string, params?: unknown[]) {
    const result = await db.query<Record<string, unknown>>(sql, params)
    return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
  } }
  try {
    for (const [run, seq] of [['hung', 1], ['hung', 2], ['healthy', 1]] as const) await db.query(`INSERT INTO lingxios.agent_run_events
      (run_id,seq,tenant_id,agent_id,kind,stage,visibility,delivery_work) VALUES($1,$2,'t','a','text','delta','user','{}')`, [run, seq])
    const delivered: unknown[] = []
    let cancelled = false
    await flushOutbox(database, 'agent_run_events', async (row, { signal }) => {
      if (row['run_id'] === 'hung') {
        signal.addEventListener('abort', () => { cancelled = true }, { once: true })
        await new Promise<void>(() => {})
      }
      delivered.push([String(row['run_id']), Number(row['seq'])])
    }, { timeoutMs: 20, maxAttempts: 1 })
    assert.equal(cancelled, true)
    assert.deepEqual(delivered, [['healthy', 1]])
    const failed = (await db.query<{ last_error: string; failed_at: unknown }>("SELECT last_error,failed_at FROM lingxios.agent_run_events WHERE run_id='hung' AND seq=1")).rows[0]!
    assert.match(failed.last_error, /deadline exceeded/)
    assert.ok(failed.failed_at)
    await flushOutbox(database, 'agent_run_events', async row => { delivered.push([String(row['run_id']), Number(row['seq'])]) }, { timeoutMs: 20, maxAttempts: 1 })
    assert.deepEqual(delivered, [['healthy', 1]])
    await db.exec("UPDATE lingxios.agent_run_events SET failed_at=NULL,attempts=0,available_at=NOW() WHERE run_id='hung' AND seq=1")
    for (let pass = 0; pass < 2; pass++) await flushOutbox(database, 'agent_run_events', async row => { delivered.push([String(row['run_id']), Number(row['seq'])]) })
    assert.deepEqual(delivered, [['healthy', 1], ['hung', 1], ['hung', 2]])
  } finally { await db.close() }
})
