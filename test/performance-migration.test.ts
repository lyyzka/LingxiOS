import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { it } from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import { PgWorkStore, type SqlPool } from '../src/control-plane/pg-store.js'

it('publishes empty wake hints after commit only, with no idle-claim notification loop', async () => {
  const db = new PGlite()
  try {
    await db.exec(await readFile(new URL('../../db/schema.sql', import.meta.url), 'utf8'))
    const migration = await readFile(new URL('../../db/migrations/011-performance-notifications.sql', import.meta.url), 'utf8')
    await db.exec(migration); await db.exec(migration)
    const payloads: string[] = []
    const unlisten = await db.listen('lingxios_work', payload => { payloads.push(payload) })
    const insert = `INSERT INTO lingxios.agent_work_items(id,tenant_id,agent_id,session_id,kind,lane,trigger_ref)
      VALUES('w','t','a','s','turn','background','m')`
    await db.exec('BEGIN')
    await db.exec(insert)
    assert.deepEqual(payloads, [])
    await db.exec('ROLLBACK')
    assert.deepEqual(payloads, [])
    await db.exec('BEGIN')
    await db.exec(insert)
    await db.exec('COMMIT')
    assert.deepEqual(payloads, [''])
    const query: SqlPool['query'] = async (sql, params) => {
      const result = await db.query<Record<string, unknown>>(sql, params)
      return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
    }
    const pool: SqlPool = { query, connect: async () => ({ query, release() {} }) }
    const store = new PgWorkStore(pool)
    assert.equal(await store.claim('worker', 'interactive-claim', ['turn'], ['interactive']), null)
    assert.deepEqual(payloads, [''])
    await assert.rejects(store.claim('worker', 'interactive-claim', ['turn'], ['background']), /lanes changed/)
    assert.equal((await store.claim('worker', 'background-claim', ['turn'], ['background']))?.id, 'w')
    assert.deepEqual(payloads, ['', ''])
    assert.equal(await store.claim('worker'), null)
    assert.deepEqual(payloads, ['', ''])
    await unlisten()
  } finally { await db.close() }
})
