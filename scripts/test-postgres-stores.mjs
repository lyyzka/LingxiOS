import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { readFile } from 'node:fs/promises'
import { createRequire } from 'node:module'
import { resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { PgActionLedger, PgSessionStore, PgWorkStore } from '../dist/src/control-plane/pg-store.js'
import { hashToken } from '../dist/src/control-plane/memory-store.js'

const connectionString = process.env.LINGXIOS_TEST_DATABASE_URL
if (!connectionString) throw new Error('LINGXIOS_TEST_DATABASE_URL must name an empty disposable PostgreSQL database')
const source = resolve(process.argv[2] ?? process.env.LINGXILOOP_SOURCE ?? fileURLToPath(new URL('../../LingxiLoop/server/src', import.meta.url)))
const { Pool } = createRequire(resolve(source, '../package.json'))('pg')
let pool = new Pool({ connectionString, max: 8, connectionTimeoutMillis: 5000 })
try {
  const existing = await pool.query("SELECT tablename FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema')")
  assert.equal(existing.rows.length, 0, 'store checks require an empty disposable PostgreSQL database')
  await pool.query("CREATE TABLE public.agent_work_items(company_id text); INSERT INTO public.agent_work_items VALUES('untouched')")
  await pool.query(await readFile(new URL('../db/schema.sql', import.meta.url), 'utf8'))
  let workStore = new PgWorkStore(pool)
  const input = { tenantId: 'tenant', agentId: 'agent', principalId: 'human', sessionId: 'session', kind: 'turn', lane: 'interactive', triggerRef: 'message' }
  await workStore.enqueue({ ...input, id: 'first' })
  const claims = await Promise.all([workStore.claim('worker-a'), workStore.claim('worker-b')])
  const leased = claims.filter(Boolean)
  assert.equal(leased.length, 1, 'one session must not execute on two workers')
  const original = leased[0]
  await workStore.enqueue({ ...input, id: 'second' })
  assert.equal(await workStore.claim(claims[0] ? 'worker-a' : 'worker-b'), null)
  assert.equal((await pool.query('SELECT * FROM lingxios.agent_os_session_leases')).rows.length, 1)
  const intent = { workId: original.id, tenantId: 'tenant', principalId: 'human', agentId: 'agent', sessionId: 'session', threadId: null, requestVersion: null,
    action: { runId: original.id, cellId: 'cell', callIndex: 0, action: 'files.save', args: { body: 'content' }, idempotencyKey: 'action' } }
  const ledger = new PgActionLedger(pool)
  const reservations = await Promise.all(Array.from({ length: 8 }, () => ledger.reserve('action', 'fingerprint', intent)))
  assert.equal(reservations.filter(value => value === 'started').length, 1)
  assert.equal(reservations.filter(value => value === 'existing').length, 7)
  await ledger.record('action', { ok: false, executionState: 'unknown', error: 'acknowledgement lost' })
  const sessions = new PgSessionStore(pool)
  const session = { key: JSON.stringify(['tenant', 'agent', 'session', null]), tenantId: 'tenant', agentId: 'agent', sessionId: 'session', history: [], appliedWorkIds: [original.id], revision: 0, compactionEpoch: 0 }
  const saves = await Promise.all([sessions.save(session), sessions.save(session)])
  assert.equal(saves.filter(result => result.ok).length, 1)
  assert.equal(saves.filter(result => result.conflict).length, 1)
  await pool.query("UPDATE lingxios.agent_work_items SET lease_expires_at=NOW()-INTERVAL '1 minute' WHERE status='leased'")
  await pool.query("UPDATE lingxios.agent_os_session_leases SET expires_at=NOW()-INTERVAL '1 minute'")
  await pool.query("UPDATE lingxios.agent_os_workers SET last_seen_at=NOW()-INTERVAL '1 day'")
  // The replacement process receives no live store or connection objects.
  await pool.end()
  pool = undefined
  const replacement = spawnSync(process.execPath, ['--input-type=module'], {
    encoding: 'utf8', timeout: 15_000,
    input: `
      import assert from 'node:assert/strict'
      import { createRequire } from 'node:module'
      import { PgActionLedger, PgSessionStore, PgWorkStore } from ${JSON.stringify(new URL('../dist/src/control-plane/pg-store.js', import.meta.url).href)}
      const { Pool } = createRequire(${JSON.stringify(resolve(source, '../package.json'))})('pg')
      const pool = new Pool({ connectionString: process.env.LINGXIOS_TEST_DATABASE_URL, max: 2, connectionTimeoutMillis: 5000 })
      try {
        const recovered = await new PgWorkStore(pool).claim('replacement')
        assert.equal(recovered.id, ${JSON.stringify(original.id)})
        assert.equal(recovered.fence, ${original.fence + 1})
        assert.equal(recovered.homeEpoch, ${original.homeEpoch + 1})
        assert.deepEqual(await new PgActionLedger(pool).find('action'), { ok: false, executionState: 'unknown', error: 'acknowledgement lost' })
        assert.deepEqual(await new PgActionLedger(pool).findIntent('action'), ${JSON.stringify(intent)})
        assert.equal((await new PgSessionStore(pool).get(${JSON.stringify(session.key)})).revision, 1)
        process.stdout.write(JSON.stringify(recovered))
      } finally { await pool.end() }
    `,
  })
  assert.ifError(replacement.error)
  assert.equal(replacement.status, 0, replacement.stderr)
  const recovered = JSON.parse(replacement.stdout)
  pool = new Pool({ connectionString, max: 8, connectionTimeoutMillis: 5000 })
  workStore = new PgWorkStore(pool)
  assert.equal(recovered.id, original.id)
  assert.equal(recovered.fence, original.fence + 1)
  assert.equal(recovered.homeEpoch, original.homeEpoch + 1)
  assert.equal(await workStore.heartbeat(original.id, original.fence, hashToken(original.leaseToken)), null)
  assert.equal(await workStore.complete(original.id, original.fence, hashToken(original.leaseToken), { status: 'failed' }), false)
  assert.deepEqual(await new PgActionLedger(pool).find('action'), { ok: false, executionState: 'unknown', error: 'acknowledgement lost' })
  assert.deepEqual(await new PgActionLedger(pool).findIntent('action'), intent)
  assert.equal((await new PgSessionStore(pool).get(session.key)).revision, 1)
  assert.equal(await workStore.requestCancel(recovered.id), true)
  assert.equal(await workStore.complete(recovered.id, recovered.fence, hashToken(recovered.leaseToken), { status: 'cancelled' }), true)
  const next = await workStore.claim('replacement')
  assert.equal(next.id, original.id === 'first' ? 'second' : 'first')
  await workStore.enqueue({ ...input, id: 'thread-a', threadId: 'thread-a' })
  await workStore.enqueue({ ...input, id: 'thread-b', threadId: 'thread-b' })
  const independent = await Promise.all([workStore.claim('thread-worker-a'), workStore.claim('thread-worker-b')])
  assert.deepEqual(independent.map(item => item.id).sort(), ['thread-a', 'thread-b'])
  assert.deepEqual((await pool.query('SELECT * FROM public.agent_work_items')).rows, [{ company_id: 'untouched' }])
  console.log('Real PostgreSQL stores passed: competing claims, session exclusion, concurrent intent reservation/CAS, separate-process recovery, stale fencing, cancellation and product-table isolation.')
} finally { await pool?.end() }
