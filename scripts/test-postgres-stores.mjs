import assert from 'node:assert/strict'
import { spawnSync } from 'node:child_process'
import { readFile } from 'node:fs/promises'
import { Pool } from 'pg'
import { PgActionLedger, PgSessionStore, PgWorkStore, withTransaction } from '../dist/src/control-plane/pg-store.js'
import { hashToken } from '../dist/src/control-plane/memory-store.js'
import { checkStorage } from '../dist/src/app/storage.js'
import { forgetMemoryScope } from '../dist/src/memory/forget.js'
import { writeMemory } from '../dist/src/memory/store.js'
import { createMemoryService } from '../dist/src/memory/service.js'
import { captureMemoryEvidence,scheduleMemoryReflection } from '../dist/src/memory/evidence.js'
import { sessionKeyOf } from '../dist/src/protocol/types.js'

const connectionString = process.env.LINGXIOS_TEST_DATABASE_URL
if (!connectionString) throw new Error('LINGXIOS_TEST_DATABASE_URL must name an empty disposable PostgreSQL database')
let pool = new Pool({ connectionString, max: 8, connectionTimeoutMillis: 5000 })
try {
  const existing = await pool.query("SELECT tablename FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema')")
  assert.equal(existing.rows.length, 0, 'store checks require an empty disposable PostgreSQL database')
  await pool.query("CREATE TABLE public.agent_work_items(company_id text); INSERT INTO public.agent_work_items VALUES('untouched')")
  await pool.query(await readFile(new URL('../test/fixtures/schema-8.sql', import.meta.url), 'utf8'))
  await pool.query(`DROP TABLE lingxios.agent_memory_scopes;
    ALTER TABLE lingxios.agent_memory_evidence DROP COLUMN scope_epochs;
    ALTER TABLE lingxios.agent_approvals DROP COLUMN tool_contract_hash;
    UPDATE lingxios.schema_version SET version=7`)
  await pool.query(await readFile(new URL('../db/migrations/008-governance.sql', import.meta.url), 'utf8'))
  await pool.query("INSERT INTO lingxios.agent_work_items(id,tenant_id,agent_id,session_id,kind,lane,trigger_ref,status) VALUES('preserved','migration','agent','s','turn','interactive','original','succeeded')")
  await pool.query(await readFile(new URL('../db/migrations/009-cognitive-memory-reset.sql', import.meta.url), 'utf8'))
  assert.equal((await pool.query("SELECT trigger_ref FROM lingxios.agent_work_items WHERE id='preserved'")).rows[0].trigger_ref,'original')
  await assert.rejects(pool.query(await readFile(new URL('../db/migrations/009-cognitive-memory-reset.sql', import.meta.url),'utf8')),/schema version 8/)
  await pool.query('ROLLBACK')
  await checkStorage(pool)
  const memoryScope={tenantId:'concurrency',scopeType:'user',scopeId:'human'}
  const memoryIdentity={tenantId:'concurrency',agentId:'agent',principalId:'human',sessionId:'admin'}
  const memoryOptions={resolveScopes:async()=>[memoryScope]}
  const memory=createMemoryService(pool,memoryOptions).api
  const document={path:'preferences.md',title:'Preference',description:'Saved preference',body:'中文图表 English diagrams',layer:'core'}
  const saved=(await memory.initialize(memoryIdentity,{scope:memoryScope,documents:[document],sourceRef:'settings',idempotencyKey:'initial'})).documents[0]
  const updates=await Promise.allSettled([1,2].map(n=>memory.apply(memoryIdentity,{scope:memoryScope,sourceRef:'settings',idempotencyKey:`update-${n}`,
    changes:[{action:'update',id:saved.id,expectedVersion:1,content:{...document,body:`Version ${n}`}}]})))
  assert.equal(updates.filter(result=>result.status==='fulfilled').length,1)
  assert.equal(updates.filter(result=>result.status==='rejected').length,1)
  const restore={scope:memoryScope,id:saved.id,expectedVersion:2,version:1,sourceRef:'settings',idempotencyKey:'restore'}
  const restored=await Promise.all([memory.restore(memoryIdentity,restore),memory.restore(memoryIdentity,restore)])
  assert.deepEqual(restored[0],restored[1])
  assert.equal(restored[0].documents[0].version,3)
  for(let n=0;n<5;n++) {
    const work={id:`evidence-${n}`,...memoryIdentity,sessionId:`session-${n}`,kind:'turn',lane:'interactive',triggerRef:`source-${n}`,fence:0,homeEpoch:1}
    await pool.query(`INSERT INTO lingxios.agent_work_items(id,tenant_id,agent_id,principal_id,session_id,kind,lane,trigger_ref,status)
      VALUES($1,$2,$3,$4,$5,'turn','interactive',$6,'succeeded')`,[work.id,work.tenantId,work.agentId,work.principalId,work.sessionId,work.triggerRef])
    const request={version:1,workId:work.id,tenantId:work.tenantId,sessionId:work.sessionId,authorId:work.principalId,sourceRef:work.triggerRef,originalText:'喜欢图表',revisions:[],attachments:[],evidence:{}}
    await pool.query(`INSERT INTO lingxios.agent_os_sessions(session_key,tenant_id,agent_id,session_id,request_snapshot) VALUES($1,$2,$3,$4,$5::jsonb)`,
      [sessionKeyOf(work),work.tenantId,work.agentId,work.sessionId,JSON.stringify(request)])
    await withTransaction(pool,db=>captureMemoryEvidence(db,work,{body:'Understood',envelope:{requestVersion:1}},[memoryScope]))
  }
  const schedules=await Promise.all([scheduleMemoryReflection(pool,memoryOptions),scheduleMemoryReflection(pool,memoryOptions)])
  assert.equal(schedules.flatMap(result=>result.jobIds).length,1)
  const jobId=schedules.flatMap(result=>result.jobIds)[0]
  assert.equal((await pool.query('SELECT COUNT(*)::int AS count FROM lingxios.agent_memory_evidence_scopes WHERE job_id=$1',[jobId])).rows[0].count,5)
  await memory.forget(memoryIdentity,memoryScope)
  assert.equal((await pool.query('SELECT status FROM lingxios.agent_work_items WHERE id=$1',[jobId])).rows[0].status,'cancelled')
  await assert.rejects(memory.restore(memoryIdentity,{...restore,expectedVersion:3,idempotencyKey:'after-forget'}),/unavailable/)
  let workStore = new PgWorkStore(pool)
  const input = { tenantId: 'tenant', agentId: 'agent', principalId: 'human', sessionId: 'session', kind: 'turn', lane: 'interactive', triggerRef: 'message' }
  await workStore.enqueue({ ...input, id: 'first' })
  const claims = await Promise.all([workStore.claim('worker-a'), workStore.claim('worker-b')])
  const leased = claims.filter(Boolean)
  assert.equal(leased.length, 1, 'one session must not execute on two workers')
  const original = leased[0]
  const scope = { tenantId: 'tenant', scopeType: 'user', scopeId: 'human' }
  const provenance = { actionId: 'memory-before-forget', workId: original.id, request: { workId: original.id, tenantId: 'tenant', authorId: 'human',
    originalText: 'A preference', revisions: [], attachments: [], sourceRef: 'message' } }
  const writer = await pool.connect(), forgetter = await pool.connect()
  try {
    await writer.query('BEGIN')
    await writeMemory(writer, scope, { method: 'note', body: 'A preference' }, provenance)
    await forgetter.query('BEGIN')
    await forgetter.query("SET LOCAL lock_timeout='100ms'")
    await assert.rejects(forgetMemoryScope(forgetter, scope), error => error.code === '55P03')
    await forgetter.query('ROLLBACK')
    await writer.query('COMMIT')
  } finally { writer.release(); forgetter.release() }
  assert.deepEqual(await withTransaction(pool, client => forgetMemoryScope(client, scope)), { epoch: 1 })
  await assert.rejects(withTransaction(pool, client => writeMemory(client, scope, { method: 'note', body: 'Old source cannot return' },
    { ...provenance, actionId: 'memory-after-forget' })), /predates forgetting/)
  assert.equal((await pool.query('SELECT id FROM lingxios.agent_memories')).rows.length, 0)
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
      import { Pool } from 'pg'
      import { PgActionLedger, PgSessionStore, PgWorkStore } from ${JSON.stringify(new URL('../dist/src/control-plane/pg-store.js', import.meta.url).href)}
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
  console.log('Real PostgreSQL stores passed: schema 7→8→9 reset, document CAS/restore races, reflection deduplication, atomic memory forgetting, competing claims, session exclusion, concurrent intent reservation/CAS, separate-process recovery, stale fencing, cancellation and product-table isolation.')
} finally { await pool?.end() }
