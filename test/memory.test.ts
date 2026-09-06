import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { it } from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import { executeMemory, recallMemoryContext } from '../src/integrations/lingxiloop/memory.js'
import { snapshotMemories } from '../src/memory/store.js'
import { captureMemoryEvidence } from '../src/memory/evidence.js'
import { checkStorage } from '../src/app/storage.js'
import { PgWorkStore } from '../src/control-plane/pg-store.js'
import { createResponseEnvelope } from '../src/outcome/envelope.js'
import { snapshotEvidence } from '../src/context/evidence.js'
import type { SqlPool } from '../src/control-plane/pg-store.js'
import { sessionKeyOf, type WorkItem } from '../src/protocol/types.js'

it('persists scoped memory provenance, filters expiry and rejects stale or unauthorized verification', async () => {
  const db = new PGlite()
  const pool: SqlPool = { query: async (sql, args) => {
    const result = await db.query<Record<string, unknown>>(sql, args)
    return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
  }, connect: async () => ({ query: pool.query, release: () => {} }) }
  const work: WorkItem = { id: 'w', tenantId: 't', agentId: 'a', principalId: 'u', sessionId: 's',
    kind: 'turn', lane: 'interactive', triggerRef: 'm', fence: 1, homeEpoch: 1, leaseToken: 'test' }
  let denied = false
  const services = { permissionService: { assertCan: async () => { if (denied) throw new Error('permission denied') } } }
  let index = 0
  const run = (method: string, args: Record<string, unknown>, scope = work) => {
    const cellId = `c${index++}`
    return executeMemory(scope, { runId: scope.id, cellId, callIndex: 0, idempotencyKey: JSON.stringify([scope.id, cellId, 0]),
      action: `memory.${method}`, args }, services, pool)
  }
  try {
    await db.exec(await readFile(new URL('../../db/schema.sql', import.meta.url), 'utf8'))
    await db.exec(`CREATE TABLE participants(id text,company_id text,kind text,departed_at timestamptz);
      CREATE TABLE im_channel_bindings(company_id text,channel_id text,profile jsonb);
      INSERT INTO participants VALUES('u','t','human',NULL),('outsider','t','human',NULL);
      INSERT INTO im_channel_bindings VALUES('t','s','{"members":["u","a","b"]}');
      INSERT INTO lingxios.agent_work_items(id,tenant_id,agent_id,principal_id,session_id,kind,lane,trigger_ref,fence,status,lease_expires_at)
        VALUES('w','t','a','u','s','turn','interactive','m',1,'leased',NOW()+INTERVAL '1 hour')`)
    const request = { workId: 'w', sourceRef: 'm', authorId: 'u', originalText: 'Remember that I prefer visual examples.' + 'x'.repeat(17_000), revisions: [] }
    await db.query(`INSERT INTO lingxios.agent_os_sessions(session_key,tenant_id,agent_id,session_id,request_snapshot)
      VALUES($1,'t','a','s',$2::jsonb)`, [sessionKeyOf(work), JSON.stringify(request)])
    const learner = await run('note', { scope: 'learner', learnerId: 'u', body: 'Prefers visual examples' }) as Record<string, unknown>
    await run('note', { scope: 'course', body: 'Working on fractions' })
    await run('note', { scope: 'agent_role', body: 'Use diagrams' })
    const recalled = await recallMemoryContext(work, services, pool)
    assert.equal(recalled.status, 'available')
    assert.deepEqual(recalled.items.map(item => item['scopeType']), ['learner', 'course', 'agent_role'])
    assert.ok(recalled.items.every(item => Number(item['version']) === 1 && Array.isArray(item['source_refs'])))
    await assert.rejects(run('note', { body: 'Ambiguous expiry', validUntil: '2099-01-01T00:00:00' }), /future ISO timestamp/)
    assert.equal((await run('recall', { scope: 'learner', learnerId: 'u', query: 'VISUAL' }, { ...work, agentId: 'b' }) as unknown[]).length, 1)
    assert.deepEqual(await run('list', { scope: 'course' }, { ...work, sessionId: 'other' }), [])
    assert.deepEqual(await run('list', { scope: 'agent_role' }, { ...work, agentId: 'b' }), [])
    assert.deepEqual(await run('list', { scope: 'course' }, { ...work, tenantId: 'other' }), [])
    await assert.rejects(run('list', { scope: 'learner', learnerId: 'outsider' }), /active human member/)
    const sources = learner['source_refs'] as Array<Record<string, unknown>>
    assert.deepEqual(Object.keys(sources[0]!).sort(), ['authorId', 'inputSha256', 'requestVersion', 'sourceRef', 'workId'])
    assert.equal(sources[0]!['requestVersion'], 1)
    assert.match(String(sources[0]!['inputSha256']), /^[a-f0-9]{64}$/)
    await db.query("UPDATE lingxios.agent_memories SET valid_until=NOW()-INTERVAL '1 second' WHERE id=$1", [learner['id']])
    assert.deepEqual(await run('list', { scope: 'learner', learnerId: 'u' }), [])
    const verification = { scope: 'learner', learnerId: 'u', id: learner['id'], expectedVersion: 1 }
    await assert.rejects(run('verify', verification), /unavailable/)
    const refreshed = await run('verify', { ...verification, validUntil: '2099-01-01T00:00:00Z' }) as Record<string, unknown>
    assert.equal(refreshed['version'], 2)
    assert.equal((refreshed['source_refs'] as unknown[]).length, 2)
    await assert.rejects(run('verify', { ...verification, validUntil: '2099-01-01T00:00:00Z' }), /stale/)
    await assert.rejects(run('verify', { scope: 'agent_role', id: learner['id'], expectedVersion: 2 }), /unavailable/)
    const otherLearnerNote = await run('note', { scope: 'learner', learnerId: 'u', body: 'A newer unpinned note' }) as Record<string, unknown>
    const mutation = { scope: 'learner', learnerId: 'u', id: learner['id'], expectedVersion: 2 }
    await assert.rejects(run('pin', { ...mutation, pinned: 'true' }), /boolean/)
    const pinned = await run('pin', { ...mutation, pinned: true }) as Record<string, unknown>
    assert.equal(pinned['version'], 3)
    assert.equal(pinned['pinned'], true)
    assert.equal((await run('list', { scope: 'learner', learnerId: 'u' }) as Array<Record<string, unknown>>)[0]!['id'], learner['id'])
    await assert.rejects(run('delete', mutation), /stale/)
    await assert.rejects(run('delete', { scope: 'course', id: learner['id'], expectedVersion: 3 }), /unavailable/)
    denied = true
    await assert.rejects(run('list', { scope: 'learner', learnerId: 'u' }), /permission denied/)
    await assert.rejects(run('note', { body: 'Must not persist' }), /permission denied/)
    await assert.rejects(run('delete', { ...mutation, expectedVersion: 3 }), /permission denied/)
    denied = false
    const versions = await db.query<{ version: number; snapshot: Record<string, unknown> }>(
      'SELECT version,snapshot FROM lingxios.agent_memory_versions WHERE tenant_id=$1 AND memory_id=$2 ORDER BY version', ['t', learner['id']])
    assert.deepEqual(versions.rows.map(row => [row.version, row.snapshot['body'], row.snapshot['pinned']]),
      [[1, 'Prefers visual examples', false], [2, 'Prefers visual examples', false]])
    assert.deepEqual(await run('delete', { ...mutation, expectedVersion: 3 }), { id: learner['id'], deleted: true })
    assert.deepEqual((await db.query('SELECT version FROM lingxios.agent_memory_versions WHERE memory_id=$1', [learner['id']])).rows, [])
    assert.deepEqual((await run('list', { scope: 'learner', learnerId: 'u' }) as Array<Record<string, unknown>>).map(item => item['id']), [otherLearnerNote['id']])
    const body = 'x' + '🧠'.repeat(9000)
    const message = { version: 2 as const, runId: work.id, agentId: work.agentId, sessionId: work.sessionId, body,
      envelope: createResponseEnvelope(body, { status: 'partial', verification: 'not_run', requestVersion: 1 }, snapshotEvidence('e', [])) }
    await db.exec('BEGIN')
    await db.query("INSERT INTO lingxios.agent_messages(run_id,tenant_id,agent_id,session_id,message) VALUES('w','t','a','s',$1::jsonb)", [JSON.stringify(message)])
    await captureMemoryEvidence(pool, work, message)
    await db.exec('ROLLBACK')
    assert.deepEqual((await db.query('SELECT source_run_id FROM lingxios.agent_memory_evidence')).rows, [])
    assert.deepEqual((await db.query("SELECT id FROM lingxios.agent_work_items WHERE kind='memory_synthesis'")).rows, [])
    await db.exec('BEGIN')
    await db.query("INSERT INTO lingxios.agent_messages(run_id,tenant_id,agent_id,session_id,message) VALUES('w','t','a','s',$1::jsonb)", [JSON.stringify(message)])
    await captureMemoryEvidence(pool, work, message)
    await db.exec('COMMIT')
    await captureMemoryEvidence(pool, work, message)
    assert.deepEqual((await db.query("SELECT id,lane,meta FROM lingxios.agent_work_items WHERE kind='memory_synthesis'")).rows,
      [{ id: 'memory-synthesis:w', lane: 'background', meta: { sourceRunId: 'w' } }])
    const evidence = (await db.query<Record<string, unknown>>('SELECT * FROM lingxios.agent_memory_evidence')).rows
    assert.equal(evidence.length, 1)
    assert.equal(evidence[0]!['input_truncated'], true)
    assert.equal(evidence[0]!['assistant_truncated'], true)
    assert.equal(String(evidence[0]!['assistant_text']).length, 15_999)
    assert.equal(evidence[0]!['status'], 'pending')
    assert.equal(evidence[0]!['request_version'], 1)
    await assert.rejects(captureMemoryEvidence(pool, work, { ...message, envelope: { ...message.envelope, requestVersion: 2 } }), /committed request version/)
    const evidenceStatus = async () => (await db.query<{ status: string }>('SELECT status FROM lingxios.agent_memory_evidence')).rows[0]!.status
    const workStore = new PgWorkStore(pool)
    await db.exec('BEGIN')
    assert.equal(await workStore.addSteer(work.id, 'Disregard the earlier preference'), true)
    assert.equal(await evidenceStatus(), 'superseded')
    await db.exec('ROLLBACK')
    assert.equal(await evidenceStatus(), 'pending')
    await db.exec('BEGIN')
    assert.equal(await workStore.requestCancel(work.id), true)
    assert.equal(await evidenceStatus(), 'superseded')
    await db.exec('ROLLBACK')
    // Ordinary completion and a later request's session snapshot do not revoke a valid source.
    await db.exec("UPDATE lingxios.agent_work_items SET status='completed' WHERE id='w'")
    await db.exec(`UPDATE lingxios.agent_os_sessions SET request_snapshot=jsonb_set(request_snapshot,'{workId}','"later-work"')`)
    assert.equal(await evidenceStatus(), 'pending')
    await db.exec("UPDATE lingxios.agent_work_items SET status='leased' WHERE id='w'")
    await db.exec("UPDATE lingxios.agent_work_items SET cancel_requested_at=NOW() WHERE id='w'")
    assert.equal(await evidenceStatus(), 'superseded')
    await assert.rejects(run('note', { body: 'Cancelled write' }), /current leased request/)
    assert.equal((await db.query('SELECT id FROM lingxios.agent_memories')).rows.length, 3)
    await checkStorage(pool)
    await db.exec('ALTER TABLE lingxios.agent_memories DISABLE TRIGGER agent_memory_version_history')
    await assert.rejects(checkStorage(pool), /version history trigger/)
    await db.exec('ALTER TABLE lingxios.agent_memories ENABLE TRIGGER agent_memory_version_history')
    await db.exec('ALTER TABLE lingxios.agent_work_items DISABLE TRIGGER agent_work_memory_evidence')
    await assert.rejects(checkStorage(pool), /invalidation trigger/)
  } finally { await db.close() }
})

it('shares the memory context budget across scopes without truncating individual records', () => {
  const groups = (['learner', 'course', 'agent_role'] as const).map(scopeType => ({
    scope: { tenantId: 't', scopeType, scopeId: scopeType },
    items: Array.from({ length: 12 }, (_, i) => ({ id: `${scopeType}-${i}`, body: '字'.repeat(1500), version: 1 })),
  }))
  const snapshot = snapshotMemories(groups)
  assert.ok(Buffer.byteLength(JSON.stringify(snapshot)) <= 12_000)
  assert.equal(snapshot.items.length + snapshot.omitted, 36)
  assert.ok(snapshot.items.every(item => String(item['body']).length === 1500))
  assert.equal(snapshotMemories(groups).id, snapshot.id)
  groups[0]!.items[0]!.version = 2
  assert.notEqual(snapshotMemories(groups).id, snapshot.id)
})
