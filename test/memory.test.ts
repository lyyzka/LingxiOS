import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { it } from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import { snapshotMemories, recallMemories, writeMemory } from '../src/memory/store.js'
import { captureMemoryEvidence } from '../src/memory/evidence.js'
import { checkStorage } from '../src/app/storage.js'
import { reviseRun, cancelRun } from '../src/app/jobs.js'
import { createResponseEnvelope } from '../src/outcome/envelope.js'
import { snapshotEvidence } from '../src/context/evidence.js'
import type { SqlPool } from '../src/control-plane/pg-store.js'
import { sessionKeyOf, type WorkItem } from '../src/protocol/types.js'

const scopes = [{ tenantId: 't', scopeType: 'learner', scopeId: 'u' }, { tenantId: 't', scopeType: 'course', scopeId: 's' }, { tenantId: 't', scopeType: 'agent_role', scopeId: 'a' }]

it('persists scoped memory provenance, filters expiry and rejects stale verification', async () => {
  const db = new PGlite()
  const pool: SqlPool = { query: async (sql, args) => {
    const result = await db.query<Record<string, unknown>>(sql, args)
    return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
  }, connect: async () => ({ query: pool.query, release: () => {} }) }
  const work: WorkItem = { id: 'w', tenantId: 't', agentId: 'a', principalId: 'u', sessionId: 's',
    kind: 'turn', lane: 'interactive', triggerRef: 'm', fence: 1, homeEpoch: 1, leaseToken: 'test' }
  try {
    await db.exec(await readFile(new URL('../../db/schema.sql', import.meta.url), 'utf8'))
    await db.exec(`      INSERT INTO lingxios.agent_work_items(id,tenant_id,agent_id,principal_id,session_id,kind,lane,trigger_ref,fence,status,lease_expires_at)
        VALUES('w','t','a','u','s','turn','interactive','m',1,'leased',NOW()+INTERVAL '1 hour')`)
    const request = { version: 1 as const, tenantId: 't', sessionId: 's', workId: 'w', sourceRef: 'm', authorId: 'u',
      originalText: 'Remember that I prefer visual examples.' + 'x'.repeat(17_000), revisions: [], attachments: [], evidence: snapshotEvidence('e', []) }
    await db.query(`INSERT INTO lingxios.agent_os_sessions(session_key,tenant_id,agent_id,session_id,request_snapshot)
      VALUES($1,'t','a','s',$2::jsonb)`, [sessionKeyOf(work), JSON.stringify(request)])
    const [scope, course, role] = scopes
    const provenance = { actionId: 'remember', workId: work.id, request }
    const learner = await writeMemory(pool, scope!, { method: 'note', body: 'Prefers visual examples' }, provenance)
    await writeMemory(pool, course!, { method: 'note', body: 'Working on fractions' }, { ...provenance, actionId: 'course' })
    await writeMemory(pool, role!, { method: 'note', body: 'Use diagrams' }, { ...provenance, actionId: 'role' })
    const recalled = snapshotMemories(await Promise.all(scopes.map(async scope => ({ scope, items: await recallMemories(pool, scope, '', 12) }))))
    assert.deepEqual(recalled.items.map(item => item['scopeType']), ['learner','course','agent_role'])
    assert.equal((await recallMemories(pool, scope!, 'VISUAL', 12)).length, 1)
    for (const override of [{ tenantId: 'other' }, { scopeType: 'course' }, { scopeId: 'other' }]) {
      assert.deepEqual(await recallMemories(pool, { ...scope!, ...override }, '', 12), [])
    }
    const sources = learner['source_refs'] as Array<Record<string, unknown>>
    assert.deepEqual(Object.keys(sources[0]!).sort(), ['actionId','authorId','inputSha256','requestVersion','sourceRef','workId'])
    assert.equal(sources[0]!['requestVersion'], 1)
    assert.match(String(sources[0]!['inputSha256']), /^[a-f0-9]{64}$/)
    await assert.rejects(writeMemory(pool, scope!, { method: 'note', body: 'Bad expiry', validUntil: '2099-01-01T00:00:00' }, provenance), /future ISO timestamp/)
    await assert.rejects(writeMemory(pool, { ...scope!, tenantId: 'other' }, { method: 'note', body: 'Other tenant' }, provenance), /provenance/)
    await db.query("UPDATE lingxios.agent_memories SET valid_until=NOW()-INTERVAL '1 second' WHERE id=$1", [learner['id']])
    assert.deepEqual(await recallMemories(pool, scope!, '', 12), [])
    const mutation = { id: String(learner['id']), expectedVersion: 1 }
    await assert.rejects(writeMemory(pool, scope!, { ...mutation, method: 'verify' }, provenance), /unavailable/)
    const refreshed = await writeMemory(pool, scope!, { ...mutation, method: 'verify', validUntil: '2099-01-01T00:00:00Z' }, { ...provenance, actionId: 'verify' })
    assert.equal(refreshed['version'], 2)
    assert.equal((refreshed['source_refs'] as unknown[]).length, 2)
    await assert.rejects(writeMemory(pool, scope!, { ...mutation, method: 'delete' }, provenance), /stale/)
    await assert.rejects(writeMemory(pool, role!, { ...mutation, expectedVersion: 2, method: 'verify' }, provenance), /unavailable/)
    const pinned = await writeMemory(pool, scope!, { ...mutation, expectedVersion: 2, method: 'pin', pinned: true }, { ...provenance, actionId: 'pin' })
    assert.equal(pinned['version'], 3)
    assert.equal(pinned['pinned'], true)
    const versions = await db.query<{ version: number; snapshot: Record<string, unknown> }>(
      'SELECT version,snapshot FROM lingxios.agent_memory_versions WHERE tenant_id=$1 AND memory_id=$2 ORDER BY version', ['t', learner['id']])
    assert.deepEqual(versions.rows.map(row => [row.version, row.snapshot['body'], row.snapshot['pinned']]),
      [[1, 'Prefers visual examples', false], [2, 'Prefers visual examples', false]])
    await assert.rejects(writeMemory(pool, course!, { ...mutation, expectedVersion: 3, method: 'delete' }, provenance), /unavailable/)
    assert.deepEqual(await writeMemory(pool, scope!, { ...mutation, expectedVersion: 3, method: 'delete' }, provenance), { id: learner['id'], deleted: true })
    assert.deepEqual((await db.query('SELECT version FROM lingxios.agent_memory_versions WHERE memory_id=$1', [learner['id']])).rows, [])
    const body = 'x' + '🧠'.repeat(9000)
    const message = { version: 2 as const, runId: work.id, agentId: work.agentId, sessionId: work.sessionId, body,
      envelope: createResponseEnvelope(body, { status: 'partial', verification: 'not_run', requestVersion: 1 }, snapshotEvidence('e', [])) }
    await db.exec('BEGIN')
    await captureMemoryEvidence(pool, work, message, scopes)
    await db.exec('ROLLBACK')
    assert.deepEqual((await db.query('SELECT source_run_id FROM lingxios.agent_memory_evidence')).rows, [])
    assert.deepEqual((await db.query("SELECT id FROM lingxios.agent_work_items WHERE kind='memory_synthesis'")).rows, [])
    await db.exec('BEGIN')
    await captureMemoryEvidence(pool, work, message, scopes)
    await db.exec('COMMIT')
    await captureMemoryEvidence(pool, work, message, scopes)
    assert.deepEqual((await db.query("SELECT id,lane,meta FROM lingxios.agent_work_items WHERE kind='memory_synthesis'")).rows,
      [{ id: 'memory-synthesis:w', lane: 'background', meta: { sourceRunId: 'w' } }])
    const evidence = (await db.query<Record<string, unknown>>('SELECT * FROM lingxios.agent_memory_evidence')).rows
    assert.equal(evidence.length, 1)
    assert.equal(evidence[0]!['input_truncated'], true)
    assert.equal(evidence[0]!['assistant_truncated'], true)
    assert.equal(String(evidence[0]!['assistant_text']).length, 15_999)
    assert.equal(evidence[0]!['status'], 'pending')
    assert.equal(evidence[0]!['request_version'], 1)
    await assert.rejects(captureMemoryEvidence(pool, work, { ...message, envelope: { ...message.envelope, requestVersion: 2 } }, scopes), /committed request version/)
    const evidenceStatus = async () => (await db.query<{ status: string }>('SELECT status FROM lingxios.agent_memory_evidence')).rows[0]!.status
    const identity = { runId: work.id, tenantId: work.tenantId, agentId: work.agentId, sessionId: work.sessionId, principalId: work.principalId! }
    await db.exec('BEGIN')
    assert.equal(await reviseRun(pool, identity, 'Disregard the earlier preference'), true)
    assert.equal(await evidenceStatus(), 'superseded')
    await db.exec('ROLLBACK')
    assert.equal(await evidenceStatus(), 'pending')
    await db.exec('BEGIN')
    assert.equal(await cancelRun(pool, identity), true)
    assert.equal(await evidenceStatus(), 'superseded')
    await db.exec('ROLLBACK')
    // Ordinary completion and a later request's session snapshot do not revoke a valid source.
    await db.exec("UPDATE lingxios.agent_work_items SET status='succeeded' WHERE id='w'")
    await db.exec(`INSERT INTO lingxios.agent_work_items(id,tenant_id,agent_id,principal_id,session_id,kind,lane,trigger_ref)
      VALUES('later-work','t','a','u','s','turn','interactive','later')`)
    await db.exec(`UPDATE lingxios.agent_os_sessions SET request_snapshot=jsonb_set(request_snapshot,'{workId}','"later-work"')`)
    assert.equal(await evidenceStatus(), 'pending')
    await db.exec("UPDATE lingxios.agent_work_items SET status='leased' WHERE id='w'")
    await db.exec("UPDATE lingxios.agent_work_items SET cancel_requested_at=NOW() WHERE id='w'")
    assert.equal(await evidenceStatus(), 'superseded')
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
