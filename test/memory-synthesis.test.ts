import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { it } from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import { PgWorkStore, type SqlPool } from '../src/control-plane/pg-store.js'
import { sessionKeyOf, type WorkItem } from '../src/protocol/types.js'
import { captureMemoryEvidence, retryMemorySynthesis } from '../src/memory/evidence.js'
import { executeMemorySynthesis, parseMemoryChanges, type MemoryBatch } from '../src/memory/synthesis.js'
import { memorySynthesisProcessor } from '../src/memory/processor.js'
import { recallMemories } from '../src/memory/store.js'
import { createResponseEnvelope } from '../src/outcome/envelope.js'
import { snapshotEvidence } from '../src/context/evidence.js'
import type { WorkProcessorContext } from '../src/runtime/runtime.js'

it('executes durable memory synthesis with independent verification, fenced atomic writes and bounded retries', async () => {
  const db = new PGlite()
  const pool: SqlPool = { query: async (sql, args) => {
    const result = await db.query<Record<string, unknown>>(sql, args)
    return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
  }, connect: async () => ({ query: pool.query, release: () => {} }) }
  let sequence = 0
  const source = async (revisionTime?: string) => {
    const work: WorkItem = { id: `source-${sequence++}`, tenantId: 't', agentId: 'a', principalId: 'u', sessionId: 's',
      kind: 'turn', lane: 'interactive', triggerRef: 'm', fence: 1, homeEpoch: 1, leaseToken: 'test' }
    await db.query(`INSERT INTO lingxios.agent_work_items(id,tenant_id,agent_id,principal_id,session_id,kind,lane,trigger_ref,status)
      VALUES($1,'t','a','u','s','turn','interactive','m','completed')`, [work.id])
    const revisions = revisionTime ? [{ id: 'revision', text: 'I still prefer visual examples.', createdAt: revisionTime }] : []
    if (revisionTime) await db.query("UPDATE lingxios.agent_work_items SET created_at='2000-01-01',steer_inputs=$2::jsonb WHERE id=$1", [work.id, JSON.stringify(revisions)])
    const request = { workId: work.id, sourceRef: 'm', authorId: 'u', originalText: 'I prefer visual examples.', revisions, attachments: [] }
    await db.query(`INSERT INTO lingxios.agent_os_sessions(session_key,tenant_id,agent_id,session_id,request_snapshot)
      VALUES($1,'t','a','s',$2::jsonb) ON CONFLICT(session_key) DO UPDATE SET request_snapshot=EXCLUDED.request_snapshot`,
    [sessionKeyOf(work), JSON.stringify(request)])
    const message = { version: 2 as const, runId: work.id, agentId: 'a', sessionId: 's', body: 'Understood.',
      envelope: createResponseEnvelope('Understood.', { status: 'partial', verification: 'not_run', requestVersion: revisions.length + 1 }, snapshotEvidence('e', [])) }
    await db.query("INSERT INTO lingxios.agent_messages(run_id,tenant_id,agent_id,session_id,message) VALUES($1,'t','a','s',$2::jsonb)", [work.id, JSON.stringify(message)])
    await captureMemoryEvidence(pool, work, message)
    const job = { ...work, id: `memory-synthesis:${work.id}`, kind: 'memory_synthesis', lane: 'background' as const, meta: { sourceRunId: work.id } }
    await db.query("UPDATE lingxios.agent_work_items SET status='leased',fence=1,attempts=1,lease_expires_at=NOW()+INTERVAL '1 hour' WHERE id=$1", [job.id])
    return job
  }
  const load = (work: WorkItem) => executeMemorySynthesis(pool, work, 'load', {}) as Promise<MemoryBatch>
  const apply = (work: WorkItem, changes: unknown[], approved = true, confidence = 0.9) => executeMemorySynthesis(pool, work, 'apply', { changes, approved, confidence })
  const create = (work: WorkItem) => ({ action: 'create', scopeType: 'learner', sourceRunIds: [work.meta!['sourceRunId']], body: 'Prefers visual examples' })
  const status = async (work: WorkItem) => (await db.query<{ status: string }>('SELECT status FROM lingxios.agent_memory_evidence WHERE source_run_id=$1', [work.meta!['sourceRunId']])).rows[0]!.status
  try {
    await db.exec(await readFile(new URL('../../db/schema.sql', import.meta.url), 'utf8'))
    const first = await source()
    let calls = 0
    const events: Array<Record<string, unknown>> = []
    const context = { signal: new AbortController().signal, emit: async (event: Record<string, unknown>) => { events.push(event) },
      host: { executeAction: async (work: WorkItem, action: { action: string; args: Record<string, unknown> }) => ({ ok: true,
        value: await executeMemorySynthesis(pool, work, action.action.split('.')[1]!, action.args) }) },
      model: { structured: async (request: { instructions: string; input: unknown }) => {
        calls++
        if (calls === 2) {
          assert.match(request.instructions, /Independently audit/)
          assert.deepEqual((request.input as { changes: unknown[] }).changes, [create(first)])
        }
        return { value: calls === 1 ? { changes: [create(first)] } : { approved: true, confidence: 0.9 }, model: 'test',
          usage: { available: true, inputTokens: 100, outputTokens: 20 } }
      } },
    } as unknown as WorkProcessorContext
    await memorySynthesisProcessor.process(first, context)
    assert.equal(calls, 2)
    assert.deepEqual(events.at(-1)?.['data'], { result: { outcome: 'committed', changeCount: 1 } })
    assert.equal(await status(first), 'processed')
    assert.equal(await load(first), null)
    assert.deepEqual(await apply(first, [create(first)]), { outcome: 'processed', changeCount: 0 })
    const memory = (await db.query<{ id: string; scope_type: string; scope_id: string; origin: string; version: number; source_refs: Array<Record<string, unknown>> }>('SELECT * FROM lingxios.agent_memories')).rows[0]!
    assert.deepEqual([memory.scope_type, memory.scope_id, memory.origin, memory.version], ['learner', 'u', 'synthesized', 1])
    assert.deepEqual([memory.source_refs[0]!['workId'], memory.source_refs[0]!['synthesisWorkId'], memory.source_refs[0]!['confidence']], [first.meta!['sourceRunId'], first.id, 0.9])

    const update = await source()
    await load(update)
    const change = { ...create(update), action: 'update', id: memory.id, expectedVersion: 1, body: 'Prefers diagrams for examples' }
    await assert.rejects(apply(update, [{ ...create(update), sourceRunIds: ['foreign'] }]), /unknown evidence/)
    await assert.rejects(apply(update, [{ ...change, expectedVersion: 2 }]), /loaded snapshot/)
    await assert.rejects(apply({ ...update, fence: 2 }, [change]), /lease or source/)
    await assert.rejects(apply({ ...update, tenantId: 'other' }, [change]), /lease or source/)
    for (const protection of ["origin='explicit'", "origin='synthesized',pinned=TRUE"]) {
      await db.query(`UPDATE lingxios.agent_memories SET ${protection} WHERE id=$1`, [memory.id])
      await assert.rejects(apply(update, [create(update), change]), /protected/)
      assert.equal((await db.query('SELECT id FROM lingxios.agent_memories')).rows.length, 1)
      assert.equal(await status(update), 'pending')
    }
    await db.query('UPDATE lingxios.agent_memories SET pinned=FALSE,version=2 WHERE id=$1', [memory.id])
    await assert.rejects(apply(update, [create(update), change]), /stale/)
    assert.equal((await db.query('SELECT id FROM lingxios.agent_memories')).rows.length, 1)
    assert.deepEqual((await db.query('SELECT version FROM lingxios.agent_memory_versions WHERE memory_id=$1 ORDER BY version', [memory.id])).rows, [{ version: 1 }])
    await load(update)
    assert.deepEqual(await apply(update, [{ ...change, expectedVersion: 2 }]), { outcome: 'committed', changeCount: 1 })
    const expire = await source()
    await load(expire)
    assert.deepEqual(await apply(expire, [{ action: 'expire', scopeType: 'learner', sourceRunIds: [expire.meta!['sourceRunId']], id: memory.id, expectedVersion: 3 }]),
      { outcome: 'committed', changeCount: 1 })
    assert.deepEqual((await db.query('SELECT body,status,version FROM lingxios.agent_memories')).rows,
      [{ body: 'Prefers diagrams for examples', status: 'expired', version: 4 }])
    assert.deepEqual((await db.query("SELECT version,snapshot->>'body' AS body,snapshot->>'status' AS status FROM lingxios.agent_memory_versions WHERE memory_id=$1 ORDER BY version", [memory.id])).rows,
      [{ version: 1, body: 'Prefers visual examples', status: 'active' }, { version: 2, body: 'Prefers visual examples', status: 'active' },
        { version: 3, body: 'Prefers diagrams for examples', status: 'active' }])
    await assert.rejects(db.query('UPDATE lingxios.agent_memories SET version=1 WHERE id=$1', [memory.id]), /versions must increase/)
    assert.deepEqual(await recallMemories(pool, { tenantId: 't', scopeType: 'learner', scopeId: 'u' }, '', 12), [])
    const renewal = await source()
    const expiredSnapshot = await load(renewal)
    assert.equal(expiredSnapshot.currentMemories.find(row => row['id'] === memory.id)?.['needsReverification'], true)
    await assert.rejects(apply(renewal, [{ ...change, sourceRunIds: [renewal.meta!['sourceRunId']], expectedVersion: 4 }]), /unavailable/)
    const renewedChange = { ...change, sourceRunIds: [renewal.meta!['sourceRunId']], expectedVersion: 4, validUntil: '2099-01-01T00:00:00Z' }
    await db.query("UPDATE lingxios.agent_work_items SET created_at='2000-01-01' WHERE id=$1", [renewal.meta!['sourceRunId']])
    await assert.rejects(apply(renewal, [renewedChange]), /unavailable/)
    assert.equal(await status(renewal), 'pending')
    await db.query('UPDATE lingxios.agent_work_items SET created_at=NOW() WHERE id=$1', [renewal.meta!['sourceRunId']])
    assert.deepEqual(await apply(renewal, [renewedChange]), { outcome: 'committed', changeCount: 1 })
    assert.equal((await recallMemories(pool, { tenantId: 't', scopeType: 'learner', scopeId: 'u' }, '', 12))[0]!['version'], 5)
    await db.query("UPDATE lingxios.agent_memories SET status='expired',version=6,updated_at=NOW()-INTERVAL '1 minute' WHERE id=$1", [memory.id])
    for (const revisionTime of ['invalid', '2099-01-01T00:00:00Z', new Date().toISOString()]) {
      const revised = await source(revisionTime)
      await load(revised)
      const proposed = [{ ...renewedChange, expectedVersion: 6, sourceRunIds: [revised.meta!['sourceRunId']] }]
      if (revisionTime === 'invalid' || revisionTime.startsWith('2099')) await assert.rejects(apply(revised, proposed), /unavailable/)
      else assert.deepEqual(await apply(revised, proposed), { outcome: 'committed', changeCount: 1 })
    }
    assert.equal((await recallMemories(pool, { tenantId: 't', scopeType: 'learner', scopeId: 'u' }, '', 12))[0]!['version'], 7)

    for (const [approved, confidence] of [[false, 0.9], [true, 0.59]] as const) {
      const rejected = await source()
      await load(rejected)
      assert.deepEqual(await apply(rejected, [create(rejected)], approved, confidence), { outcome: 'rejected', changeCount: 0 })
      assert.equal(await status(rejected), 'rejected')
    }
    const cancelled = await source()
    await load(cancelled)
    await db.query('UPDATE lingxios.agent_work_items SET cancel_requested_at=NOW() WHERE id=$1', [cancelled.meta!['sourceRunId']])
    await assert.rejects(apply(cancelled, [create(cancelled)]), /lease or source/)
    assert.equal(await status(cancelled), 'superseded')
    assert.equal((await db.query('SELECT id FROM lingxios.agent_memories')).rows.length, 1)
    assert.throws(() => parseMemoryChanges([{ ...change, body: 'x'.repeat(501) }]), /body/)
    assert.throws(() => parseMemoryChanges([change, change]), /unique identity/)
    assert.throws(() => parseMemoryChanges([{ ...change, scopeId: 'foreign' }]), /invalid/)

    const retry = await source()
    await db.query("UPDATE lingxios.agent_work_items SET status='failed',updated_at=NOW() WHERE id=$1", [retry.id])
    await retryMemorySynthesis(pool)
    assert.deepEqual((await db.query('SELECT status FROM lingxios.agent_work_items WHERE id=$1', [retry.id])).rows, [{ status: 'failed' }])
    await db.query("UPDATE lingxios.agent_work_items SET updated_at=NOW()-INTERVAL '61 seconds' WHERE id=$1", [retry.id])
    await retryMemorySynthesis(pool)
    assert.deepEqual((await db.query('SELECT status FROM lingxios.agent_work_items WHERE id=$1', [retry.id])).rows, [{ status: 'queued' }])
    await db.query("UPDATE lingxios.agent_work_items SET status='leased',attempts=3,lease_expires_at=NOW()-INTERVAL '1 second' WHERE id=$1", [retry.id])
    assert.equal(await new PgWorkStore(pool).claim('test-worker'), null)
    await db.query("UPDATE lingxios.agent_work_items SET status='queued' WHERE id=$1", [retry.id])
    assert.equal(await new PgWorkStore(pool).claim('test-worker'), null)
    await retryMemorySynthesis(pool)
    assert.deepEqual((await db.query('SELECT status FROM lingxios.agent_work_items WHERE id=$1', [retry.id])).rows, [{ status: 'failed' }])
    assert.equal(await status(retry), 'rejected')
    const revoked = await source()
    await load(revoked)
    await apply(revoked, [create(revoked), { ...create(revoked), scopeType: 'course' }, { ...create(revoked), scopeType: 'agent_role' }])
    await db.exec("UPDATE lingxios.agent_memories SET pinned=TRUE WHERE scope_type='course'; UPDATE lingxios.agent_memories SET origin='explicit' WHERE scope_type='agent_role'")
    await db.query('UPDATE lingxios.agent_work_items SET cancel_requested_at=NOW() WHERE id=$1', [revoked.meta!['sourceRunId']])
    assert.equal(await status(revoked), 'superseded')
    assert.deepEqual((await db.query("SELECT scope_type,status,version FROM lingxios.agent_memories WHERE id<>$1 ORDER BY scope_type", [memory.id])).rows,
      [{ scope_type: 'agent_role', status: 'active', version: 1 }, { scope_type: 'course', status: 'active', version: 1 }, { scope_type: 'learner', status: 'expired', version: 2 }])
  } finally { await db.close() }
})
