import { snapshotEvidence } from '../src/context/evidence.js'
import { checkStorage } from '../src/app/storage.js'
import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { it } from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import { PgActionLedger, PgEventStore, PgModelBudgetStore, PgSessionStore, PgWorkStore, type SqlPool } from '../src/control-plane/pg-store.js'
import { hashToken } from '../src/control-plane/memory-store.js'
import type { SessionRecord } from '../src/protocol/types.js'

it('runs namespaced stores on PostgreSQL without touching product tables', async () => {
  const db = new PGlite()
  // The test supplies a database resource, not a replacement store implementation.
  const pool: SqlPool = {
    query: async (sql, params) => {
      const result = await db.query<Record<string, unknown>>(sql, params)
      return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
    },
    connect: async () => ({ query: pool.query, release: () => {} }),
  }
  try {
    await db.exec("CREATE TABLE public.agent_work_items(company_id text); INSERT INTO public.agent_work_items VALUES ('keep')")
    await db.exec(await readFile(new URL('../../db/schema.sql', import.meta.url), 'utf8'))
    const workStore = new PgWorkStore(pool)
    const input = { id: 'w', tenantId: 't', agentId: 'a', sessionId: 's', kind: 'turn', lane: 'interactive' as const, triggerRef: 'm', meta: { text: 'original' } }
    assert.deepEqual(await workStore.enqueue(input), { id: 'w', deduplicated: false })
    assert.deepEqual(await workStore.enqueue(input), { id: 'w', deduplicated: true })
    await assert.rejects(workStore.enqueue({ ...input, tenantId: 'other' }), /different request/)
    await assert.rejects(workStore.enqueue({ ...input, meta: { text: 'different' } }), /different request/)
    const work = (await workStore.claim('worker', 'claim-request-0001'))!
    assert.deepEqual(await workStore.claim('worker', 'claim-request-0001'), work)
    assert.equal(work.id, 'w')
    assert.equal(work.fence, 1)
    const budgets = new PgModelBudgetStore(pool)
    const limits = { maxModelCalls: 2, maxTokens: 100, maxCostMicros: 100, deadlineAt: '2099-01-01T00:00:00.000Z' }
    assert.equal((await budgets.reserve('w', 'call-1', limits)).remainingCalls, 1)
    assert.equal((await budgets.reserve('w', 'call-1', limits)).remainingCalls, 1)
    await budgets.record('w', 'call-1', 20, 10, 5)
    await budgets.record('w', 'call-1', 20, 10, 5)
    assert.equal((await budgets.reserve('w', 'call-2', limits)).remainingTokens, 70)
    assert.equal((await budgets.reserve('w', 'call-3', limits)).allowed, false)
    const token = hashToken(work.leaseToken)
    assert.equal(await workStore.complete('w', 1, token, { status: 'completed' }), false)
    assert.equal((await workStore.getLeased('w', 1, token))?.work.id, 'w')
    assert.ok(await workStore.addSteer('w', 'new requirement'))
    assert.equal((await workStore.heartbeat('w', 1, token))?.steer[0]?.text, 'new requirement')
    await workStore.enqueue({ ...input, id: 'w2' })
    assert.equal(await workStore.claim('worker'), null)

    const sessions = new PgSessionStore(pool)
    const session: SessionRecord = {
      key: '[\"t\",\"a\",\"s\",null]', tenantId: 't', agentId: 'a', sessionId: 's', history: [], appliedWorkIds: ['w'], revision: 0, compactionEpoch: 0,
      request: { version: 1, workId: 'w', tenantId: 't', sessionId: 's', authorId: 'u', sourceRef: 'm', evidence: snapshotEvidence('w:evidence:1', []), attachments: [], originalText: 'original', revisions: [] },
    }
    assert.deepEqual(await sessions.save(session), { ok: true, revision: 1 })
    assert.deepEqual(await sessions.save(session), { ok: false, conflict: true })
    session.revision = 1
    session.history = [{ role: 'user', content: 'preserved' }]
    assert.deepEqual(await sessions.save(session), { ok: true, revision: 2 })
    assert.deepEqual(await sessions.get(session.key), { ...session, revision: 2 })
    assert.deepEqual(await sessions.save({ ...session, key: 'missing' }), { ok: false, conflict: true })

    const ledger = new PgActionLedger(pool)
    await assert.rejects(ledger.record('orphan', { ok: true }), /foreign key/)
    const intent = { workId: 'w', tenantId: 't', principalId: 'u', agentId: 'a', sessionId: 's', threadId: null, requestVersion: 1,
      action: { runId: 'w', cellId: 'c', callIndex: 0, action: 'files.save', args: { title: 'Original' }, idempotencyKey: '[\"w\",\"c\",0]' } }
    assert.equal(await ledger.reserve('[\"w\",\"c\",0]', 'intent', intent), 'started')
    assert.deepEqual(await new PgActionLedger(pool).findIntent('[\"w\",\"c\",0]'), intent)
    assert.equal(await ledger.reserve('[\"w\",\"c\",0]', 'intent', { ...intent, requestVersion: 2 }), 'existing')
    assert.deepEqual(await ledger.findIntent('[\"w\",\"c\",0]'), intent)
    assert.deepEqual(await ledger.unsettled('w'), [{ actionKey: '[\"w\",\"c\",0]', action: 'files.save', state: 'unknown' }])
    assert.equal(await ledger.hasSuccessfulAction('w', 1, ['files.save']), false)
    assert.deepEqual(await ledger.unsettled('other'), [])

    assert.equal(await ledger.reserve('[\"w\",\"c\",0]', 'intent', intent), 'existing')
    await assert.rejects(ledger.reserve('[\"w\",\"c\",0]', 'changed', intent), /mismatch/)
    assert.deepEqual(await ledger.record('[\"w\",\"c\",0]', { ok: true, value: 'receipt' }), { ok: true, value: 'receipt' })
    assert.equal(await ledger.hasSuccessfulAction('w', 1, ['files.save']), true)
    assert.equal(await ledger.hasSuccessfulAction('w', 2, ['files.save']), false)
    assert.equal(await ledger.hasSuccessfulAction('w', 1, ['mail.send']), false)
    assert.deepEqual(await ledger.unsettled('w'), [])
    assert.equal(await ledger.reserve('[\"w\",\"c\",0]', 'intent', intent), 'existing')
    assert.deepEqual(await ledger.find('[\"w\",\"c\",0]'), { ok: true, value: 'receipt' })
    const resolution = { id: 'resolution-1', actionKey: '[\"w\",\"c\",0]', result: { ok: true, value: 'reconciled' },
      evidence: { source: 'readback' }, resolvedBy: 'operator:test' }
    assert.equal(await ledger.recordResolution(resolution), 'recorded')
    assert.equal(await ledger.recordResolution(resolution), 'existing')
    assert.deepEqual(await ledger.find('[\"w\",\"c\",0]'), resolution.result)

    const events = new PgEventStore(pool)
    const event = { runId: 'w', seq: 1, tenantId: 't', agentId: 'a', recordedAt: '2026-09-05T00:00:00.000Z', kind: 'test', stage: 'completed' as const, visibility: 'internal' as const, data: {} }
    assert.equal(await events.append(event), true)
    assert.equal(await events.append(event), false)
    assert.deepEqual(await events.listRange('w', 0, 10), [event])
    const fencedSession = { ...session, revision: 2, history: [{ role: 'user' as const, content: 'fenced' }] }
    assert.deepEqual(await sessions.save(fencedSession, { workId: work.id, fence: work.fence, leaseTokenHash: token }), { ok: true, revision: 3 })
    assert.deepEqual(await sessions.save({ ...fencedSession, revision: 3, history: [] },
      { workId: work.id, fence: work.fence, leaseTokenHash: 'wrong' }), { ok: false, conflict: true })
    assert.deepEqual((await sessions.get(session.key))?.history, fencedSession.history)
    assert.equal(await events.append({ ...event, seq: 2 }, { workId: work.id, fence: work.fence, leaseTokenHash: 'wrong' }), false)
    assert.equal(await workStore.complete('w', 1, token, { status: 'completed', goalOutcome: { status: 'partial', verification: 'not_run', requestVersion: 1 } }), false)
    assert.ok(await workStore.getLeased('w', 1, token))
    assert.ok(await workStore.complete('w', 1, token, { status: 'completed', goalOutcome: { status: 'partial', verification: 'not_run', requestVersion: 2 } }))
    const cancelled = (await workStore.claim('worker'))!
    assert.equal(cancelled.id, 'w2')
    assert.equal(await workStore.requestCancel(cancelled.id), true)
    assert.equal(await workStore.complete(cancelled.id, cancelled.fence, hashToken(cancelled.leaseToken), { status: 'completed' }), false)
    assert.equal(await workStore.complete(cancelled.id, cancelled.fence, hashToken(cancelled.leaseToken), { status: 'cancelled' }), true)
    await workStore.enqueue({ ...input, id: 'expired', sessionId: 'other-session' })
    const expired = (await workStore.claim('worker'))!
    assert.equal(expired.id, 'expired')
    await workStore.requestPreempt(expired.id)
    await db.query("UPDATE lingxios.agent_work_items SET lease_expires_at=NOW()-INTERVAL '1 second' WHERE id=$1", [expired.id])
    assert.equal(await workStore.complete(expired.id, expired.fence, hashToken(expired.leaseToken), { status: 'completed' }), false)
    assert.equal(await workStore.yieldWork(expired.id, expired.fence, hashToken(expired.leaseToken)), false)
    assert.deepEqual((await db.query('SELECT status FROM lingxios.agent_work_items WHERE id=$1', [expired.id])).rows, [{ status: 'leased' }])
    assert.deepEqual((await db.query('SELECT * FROM public.agent_work_items')).rows, [{ company_id: 'keep' }])
    await checkStorage(pool)
    await db.exec('ALTER TABLE lingxios.agent_request_snapshots DROP COLUMN request_snapshot')
    await assert.rejects(checkStorage(pool), /request_snapshot/)
  } finally {
    await db.close()
  }
})
