import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { setTimeout as delay } from 'node:timers/promises'
import { it } from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import { createLingxiOS, type LingxiOSOptions } from '../src/app/index.js'
import type { SqlPool } from '../src/control-plane/pg-store.js'
import { resumeDependents } from '../src/control-plane/dependencies.js'
import { checkStorage } from '../src/app/storage.js'
import { actionKeyOf, sessionKeyOf, type WorkItem, type SessionRecord } from '../src/protocol/types.js'
import { snapshotRequest } from '../src/context/request.js'
import { createResponseEnvelope } from '../src/outcome/envelope.js'
import { candidateHash } from '../src/outcome/verification.js'
import type { ConversationPolicy, IMMessageInput } from '../src/collaboration/types.js'
import { graphNodes } from '../src/collaboration/graphs.js'
import { authorizedScopes, identityOf } from '../src/memory/access.js'
import { imDeliveryContext } from '../src/collaboration/conversations.js'
import { validateStateUpdate } from '../src/collaboration/state.js'

const policy: ConversationPolicy = { tenantId: 'tenant', conversationId: 'room', version: 1, kind: 'group',
  owner: { kind: 'participant', id: 'u' }, defaultAgentId: 'lead', participants: [
    { id: 'u', kind: 'human', capabilities: ['read', 'execute'] }, { id: 'v', kind: 'human', capabilities: ['read', 'execute'] },
    ...['lead', 'helper', 'other'].map(id => ({ id, kind: 'agent' as const, capabilities: ['read', 'execute', 'speak'] as Array<'read' | 'execute' | 'speak'> })),
  ] }
const message = (id: string, extra: Partial<IMMessageInput> = {}): IMMessageInput => ({ tenantId: 'tenant', conversationId: 'room',
  policyVersion: 1, messageId: id, version: 1, author: { id: 'u', kind: 'human' }, text: 'Summarize the collaborative task.', ...extra })

async function setup(options: Partial<LingxiOSOptions> = {}) {
  const db = new PGlite()
  const pool: SqlPool = { query: async (sql, params) => { const result = await db.query<Record<string, unknown>>(sql, params); return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length } },
    connect: async () => ({ query: pool.query, release() {} }) }
  await db.exec(await readFile(new URL('../../db/schema.sql', import.meta.url), 'utf8'))
  const control = await createLingxiOS({ ...options, database: pool }), host = control.connectWorker({ workerId: 'collaboration-test', workKinds: ['turn'] })
  await control.conversations.sync(structuredClone(policy))
  async function save(work: WorkItem) {
    const existing = await host.loadSession(work, sessionKeyOf(work))
    if (existing) return existing
    const context = await host.loadContext(work)
    const session: SessionRecord = { key: sessionKeyOf(work), tenantId: work.tenantId, agentId: work.agentId, sessionId: work.sessionId,
      ...(work.threadId === undefined ? {} : { threadId: work.threadId }), revision: 0, compactionEpoch: 0, history: [], appliedWorkIds: [work.id], request: snapshotRequest(context) }
    await host.saveSession(work, session)
    return session
  }
  async function commit(work: WorkItem, body: string) {
    const session = await save(work), version = session.request!.revisions.length + 1
    const candidate = { body, requestVersion: version, artifacts: [] }, checked = await host.verifyCandidate(work, candidate)
    assert.ok(checked.records.every(record => record.status === 'passed'), JSON.stringify(checked.records))
    const outcome = { status: 'satisfied' as const, requestVersion: version, verification: 'not_run' as const }
    const assessment = { status: 'satisfied' as const, gaps: [], checks: [{ requirement: session.request!.originalText, status: 'met' as const, basis: body }] }
    const hash = candidateHash(candidate)
    await host.saveStep(work, { id: `review:${hash}`, kind: 'runtime.review', requestVersion: version,
      input: { workId: work.id, candidateHash: hash }, output: JSON.stringify({ workId: work.id, candidateHash: hash, requestVersion: version, missing: [] }), artifacts: [] })
    const base = Number((await db.query<{ seq: number }>('SELECT COALESCE(MAX(seq),$2) AS seq FROM lingxios.agent_run_events WHERE run_id=$1', [work.id, (work.fence - 1) * 100000])).rows[0]!.seq)
    await host.emitEvent(work, { runId: work.id, seq: base + 1, kind: 'model.completed', stage: 'completed', visibility: 'internal', data: {} })
    await host.emitEvent(work, { runId: work.id, seq: base + 2, kind: 'model.delta', stage: 'delta', visibility: 'user', data: { partType: 'text', delta: body } })
    await host.emitEvent(work, { runId: work.id, seq: base + 3, kind: 'response.assessed', stage: 'completed', visibility: 'internal', data: { body, assessment, goalOutcome: outcome } })
    await host.commitResult(work, { version: 2, runId: work.id, agentId: work.agentId, sessionId: work.sessionId,
      ...(work.threadId === undefined ? {} : { threadId: work.threadId }), body,
      envelope: createResponseEnvelope(body, outcome, session.request!.evidence, [], undefined, undefined, assessment) })
  }
  const action = (work: WorkItem, cellId: string, action: string, args: Record<string, unknown>) => {
    const scope = { runId: work.id, cellId, callIndex: 0 }
    return host.executeAction(work, { ...scope, action, args, idempotencyKey: actionKeyOf(scope) })
  }
  return { db, pool, control, host, save, commit, action, async close() { await control.stop(); await db.close() } }
}

it('binds IM ownership, audiences, targeted reply slots, causal identities and execution sessions', async () => {
  const f = await setup()
  try {
    const both = message('both', { mentions: ['helper', 'lead', 'helper'] }), first = await f.control.conversations.ingest(both)
    assert.deepEqual(first.runs.map(run => run.agentId), ['helper', 'lead'])
    assert.deepEqual(await f.control.conversations.ingest(both), { ...first, deduplicated: true })
    await assert.rejects(f.control.conversations.ingest({ ...both, text: 'changed' }), /identity reused/)
    const other = await f.control.conversations.ingest(message('other-user', { author: { id: 'v', kind: 'human' } }))
    assert.notEqual(other.runs[0]!.sessionId, first.runs[0]!.sessionId)
    const privateRun = await f.control.conversations.ingest(message('private', { audience: { visibility: 'participants', participantIds: ['u', 'lead'] } }))
    assert.notEqual(privateRun.runs[0]!.sessionId, first.runs[0]!.sessionId)
    await assert.rejects(f.control.conversations.ingest(message('wrong-thread', { threadId: 't' })), /thread is outside/)
    await f.control.conversations.registerThread({ tenantId: 'tenant', conversationId: 'room', threadId: 't', policyVersion: 1 })
    const thread = await f.control.conversations.ingest(message('thread', { threadId: 't' }))
    assert.notEqual(thread.runs[0]!.sessionId, first.runs[0]!.sessionId)
    await assert.rejects(f.control.conversations.ingest(message('reply', { replyTo: { messageId: 'private', version: 1 } })), /audience/)
    assert.equal((await f.control.conversations.ingest(message('agent', { author: { id: 'helper', kind: 'agent' }, mentions: ['lead'] }))).reason, 'agent_message')
    assert.equal((await f.control.conversations.ingest(message('no-speaker', { mentions: ['missing'] }))).reason, 'no_speaker')
    await assert.rejects(f.control.readMessage({ ...privateRun.runs[0]!, principalId: 'v' }), /audience/)
    const { principalId: _principal, ...anonymous } = privateRun.runs[0]!
    await assert.rejects(f.control.readMessage(anonymous), /authenticated/)
    const cancel = { tenantId: 'tenant', conversationId: 'room', principalId: 'u', runId: other.runs[0]!.runId, commandId: 'cancel' }
    await assert.rejects(f.control.cancelConversationRun(cancel), /control capability/)
    const updated = structuredClone(policy); updated.version = 2; updated.participants[0]!.capabilities.push('control')
    await f.control.conversations.sync(updated)
    assert.equal(await f.control.cancelConversationRun(cancel), true)
    await assert.rejects(f.control.conversations.sync(policy), /stale/)
    const edit = await f.control.conversations.ingest({ ...both, version: 2, policyVersion: 2 })
    assert.notEqual(edit.runs[0]!.runId, first.runs[0]!.runId)
    assert.equal((await f.control.readRun(first.runs[0]!))?.status, 'cancelled')
  } finally { await f.close() }
})

it('merges independent state fields, preserves tombstone versions and audits deduplicated conflicts', async () => {
  const f = await setup(), scope = { tenantId: 'tenant', conversationId: 'room', stateId: 'canvas', principalId: 'u' }
  try {
    await f.control.sharedState.create(scope, { visibility: 'participants', participantIds: ['u', 'lead'] })
    const first = { operationId: 'first', changes: [{ field: 'title', expectedVersion: 0, value: 'A' }] }, saved = await f.control.sharedState.apply(scope, first)
    assert.equal(saved.ok, true)
    assert.deepEqual(await f.control.sharedState.apply(scope, first), { ...saved, deduplicated: true })
    assert.equal((await f.control.sharedState.apply(scope, { operationId: 'other', changes: [{ field: 'color', expectedVersion: 0, value: 'blue' }] })).ok, true)
    const conflict = await f.control.sharedState.apply(scope, { operationId: 'stale', changes: [{ field: 'title', expectedVersion: 0, value: 'B' }] })
    assert.equal(conflict.ok, false)
    if (!conflict.ok) assert.deepEqual(conflict.conflicts, ['title'])
    await assert.rejects(f.control.sharedState.apply(scope, { ...first, changes: [{ field: 'title', expectedVersion: 1, value: 'B' }] }), /identity reused/)
    assert.equal((await f.control.sharedState.apply(scope, { operationId: 'delete', changes: [{ field: 'title', expectedVersion: 1, delete: true }] })).ok, true)
    assert.equal((await f.control.sharedState.apply(scope, { operationId: 'recreate-stale', changes: [{ field: 'title', expectedVersion: 0, value: 'C' }] })).ok, false)
    assert.equal((await f.control.sharedState.apply(scope, { operationId: 'recreate', changes: [{ field: 'title', expectedVersion: 2, value: 'C' }] })).ok, true)
    await assert.rejects(f.control.sharedState.read({ ...scope, principalId: 'v' }), /audience/)
    assert.equal((await f.control.sharedState.history(scope)).items.length, 6)
    await f.control.conversations.ingest(message('public'))
    const work = (await f.host.claimWork())!; await f.save(work)
    const denied = await f.action(work, 'private-state', 'shared_state.read', { id: scope.stateId })
    assert.equal(denied.ok, false); assert.match(denied.error!, /audience/)
  } finally { await f.close() }
})

it('runs a durable diamond DAG through state actions, fan-in, verified result and deduplicated IM delivery', async () => {
  const sent = new Map<string, string>(), calls: string[] = []; let attempts = 0
  const f = await setup({ delivery: { onEvent: async () => {}, async deliverMessage(work, _message, context) {
    assert.equal(work.conversation?.internal, false); assert.ok(context?.im)
    const key = context.im.messageKey
    if (!sent.has(key)) sent.set(key, `upstream-${sent.size + 1}`)
    calls.push(key)
    if (++attempts === 1) throw new Error('transport accepted the message but its acknowledgement was lost')
    return { messageId: sent.get(key)! }
  } } })
  try {
    await f.control.sharedState.create({ tenantId: 'tenant', conversationId: 'room', principalId: 'u', stateId: 'canvas' })
    const run = (await f.control.conversations.ingest(message('dag'))).runs[0]!
    let parent = (await f.host.claimWork())!; await f.save(parent)
    const input = { id: 'research', nodes: [{ id: 'a', agentId: 'helper', text: 'Research A.' }, { id: 'b', agentId: 'helper', text: 'Research B.' },
      { id: 'join', agentId: 'other', text: 'Combine A and B.', dependsOn: ['a', 'b'] }] }
    const started = await f.action(parent, 'start', 'graph.start', input)
    assert.equal(started.ok, true, started.error)
    assert.equal(((await f.action(parent, 'inspect', 'task.inspect', {})).value as Record<string, unknown>)['completedBusinessAction'], false)
    assert.deepEqual(await f.action(parent, 'start', 'graph.start', input), started)
    await f.host.waitWork(parent, { status: 'delegated', taskRef: String(started.directive!.data!['taskRef']), requestVersion: 1, verification: 'not_run' })
    const a = (await f.host.claimWork())!, b = (await f.host.claimWork())!
    assert.notEqual(a.sessionId, b.sessionId); assert.equal(await f.host.claimWork(), null)
    for (const [work, field] of [[a, 'a'], [b, 'b']] as const) {
      await f.save(work)
      const result = await f.action(work, 'update', 'shared_state.update', { id: 'canvas', changes: [{ field, expectedVersion: 0, value: field }] })
      assert.equal(result.ok, true, result.error)
      await f.commit(work, `Verified ${field}.`)
    }
    const join = (await f.host.claimWork())!
    assert.equal((await f.host.loadContext(join)).dependencies?.length, 2)
    await f.commit(join, 'Verified combined research.')
    await resumeDependents(f.pool)
    parent = (await f.host.claimWork())!; assert.equal(parent.id, run.runId); assert.ok(parent.fence > 1)
    await f.commit(parent, 'Verified complete research.')
    for (let n = 0; n < 100 && !attempts; n++) await delay(25)
    assert.equal(attempts, 1)
    await f.db.query('UPDATE lingxios.agent_delivery_outbox SET available_at=NOW() WHERE delivered_at IS NULL')
    let delivered = false
    for (let n = 0; n < 100 && !delivered; n++) { delivered = (await f.control.readDelivery(run)) === 'delivered'; if (!delivered) await delay(25) }
    assert.equal(delivered, true); assert.equal(sent.size, 1); assert.equal(calls.length, 2); assert.equal(calls[0], calls[1])
    const trace = await f.control.readConversationTrace({ tenantId: 'tenant', conversationId: 'room', principalId: 'u', messageId: 'dag', version: 1 })
    assert.deepEqual([trace!.runs.length, trace!.actions.length, trace!.states.length, trace!.deliveries.length], [4, 4, 2, 1])
    assert.equal((trace!.deliveries[0]!['receipt'] as { messageId: string }).messageId, 'upstream-1')
    const resultId = String(trace!.deliveries[0]!['result_id'])
    assert.equal((await f.control.conversations.ingest(message('echo', { author: { id: 'lead', kind: 'agent' }, causedBy: { resultId }, mentions: ['helper'] }))).reason, 'outbox_echo')
  } finally { await f.close() }
})

it('rejects cycles, blocks failed dependencies, resumes the parent and fences cancelled branches', async () => {
  assert.throws(() => graphNodes({ id: 'x', nodes: [{ id: 'a', agentId: 'helper', text: 'A', dependsOn: ['a'] }] }), /cyclic/)
  assert.throws(() => graphNodes({ id: 'x', nodes: [{ id: 'a', agentId: 'helper', text: 'A', dependsOn: ['missing'] }] }), /missing/)
  const f = await setup()
  try {
    const run = (await f.control.conversations.ingest(message('failure'))).runs[0]!, parent = (await f.host.claimWork())!; await f.save(parent)
    const graph = await f.control.graphs.enqueue(run, { id: 'failing', nodes: [
      { id: 'a', agentId: 'helper', text: 'A' }, { id: 'b', agentId: 'other', text: 'B', dependsOn: ['a'] }] })
    const wait = await f.control.graphs.waitForChildren(run, graph.nodes.map(node => node.workId))
    await f.host.waitWork(parent, { status: 'delegated', taskRef: String(wait.data!['taskRef']), requestVersion: 1, verification: 'not_run' })
    const child = (await f.host.claimWork())!; await f.host.completeWork(child, { status: 'failed', error: 'fixture failure' })
    await resumeDependents(f.pool)
    assert.deepEqual((await f.control.graphs.read(run, 'failing'))!.nodes.map(node => node.status), ['failed', 'blocked'])
    const resumed = (await f.host.claimWork())!; assert.equal(resumed.id, run.runId)
    assert.ok((await f.host.verifyCandidate(resumed, { body: 'Done', requestVersion: 1, artifacts: [] })).records.some(record => record.status !== 'passed'))
    await assert.rejects(f.control.graphs.enqueue(run, { id: 'rollback', nodes: [{ id: 'a', agentId: 'helper', text: 'A' }, { id: 'b', agentId: 'missing', text: 'B' }] }), /capability/)
    assert.equal(await f.control.graphs.read(run, 'rollback'), null)
    assert.equal(await f.control.cancel(run), true)
    await assert.rejects(f.action(resumed, 'late', 'graph.start', { id: 'late', nodes: [{ id: 'a', agentId: 'helper', text: 'A' }] }), /cancel/)
  } finally { await f.close() }
})

it('partitions IM memory by principal and revokes restored sessions', async () => {
  const f = await setup()
  try {
    await f.control.conversations.ingest(message('one'))
    await f.control.conversations.ingest(message('two', { author: { id: 'v', kind: 'human' } }))
    const a = (await f.host.claimWork())!, b = (await f.host.claimWork())!; await f.save(a); await f.save(b)
    const options = { resolveScopes: async () => [{ tenantId: 'tenant', scopeType: 'shared', scopeId: 'legacy' }] }
    const scopes = await authorizedScopes(options, identityOf(a), f.pool)
    assert.notDeepEqual(scopes, await authorizedScopes(options, identityOf(b), f.pool)); assert.notEqual(scopes[0]!.scopeId, 'legacy')
    const revoked = structuredClone(policy); revoked.version = 2; revoked.participants = revoked.participants.filter(member => member.id !== 'v')
    await f.control.conversations.sync(revoked)
    await assert.rejects(f.host.loadSession(b, sessionKeyOf(b)), /capability/)
  } finally { await f.close() }
})

it('migrates schema 9 without resetting business data and matches the fresh schema', async () => {
  const migrated = new PGlite(), fresh = new PGlite()
  try {
    await migrated.exec(await readFile(new URL('../../test/fixtures/schema-9.sql', import.meta.url), 'utf8'))
    await migrated.exec("INSERT INTO lingxios.agent_work_items(id,tenant_id,agent_id,session_id,kind,lane,trigger_ref,status) VALUES('old','t','a','s','turn','interactive','m','succeeded')")
    await migrated.exec(await readFile(new URL('../../db/migrations/010-im-collaboration.sql', import.meta.url), 'utf8'))
    await fresh.exec(await readFile(new URL('../../db/schema.sql', import.meta.url), 'utf8'))
    const columns = "SELECT table_name,column_name,data_type,is_nullable FROM information_schema.columns WHERE table_schema='lingxios' ORDER BY table_name,ordinal_position"
    assert.deepEqual((await migrated.query(columns)).rows, (await fresh.query(columns)).rows)
    const indexes = "SELECT tablename,indexname,indexdef FROM pg_indexes WHERE schemaname='lingxios' ORDER BY tablename,indexname"
    assert.deepEqual((await migrated.query(indexes)).rows, (await fresh.query(indexes)).rows)
    assert.deepEqual((await migrated.query("SELECT id,status,conversation FROM lingxios.agent_work_items WHERE id='old'")).rows, [{ id: 'old', status: 'succeeded', conversation: null }])
    await checkStorage({ query: async (sql, params) => { const result = await migrated.query<Record<string, unknown>>(sql, params); return { rows: result.rows, rowCount: result.affectedRows ?? null } } })
  } finally { await migrated.close(); await fresh.close() }
})

it('resumes an exact wait set after early completion and rejects stale fences and widened delegate capabilities', async () => {
  const f = await setup({ capabilityResolver: { resolve: async work => [
    { name: 'graph', methods: ['start', 'read'] },
    { name: 'shared_state', methods: work.agentId === 'lead' ? ['read'] : ['create', 'read', 'update'] },
  ] } })
  try {
    const run = (await f.control.conversations.ingest(message('early'))).runs[0]!, parent = (await f.host.claimWork())!
    await f.save(parent)
    const graph = await f.control.graphs.enqueue(run, { id: 'early', nodes: [
      { id: 'a', agentId: 'helper', text: 'Finish A.' }, { id: 'b', agentId: 'other', text: 'Independent B.' }] })
    const a = (await f.host.claimWork())!; await f.save(a)
    const denied = await f.action(a, 'widen', 'shared_state.create', { id: 'forbidden' })
    assert.equal(denied.ok, false)
    assert.equal((await f.host.loadContext(a)).tools!.some(tool => tool.action === 'shared_state.create'), false)
    await f.commit(a, 'A is complete.')
    const wait = await f.control.graphs.waitForChildren(run, [a.id])
    await f.host.waitWork(parent, { status: 'delegated', taskRef: String(wait.data!['taskRef']), requestVersion: 1, verification: 'not_run' })
    await resumeDependents(f.pool)
    assert.equal((await f.control.readRun(run))!.status, 'queued')
    assert.equal((await f.control.graphs.read(run, 'early'))!.nodes.find(node => node.workId !== a.id)!.status, 'queued')
    const resumed = (await f.host.claimWork())!; assert.equal(resumed.id, parent.id)
    await assert.rejects(f.action(parent, 'old-fence', 'graph.start', { id: 'stale', nodes: [{ id: 'x', agentId: 'helper', text: 'X' }] }), /lease|fenc/i)
    assert.equal(await f.control.revise(run, 'Changed the task.'), true)
    assert.ok((await f.control.graphs.read(run, 'early')) === null)
    assert.equal((await f.db.query<{ status: string }>('SELECT status FROM lingxios.agent_work_items WHERE id=$1', [graph.nodes.find(node => node.workId !== a.id)!.workId])).rows[0]!.status, 'cancelled')
    const stale = await f.action(resumed, 'old-request', 'graph.start', { id: 'revised', nodes: [{ id: 'x', agentId: 'helper', text: 'X' }] })
    assert.equal(stale.ok, false); assert.match(stale.error!, /request|revis/i)
  } finally { await f.close() }
})

it('isolates tenants and direct audiences, rejects malformed state and rechecks speak and execute revocation', async () => {
  const f = await setup()
  try {
    for (const value of [Infinity, undefined, new Date(), { nested: NaN }]) assert.throws(() => validateStateUpdate({
      operationId: 'invalid', changes: [{ field: 'value', expectedVersion: 0, value }] }), /invalid|JSON/)
    const otherPolicy = { ...structuredClone(policy), tenantId: 'other-tenant', kind: 'direct' as const }
    await f.control.conversations.sync(otherPolicy)
    await f.control.conversations.ingest(message('private', { audience: { visibility: 'participants', participantIds: ['u', 'lead'] }, text: 'private secret' }))
    const privateWork = (await f.host.claimWork())!; await f.save(privateWork); await f.commit(privateWork, 'Private answer.')
    const run = (await f.control.conversations.ingest(message('public'))).runs[0]!, work = (await f.host.claimWork())!; await f.save(work)
    assert.equal((await f.host.loadContext(work)).messages.some(item => item.body.includes('private secret')), false)
    const otherRun = (await f.control.conversations.ingest(message('public', { tenantId: 'other-tenant', text: 'Other tenant.' }))).runs[0]!
    assert.notEqual(otherRun.runId, run.runId)
    const otherWork = (await f.host.claimWork())!
    assert.equal((await f.host.loadContext(otherWork)).messages.some(item => item.body.includes('private secret')), false)
    await f.commit(work, 'Public answer.')
    const resultId = (await f.control.readRun(run))!.resultId!
    assert.ok(await imDeliveryContext(f.pool, work, resultId))
    const revoked = structuredClone(policy); revoked.version = 2
    revoked.participants.find(member => member.id === 'lead')!.capabilities = ['read', 'execute']
    await f.control.conversations.sync(revoked)
    await assert.rejects(imDeliveryContext(f.pool, work, resultId), /speak/)
    revoked.version = 3; revoked.participants[0]!.capabilities = ['read']
    await f.control.conversations.sync(revoked)
    await assert.rejects(f.control.revise(run, 'Resume.'), /execute/)
    await assert.rejects(f.control.continueInput({ ...run, inputId: 'late', requestVersion: 1, text: 'Resume.' }), /execute/)
    assert.ok(await f.host.loadContext(otherWork), 'revocation in one tenant leaves the other tenant authorized')
  } finally { await f.close() }
})

it('reauthorizes IM approvals and prevents cancelled workers from writing shared state', async () => {
  let effects = 0
  const f = await setup({ tools: [{ name: 'documents__save', action: 'documents.save', description: 'Save with approval.',
    effect: 'transaction', approval: true, parameters: { type: 'object', properties: {}, additionalProperties: false },
    parse: () => ({}), authorize: async () => {}, preview: async () => ({ version: 1 }),
    execute: async () => { effects++; return { ok: true, value: { saved: true } } },
  }] })
  try {
    const run = (await f.control.conversations.ingest(message('approval'))).runs[0]!, work = (await f.host.claimWork())!; await f.save(work)
    const pending = await f.action(work, 'save', 'documents.save', {}), approvalId = pending.approval!.id
    const revoked = structuredClone(policy); revoked.version = 2; revoked.participants[0]!.capabilities = ['read']
    await f.control.conversations.sync(revoked)
    await assert.rejects(f.control.decideApproval({ ...run, approvalId, approved: true }), /execute/)
    assert.equal(effects, 0)
    const restored = structuredClone(policy); restored.version = 3; await f.control.conversations.sync(restored)
    await f.control.decideApproval({ ...run, approvalId, approved: true })
    restored.version = 4; restored.participants[0]!.capabilities = ['read']; await f.control.conversations.sync(restored)
    await assert.rejects(f.action(work, 'save', 'documents.save', {}), /execute/)
    assert.equal(effects, 0, 'an earlier approval cannot survive execute revocation')
    restored.version = 5; restored.participants[0]!.capabilities = ['read', 'execute']; await f.control.conversations.sync(restored)
    const scope = { tenantId: 'tenant', conversationId: 'room', principalId: 'u', stateId: 'cancelled-state' }
    await f.control.sharedState.create(scope)
    await f.control.cancel(run)
    await assert.rejects(f.action(work, 'late-state', 'shared_state.update', { id: scope.stateId,
      changes: [{ field: 'title', expectedVersion: 0, value: 'late' }] }), /cancel/)
    assert.deepEqual((await f.control.sharedState.read(scope))!.fields, {})
  } finally { await f.close() }
})
