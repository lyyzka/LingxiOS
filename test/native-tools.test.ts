import { PgStepStore } from '../src/control-plane/steps.js'
import { PgModelBudgetStore } from '../src/control-plane/pg-store.js'
import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { it } from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import { snapshotEvidence } from '../src/context/evidence.js'
import { toolContractHash } from '../src/tools/contracts.js'
import { PgActionLedger, PgEventStore, PgSessionStore, PgWorkStore, type SqlPool } from '../src/control-plane/pg-store.js'
import { ControlPlaneService, actionFingerprint } from '../src/control-plane/service.js'
import { actionKeyOf, sessionKeyOf, type HostAction } from '../src/protocol/types.js'
import { NoEffectError, type ToolDefinition } from '../src/tools/definition.js'
import { toolExecutor } from '../src/tools/executor.js'
import { decideApproval, resumeDecidedApprovals, executeDecidedApprovals } from '../src/control-plane/approvals.js'

it('validates before intent, rolls back effects with receipts, and recovers a lost commit acknowledgement', async () => {
  const db = new PGlite()
  let loseCommit = false
  const pool: SqlPool = {
    query: async (sql, params) => {
      const result = await db.query<Record<string, unknown>>(sql, params)
      if (sql === 'COMMIT' && loseCommit) { loseCommit = false; throw new Error('commit acknowledgement lost') }
      return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
    },
    connect: async () => ({ query: pool.query, release() {} }),
  }
  await db.exec(await readFile(new URL('../../db/schema.sql', import.meta.url), 'utf8'))
  await db.exec('CREATE TABLE native_documents(id text PRIMARY KEY, title text NOT NULL)')
  let failAfterWrite = false
  let denied = false
  let executions = 0
  const tool: ToolDefinition = {
    name: 'documents__create', action: 'documents.create', description: 'Create a document', effect: 'transaction', approval: false,
    parameters: { type: 'object', properties: { title: { type: 'string' } }, required: ['title'], additionalProperties: false },
    parse(input) {
      const title = (input as Record<string, unknown>)['title']
      if (typeof title !== 'string' || !title.trim()) throw new Error('title is required')
      return { title }
    },
    async authorize() { if (denied) throw new NoEffectError('permission revoked', 'forbidden') },
    async execute(ctx, input) {
      executions++
      await ctx.database.query('INSERT INTO native_documents VALUES($1,$2)', [ctx.action.idempotencyKey, input['title']])
      if (failAfterWrite) throw new Error('interrupted after native mutation')
      return { ok: true, executionState: 'succeeded', value: { title: input['title'] } }
    },
  }
  const actions = new PgActionLedger(pool)
  const service = new ControlPlaneService({ steps: new PgStepStore(pool), modelBudgets: new PgModelBudgetStore(pool),
    tools: [tool], work: new PgWorkStore(pool), actions, events: new PgEventStore(pool), sessions: new PgSessionStore(pool),
    actionExecutor: toolExecutor(pool, [tool]), capabilityResolver: { resolve: async () => [{ name: 'documents' }] },
    contextProvider: { loadContext: async () => ({ persona: { name: '', role: '', instructions: '' }, capabilities: [], messages: [] }) },
    delivery: { onEvent: async () => {}, deliverMessage: async () => {} },
  })
  try {
    await service.enqueue({ id: 'w', tenantId: 't', agentId: 'a', sessionId: 's', principalId: 'u', kind: 'turn', lane: 'interactive', triggerRef: 'm', meta: { text: 'Create a document' } })
    const work = (await service.claim('worker'))!
    await service.saveSession(work, { key: sessionKeyOf(work), tenantId: 't', agentId: 'a', sessionId: 's', history: [], appliedWorkIds: ['w'], revision: 0, compactionEpoch: 0,
      request: { version: 1, workId: 'w', tenantId: 't', sessionId: 's', authorId: 'u', sourceRef: 'm', originalText: 'Create a document', revisions: [], attachments: [], evidence: snapshotEvidence('e', []) } })
    const action = (cellId: string, args: Record<string, unknown>): HostAction => {
      const identity = { runId: 'w', cellId, callIndex: 0 }
      return { ...identity, action: tool.action, args, idempotencyKey: actionKeyOf(identity) }
    }
    const invalid = action('invalid', {})
    assert.equal((await service.executeAction(work, invalid)).executionState, 'rejected')
    assert.equal(await actions.findIntent(invalid.idempotencyKey), null)
    assert.equal(executions, 0)
    denied = true
    assert.equal((await service.executeAction(work, action('denied', { title: 'x' }))).code, 'forbidden')
    denied = false
    failAfterWrite = true
    assert.equal((await service.executeAction(work, action('rollback', { title: 'x' }))).executionState, 'no_effect')
    assert.deepEqual((await db.query('SELECT * FROM native_documents')).rows, [])
    assert.deepEqual(await actions.unsettled('w'), [])
    failAfterWrite = false
    const saved = action('saved', { title: 'Corrected' })
    loseCommit = true
    assert.deepEqual(await service.executeAction(work, saved), { ok: true, executionState: 'succeeded', value: { title: 'Corrected' } })
    assert.equal(executions, 2)
    assert.deepEqual(await service.executeAction(work, saved), await actions.find(saved.idempotencyKey))
    assert.equal(executions, 2)
    const interrupted = action('intent-only', { title: 'Recovered' })
    await actions.reserve(interrupted.idempotencyKey, actionFingerprint(work, interrupted), {
      workId: 'w', tenantId: 't', agentId: 'a', sessionId: 's', principalId: 'u', threadId: null, requestVersion: 1, action: interrupted, toolContractHash: toolContractHash(tool),
    })
    assert.equal((await service.executeAction(work, interrupted)).ok, true)
    assert.deepEqual((await db.query('SELECT title FROM native_documents ORDER BY title')).rows, [{ title: 'Corrected' }, { title: 'Recovered' }])

    let resourceVersion = 1
    tool.approval = true
    tool.preview = async (_context, input) => ({ title: input['title'], resourceVersion })
    const approvedAction = action('approval', { title: 'Approved' })
    const session = (await service.getSession(work, sessionKeyOf(work)))!
    session.history.push({ type: 'function_call', name: tool.name, callId: 'approval-call', stepId: 'approval', arguments: JSON.stringify(approvedAction.args) })
    await service.saveSession(work, session)
    const pending = await service.executeAction(work, approvedAction)
    assert.equal(pending.executionState, 'awaiting_approval')
    assert.equal(executions, 3)
    const decision = { approvalId: pending.approval!.id, approved: true, tenantId: 't', agentId: 'a', sessionId: 's', principalId: 'u' }
    await assert.rejects(decideApproval(pool, { ...decision, principalId: 'other' }), /outside this principal/)
    await decideApproval(pool, decision) // Decision arrives before the worker parks.
    await service.waitWork(work, { status: 'awaiting_approval', verification: 'not_run', requestVersion: 1, approvalId: pending.approval!.id })
    await resumeDecidedApprovals(pool)
    const resumed = (await service.claim('replacement-worker'))!
    assert.ok(resumed.fence > work.fence)
    await executeDecidedApprovals(pool, service, resumed)
    await executeDecidedApprovals(pool, service, resumed)
    assert.equal(executions, 4, 'approved mutation executes once across replacement workers')
    assert.equal((await actions.find(approvedAction.idempotencyKey))?.ok, true)
    assert.match(JSON.stringify((await service.getSession(resumed, sessionKeyOf(resumed)))!.history), /Approved/)

    const staleAction = action('stale-approval', { title: 'Stale' })
    const stale = await service.executeAction(resumed, staleAction)
    await decideApproval(pool, { ...decision, approvalId: stale.approval!.id })
    resourceVersion++
    assert.equal((await service.executeAction(resumed, staleAction)).code, 'approval_stale')
    assert.equal((await actions.find(staleAction.idempotencyKey))?.executionState, 'no_effect')
    const revokedAction = action('revoked-approval', { title: 'Forbidden' })
    const revoked = await service.executeAction(resumed, revokedAction)
    await decideApproval(pool, { ...decision, approvalId: revoked.approval!.id })
    denied = true
    assert.equal((await service.executeAction(resumed, revokedAction)).code, 'forbidden')
    assert.deepEqual(await actions.unsettled('w'), [])
    assert.equal(executions, 4)
    denied = false
    const changedAction = action('changed-contract', { title: 'Old semantics' })
    const changed = await service.executeAction(resumed, changedAction)
    await decideApproval(pool, { ...decision, approvalId: changed.approval!.id })
    tool.semanticVersion = '2'
    assert.equal((await service.executeAction(resumed, changedAction)).code, 'tool_contract_changed')
    assert.equal((await actions.find(changedAction.idempotencyKey))?.executionState, 'no_effect')
    assert.equal(executions, 4)
    tool.approval = false
    tool.effect = 'uncertain'
    failAfterWrite = true
    tool.reconcile = async (context) => {
      const result = await context.database.query('SELECT title FROM native_documents WHERE id=$1', [context.action.idempotencyKey])
      return result.rows[0] ? { ok: true, value: result.rows[0] } : null
    }
    const external = action('external-ack-lost', { title: 'External effect' })
    assert.equal((await service.executeAction(resumed, external)).executionState, 'unknown')
    assert.equal(executions, 5)
    tool.semanticVersion = '3'
    assert.equal((await service.executeAction(resumed, external)).executionState, 'unknown')
    assert.equal(executions, 5)
    tool.semanticVersion = '2'
    assert.deepEqual(await service.executeAction(resumed, external), { ok: true, executionState: 'succeeded', value: { title: 'External effect' } })
    assert.equal(executions, 5, 'reconciliation reads the effect without repeating the external call')
    assert.deepEqual(await actions.unsettled('w'), [])
  } finally { await db.close() }
})
