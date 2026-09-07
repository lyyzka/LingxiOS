import { abortable } from '../deadline.js'
import { errorMessage } from '../errors.js'
import { lockAction } from '../control-plane/action-transaction.js'
import type { ActionExecutor, ActionExecutionOptions } from '../control-plane/stores.js'
import type { SqlPool, SqlQueryable } from '../control-plane/pg-store.js'
import type { HostAction, HostActionResult, WorkItem } from '../protocol/types.js'
import { NoEffectError, type ActionContext, type ArtifactInput, type ToolDefinition } from './definition.js'
import type { KernelArtifact } from '../protocol/types.js'
import { approvalGate } from '../control-plane/approvals.js'
import { withTransaction } from '../control-plane/pg-store.js'
import { requestSnapshot, enqueueChild, childIdentity, readRun, cancelRun, reviseRun } from '../app/jobs.js'
import { deadlinePool } from '../control-plane/deadline-pool.js'

export function toolExecutor(database: SqlPool, definitions: readonly ToolDefinition[],
  createArtifact?: (work: Omit<WorkItem, 'leaseToken'>, input: ArtifactInput) => Promise<KernelArtifact>): ActionExecutor {
  const tools = new Map(definitions.map(tool => [tool.action, tool]))
  if (tools.size !== definitions.length || new Set(definitions.map(tool => tool.name)).size !== definitions.length) throw new Error('duplicate tool definition')
  for (const tool of definitions) {
    if (!/^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$/.test(tool.action) || tool.name !== tool.action.replace('.', '__')) throw new Error('invalid tool identity')
    if (tool.approval && !tool.preview) throw new Error(`${tool.action} requires an approval preview`)
  }
  function definition(action: HostAction) {
    const tool = tools.get(action.action)
    if (!tool) throw new NoEffectError('unknown native tool', 'unknown_tool')
    return tool
  }
  function context(work: Omit<WorkItem, 'leaseToken'>, action: HostAction, options: ActionExecutionOptions, db: SqlQueryable = database): ActionContext {
    const write = () => { options.signal.throwIfAborted(); if (db === database) throw new NoEffectError('child mutations require an action transaction') }
    const queryable = db === database ? deadlinePool(database,options.signal) : db
    return { work, action, database: queryable, ...options,
      requestSnapshot: () => requestSnapshot(queryable, work.id, options.requestVersion),
      enqueueChild: input => { write(); return enqueueChild(db, work, options.requestVersion, input) },
      readChild: async id => readRun(queryable, await childIdentity(queryable, work, id)),
      cancelChild: async id => { write(); return cancelRun(db, await childIdentity(db, work, id)) },
      reviseChild: async (id, text) => { write(); return reviseRun(db, await childIdentity(db, work, id), text, { id: work.agentId, kind: 'agent' }) },
      createArtifact: async input => {
      options.signal.throwIfAborted()
      if (!createArtifact) throw new NoEffectError('artifact storage is unavailable')
      return createArtifact(work, input)
    } }
  }
  return {
    async reconcile(work, action, options) {
      const tool = definition(action)
      if (!tool.reconcile) return null
      const input = tool.parse(action.args), ctx = context(work, action, options)
      await abortable(tool.authorize(ctx, input), options.signal)
      return abortable(tool.reconcile(ctx, input), options.signal)
    },
    async prepare(work, action, options) {
      const tool = definition(action)
      let input: Record<string, unknown>
      try { input = tool.parse(action.args) }
      catch (error) { throw new NoEffectError(errorMessage(error), 'invalid_arguments') }
      try { await abortable(tool.authorize(context(work, action, options), input), options.signal) }
      catch (error) { throw error instanceof NoEffectError ? error : new NoEffectError('Native authorization was unavailable', 'authorization_unavailable') }
    },
    async execute(work, action, options) {
      const tool = definition(action)
      const input = tool.parse(action.args)
      const run = async (db: SqlQueryable) => {
        const ctx = context(work, action, options, db)
        await tool.authorize(ctx, input)
        options.signal.throwIfAborted()
        const pending = await approvalGate(tool, ctx, input)
        return pending ?? tool.execute(ctx, input)
      }
      if (tool.effect !== 'transaction') {
        const ctx = context(work, action, options)
        await abortable(tool.authorize(ctx, input), options.signal)
        if (tool.approval) {
          const pending = await withTransaction(deadlinePool(database,options.signal), async db => {
            await lockAction(db, work, action)
            const approvedContext = context(work, action, options, db)
            await tool.authorize(approvedContext, input)
            const pending = await approvalGate(tool, approvedContext, input)
            if (approvedContext.approvedPreview) ctx.approvedPreview = approvedContext.approvedPreview
            // The external call may outlive this process. Persist uncertainty before sending it.
            if (!pending) await db.query(`UPDATE lingxios.agent_action_ledger SET result=$2::jsonb
              WHERE idempotency_key=$1 AND result->'approval'->>'status'='PENDING'`,
            [action.idempotencyKey,JSON.stringify({ ok: false, executionState: 'unknown', code: 'external_in_flight', error: 'Approved external operation is in flight; reconcile before retrying' })])
            return pending
          })
          if (pending) return pending
        }
        let result: HostActionResult
        try { result = await abortable(tool.execute(ctx, input), options.signal) }
        catch (error) {
          if (!(error instanceof NoEffectError)) throw error
          result = { ok: false, executionState: 'no_effect', code: error.code, error: error.message }
        }
        if (tool.approval) await database.query(`UPDATE lingxios.agent_action_ledger SET result=$2::jsonb
          WHERE idempotency_key=$1 AND result->>'code'='external_in_flight'`, [action.idempotencyKey,JSON.stringify(result)])
        return result
      }
      const client = await deadlinePool(database,options.signal,Math.max(1,Date.parse(options.deadlineAt)-Date.now())).connect()
      let active = true
      let committing = false
      // A late callback cannot use a released connection after cancellation.
      const scoped: SqlQueryable = { async query(sql, params) {
        options.signal.throwIfAborted()
        if (!active) throw new NoEffectError('action transaction is closed')
        return client.query(sql, params)
      } }
      try {
        await client.query('BEGIN ISOLATION LEVEL SERIALIZABLE')
        await client.query("SELECT set_config('statement_timeout',$1,true),set_config('lock_timeout',$1,true)",
          [String(Math.max(1, Math.min(30_000, Date.parse(options.deadlineAt) - Date.now())))])
        await lockAction(scoped, work, action)
        const prior = await scoped.query('SELECT result FROM lingxios.agent_action_ledger WHERE idempotency_key=$1', [action.idempotencyKey])
        const previous = prior.rows[0]?.['result'] as HostActionResult | undefined
        const result = previous && !previous.approval ? previous : await abortable(run(scoped), options.signal)
        options.signal.throwIfAborted()
        if (result.executionState === 'unknown') throw new NoEffectError('transactional tool must return a settled result')
        if (!result.ok && !result.approval) {
          active = false
          await client.query('ROLLBACK')
          return { ...result, executionState: 'no_effect' }
        }
        await scoped.query(`INSERT INTO lingxios.agent_action_ledger(idempotency_key,result) VALUES($1,$2::jsonb)
          ON CONFLICT(idempotency_key) DO UPDATE SET result=EXCLUDED.result,recorded_at=NOW()
          WHERE lingxios.agent_action_ledger.result->'approval'->>'status'='PENDING'`,
          [action.idempotencyKey, JSON.stringify(result)])
        active = false
        committing = true
        await client.query('COMMIT')
        return result
      } catch (error) {
        active = false
        // A lost COMMIT acknowledgement cannot establish rollback.
        if (committing) throw error
        await client.query('ROLLBACK')
        throw error instanceof NoEffectError ? error : new NoEffectError(errorMessage(error), 'transaction_rolled_back')
      } finally { active = false; client.release() }
    },
    async readResource(work, action, options) {
      const tool = definition(action)
      if (tool.effect !== 'read') throw new NoEffectError('resource check requires a read tool')
      const input = tool.parse(action.args)
      const ctx = context(work, action, options)
      await tool.authorize(ctx, input)
      const result = await abortable(tool.execute(ctx, input), options.signal)
      if (!result.ok) throw new NoEffectError(result.error ?? 'resource read failed')
      return result.value
    },
    async verifyResult(work, action, value, options) {
      const tool = definition(action)
      if (!tool.verify) return { status: 'inconclusive', evidence: { reason: 'Native tool has no independent resource check' } }
      const input = tool.parse(action.args)
      const ctx = context(work, action, options)
      // The independent verifier authorizes its own read scope, including deleted resources.
      return abortable(tool.verify(ctx, input, value), options.signal)
    },
  }
}
