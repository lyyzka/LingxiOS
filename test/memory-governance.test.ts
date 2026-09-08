import assert from 'node:assert/strict'
import { it } from 'node:test'
import { readFile } from 'node:fs/promises'
import { PGlite } from '@electric-sql/pglite'
import { createLingxiOS } from '../src/app/index.js'
import { withTransaction, type SqlPool } from '../src/control-plane/pg-store.js'
import { snapshotRequest } from '../src/context/request.js'
import { snapshotEvidence } from '../src/context/evidence.js'
import { createResponseEnvelope } from '../src/outcome/envelope.js'
import { actionKeyOf, sessionKeyOf, type WorkItem } from '../src/protocol/types.js'
import { captureMemoryEvidence } from '../src/memory/evidence.js'
import { executeMemorySynthesis } from '../src/memory/synthesis.js'
import { memorySynthesisProcessor } from '../src/memory/processor.js'
import { memoryWriteBody, type MemoryWritePolicy } from '../src/memory/policy.js'
import type { ToolDefinition } from '../src/tools/definition.js'
import type { WorkProcessorContext } from '../src/runtime/runtime.js'

it('shares content policy across native notes and twice-approved synthesis, and fences forgetting across retries', async () => {
  const db = new PGlite()
  const pool: SqlPool = { query: async (sql, params) => {
    const result = await db.query<Record<string, unknown>>(sql, params)
    return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
  }, connect: async () => ({ query: pool.query, release() {} }) }
  await db.exec(await readFile(new URL('../../db/schema.sql', import.meta.url), 'utf8'))
  const scope = { tenantId: 't', scopeType: 'learner', scopeId: 'u' }
  const policy: MemoryWritePolicy = input => input.body.includes('restricted') ? { action: 'reject', code: 'restricted' }
    : input.body.includes('private') ? { action: 'redact', body: input.body.replace('private', '[redacted]') } : { action: 'allow' }
  const tool: ToolDefinition = { name: 'notes__save', action: 'notes.save', description: 'Save a memory', effect: 'transaction', approval: false,
    parameters: { type: 'object', properties: { body: { type: 'string' } }, required: ['body'], additionalProperties: false },
    parse: value => { const body = (value as Record<string, unknown>)['body']; if (typeof body !== 'string') throw new Error('invalid body'); return { body } },
    authorize: async () => {}, execute: async (context, input) => ({ ok: true, value: await context.writeMemory(scope, { method: 'note', body: String(input['body']) }) }) }
  const app = await createLingxiOS({ database: pool, tools: [tool], memory: {
    writePolicy: policy, resolveScopes: async work => work.tenantId === 't' && work.principalId === 'u' ? [scope] : [],
  } })
  try {
    await app.enqueue({ id: 'writer', tenantId: 't', agentId: 'a', sessionId: 'writer', principalId: 'u', text: 'Remember this.' })
    const host = app.connectWorker({ workerId: 'writer-worker', workKinds: ['turn'] })
    const work = (await host.claimWork())!
    const request = snapshotRequest(await host.loadContext(work))
    await host.saveSession(work, { key: sessionKeyOf(work), tenantId: 't', agentId: 'a', sessionId: 'writer',
      history: [], appliedWorkIds: ['writer'], revision: 0, compactionEpoch: 0, request })
    const save = (body: string) => { const identity = { runId: work.id, cellId: body, callIndex: 0 }; return host.executeAction(work,
      { ...identity, idempotencyKey: actionKeyOf(identity), action: tool.action, args: { body } }) }
    assert.equal((await save('restricted')).executionState, 'no_effect')
    assert.deepEqual((await db.query('SELECT id FROM lingxios.agent_memories')).rows, [])
    assert.equal(((await save('private preference')).value as { body: string }).body, '[redacted] preference')
    await assert.rejects(memoryWriteBody({ scope, principalId: 'u', sourceWorkId: 'writer', origin: 'explicit', kind: 'note', body: 'api_key=abcdef123456' }), /credential policy/)
    await assert.rejects(memoryWriteBody({ scope, principalId: 'u', sourceWorkId: 'writer', origin: 'explicit', kind: 'note', body: 'ordinary note' },
      (() => undefined) as unknown as MemoryWritePolicy), /invalid memory write policy decision/)

    let sequence = 0
    const source = async (capture = true) => {
      const id = `source-${sequence++}`
      await db.query(`INSERT INTO lingxios.agent_work_items(id,tenant_id,agent_id,principal_id,session_id,kind,lane,trigger_ref,status)
        VALUES($1,'t','a','u',$1,'turn','interactive','m','succeeded')`, [id])
      const sourceWork: WorkItem = { ...work, id, sessionId: id, triggerRef: 'm' }
      const snapshot = { ...request, workId: id, sessionId: id, sourceRef: 'm' }
      await db.query(`INSERT INTO lingxios.agent_os_sessions(session_key,tenant_id,agent_id,session_id,request_snapshot)
        VALUES($1,'t','a',$2,$3::jsonb)`, [sessionKeyOf(sourceWork), id, JSON.stringify(snapshot)])
      const message = { version: 2 as const, runId: id, agentId: 'a', sessionId: id, body: 'Recorded.',
        envelope: createResponseEnvelope('Recorded.', { status: 'partial', verification: 'not_run', requestVersion: 1 }, snapshotEvidence('e', [])) }
      if (capture) await withTransaction(pool, client => captureMemoryEvidence(client, sourceWork, message, [scope]))
      const job = { ...sourceWork, id: `memory-synthesis:${id}`, kind: 'memory_synthesis', meta: { sourceRunId: id } }
      if (capture) await db.query("UPDATE lingxios.agent_work_items SET status='leased',fence=1,lease_expires_at=NOW()+INTERVAL '1 hour' WHERE id=$1", [job.id])
      return { sourceWork, job, message }
    }
    const apply = (job: WorkItem, changes: unknown[]) => withTransaction(pool, client => executeMemorySynthesis(client, job, 'apply', { changes, approved: true, confidence: 1 }, [scope], policy))
    const load = (job: WorkItem) => withTransaction(pool, client => executeMemorySynthesis(client, job, 'load', {}, [scope], policy))
    const first = await source()
    let calls = 0
    const change = { action: 'create', scopeType: scope.scopeType, sourceRunIds: [first.sourceWork.id], body: 'restricted' }
    const context = { signal: new AbortController().signal, emit: async () => {},
      host: { executeAction: async (_work: WorkItem, action: { action: string; args: Record<string, unknown> }) => ({ ok: true,
        value: await withTransaction(pool, client => executeMemorySynthesis(client, first.job, action.action.split('.')[1]!, action.args, [scope], policy)) }) },
      model: { structured: async () => ({ value: ++calls === 1 ? { changes: [change] } : { approved: true, confidence: 1 },
        model: 'main', usage: { available: true, inputTokens: 10, outputTokens: 10 } }) },
    } as unknown as WorkProcessorContext
    await assert.rejects(memorySynthesisProcessor.process(first.job, context), /content policy/)
    assert.equal(calls, 2)
    assert.equal((await db.query('SELECT id FROM lingxios.agent_memories')).rows.length, 1)
    await apply(first.job, [{ ...change, body: 'Allowed observation' }])
    await db.exec(`INSERT INTO lingxios.agent_memory_embeddings(tenant_id,memory_id,version,model_key,model,embedding)
      SELECT tenant_id,id,version,'test','test',ARRAY[1.0] FROM lingxios.agent_memories`)

    const waiting = await source(), late = await source(false)
    await load(waiting.job)
    await assert.rejects(app.forgetMemory({ ...work, principalId: 'other' }, scope), /revoked/)
    assert.deepEqual(await app.forgetMemory(work, scope), { epoch: 1 })
    assert.deepEqual(await apply(waiting.job, [{ ...change, sourceRunIds: [waiting.sourceWork.id], body: 'Allowed observation' }]), { outcome: 'superseded', changeCount: 0 })
    await db.query('UPDATE lingxios.agent_work_items SET fence=2 WHERE id=$1', [waiting.job.id])
    assert.equal(await load({ ...waiting.job, fence: 2 }), null)
    await withTransaction(pool, client => captureMemoryEvidence(client, late.sourceWork, late.message, [scope]))
    assert.deepEqual((await db.query('SELECT source_run_id FROM lingxios.agent_memory_evidence WHERE source_run_id=$1', [late.sourceWork.id])).rows, [])
    assert.deepEqual((await db.query('SELECT id FROM lingxios.agent_memories')).rows, [])
    assert.deepEqual((await db.query('SELECT memory_id FROM lingxios.agent_memory_embeddings')).rows, [])
    assert.deepEqual((await db.query('SELECT memory_id FROM lingxios.agent_memory_versions')).rows, [])
    assert.ok((await db.query<Record<string, unknown>>('SELECT input_text,assistant_text FROM lingxios.agent_memory_evidence')).rows.every(row => !row['input_text'] && !row['assistant_text']))
    assert.equal((await save('Allowed after forgetting from old request')).executionState, 'no_effect')
    const fresh = await source()
    await load(fresh.job)
    assert.deepEqual(await apply(fresh.job, [{ ...change, sourceRunIds: [fresh.sourceWork.id], body: 'New user observation' }]), { outcome: 'committed', changeCount: 1 })
  } finally { await app.stop(); await db.close() }
})
