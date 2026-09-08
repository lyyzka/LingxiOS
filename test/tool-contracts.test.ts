import assert from 'node:assert/strict'
import { it } from 'node:test'
import { readFile } from 'node:fs/promises'
import { PGlite } from '@electric-sql/pglite'
import { createLingxiOS } from '../src/app/index.js'
import { snapshotRequest } from '../src/context/request.js'
import { actionKeyOf, sessionKeyOf, type WorkItem } from '../src/protocol/types.js'
import type { SqlPool } from '../src/control-plane/pg-store.js'
import type { ToolDefinition } from '../src/tools/definition.js'
import { toolContractHash } from '../src/tools/contracts.js'

it('requires full current scoped read receipts before modifying a resource', async () => {
  const db = new PGlite()
  const pool: SqlPool = { query: async (sql, params) => {
    const result = await db.query<Record<string, unknown>>(sql, params)
    return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
  }, connect: async () => ({ query: pool.query, release() {} }) }
  await db.exec(await readFile(new URL('../../db/schema.sql', import.meta.url), 'utf8'))
  await db.exec("CREATE TABLE documents(id text PRIMARY KEY, version int, title text); INSERT INTO documents VALUES('d',1,'Original')")
  let denied = false, executions = 0
  const read: ToolDefinition = { action: 'documents.get', name: 'documents__get', description: 'Read a document', effect: 'read', approval: false,
    observation: { resourceType: 'document', completeness: 'full' },
    parameters: { type: 'object', properties: { id: { type: 'string' } }, required: ['id'], additionalProperties: false },
    parse: input => { const id = (input as Record<string, unknown>)['id']; if (typeof id !== 'string') throw new Error('invalid id'); return { id } },
    authorize: async () => { if (denied) throw new Error('denied') },
    execute: async (context, input) => ({ ok: true, value: (await context.database.query('SELECT * FROM documents WHERE id=$1', [input['id']])).rows[0] }),
    observe: async (_context, _input, value) => [{ resourceId: String((value as Record<string, unknown>)['id']), version: String((value as Record<string, unknown>)['version']) }],
  }
  const write: ToolDefinition = { action: 'documents.rename', name: 'documents__rename', description: 'Rename a document', effect: 'transaction', approval: false,
    preconditions: { readAction: read.action, resourceType: 'document' },
    parameters: { type: 'object', properties: { id: { type: 'string' }, readKey: { type: 'string' } }, required: ['id', 'readKey'], additionalProperties: false },
    parse: value => { const input = value as Record<string, unknown>; if (typeof input['id'] !== 'string' || typeof input['readKey'] !== 'string') throw new Error('invalid input'); return input },
    authorize: async () => {},
    observationRequirement: async (context, input) => ({ actionKey: String(input['readKey']), resourceId: String(input['id']),
      currentVersion: String((await context.database.query('SELECT version FROM documents WHERE id=$1 FOR UPDATE', [input['id']])).rows[0]?.['version'] ?? '') }),
    execute: async context => { executions++; await context.database.query("UPDATE documents SET title='Updated',version=version+1 WHERE id='d'"); return { ok: true, value: { id: 'd' } } },
  }
  assert.equal(toolContractHash(read), toolContractHash({ ...read, parameters: { additionalProperties: false, required: ['id'], properties: { id: { type: 'string' } }, type: 'object' } }))
  const app = await createLingxiOS({ database: pool, tools: [read, write] })
  try {
    const host = app.connectWorker({ workerId: 'worker', workKinds: ['turn'] })
    const start = async (id: string, principalId: string) => {
      await app.enqueue({ id, tenantId: 't', agentId: 'a', sessionId: id, principalId, text: 'Rename the document.' })
      const work = (await host.claimWork())!
      await host.saveSession(work, { key: sessionKeyOf(work), tenantId: 't', agentId: 'a', sessionId: id, history: [], appliedWorkIds: [id], revision: 0,
        compactionEpoch: 0, request: snapshotRequest(await host.loadContext(work)) })
      return work
    }
    const work = await start('main', 'u'), other = await start('other', 'other-user')
    const action = (owner: WorkItem, cellId: string, name: string, args: Record<string, unknown>) => {
      const identity = { runId: owner.id, cellId, callIndex: 0 }
      return { ...identity, idempotencyKey: actionKeyOf(identity), action: name, args }
    }
    const modify = (id: string, key: string) => host.executeAction(work, action(work, id, write.action, { id: 'd', readKey: key }))
    assert.equal((await modify('missing', 'invented')).code, 'observation_required')
    read.observation!.completeness = 'summary'
    const summary = action(work, 'summary', read.action, { id: 'd' })
    await host.executeAction(work, summary)
    assert.equal((await modify('summary-write', summary.idempotencyKey)).code, 'observation_required')
    read.observation!.completeness = 'full'
    const original = action(work, 'full', read.action, { id: 'd' })
    const observed = await host.executeAction(work, original)
    assert.deepEqual(observed.observations, [{ resourceType: 'document', resourceId: 'd', version: '1', completeness: 'full' }])
    await db.exec("UPDATE documents SET version=2 WHERE id='d'")
    assert.equal((await modify('stale', original.idempotencyKey)).code, 'observation_required')
    const foreign = action(other, 'read', read.action, { id: 'd' })
    await host.executeAction(other, foreign)
    assert.equal((await modify('foreign', foreign.idempotencyKey)).code, 'observation_required')
    const fresh = action(work, 'fresh', read.action, { id: 'd' })
    await host.executeAction(work, fresh)
    denied = true
    assert.equal((await modify('revoked', fresh.idempotencyKey)).code, 'observation_required')
    denied = false
    assert.equal((await modify('valid', fresh.idempotencyKey)).ok, true)
    assert.equal((await modify('valid', fresh.idempotencyKey)).ok, true)
    assert.equal(executions, 1)
    assert.deepEqual((await db.query('SELECT * FROM documents')).rows, [{ id: 'd', version: 3, title: 'Updated' }])
  } finally { await app.stop(); await db.close() }
})
