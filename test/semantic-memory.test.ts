import assert from 'node:assert/strict'
import http from 'node:http'
import { createHash } from 'node:crypto'
import { readFile } from 'node:fs/promises'
import { it } from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import { PgWorkStore, type SqlPool } from '../src/control-plane/pg-store.js'
import { createSemanticMemory } from '../src/memory/semantic.js'
import type { WorkItem } from '../src/protocol/types.js'
import { memorySearchText } from '../src/memory/text.js'
import type { MemoryHit, MemorySearchResult } from '../src/memory/types.js'
const hits=(result:MemorySearchResult)=>result.items as MemoryHit[]

it('indexes scoped memories durably and ranks old semantic matches without trusting stale vectors', async () => {
  const db = new PGlite()
  const pool: SqlPool = { query: async (sql, args) => {
    const result = await db.query<Record<string, unknown>>(sql, args)
    return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
  }, connect: async () => ({ query: pool.query, release: () => {} }) }
  let fail = false
  let model = 'test-embedding'
  let requests = 0
  let indexedTail=false
  let beforeResponse: (() => Promise<void>) | undefined
  const server = http.createServer((req, res) => {
    const chunks: Buffer[] = []
    req.on('data', (chunk: Buffer) => chunks.push(chunk))
    req.on('end', () => { void (async () => {
      requests++
      const payload = JSON.parse(Buffer.concat(chunks).toString()) as { input: string[] }
      if(payload.input.some(text=>text.includes('tail marker'))) {
        indexedTail=true
        assert.equal(payload.input.join(''),'detail '.repeat(1500)+'tail marker')
        assert.ok(payload.input.every(text=>Buffer.byteLength(text)<=8000))
      }
      await beforeResponse?.()
      beforeResponse = undefined
      res.writeHead(fail ? 503 : 200, { 'content-type': 'application/json' })
      res.end(JSON.stringify(fail ? { error: 'unavailable' } : { model,
        data: payload.input.map((text, index) => ({ index, embedding: /pictures|diagrams/.test(text) ? [1, 0] : [0, 1] })) }))
    })().catch(error => { res.writeHead(500); res.end(String(error)) }) })
  })
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  assert.ok(address && typeof address === 'object')
  const semantic = createSemanticMemory(pool, { id: 'test-embedding', apiKey: 'test', baseUrl: `http://127.0.0.1:${address.port}`, dimensions: 2 })
  const work: WorkItem = { id: 'turn', tenantId: 't', agentId: 'a', principalId: 'u', sessionId: 's', kind: 'turn', lane: 'interactive',
    triggerRef: 'm', fence: 1, homeEpoch: 1, leaseToken: 'test' }
  const scope = { tenantId: 't', scopeType: 'learner' as const, scopeId: 'u' }
  const store = new PgWorkStore(pool)
  const drain = async () => {
    let count = 0
    for (let next = await store.claim('index-worker'); next; next = await store.claim('index-worker')) {
      assert.equal(next.kind, 'memory_index')
      assert.equal((await semantic.refresh(next, async () => {})).outcome, 'indexed')
      assert.equal(await store.complete(next.id, next.fence, createHash('sha256').update(next.leaseToken).digest('hex'), { status: 'completed' }), true)
      count++
    }
    return count
  }
  try {
    await db.exec(await readFile(new URL('../../db/schema.sql', import.meta.url), 'utf8'))
    await db.exec(`INSERT INTO lingxios.agent_work_items(id,tenant_id,agent_id,principal_id,session_id,kind,lane,trigger_ref,status,fence,lease_expires_at,lease_token_hash)
      VALUES('turn','t','a','u','s','turn','interactive','m','leased',1,NOW()+INTERVAL '1 hour','${createHash('sha256').update(work.leaseToken).digest('hex')}')`)
    for (let i = 0; i < 40; i++) await db.query(`INSERT INTO lingxios.agent_memories(tenant_id,id,scope_type,scope_id,body,kind,origin,source_refs,updated_at,path,title,description,search_text)
      VALUES('t',$1,'learner','u',$2,'observation','explicit','[{"workId":"turn"}]',NOW()+$3*INTERVAL '1 second',$1||'.md','Preference','User preference',$4)`,
    [`m${i}`, i === 0 ? 'Prefers diagrams' : i===1?'detail '.repeat(1500)+'tail marker':`Unrelated ${i}`, i,memorySearchText(i===0?'Prefers diagrams':`Unrelated ${i}`)])
    await db.exec(`INSERT INTO lingxios.agent_memories(tenant_id,id,scope_type,scope_id,body,kind,origin,source_refs,path,title,description,search_text)
      VALUES('foreign','secret','learner','u','Prefers diagrams','observation','explicit','[{"workId":"turn"}]','secret.md','Preference','Preference','prefers diagrams'),
      ('t','other-scope','course','elsewhere','Prefers diagrams','observation','explicit','[{"workId":"turn"}]','secret.md','Preference','Preference','prefers diagrams')`)
    const initial = await semantic.recall(work, scope, 'pictures', 3)
    assert.deepEqual(initial.items,[])
    assert.equal(await drain(), 32)
    await semantic.recall(work, scope, 'pictures', 3)
    assert.equal(await drain(), 8)
    assert.equal(indexedTail,true)
    const ranked = await semantic.recall(work, scope, 'pictures', 3)
    assert.equal(hits(ranked)[0]!.id, 'm0')
    assert.equal(ranked.retrieval, 'hybrid')
    assert.equal(requests, 41) // One cached query plus each of the 40 indexed bodies.
    assert.equal((await db.query('SELECT memory_id FROM lingxios.agent_memory_embeddings')).rows.length, 40)
    await db.exec("UPDATE lingxios.agent_memories SET body='Now prefers text',version=2 WHERE id='m0' AND tenant_id='t'")
    assert.equal((await semantic.recall(work, scope, 'pictures', 3)).items.length,0)
    const pending = await store.claim('index-worker')
    assert.ok(pending)
    beforeResponse = async () => { await db.exec("UPDATE lingxios.agent_memories SET body='Prefers diagrams again',version=3 WHERE id='m0' AND tenant_id='t'") }
    assert.equal((await semantic.refresh(pending, async () => {})).outcome, 'stale')
    assert.deepEqual((await db.query("SELECT version FROM lingxios.agent_memory_embeddings WHERE memory_id='m0'")).rows, [{ version: 1 }])
    await store.complete(pending.id, pending.fence, createHash('sha256').update(pending.leaseToken).digest('hex'), { status: 'completed' })
    await semantic.recall(work, scope, 'pictures', 3)
    const forbidden = await store.claim('index-worker')
    assert.ok(forbidden)
    const beforeDenied = requests
    await assert.rejects(semantic.refresh(forbidden, async () => { throw new Error('permission revoked') }), /permission revoked/)
    assert.equal(requests, beforeDenied)
    let authCalls = 0
    await assert.rejects(semantic.refresh(forbidden, async () => { if (++authCalls === 2) throw new Error('permission revoked') }), /permission revoked/)
    assert.equal((await semantic.refresh(forbidden, async () => {})).outcome, 'indexed')
    assert.equal(hits(await semantic.recall(work, scope, 'pictures', 3))[0]!.id, 'm0')
    await store.complete(forbidden.id, forbidden.fence, createHash('sha256').update(forbidden.leaseToken).digest('hex'), { status: 'completed' })
    model = 'test-embedding-v2'
    assert.deepEqual((await semantic.recall(work, scope, 'pictures after model change', 3)).items,[])
    assert.equal(await drain(), 32)
    await semantic.recall(work, scope, 'pictures after model change', 3)
    assert.equal(await drain(), 8)
    assert.equal(hits(await semantic.recall(work, scope, 'pictures after model change', 3))[0]!.id, 'm0')
    await assert.rejects(semantic.recall(work, { ...scope, tenantId: 'foreign' }, 'pictures', 3), /scope/)
    fail = true
    const fallback=await semantic.recall(work,scope,'unrelated',3)
    assert.equal(fallback.retrieval,'keyword_embedding_unavailable')
    assert.equal(fallback.items.length,3)
    await db.exec("DELETE FROM lingxios.agent_memories WHERE tenant_id='t' AND id='m0'")
    assert.deepEqual((await db.query("SELECT memory_id FROM lingxios.agent_memory_embeddings WHERE memory_id='m0'")).rows, [])
  } finally {
    server.closeAllConnections()
    await new Promise<void>(resolve => server.close(() => resolve()))
    await db.close()
  }
})
