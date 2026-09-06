import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { it } from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import { createLingxiOS } from '../src/app/index.js'
import { sessionKeyOf } from '../src/protocol/types.js'
import type { SqlPool } from '../src/control-plane/pg-store.js'
import http from 'node:http'

it('asks through real Python, exposes the question and continues after a human reply', async () => {
  const db = new PGlite()
  const pool: SqlPool = { query: async (sql, params) => {
    const result = await db.query<Record<string, unknown>>(sql, params)
    return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
  }, connect: async () => ({ query: pool.query, release() {} }) }
  let requests = 0
  let resumedContext = ''
  const server = http.createServer((request, response) => {
    let body = ''
    request.on('data', chunk => { body += chunk })
    request.on('end', () => {
      requests++
      if (requests > 1) resumedContext = body
      const delta = requests === 1 ? { tool_calls: [{ index: 0, id: 'ask', function: { name: 'ipython',
        arguments: JSON.stringify({ code: 'host.task.ask(question="Which city?")\nraise RuntimeError("must not execute after asking")' }) } }] } : { content: JSON.stringify({ body: 'Use Shanghai.', status: 'blocked',
          checks: [{ requirement: 'Shanghai', status: 'met', basis: 'The continuation supplied the city.' }], gaps: ['This fixture supplies no itinerary.'] }) }
      response.writeHead(200, { 'content-type': 'text/event-stream' })
      response.end(`data: ${JSON.stringify({ choices: [{ delta, finish_reason: requests === 1 ? 'tool_calls' : 'stop' }] })}\n\ndata: [DONE]\n\n`)
    })
  })
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  assert.ok(address && typeof address === 'object')
  await db.exec(await readFile(new URL('../../db/schema.sql', import.meta.url), 'utf8'))
  const app = await createLingxiOS({ database: pool, model: { id: 'test', apiKey: 'test', baseUrl: `http://127.0.0.1:${address.port}` } })
  try {
    const identity = { runId: 'ask-work', tenantId: 't', agentId: 'a', sessionId: 's' }
    const attachment = { id: 'guide', sourceVersion: 'sha256:content-version', name: 'guide.txt', mimeType: 'text/plain', size: 17, text: 'Prefer quiet parks' }
    await app.enqueue({ ...identity, id: identity.runId, principalId: 'u', text: 'Plan a visit in my city.', attachments: [attachment] })
    attachment.text = 'Changed after submission'
    assert.equal(await app.runNext(), true)
    assert.deepEqual(await app.readOutcome(identity), { status: 'awaiting_input', verification: 'not_run', requestVersion: 1, question: 'Which city?' })
    assert.equal(await app.readMessage(identity), null)
    assert.equal(requests, 1)
    const page = await app.readEvents(identity)
    assert.ok(page.events.some(event => event.kind === 'goal.waiting' && (event.data['goalOutcome'] as { question: string }).question === 'Which city?'))
    assert.ok(page.events.every(event => event.visibility === 'user'))
    assert.deepEqual(await app.readEvents({ ...identity, tenantId: 'other' }), { events: [], nextSeq: 0 })
    assert.deepEqual(await app.readEvents(identity, page.nextSeq), { events: [], nextSeq: page.nextSeq })
    await assert.rejects(app.readEvents(identity, -1), /cursor/)
    // Simulate process loss after the ask receipt, before session/output and wait completion.
    await db.exec(`UPDATE lingxios.agent_work_items SET status='queued',goal_outcome=NULL,finished_at=NULL;
      UPDATE lingxios.agent_os_sessions SET history=history- (jsonb_array_length(history)-1)`)
    assert.equal(await app.runNext(), false)
    assert.equal(requests, 1)
    assert.deepEqual(await app.readOutcome(identity), { status: 'awaiting_input', verification: 'not_run', requestVersion: 1, question: 'Which city?' })
    const recovered = (await db.query<{ history: Array<{ output?: string }> }>('SELECT history FROM lingxios.agent_os_sessions')).rows[0]!.history.at(-1)!
    assert.equal(JSON.parse(recovered.output!).recovered, true)
    // A later intent with a lost receipt must not disappear behind the terminal ask receipt.
    await db.exec(`UPDATE lingxios.agent_work_items SET status='queued',goal_outcome=NULL,finished_at=NULL;
      UPDATE lingxios.agent_os_sessions SET history=history-(jsonb_array_length(history)-1);
      INSERT INTO lingxios.agent_action_intents(idempotency_key,fingerprint,intent)
        SELECT 'lost-after-ask','lost',jsonb_set(intent,'{action,callIndex}','1'::jsonb) FROM lingxios.agent_action_intents LIMIT 1`)
    assert.equal(await app.runNext(), true)
    assert.equal(requests, 1)
    assert.equal((await app.readOutcome(identity))?.status, 'blocked')
    await db.exec(`DELETE FROM lingxios.agent_action_intents WHERE idempotency_key='lost-after-ask';
      UPDATE lingxios.agent_work_items SET status='queued',goal_outcome=NULL,finished_at=NULL`)
    assert.equal(await app.runNext(), false)
    assert.equal((await app.readOutcome(identity))?.status, 'awaiting_input')
    const replyAttachment = { id: 'schedule', sourceVersion: 'v1', name: 'schedule.txt', mimeType: 'text/plain', size: 14, text: 'Avoid Mondays.' }
    const reply = { ...identity, principalId: 'u', inputId: 'reply', requestVersion: 1, text: 'Shanghai', attachments: [replyAttachment] }
    await assert.rejects(app.continueInput({ ...reply, attachments: Array.from({ length: 20 }, (_, index) => ({ ...replyAttachment, id: String(index) })) }), /request attachment limit/)
    await app.continueInput(reply)
    assert.equal((await app.continueInput(reply)).status, 'already_resumed')
    await assert.rejects(app.continueInput({ ...reply, attachments: [{ ...replyAttachment, sourceVersion: 'v2' }] }), /different text or attachments/)
    replyAttachment.text = 'Changed reply attachment'
    assert.equal(await app.runNext(), true)
    assert.match(resumedContext, /Plan a visit in my city/)
    assert.match(resumedContext, /Shanghai/)
    assert.match(resumedContext, /Prefer quiet parks/)
    assert.match(resumedContext, /Avoid Mondays/)
    assert.doesNotMatch(resumedContext, /Changed reply attachment/)
    assert.doesNotMatch(resumedContext, /Changed after submission/)
    assert.equal((await app.readMessage(identity))?.body, 'Use Shanghai.')
    assert.equal((await app.readOutcome(identity))?.requestVersion, 2)
  } finally {
    await app.stop()
    await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()))
    await db.close()
  }
})

it('resumes the same waiting request once with authenticated identity and version checks', async () => {
  const db = new PGlite()
  const pool: SqlPool = { query: async (sql, params) => {
    const result = await db.query<Record<string, unknown>>(sql, params)
    return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
  }, connect: async () => ({ query: pool.query, release() {} }) }
  await db.exec(await readFile(new URL('../../db/schema.sql', import.meta.url), 'utf8'))
  const app = await createLingxiOS({ database: pool, model: { id: 'unused', apiKey: 'unused' } })
  try {
    await app.enqueue({ id: 'w', tenantId: 't', agentId: 'a', sessionId: 's', threadId: 'thread', principalId: 'u', text: 'Original goal' })
    const input = { runId: 'w', tenantId: 't', agentId: 'a', sessionId: 's', threadId: 'thread', principalId: 'u', requestVersion: 1, inputId: 'reply', text: 'Missing detail' }
    const request = { version: 1, workId: 'w', tenantId: 't', sessionId: 's', authorId: 'u', sourceRef: 'w', attachments: [], originalText: 'Original goal', revisions: [] }
    await db.query(`INSERT INTO lingxios.agent_os_sessions(session_key,tenant_id,agent_id,session_id,thread_id,request_snapshot)
      VALUES($1,'t','a','s','thread',$2)`, [sessionKeyOf(input), JSON.stringify(request)])
    await db.exec(`UPDATE lingxios.agent_work_items SET status='completed',goal_outcome='{"status":"awaiting_input","verification":"not_run","requestVersion":1}'`)
    for (const change of [{ principalId: 'other' }, { tenantId: 'other' }, { threadId: 'other' }, { requestVersion: 2 }]) {
      await assert.rejects(app.continueInput({ ...input, ...change }), /identity|version/)
    }
    await db.query(`INSERT INTO lingxios.agent_os_session_leases(session_key,work_id,fence,expires_at) VALUES($1,'active',1,NOW()+INTERVAL '1 hour')`, [sessionKeyOf(input)])
    await assert.rejects(app.continueInput(input), /session is active/)
    assert.deepEqual((await db.query('SELECT request_snapshot FROM lingxios.agent_os_sessions')).rows, [{ request_snapshot: request }])
    await db.exec('DELETE FROM lingxios.agent_os_session_leases')
    assert.deepEqual(await app.continueInput(input), { status: 'resumed', workId: 'w' })
    assert.deepEqual(await app.continueInput(input), { status: 'already_resumed', workId: 'w' })
    await assert.rejects(app.continueInput({ ...input, text: 'Changed reply' }), /different text/)
    await assert.rejects(app.continueInput({ ...input, inputId: 'another' }), /not waiting/)
    const work = (await db.query<{ status: string; steer_inputs: unknown[]; goal_outcome: unknown; thread_id: string; meta: { text: string } }>('SELECT status,steer_inputs,goal_outcome,thread_id,meta FROM lingxios.agent_work_items')).rows[0]!
    const session = (await db.query<{ request_snapshot: unknown; revision: number }>('SELECT request_snapshot,revision FROM lingxios.agent_os_sessions')).rows[0]!
    assert.equal(work.status, 'queued')
    assert.equal(work.thread_id, 'thread')
    assert.equal(work.goal_outcome, null)
    assert.equal((work.meta as { text: string }).text, 'Original goal')
    assert.deepEqual(session.request_snapshot, { ...request, revisions: work.steer_inputs })
    assert.equal((work.steer_inputs as unknown[]).length, 1)
    assert.equal(session.revision, 1)
    assert.equal(await app.cancel({ ...input, principalId: 'other' }), false)
    assert.equal(await app.cancel({ ...input, threadId: 'other' }), false)
    await db.exec(`UPDATE lingxios.agent_work_items SET status='completed',goal_outcome='{"status":"awaiting_input","verification":"not_run","requestVersion":2}'`)
    assert.equal(await app.cancel(input), true)
    assert.equal(await app.cancel(input), false)
    assert.deepEqual((await db.query('SELECT status FROM lingxios.agent_work_items')).rows, [{ status: 'cancelled' }])
    assert.equal((await app.readOutcome(input))?.status, 'blocked')
    await assert.rejects(app.continueInput({ ...input, inputId: 'next-reply', requestVersion: 2 }), /not waiting/)
  } finally { await app.stop(); await db.close() }
})
