import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { it } from 'node:test'
import { PGlite } from '@electric-sql/pglite'
import { PgWorkStore, type SqlPool } from '../src/control-plane/pg-store.js'
import { createRealtime, type RunStreamEvent } from '../src/app/realtime.js'
import { Wakeup } from '../src/control-plane/wakeup.js'
import type { PreviewFrame } from '../src/protocol/preview.js'

it('resets retry/revision/gap/crash drafts, rechecks host policy, and fences an old uploader without clearing its successor', async () => {
  const db = new PGlite(), shutdown = new AbortController(), wake = new Wakeup()
  const query: SqlPool['query'] = async (sql, params) => {
    const result = await db.query<Record<string, unknown>>(sql, params)
    return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
  }
  const pool: SqlPool = { query, connect: async () => ({ query, release() {} }) }
  const releases: Array<() => void> = [], uploads: Promise<void>[] = []
  let allowed = true
  const realtime = createRealtime(pool, wake, shutdown.signal, { allowDraft: () => allowed, maxSubscribers: 1 })
  const identity = { runId: 'w', tenantId: 't', agentId: 'a', sessionId: 's', principalId: 'u' }
  const snapshot = async () => {
    const response = await realtime.response(identity, { signal: AbortSignal.timeout(2000) })
    const reader = response.body!.getReader()
    try {
      for (;;) {
        const { value, done } = await reader.read()
        assert.equal(done, false)
        const data = new TextDecoder().decode(value).split('\n').find(line => line.startsWith('data: '))
        if (!data) continue
        const item = JSON.parse(data.slice(6)) as RunStreamEvent
        if (item.type === 'preview' || item.type === 'reset') return item
      }
    } finally { await reader.cancel() }
  }
  try {
    await db.exec(await readFile(new URL('../../db/schema.sql', import.meta.url), 'utf8'))
    const store = new PgWorkStore(pool)
    await store.enqueue({ id: 'w', tenantId: 't', agentId: 'a', principalId: 'u', sessionId: 's', kind: 'turn', lane: 'interactive', triggerRef: 'm' })
    let work = (await store.claim('worker'))!
    const upload = async (frames: PreviewFrame[]) => {
      let release!: () => void, entered!: () => void
      const hold = new Promise<void>(resolve => { release = resolve })
      const ready = new Promise<void>(resolve => { entered = resolve })
      const task = realtime.receive(work, (async function* () {
        for (const frame of frames) yield frame
        entered(); await hold
      })(), shutdown.signal)
      uploads.push(task); releases.push(release)
      await Promise.race([ready, task.then(() => assert.fail('upload ended early'))])
      return { release, task }
    }
    const frames = (attemptId: string, text: string, requestVersion = 1): PreviewFrame[] => [
      { kind: 'reset', text: '', seq: 1, attemptId, requestVersion },
      { kind: 'delta', text, seq: 2, attemptId, requestVersion },
    ]
    const first = await upload(frames('first', 'old'))
    assert.equal((await snapshot()).type, 'preview')
    allowed = false
    assert.equal((await snapshot()).type, 'reset')
    allowed = true
    first.release(); await first.task
    assert.equal((await snapshot()).type, 'reset')
    const retry = await upload([...frames('retry', 'new'),
      { kind: 'reset', text: '', seq: 3, attemptId: 'correction', requestVersion: 1 },
      { kind: 'delta', text: 'replacement', seq: 4, attemptId: 'correction', requestVersion: 1 }])
    assert.equal(((await snapshot()) as { preview: { draft: string } }).preview.draft, 'replacement')
    const oldWork = work
    await db.query("UPDATE lingxios.agent_work_items SET lease_expires_at=NOW()-INTERVAL '1 second' WHERE id='w'")
    await db.query("UPDATE lingxios.agent_os_session_leases SET expires_at=NOW()-INTERVAL '1 second' WHERE work_id='w'")
    work = (await store.claim('successor'))!
    assert.equal(work.fence, 2)
    const successor = await upload(frames('successor', 'current'))
    retry.release(); await retry.task
    assert.equal(((await snapshot()) as { preview: { draft: string } }).preview.draft, 'current')
    await assert.rejects(realtime.receive(oldWork, (async function* () { yield frames('stale', '')[0]! })(), shutdown.signal), /lease/)
    assert.equal(((await snapshot()) as { preview: { draft: string } }).preview.draft, 'current')
    await store.addSteer('w', 'revised')
    assert.equal((await snapshot()).type, 'reset')
    successor.release(); await successor.task
    const gap = await upload([{ kind: 'reset', text: '', seq: 1, attemptId: 'revised', requestVersion: 2 },
      { kind: 'delta', text: 'gap', seq: 3, attemptId: 'revised', requestVersion: 2 }])
    assert.equal((await snapshot()).type, 'reset')
    gap.release(); await gap.task
    await assert.rejects(realtime.response({ ...identity, principalId: 'stranger' }), /identity/)
  } finally {
    shutdown.abort(); releases.forEach(release => release())
    await Promise.allSettled(uploads); await db.close()
  }
})
