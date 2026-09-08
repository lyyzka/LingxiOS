import assert from 'node:assert/strict'
import { EventEmitter } from 'node:events'
import { it } from 'node:test'
import { Wakeup, listenWakeups } from '../src/control-plane/wakeup.js'
import { nullLogger } from '../src/logging.js'
import { AgentWorker } from '../src/worker/worker.js'
import type { WorkItem } from '../src/protocol/types.js'

it('does not lose notifications between the empty scan and subscribing; abort cleans up waiters', async () => {
  const wake = new Wakeup(), after = wake.version
  wake.notify()
  await wake.wait(after, 60_000)
  const controller = new AbortController()
  const pending = wake.wait(wake.version, 60_000, controller.signal)
  controller.abort(new Error('stop'))
  await assert.rejects(pending, /stop/)
  const next = wake.wait(wake.version, 60_000)
  wake.notify()
  await next
})

it('scans after LISTEN and reconnect, never releases a listening connection into the pool', async () => {
  const stop = new AbortController(), clients: Array<EventEmitter & { release: (error?: Error) => void }> = []
  const scans: string[] = [], released: Error[] = []
  let listening = 0, connected!: () => void
  const ready = new Promise<void>(resolve => { connected = resolve })
  const pool = { query: async () => ({ rows: [], rowCount: 0 }), async connect() {
    const client = Object.assign(new EventEmitter(), {
      query: async (sql: string) => { assert.match(sql, /LISTEN lingxios_work/); listening++; return { rows: [], rowCount: 0 } },
      release: (error?: Error) => { assert.ok(error); released.push(error) },
    })
    clients.push(client)
    return client
  } }
  const loop = listenWakeups(pool, channel => {
    assert.ok(listening > 0)
    scans.push(channel)
    if (scans.length === 2) { connected(); clients[0]!.emit('error', new Error('disconnect')) }
    if (scans.length === 4) stop.abort()
  }, stop.signal, nullLogger)
  const keepAlive = setTimeout(() => stop.abort(), 5000)
  try { await ready; await loop }
  finally { clearTimeout(keepAlive) }
  assert.deepEqual(scans, ['work', 'outbox', 'work', 'outbox'])
  assert.equal(released.length, 2)
  assert.equal(clients[0]!.listenerCount('notification'), 0)
})

it('wakes an idle worker without waiting for the poll deadline and preserves a periodic fallback', async () => {
  const wake = new Wakeup()
  let queued = false, executed!: () => void, empty!: () => void
  const running = new Promise<void>(resolve => { executed = resolve })
  const scanned = new Promise<void>(resolve => { empty = resolve })
  const worker = new AgentWorker({ workerId: 'wake-test', maxConcurrentRuns: 1, shutdownGraceMs: 1000, pollIdleMs: 25_000, healthPort: 0,
    host: {
      claimWork: async () => {
        if (!queued) { empty(); return null }
        queued = false
        return { id: 'wake-work' } as WorkItem
      },
      waitForWork: async (cursor, timeout, signal) => {
        await wake.wait(Number(cursor ?? 0), timeout, signal)
        return String(wake.version)
      },
    }, runtime: { runWork: async () => { executed() } },
  })
  await worker.start()
  try {
    await scanned
    queued = true
    wake.notify()
    await running
  } finally { assert.deepEqual(await worker.stop(), { timedOut: false }) }
})

it('keeps reserved capacity for interactive lanes while a background run stalls', async () => {
  const calls: unknown[] = []
  let release!: () => void, entered!: () => void
  const background = new Promise<void>(resolve => { release = resolve })
  const interactive = new Promise<void>(resolve => { entered = resolve })
  const worker = new AgentWorker({ workerId: 'reserved', maxConcurrentRuns: 2, reservedInteractiveRuns: 1,
    shutdownGraceMs: 1000, pollIdleMs: 10, healthPort: 0,
    host: { claimWork: async (_signal, lanes) => {
      calls.push(lanes)
      if (calls.length === 1) return { id: 'background', kind: 'turn', lane: 'background' } as WorkItem
      if (calls.length === 2) return { id: 'interactive', kind: 'turn', lane: 'interactive' } as WorkItem
      return null
    } }, runtime: { runWork: async work => { if (work.lane === 'background') await background; else entered() } } })
  await worker.start()
  try {
    await interactive
    assert.deepEqual(calls.slice(0, 2), [undefined, ['interactive','approval']])
  } finally { release(); await worker.stop() }
})
