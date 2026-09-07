import assert from 'node:assert/strict'
import { it } from 'node:test'
import { deadlinePool } from '../src/control-plane/deadline-pool.js'
import { deadlineHost } from '../src/host/deadline-host.js'
import type { SqlClient, SqlPool } from '../src/control-plane/pg-store.js'
import type { HostPort } from '../src/host/port.js'
import type { WorkItem } from '../src/protocol/types.js'

it('discards late acquisitions and unfinished database transactions instead of returning them to the pool', async () => {
  const releases: Array<Error | undefined> = []
  const raw: SqlClient = { query: async () => new Promise(() => {}), release: error => { releases.push(error) } }
  let acquired!: (client: SqlClient) => void
  const controller = new AbortController()
  const pool: SqlPool = { query: raw.query, connect: () => new Promise(resolve => { acquired = resolve }) }
  const connecting = deadlinePool(pool,controller.signal).connect()
  controller.abort(new Error('cancel acquisition'))
  await assert.rejects(connecting,/cancel acquisition/)
  acquired(raw)
  await new Promise<void>(resolve => setImmediate(resolve))
  assert.equal(releases[0]?.message,'cancel acquisition')
  const transaction = new AbortController()
  const client = await deadlinePool({ ...pool, connect: async () => raw },transaction.signal).connect()
  const committing = client.query('COMMIT')
  transaction.abort(new Error('commit acknowledgement deadline'))
  await assert.rejects(committing,/acknowledgement/)
  await assert.rejects(client.query('SELECT 1'),/acknowledgement/)
  client.release()
  assert.equal(releases.length,2)
  assert.equal(releases[1]?.message,'commit acknowledgement deadline')
})

it('cancels every host read while allowing bounded billing settlement after attempt cancellation', async () => {
  const controller = new AbortController()
  let observed: AbortSignal | undefined, settled = false
  const host = deadlineHost({ loadContext: async (_work: WorkItem, signal?: AbortSignal) => {
    observed = signal
    return new Promise(() => {})
  }, recordModelUsage: async (_work: WorkItem, _id: string, _usage: unknown, observation?: unknown, signal?: AbortSignal) => {
    assert.equal(observation,undefined)
    assert.equal(signal?.aborted,false)
    settled = true
  } } as unknown as HostPort,controller.signal)
  const pending = host.loadContext({ id: 'w' } as WorkItem)
  controller.abort(new Error('attempt cancelled'))
  await assert.rejects(pending,/attempt cancelled/)
  assert.equal(observed?.aborted,true)
  await host.recordModelUsage({ id: 'w' } as WorkItem,'call',{ inputTokens: 10,outputTokens: 2,costMicros: 1 })
  assert.equal(settled,true)
})
