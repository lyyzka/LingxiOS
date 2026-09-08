import assert from 'node:assert/strict'
import { it } from 'node:test'
import { ResourceQuota } from '../src/resource-quota.js'
import { limitModel } from '../src/model/quota.js'
import type { ModelDriver } from '../src/model/driver.js'

it('bounds resource admission, removes cancelled waiters and releases failed operations', async () => {
  const quota = new ResourceQuota(1)
  let release!: () => void
  const first = quota.run(() => new Promise<void>(resolve => { release = resolve }))
  const stop = new AbortController()
  const cancelled = quota.run(async () => { assert.fail('cancelled waiter ran') }, stop.signal)
  const order: number[] = []
  const next = quota.run(async () => { order.push(1); throw new Error('failure') })
  stop.abort(new Error('cancelled'))
  await assert.rejects(cancelled, /cancelled/)
  assert.deepEqual(order, [])
  release()
  await first
  await assert.rejects(next, /failure/)
  assert.equal(await quota.run(async () => 42), 42)
})

it('preserves the shared model quota through single-attempt driver creation', async () => {
  const quota = new ResourceQuota(1)
  let running = 0, peak = 0
  const run = async () => { peak = Math.max(peak, ++running); await new Promise(resolve => setTimeout(resolve, 5)); running--; return {} }
  const source = { run, structured: run, compact: run, singleAttempt() { return this } } as unknown as ModelDriver
  const model = limitModel(source, quota).singleAttempt!()
  await Promise.all([model.run({ instructions: '', items: [] }), model.compact({ instructions: '', items: [] })])
  assert.equal(peak, 1)
})
