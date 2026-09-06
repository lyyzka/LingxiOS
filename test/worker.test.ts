import assert from 'node:assert/strict'
import { it } from 'node:test'
import { startWorker } from '../src/worker/index.js'
import { AgentWorker } from '../src/worker/worker.js'
import type { WorkItem } from '../src/protocol/types.js'

it('cancels a pending claim without exhausting the shutdown grace period', async () => {
  let begin!: () => void
  const started = new Promise<void>((resolve) => { begin = resolve })
  let aborted = false
  const worker = new AgentWorker({
    workerId: 'test', maxConcurrentRuns: 1, shutdownGraceMs: 100, healthPort: 0,
    host: { claimWork: async (signal) => {
      begin()
      return new Promise<null>((resolve) => signal?.addEventListener('abort', () => {
        aborted = true
        resolve(null)
      }, { once: true }))
    } },
    runtime: { runWork: async () => { assert.fail('no work was claimed') } },
  })
  await worker.start()
  await started
  assert.deepEqual(await worker.stop(), { timedOut: false })
  assert.equal(aborted, true)
})

it('aborts in-flight model work when graceful shutdown expires', async () => {
  let claimed = false
  let begin!: () => void
  const started = new Promise<void>((resolve) => { begin = resolve })
  let aborted = false
  const worker = new AgentWorker({
    workerId: 'test', maxConcurrentRuns: 1, shutdownGraceMs: 10, healthPort: 0,
    host: { claimWork: async () => {
      if (claimed) return null
      claimed = true
      return { id: 'w' } as WorkItem
    } },
    runtime: { runWork: async (_work, signal) => {
      begin()
      await new Promise<void>((resolve) => signal!.addEventListener('abort', () => { aborted = true; resolve() }, { once: true }))
    } },
  })
  await worker.start()
  try {
    await started
    assert.deepEqual(await worker.stop(), { timedOut: true })
    assert.equal(aborted, true)
    await assert.rejects(worker.start(), /already/)
  } finally {
    await worker.stop()
  }
})

it('interrupts idle polling delay during shutdown', async () => {
  const worker = new AgentWorker({
    workerId: 'test', maxConcurrentRuns: 1, shutdownGraceMs: 100, healthPort: 0, pollIdleMs: 60_000,
    host: { claimWork: async () => null },
    runtime: { runWork: async () => { assert.fail('no work was claimed') } },
  })
  await worker.start()
  await new Promise<void>((resolve) => setImmediate(resolve))
  assert.deepEqual(await worker.stop(), { timedOut: false })
})

it('validates kernel configuration from the explicitly supplied worker environment', async () => {
  await assert.rejects(startWorker({
    AGENT_OS_CONTROL_PLANE_URL: 'http://127.0.0.1:1',
    AGENT_OS_SERVICE_TOKEN: 'test-token',
    AGENT_OS_MODEL: 'test-model',
    AGENT_OS_MODEL_API_KEY: 'test-key',
    AGENT_OS_KERNEL_IDLE_MS: 'invalid',
  }), /AGENT_OS_KERNEL_IDLE_MS/)
})

it('refuses the process kernel as an implicit production security boundary', async () => {
  await assert.rejects(startWorker({
    NODE_ENV: 'production',
    AGENT_OS_CONTROL_PLANE_URL: 'http://127.0.0.1:1',
    AGENT_OS_SERVICE_TOKEN: 'test-token',
    AGENT_OS_MODEL_API_KEY: 'test-key',
  }), /OS-isolated kernelFactory/)
})

it('rejects an unknown runtime policy deployment', async () => {
  await assert.rejects(startWorker({
    AGENT_OS_CONTROL_PLANE_URL: 'http://127.0.0.1:1', AGENT_OS_SERVICE_TOKEN: 'test-token',
    AGENT_OS_MODEL_API_KEY: 'test-key', AGENT_OS_RUNTIME_POLICY: 'unknown',
  }), /AGENT_OS_RUNTIME_POLICY/)
})
