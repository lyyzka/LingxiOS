import assert from 'node:assert/strict'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { it } from 'node:test'
import { KernelManager } from '../src/kernel/manager.js'
import type { HostActionResult, WorkItem } from '../src/protocol/types.js'

const work: WorkItem = {
  id: 'w', fence: 1, homeEpoch: 1, leaseToken: 'token', tenantId: 't', agentId: 'a',
  sessionId: 's', kind: 'turn', lane: 'interactive', triggerRef: 'm', createdAt: 'now',
  availableAt: 'now', attempts: 1, preemptions: 0,
}

it('bounds captured output while the cell runs', async () => {
  const manager = new KernelManager({ execute: async () => ({ ok: true }) }, { maxOutputChars: 100 })
  try {
    const result = await manager.execute(work, 'w', 'cell', 'print("x" * 10000)')
    assert.equal(result.stdout.length, 100)
    assert.equal(result.truncated, true)
  } finally { manager.close() }
})

it('marks a lost host bridge response unknown', async () => {
  let observed: HostActionResult | undefined
  const manager = new KernelManager({ execute: async () => { throw new Error('response lost') } })
  try {
    await assert.rejects(manager.execute(work, 'w', 'cell', 'host.files.save()', undefined, {
      capabilities: [{ name: 'files', methods: ['save'] }],
      onHostAction: async event => { if (event.result) observed = event.result },
    }), /response lost/)
    assert.equal(observed?.executionState, 'unknown')
  } finally { manager.close() }
})

it('bounds host action waits and preserves the unknown outcome', async () => {
  let observed: HostActionResult | undefined
  const manager = new KernelManager({ execute: async () => new Promise(() => {}) }, { hostActionTimeoutMs: 50 })
  try {
    await assert.rejects(manager.execute(work, 'w', 'cell', 'host.files.save()', undefined, {
      capabilities: [{ name: 'files', methods: ['save'] }],
      onHostAction: async event => { if (event.result) observed = event.result },
    }), /outcome requires reconciliation/)
    assert.equal(observed?.executionState, 'unknown')
  } finally { manager.close() }
})

it('kills a kernel that never becomes ready', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'lingxios-kernel-'))
  const runner = join(directory, 'silent.py')
  await writeFile(runner, 'import time\ntime.sleep(60)\n')
  const manager = new KernelManager({ execute: async () => ({ ok: true }) }, {
    runnerPath: runner, homesRoot: join(directory, 'homes'), startupTimeoutMs: 50,
  })
  try {
    await assert.rejects(manager.execute(work, 'w', 'cell', '1'), /did not become ready/)
  } finally {
    manager.close()
    await rm(directory, { recursive: true, force: true, maxRetries: 10, retryDelay: 50 })
  }
})
