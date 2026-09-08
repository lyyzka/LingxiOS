import assert from 'node:assert/strict'
import { it } from 'node:test'
import { checkpointHost } from '../src/host/checkpoint-host.js'
import type { HostPort } from '../src/host/port.js'
import type { SessionRecord, WorkItem } from '../src/protocol/types.js'

it('deduplicates only acknowledged identical checkpoints and serializes concurrent CAS saves', async () => {
  const saved: SessionRecord[] = []
  let revision = 0, active = 0, fail = false
  const host = checkpointHost({ async saveSession(_work: WorkItem, session: SessionRecord) {
    assert.equal(active++, 0)
    try {
      await new Promise<void>(resolve => setImmediate(resolve))
      if (fail) { fail = false; throw new Error('save failed') }
      assert.equal(session.revision, revision)
      session.revision = ++revision
      saved.push(structuredClone(session))
    } finally { active-- }
  } } as HostPort)
  const work = { id: 'w', fence: 1, leaseToken: 'token' } as WorkItem
  const session: SessionRecord = { key: 's', tenantId: 't', agentId: 'a', sessionId: 's', history: [], appliedWorkIds: [], revision: 0, compactionEpoch: 0 }
  await Promise.all([host.saveSession(work, session), host.saveSession(work, session)])
  assert.equal(saved.length, 1)
  assert.equal(session.revision, 1)
  session.history.push({ role: 'assistant', content: 'tool output' })
  await Promise.all([host.saveSession(work, session), host.saveSession(work, session)])
  assert.equal(saved.length, 2)
  fail = true
  session.history.push({ role: 'user', content: 'revision' })
  await assert.rejects(host.saveSession(work, session), /save failed/)
  await host.saveSession(work, session)
  assert.equal(saved.length, 3)
  await host.saveSession({ ...work, fence: 2, leaseToken: 'new' }, session)
  assert.equal(saved.length, 4)
  assert.deepEqual(saved.at(-1)?.history, session.history)
})
