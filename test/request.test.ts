import { createTaskContract } from '../src/context/task-contract.js'
import assert from 'node:assert/strict'
import { it } from 'node:test'
import { requestItems, snapshotRequest } from '../src/context/request.js'
import type { TurnContext } from '../src/protocol/types.js'
import { snapshotAttachments } from '../src/context/attachments.js'

it('freezes bounded attachment descriptors without inventing readable content', () => {
  const attachment = { id: 'file', sourceVersion: 'version-1', name: 'photo.png', mimeType: 'image/png', size: 1024 }
  const snapshot = snapshotAttachments([attachment])
  attachment.name = 'changed.png'
  assert.deepEqual(snapshot, [{ ...attachment, name: 'photo.png' }])
  assert.equal('text' in snapshot[0]!, false)
  for (const value of [[attachment, attachment], [{ ...attachment, size: -1 }], [{ ...attachment, sourceVersion: '' }],
    [{ ...attachment, text: 'x'.repeat(1_000_001) }], [{ ...attachment, path: '/private/file' }]]) {
    assert.throws(() => snapshotAttachments(value), /attachment/)
  }
})

it('preserves exact long input and ordered revisions independently of history', () => {
  const text = `${'很长的请求。'.repeat(20_000)}\n最后约束：仅提示，不要完整答案。`
  const context: TurnContext = {
    work: { id: 'w', tenantId: 't', agentId: 'a', sessionId: 's', kind: 'turn', lane: 'interactive', triggerRef: 'm', fence: 1, homeEpoch: 1, leaseToken: 'token' },
    persona: { name: 'Assistant', role: 'assistant', instructions: '' }, capabilities: [],
    messages: [{ ref: 'm', authorId: 'u', authorName: 'User', authorKind: 'human', body: text, createdAt: 'now' }],
  }
  assert.throws(() => snapshotRequest({ ...context, messages: [] }), /trigger message is required/)
  assert.throws(() => snapshotRequest({ ...context, work: { ...context.work, triggerRef: 'missing' } }), /trigger message is required/)
  assert.throws(() => snapshotRequest({ ...context, messages: [context.messages[0]!, { ...context.messages[0]!, body: 'Conflicting input' }] }), /exactly one trigger message/)
  const snapshot = snapshotRequest(context)
  context.messages[0]!.body = 'changed'
  snapshot.revisions.push({ id: 'r', text: '现在请给完整答案。', createdAt: 'later' })
  assert.deepEqual(requestItems(snapshot), [{ role: 'user', content: text }, { role: 'user', content: '现在请给完整答案。' }])
  assert.equal(snapshot.authorId, 'u')
  snapshot.contract = createTaskContract(text, 2, { deliverables: ['Answer'], constraints: [], actions: [], acceptance: ['Complete'] })
  assert.equal(requestItems(snapshot).length, 3)
  assert.deepEqual(requestItems(snapshot)[0], { role: 'user', content: text })
  snapshot.revisions.push({ id: 'r2', text: 'New constraint', createdAt: 'later' })
  assert.equal(requestItems(snapshot).length, 3)
  assert.equal(JSON.stringify(requestItems(snapshot)).includes('Derived task checklist'), false)
})
