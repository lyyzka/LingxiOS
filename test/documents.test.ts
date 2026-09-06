import assert from 'node:assert/strict'
import { it } from 'node:test'
import { executeDocument } from '../src/integrations/lingxiloop/documents.js'
import type { LingxiLoopServices } from '../src/integrations/lingxiloop/service-contracts.js'
import type { WorkItem } from '../src/protocol/types.js'

it('scopes document reads to the conversation and human, bounds output and rejects revocation', async () => {
  const work: WorkItem = { id: 'w', tenantId: 't', agentId: 'a', principalId: 'human', sessionId: 'conversation', kind: 'turn', lane: 'interactive', triggerRef: 'message', fence: 1, homeEpoch: 1, leaseToken: 'token' }
  const action = { runId: 'w', cellId: 'c', callIndex: 0, idempotencyKey: 'key', action: 'documents.read', args: { documentId: 'doc' } }
  const metadata = { id: 'doc', title: 'Document', createdBy: 'a', conversationId: null, createdAt: 'now', updatedAt: 'now' }
  let revoked = false
  let revokeDuringRead = false
  let reads = 0
  const permissions: unknown[] = []
  const db = { query: async (_sql: string, params?: readonly unknown[]) => {
    assert.deepEqual(params, ['t', 'conversation'])
    return { rows: [{ project_id: 'project' }], rowCount: 1 }
  } }
  const services: Pick<LingxiLoopServices, 'documents' | 'permissionService'> = {
    permissionService: { assertCan: async request => {
      assert.equal(request.actorUserId, 'human')
      permissions.push(request)
      if (revoked) throw new Error('revoked')
    } },
    documents: {
      listAgentDocuments: async scope => {
        assert.equal(scope.projectId, 'project')
        return Array.from({ length: 101 }, (_, index) => ({ ...metadata, id: String(index) }))
      },
      listRecentAgentDocumentCreations: async (scope, sinceMinutes) => {
        assert.deepEqual([scope, sinceMinutes], [{ companyId: 't', projectId: 'project', userId: 'human' }, 30])
        return [{ id: 'recent', title: 'Recent', createdBy: 'other', createdAt: 'now' }]
      },
      readAgentDocument: async (scope, id) => {
        reads++
        assert.deepEqual([scope, id], [{ companyId: 't', projectId: 'project', userId: 'human' }, 'doc'])
        if (revokeDuringRead) revoked = true
        return { ...metadata, body: 'x'.repeat(64_001) }
      },
    },
  }
  assert.deepEqual(await executeDocument(db, services, work, action), { ...metadata, body: 'x'.repeat(64_000), bodyTruncated: true })
  assert.deepEqual(permissions.at(-1), { actorUserId: 'human', companyId: 't', projectId: 'project', action: 'document:read', resource: { type: 'document', id: 'doc' } })
  const listed = await executeDocument(db, services, work, { ...action, action: 'documents.list', args: {} })
  assert.ok('documents' in listed)
  assert.equal(listed.documents.length, 100)
  assert.equal(listed.truncated, true)
  assert.deepEqual(await executeDocument(db, services, work, { ...action, action: 'documents.recent', args: { sinceMinutes: 30 } }),
    { documents: [{ id: 'recent', title: 'Recent', createdBy: 'other', createdAt: 'now' }], truncated: false })
  await assert.rejects(executeDocument(db, services, work, { ...action, args: { ...action.args, projectId: 'foreign' } }), /unknown document argument/)
  const { principalId: _principal, ...anonymous } = work
  await assert.rejects(executeDocument(db, services, anonymous, action), /original human/)
  await assert.rejects(executeDocument({ query: async () => ({ rows: [], rowCount: 0 }) }, services, work, action), /project scope/)
  assert.equal(reads, 1)
  revokeDuringRead = true
  await assert.rejects(executeDocument(db, services, work, action), /revoked/)
  await assert.rejects(executeDocument(db, services, work, action), /revoked/)
  assert.equal(reads, 2)
})
