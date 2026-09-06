import assert from 'node:assert/strict'
import test from 'node:test'
import { executeEmail } from '../src/integrations/lingxiloop/email.js'
import { prepareEmailApproval } from '../src/integrations/lingxiloop/email-approvals.js'
import type { LingxiLoopServices } from '../src/integrations/lingxiloop/service-contracts.js'
import type { HostAction } from '../src/protocol/types.js'

const work = { id: 'w', fence: 1, homeEpoch: 1, tenantId: 'company', agentId: 'agent', sessionId: 'room', principalId: 'human',
  triggerRef: 'message', kind: 'request' as const, lane: 'interactive' as const }
const action = (name: string, args: Record<string, unknown>): HostAction => ({ runId: 'w', cellId: 'c', callIndex: 0, action: name, args, idempotencyKey: name })

test('email reads use the agent mailbox after authorizing the persisted human', async () => {
  const calls: unknown[] = []
  const services = {
    permissionService: { assertCan: async (input: unknown) => { calls.push(input) } },
    email: {
      getAgentEmailIdentity: async () => null,
      listAgentEmailContacts: async () => [],
      listAgentEmailInbox: async (scope: unknown, input: unknown) => { calls.push(scope, input); return [{ conversationId: 'mail' }] },
      getAgentEmailThread: async () => ({}),
    },
  } as unknown as LingxiLoopServices
  assert.deepEqual(await executeEmail(work, action('email.inbox', { unreadOnly: true, limit: 5 }), services), [{ conversationId: 'mail' }])
  assert.deepEqual(calls, [
    { companyId: 'company', userId: 'agent' }, { unreadOnly: true, limit: 5 },
    { actorUserId: 'human', companyId: 'company', action: 'email:read', resource: { type: 'conversation', id: 'mail' } },
  ])
  await assert.rejects(executeEmail(work, action('email.inbox', { limit: 51 }), services), /limit 1\.\.50/)
})

test('email approval binds committed attachments and the native idempotency key', async () => {
  const sent: unknown[] = []
  const services = {
    permissionService: { assertCan: async () => {} },
    wukongClient: () => ({ syncMessages: async () => [{ clientMsgNo: 'attachment', messageSeq: 1, channelId: 'room', channelType: 2, fromUid: 'human',
      payload: { version: 1, kind: 'attachment', data: { key: 'attachments/company/file', name: 'report.txt', mime: 'text/plain', size: 6 } } }] }),
    email: {
      sendAgentEmail: async (...args: unknown[]) => { sent.push(args); return { transportStatus: 'sent' } },
      replyToAgentEmail: async () => ({}), getAgentEmailIdentity: async () => null, listAgentEmailContacts: async () => [],
      listAgentEmailInbox: async () => [], getAgentEmailThread: async () => ({}),
    },
  } as unknown as LingxiLoopServices
  const approved = await prepareEmailApproval(services, work, action('email.send', {
    to: ['person@example.com'], subject: 'Subject', body: 'Body', attachmentClientMsgNos: ['attachment'],
  }), 2)
  assert.deepEqual(approved.preview, { method: 'send', to: ['person@example.com'], cc: [], subject: 'Subject', body: 'Body',
    attachments: [{ key: 'attachments/company/file', filename: 'report.txt', mimeType: 'text/plain', sizeBytes: 6 }] })
  assert.deepEqual(await approved.execute(), { transportStatus: 'sent' })
  assert.equal((sent[0] as unknown[])[2] && ((sent[0] as unknown[])[2] as Record<string, unknown>)['idempotencyKey'], 'email.send')
  await assert.rejects(prepareEmailApproval(services, work, action('email.send', {
    to: ['person@example.com'], subject: 'Subject', body: 'Body', attachmentClientMsgNos: ['foreign'],
  }), 2), /committed attachment not found/)
})
