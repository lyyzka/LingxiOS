import assert from 'node:assert/strict'
import { it } from 'node:test'
import { ControlPlaneService } from '../src/control-plane/service.js'
import { MemoryActionLedger, MemoryEventStore, MemorySessionStore, MemoryWorkStore } from '../src/control-plane/memory-store.js'
import { snapshotEvidence } from '../src/context/evidence.js'
import { createResponseEnvelope } from '../src/outcome/envelope.js'
import type { GoalAssessment } from '../src/outcome/assessment.js'
import type { AssistantMessage, SessionRecord } from '../src/protocol/types.js'

it('requires settled actions and a committed assessment, and binds delegation to real scoped child work', async () => {
  for (const delegated of [false, true]) {
    const workStore = new MemoryWorkStore(), actions = new MemoryActionLedger()
    let committed: AssistantMessage | null = null
    const service = new ControlPlaneService({ work: workStore, actions, sessions: new MemorySessionStore(), events: new MemoryEventStore(),
      contextProvider: { loadContext: async () => { throw new Error('unexpected') } },
      capabilityResolver: { resolve: async () => [] }, actionExecutor: { execute: async () => { throw new Error('unexpected') } },
      delivery: { onEvent: async () => {}, getMessage: async () => committed,
        deliverMessage: async (_work, message) => { committed = structuredClone(message) } } })
    await service.enqueue({ id: 'w', tenantId: 't', principalId: 'u', agentId: 'a', sessionId: 's', triggerRef: 'm', kind: 'turn', lane: 'interactive', meta: { text: 'Create a file.' } })
    const work = (await service.claim('worker'))!
    const evidence = snapshotEvidence('w:evidence:1', [])
    const session: SessionRecord = { key: '["t","a","s",null]', tenantId: 't', agentId: 'a', sessionId: 's', history: [], appliedWorkIds: ['w'], revision: 0, compactionEpoch: 0,
      request: { version: 1, workId: 'w', tenantId: 't', sessionId: 's', authorId: 'u', sourceRef: 'm', originalText: 'Create a file.', revisions: [], attachments: [], evidence } }
    await service.saveSession(work, session)
    await assert.rejects(service.complete(work, { status: 'completed', goalOutcome: {
      status: 'awaiting_approval', approvalId: 'invented', requestVersion: 1, verification: 'not_run',
    } }), /durable pending receipt/)
    await assert.rejects(service.complete(work, { status: 'completed', goalOutcome: {
      status: 'awaiting_input', question: 'Invented question?', requestVersion: 1, verification: 'not_run',
    } }), /durable question receipt/)
    const assessment: GoalAssessment = { status: delegated ? 'delegated' : 'satisfied', ...(delegated ? { taskRef: 'child' } : {}),
      gaps: delegated ? ['Child work is pending'] : [], checks: [{ requirement: 'Create a file.', status: delegated ? 'unknown' : 'met', basis: 'Execution observation' }] }
    const outcome = delegated ? { status: 'delegated' as const, taskRef: 'child', gaps: assessment.gaps, verification: 'not_run' as const, requestVersion: 1 }
      : { status: 'satisfied' as const, verification: 'not_run' as const, requestVersion: 1 }
    const message: AssistantMessage = { version: 2, runId: 'w', agentId: 'a', sessionId: 's', body: 'Result',
      envelope: createResponseEnvelope('Result', outcome, evidence, [], undefined, undefined, assessment) }
    await service.recordEvent(work, { runId: 'w', seq: 1, kind: 'model.completed', stage: 'completed', visibility: 'internal', data: {} })
    await service.recordEvent(work, { runId: 'w', seq: 2, kind: 'model.delta', stage: 'delta', visibility: 'user', data: { partType: 'text', delta: 'Result' } })
    await service.recordEvent(work, { runId: 'w', seq: 3, kind: 'response.assessed', stage: 'completed', visibility: 'internal',
      data: { body: message.body, assessment, goalOutcome: outcome } })
    await assert.rejects(service.complete(work, { status: 'completed', resultText: message.body, goalOutcome: outcome }), /committed assessed response/)
    if (delegated) {
      await assert.rejects(service.commitResult(work, message), /delegated task is not pending/)
      await workStore.enqueue({ id: 'child', tenantId: 't', principalId: 'u', agentId: 'child-agent', sessionId: 's', triggerRef: 'm', kind: 'turn', lane: 'collaboration',
        meta: { parentWorkId: 'w', parentRequestVersion: 1 } })
    } else {
      const action = { runId: 'w', cellId: 'c', callIndex: 0, action: 'files.save', args: {}, idempotencyKey: '["w","c",0]' }
      await actions.reserve(action.idempotencyKey, 'fingerprint', { workId: 'w', tenantId: 't', principalId: 'u', agentId: 'a', sessionId: 's', threadId: null, requestVersion: 1, action })
      await assert.rejects(service.commitResult(work, message), /evidence or artifact records/)
      await actions.record(action.idempotencyKey, { ok: true, value: 'saved' })
    }
    await service.commitResult(work, message)
    if (delegated) await workStore.requestCancel('child')
    await service.complete(work, { status: 'completed', resultText: message.body, goalOutcome: outcome })
    assert.deepEqual(workStore.inspect('w')?.goalOutcome, outcome)
  }
})
