import { createResponseEnvelope } from '../src/outcome/envelope.js'
import { snapshotEvidence } from '../src/context/evidence.js'
import type { AssistantMessage } from '../src/protocol/types.js'
import assert from 'node:assert/strict'
import { it } from 'node:test'
import { isGoalOutcome } from '../src/protocol/outcome.js'
import { parseFinalCandidate } from '../src/outcome/assessment.js'
import type { RequestSnapshot } from '../src/context/request.js'
import { consumeAssistantMessage as consumeCommitted, consumeRunEvent, createRunView, consumeRunState } from '../src/ui/index.js'
import { RUN_SEQUENCE_SPAN } from '../src/protocol/constants.js'
const consumeAssistantMessage = (view: ReturnType<typeof createRunView>, message: AssistantMessage) =>
  consumeCommitted(view,message,{ resultId: `result:${message.envelope?.requestVersion ?? 1}`, fence: message.envelope?.requestVersion ?? 1 })

it('grounds self-checks without treating model assertions as verification', () => {
  const request: RequestSnapshot = { version: 1, workId: 'w', tenantId: 't', sessionId: 's', authorId: 'u', sourceRef: 'm',
    originalText: 'Say hello.', revisions: [], attachments: [], evidence: snapshotEvidence('e', []) }
  const candidate = { body: 'Hello.', status: 'satisfied', gaps: [], checks: [{ requirement: 'Say hello.', status: 'met', basis: 'The answer says hello.' }] }
  assert.deepEqual(parseFinalCandidate(JSON.stringify(candidate), request), { body: 'Hello.',
    assessment: { status: 'satisfied', gaps: [], checks: candidate.checks } })
  for (const invalid of [{ ...candidate, verification: 'passed' }, { ...candidate, gaps: ['Unfinished'] },
    { ...candidate, checks: [{ ...candidate.checks[0], status: 'unknown' }] },
    { ...candidate, checks: [{ ...candidate.checks[0], requirement: 'Invented requirement' }] },
    { ...candidate, status: 'blocked' }]) assert.throws(() => parseFinalCandidate(JSON.stringify(invalid), request))
})

it('requires versioned approval and delegation references', () => {
  const base = { verification: 'not_run', requestVersion: 1 }
  assert.equal(isGoalOutcome({ ...base, status: 'awaiting_approval' }), false)
  assert.equal(isGoalOutcome({ ...base, status: 'delegated' }), false)
  assert.equal(isGoalOutcome({ ...base, status: 'awaiting_approval', approvalId: 'approval' }), true)
  assert.equal(isGoalOutcome({ ...base, status: 'delegated', taskRef: 'work' }), true)
  assert.equal(isGoalOutcome({ ...base, status: 'partial', requestVersion: 0 }), false)
  assert.equal(isGoalOutcome({ ...base, status: 'satisfied' }), true)
  assert.equal(isGoalOutcome({ ...base, status: 'satisfied', verification: 'passed', gaps: ['Unfinished'] }), false)
  assert.equal(isGoalOutcome({ ...base, status: 'satisfied', verification: 'passed' }), true)
})

it('keeps draft delivery, committed messages and waiting outcomes separate', () => {
  const event = { runId: 'w', seq: 1, visibility: 'user' as const, kind: 'model.delta', stage: 'delta' as const, data: { partType: 'text', delta: 'Draft' } }
  let view = consumeRunEvent(createRunView('w'), event)
  assert.equal(view.draft, 'Draft')
  assert.equal(view.message, null)
  assert.equal(consumeRunEvent(view, event), view)
  assert.equal(consumeRunEvent(view, { ...event, runId: 'other', seq: 2 }), view)
  const goalOutcome = { status: 'awaiting_approval', approvalId: 'a', verification: 'not_run', requestVersion: 1 } as const
  view = consumeRunEvent(view, { ...event, seq: 2, kind: 'approval.pending', data: { goalOutcome } } as Parameters<typeof consumeRunEvent>[1])
  assert.deepEqual(view.goalOutcome, goalOutcome)
  assert.equal(view.message, null)
  view = consumeAssistantMessage(view, { version: 2, runId: 'w', agentId: 'a', sessionId: 's', body: 'Awaiting your approval.', envelope: createResponseEnvelope('Awaiting your approval.', goalOutcome, snapshotEvidence('e', [])) })
  assert.equal(view.draft, '')
  assert.equal(view.message?.body, 'Awaiting your approval.')
  assert.equal(view.goalOutcome?.status, 'awaiting_approval')
})

it('clears a pending question on continuation and rejects delayed waiting events', () => {
  const event = { runId: 'w', seq: 1, visibility: 'user' as const, kind: 'model.delta', stage: 'delta' as const, data: { partType: 'text', delta: 'Old draft' } }
  let view = consumeRunEvent(createRunView('w'), event)
  const waiting = { ...event, seq: 2, kind: 'goal.waiting', data: { goalOutcome: { status: 'awaiting_input', question: 'Which city?', verification: 'not_run', requestVersion: 1 } } }
  view = consumeRunEvent(view, waiting)
  assert.equal(view.draft, '')
  assert.equal(view.goalOutcome?.question, 'Which city?')
  view = consumeRunEvent(view, { ...event, seq: 100, kind: 'run.started', data: {} })
  assert.equal(view.goalOutcome, null)
  assert.equal(consumeRunEvent(view, waiting), view)
  view = consumeRunEvent(view, { ...event, seq: 101, data: { partType: 'text', delta: 'New answer' } })
  assert.equal(view.draft, 'New answer')
  view = consumeAssistantMessage(view, { version: 2, runId: 'w', agentId: 'a', sessionId: 's', body: 'Final answer',
    envelope: createResponseEnvelope('Final answer', { status: 'partial', verification: 'not_run', requestVersion: 2 }, snapshotEvidence('e', [])) })
  const recovered = consumeRunEvent(view, { ...event, seq: 200, kind: 'run.started', data: {} })
  assert.deepEqual(recovered.message, view.message)
  assert.deepEqual(recovered.goalOutcome, view.goalOutcome)
  const delayed = consumeRunEvent(recovered, { ...waiting, seq: 201 })
  assert.deepEqual(delayed.goalOutcome, view.message!.envelope!.goalOutcome)
  assert.equal(delayed.lastSeq, 201)
})

it('rejects messages without the first-release envelope', () => {
  const message = { version: 2, runId: 'w', agentId: 'a', sessionId: 's', body: 'Old answer', data: { goalOutcome: { status: 'partial', verification: 'not_run', requestVersion: 1 } } }
  assert.throws(() => consumeAssistantMessage(createRunView('w'), message as unknown as AssistantMessage), /invalid committed assistant message/)
  const { data: _data, ...withoutEnvelope } = message
  assert.throws(() => consumeAssistantMessage(createRunView('w'), withoutEnvelope as AssistantMessage), /inconsistent envelope/)
  const envelope = createResponseEnvelope(message.body, { status: 'partial', verification: 'not_run', requestVersion: 1 }, snapshotEvidence('e', []))
  assert.throws(() => consumeAssistantMessage(createRunView('w'), { ...message, envelope } as AssistantMessage), /invalid committed assistant message/)
})

it('keeps newer attempts and committed results when approval messages or revisions arrive late', () => {
  const waiting: AssistantMessage = { version: 2, runId: 'w', agentId: 'a', sessionId: 's', body: 'Approval needed',
    envelope: createResponseEnvelope('Approval needed',{ status: 'awaiting_approval', approvalId: 'approval', requestVersion: 1, verification: 'not_run' },snapshotEvidence('e',[])) }
  let view = consumeCommitted(createRunView('w'),waiting,{ resultId: 'first', fence: 1 })
  view = consumeRunEvent(view,{ runId: 'w', seq: RUN_SEQUENCE_SPAN+1, kind: 'run.started', stage: 'started', visibility: 'user', data: {} })
  assert.equal(view.lifecycle,'leased')
  assert.equal(view.goalOutcome,null)
  assert.equal(view.message?.body,'Approval needed')
  const completed = { ...waiting, body: 'Confirmed', envelope: createResponseEnvelope('Confirmed',
    { status: 'satisfied', requestVersion: 1, verification: 'passed' },snapshotEvidence('e',[])) }
  view = consumeCommitted(view,completed,{ resultId: 'second', fence: 2 })
  assert.equal(consumeCommitted(view,waiting,{ resultId: 'first', fence: 1 }),view)
  view = consumeRunState(view,{ run: { id: 'w', fence: 2, resultFence: 2, resultId: 'second', status: 'succeeded', kind: 'turn', attempts: 2,
    requestVersion: 1, createdAt: '', availableAt: '', heartbeatAt: null, lastProgressAt: null, goalOutcome: completed.envelope.goalOutcome, error: null },
    message: completed, delivery: 'failed' })
  assert.equal(view.delivery,'failed')
  assert.equal(view.goalOutcome?.status,'satisfied')
  const queued = { ...view, lifecycle: 'queued' as const, goalOutcome: null }
  assert.equal(consumeCommitted(queued,completed,{ resultId: 'second', fence: 2 }),queued)
  assert.equal(consumeRunState(view,{ run: { id: 'w', fence: 2, resultFence: 1, resultId: 'first', status: 'leased', kind: 'turn', attempts: 2,
    requestVersion: 1, createdAt: '', availableAt: '', heartbeatAt: null, lastProgressAt: null, goalOutcome: null, error: null },
    message: waiting, delivery: 'delivered' }),view)
  const revised = { ...completed, envelope: createResponseEnvelope(completed.body,
    { status: 'partial', requestVersion: 2, verification: 'inconclusive', gaps: ['Missing edited attachment'] },snapshotEvidence('e',[])) }
  view = consumeCommitted(view,revised,{ resultId: 'third', fence: 3 })
  assert.equal(consumeCommitted(view,completed,{ resultId: 'second', fence: 2 }),view)
  assert.equal(view.lifecycle,'partial')
})
