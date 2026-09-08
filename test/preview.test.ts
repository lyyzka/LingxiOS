import assert from 'node:assert/strict'
import { it } from 'node:test'
import { CandidateBodyParser } from '../src/model/preview.js'
import { PreviewBuffer } from '../src/runtime/preview.js'
import { consumePreview, consumeRunEvent, createRunView, consumeRunState } from '../src/ui/index.js'
import type { PreviewFrame } from '../src/protocol/preview.js'

it('incrementally extracts only the candidate body across every split, including escapes and nested metadata', () => {
  const body = '正文 "quoted"\nemoji 😀 and \\ slash'
  const value = JSON.stringify({ status: 'satisfied', checks: [{ body: 'PRIVATE', basis: 'INTERNAL' }], body, gaps: [] })
  for (let index = 0; index <= value.length; index++) {
    const parser = new CandidateBodyParser()
    assert.equal(parser.push(value.slice(0, index)) + parser.push(value.slice(index)), body)
    assert.equal(parser.invalid, false)
  }
  const parser = new CandidateBodyParser()
  assert.equal([...'{"body":"\\uD83D\\uDE00\\ttext","gaps":[]}'].map(char => parser.push(char)).join(''), '😀\ttext')
  for (const input of ['plain text', '<think>secret</think>', '{"tool":{"body":"secret"}}', '{"body":123}', '{"body":"bad\\q"}']) {
    const unsafe = new CandidateBodyParser()
    assert.equal(unsafe.push(input), '')
    assert.equal(unsafe.invalid, true)
  }
})

it('sends the first body before generation ends, coalesces later tokens, and bounds a slow consumer', async () => {
  const buffer = new PreviewBuffer(), iterator = buffer[Symbol.asyncIterator]()
  buffer.reset('attempt-1', 1)
  assert.equal((await iterator.next()).value?.kind, 'reset')
  const first = iterator.next()
  buffer.push('first')
  assert.equal((await first).value?.text, 'first')
  buffer.push('second'); buffer.push(' third')
  assert.equal((await iterator.next()).value?.text, 'second third')
  buffer.push('x'.repeat(16_385))
  assert.equal((await iterator.next()).value?.kind, 'reset')
  buffer.push('must not follow the overflow reset')
  buffer.reset('attempt-2', 2); buffer.push('replacement'); buffer.close()
  const rest: PreviewFrame[] = []
  for await (const frame of iterator) rest.push(frame)
  assert.deepEqual(rest.map(({ kind, text, attemptId, requestVersion }) => ({ kind, text, attemptId, requestVersion })), [
    { kind: 'reset', text: '', attemptId: 'attempt-2', requestVersion: 2 },
    { kind: 'delta', text: 'replacement', attemptId: 'attempt-2', requestVersion: 2 },
  ])
})

it('replaces retried drafts, discards sequence gaps and revisions, and rejects previews after committed/failed state', () => {
  const base = { runId: 'run', fence: 1, requestVersion: 1, attemptId: 'one', seq: 2 }
  let view = consumePreview(createRunView('run'), { ...base, kind: 'snapshot', draft: 'old' })
  view = consumePreview(view, { ...base, seq: 4, fromSeq: 3, kind: 'delta', delta: 'gap' })
  assert.equal(view.draft, '')
  view = consumePreview(view, { ...base, attemptId: 'two', seq: 5, kind: 'snapshot', draft: 'new' })
  assert.equal(view.draft, 'new')
  view = consumePreview(view, { ...base, kind: 'snapshot', draft: 'late' })
  assert.equal(view.draft, 'new')
  view = consumeRunEvent(view, { runId: 'run', seq: 1, kind: 'response.committed', stage: 'completed', visibility: 'user', data: {} })
  assert.equal(consumePreview(view, { ...base, seq: 6, kind: 'snapshot', draft: 'too late' }).draft, '')
  const active = consumePreview(createRunView('run'), { ...base, kind: 'snapshot', draft: 'old' })
  const revised = consumeRunState(active, { run: { id: 'run', fence: 1, requestVersion: 2, status: 'leased', kind: 'turn', attempts: 1,
    resultId: null, resultFence: null, createdAt: '', availableAt: '', heartbeatAt: null, lastProgressAt: null, goalOutcome: null, error: null }, message: null, delivery: null })
  assert.equal(revised.draft, '')
  assert.equal(consumePreview(revised, { ...base, seq: 3, kind: 'snapshot', draft: 'old revision' }).draft, '')
  const failed = consumeRunState(active, { run: { id: 'run', fence: 1, requestVersion: 1, status: 'failed', kind: 'turn', attempts: 1,
    resultId: null, resultFence: null, createdAt: '', availableAt: '', heartbeatAt: null, lastProgressAt: null, goalOutcome: null, error: 'failed' }, message: null, delivery: null })
  assert.equal(consumePreview(failed, { ...base, seq: 3, kind: 'snapshot', draft: 'late failure' }).draft, '')
})
