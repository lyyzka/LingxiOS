import assert from 'node:assert/strict'
import { describe, it } from 'node:test'
import {
  DEFAULT_COMPACTION, HardLimitExceededError, boundSummary, compactIfNeeded, estimateTokens, summaryItem,
} from '../src/runtime/compaction.js'
import type {
  CompactionRequest, CompactionResult, ModelDriver, ModelTurnRequest, ModelTurnResult,
  StructuredCallRequest, StructuredCallResult,
} from '../src/model/driver.js'
import type { ModelItem, SessionRecord } from '../src/protocol/types.js'

function fakeDriver(overrides: Partial<ModelDriver> = {}): ModelDriver {
  return {
    modelId: 'fake-model',
    run(_request: ModelTurnRequest): Promise<ModelTurnResult> {
      throw new Error('not implemented')
    },
    structured(_request: StructuredCallRequest): Promise<StructuredCallResult> {
      throw new Error('not implemented')
    },
    compact(request: CompactionRequest): Promise<CompactionResult> {
      return Promise.resolve({
        value: JSON.stringify({ observedResults: `summary of ${request.items.length} items`, decisions: '', remainingWork: '', uncertainties: '' }),
        model: 'fake-model',
        usage: { available: true, inputTokens: 10, outputTokens: 5 },
      })
    },
    ...overrides,
  }
}

function session(history: ModelItem[], summary?: string): SessionRecord {
  return {
    key: '[\"t1\",\"a1\",\"s1\",null]', tenantId: 't1', agentId: 'a1', sessionId: 's1',
    history, appliedWorkIds: [], revision: 1, compactionEpoch: 0,
    ...(summary !== undefined ? { summary } : {}),
  }
}

function longHistory(count: number): ModelItem[] {
  return Array.from({ length: count }, (_, i) => ({
    role: 'user' as const,
    content: `message ${i} `.repeat(200), // long enough to cross the soft threshold
  }))
}

describe('estimateTokens', () => {
  it('uses a conservative byte bound', () => {
    const items: ModelItem[] = [{ role: 'user', content: 'a'.repeat(400) }]
    const estimated = estimateTokens(items)
    const expected = new TextEncoder().encode(JSON.stringify(items)).length
    assert.equal(estimated, expected)
  })
})

describe('summaryItem', () => {
  it('wraps the summary text with the untrusted-context preamble', () => {
    const item = summaryItem('the user asked about X') as { role: string; content: string }
    assert.equal(item.role, 'user')
    assert.match(item.content, /untrusted context/)
    assert.match(item.content, /the user asked about X/)
  })
})

describe('compactIfNeeded', () => {
  it('keeps tool pairs together and folds a prior summary only once', async () => {
    const history: ModelItem[] = [summaryItem('old facts'), ...longHistory(5),
      { type: 'function_call', callId: 'c1', name: 'ipython', arguments: '{}' },
      { type: 'function_call_output', callId: 'c1', output: 'ok' },
      { role: 'assistant', content: 'done' }]
    const s = session(history, 'old facts')
    const driver = fakeDriver({ compact: async (request) => {
      assert.equal(request.items.filter((item) => 'role' in item && item.content.includes('old facts')).length, 1)
      return { value: JSON.stringify({ observedResults: 'new summary', decisions: '', remainingWork: '', uncertainties: '' }), model: 'test', usage: { available: true, inputTokens: 1, outputTokens: 1 } }
    } })
    await compactIfNeeded(s, '', driver, { ...DEFAULT_COMPACTION, contextWindowTokens: 100, keepTailItems: 2 })
    assert.deepEqual(s.history, [summaryItem(s.summary!), ...history.slice(-3)])
  })

  it('closes a moved boundary over newly included tool results', async () => {
    const history: ModelItem[] = [
      { type: 'function_call', callId: 'a', name: 'ipython', arguments: '{}' },
      { type: 'function_call', callId: 'b', name: 'ipython', arguments: '{}' },
      { type: 'function_call_output', callId: 'a', output: 'a' },
      { type: 'function_call_output', callId: 'b', output: 'b' },
      ...longHistory(19),
    ]
    const s = session(history)
    const outcome = await compactIfNeeded(s, '', fakeDriver(), {
      ...DEFAULT_COMPACTION, contextWindowTokens: 100, keepTailItems: 20,
    })
    assert.equal(outcome.compacted, false)
    assert.deepEqual(s.history, history)
  })
  const smallOptions = {
    ...DEFAULT_COMPACTION,
    contextWindowTokens: 1_000,
    keepTailItems: 2,
  }

  it('does nothing when under the soft threshold', async () => {
    const s = session([{ role: 'user', content: 'hi' }])
    const outcome = await compactIfNeeded(s, 'instructions', fakeDriver(), smallOptions)
    assert.equal(outcome.compacted, false)
    assert.equal(s.compactionEpoch, 0)
  })

  it('does nothing when history is not longer than the kept tail', async () => {
    const s = session(longHistory(2))
    const outcome = await compactIfNeeded(s, 'instructions', fakeDriver(), smallOptions)
    assert.equal(outcome.compacted, false)
  })

  it('compacts older history into a summary, keeping the tail verbatim', async () => {
    const history = longHistory(10)
    const s = session(history)
    const outcome = await compactIfNeeded(s, 'instructions', fakeDriver(), smallOptions)
    assert.equal(outcome.compacted, true)
    assert.equal(s.compactionEpoch, 1)
    assert.equal(s.history.length, smallOptions.keepTailItems + 1) // summary + tail
    assert.deepEqual(s.history.slice(1), history.slice(-smallOptions.keepTailItems))
    assert.match(s.summary ?? '', /summary of 8 items/)
  })

  it('bounds each summary field without a second model call or broken JSON', async () => {
    const history = longHistory(10), s = session(history, 'existing summary')
    let calls = 0
    const raw = JSON.stringify({ observedResults: 'x'.repeat(500), decisions: 'Keep receipts.', remainingWork: 'Verify file.', uncertainties: 'Unknown write.' })
    const driver = fakeDriver({ compact: async request => {
      calls++
      assert.match(JSON.stringify(request.items), /existing summary/)
      return { value: raw, model: 'fake-model', usage: { available: true, inputTokens: 10, outputTokens: 5 } }
    } })
    const outcome = await compactIfNeeded(s, '', driver, { ...smallOptions, maxSummaryChars: 640 })
    assert.equal(calls, 1)
    assert.equal(s.summary, boundSummary(raw, 640))
    assert.ok(s.summary!.length <= 640)
    assert.deepEqual(JSON.parse(s.summary!).truncated, ['observedResults'])
    assert.deepEqual(outcome.usage, { model: 'fake-model', available: true, inputTokens: 10, outputTokens: 5 })
  })

  it('tolerates compaction failure below the hard limit', async () => {
    const history = longHistory(10)
    const s = session(history)
    const driver = fakeDriver({
      compact() {
        return Promise.reject(new Error('model unavailable'))
      },
    })
    const outcome = await compactIfNeeded(s, 'instructions', driver, {
      ...smallOptions, hardRatio: 100, // hard limit far above estimated tokens
    })
    assert.equal(outcome.compacted, false)
    assert.equal(s.compactionEpoch, 0)
  })

  it('throws HardLimitExceededError when compaction fails past the hard limit', async () => {
    const history = longHistory(10)
    const s = session(history)
    const driver = fakeDriver({
      compact() {
        return Promise.reject(new Error('model unavailable'))
      },
    })
    await assert.rejects(
      compactIfNeeded(s, 'instructions', driver, { ...smallOptions, hardRatio: 0.0001 }),
      HardLimitExceededError,
    )
  })
})
