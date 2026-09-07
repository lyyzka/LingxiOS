import assert from 'node:assert/strict'
import { describe, it } from 'node:test'
import { DefaultRuntimePolicy } from '../src/runtime/policy.js'
import type { ContextMessage, PromptContext, TurnContext, WorkItem } from '../src/protocol/types.js'

function work(overrides: Partial<WorkItem> = {}): WorkItem {
  return {
    id: 'w1', fence: 1, homeEpoch: 1, tenantId: 't1', agentId: 'a1', sessionId: 's1',
    kind: 'turn', lane: 'interactive', triggerRef: 'm1', leaseToken: 'tok', ...overrides,
  }
}

function message(overrides: Partial<ContextMessage> = {}): ContextMessage {
  return {
    ref: 'm1', authorId: 'u1', authorName: 'Alice', authorKind: 'human',
    body: 'hello there', createdAt: '2026-01-01T00:00:00Z', ...overrides,
  }
}

function context(overrides: Partial<TurnContext> = {}): TurnContext {
  return {
    work: work(), persona: { name: 'Bot', role: 'assistant', instructions: 'Be helpful.' },
    capabilities: ['fs', 'email'], messages: [message()], ...overrides,
  }
}

function promptContext(overrides: Partial<PromptContext> = {}): PromptContext {
  return {
    version: 3, epoch: 1, assembledAt: '2026-01-01T00:00:00Z',
    systemInstructions: '', persona: { name: 'Bot', role: 'assistant', instructions: 'Be helpful.' },
    capabilities: ['fs', 'email'], sourceVersions: {}, ...overrides,
  }
}

describe('DefaultRuntimePolicy.kernelCapabilities', () => {
  it('grants the persona-declared capabilities verbatim, without method restrictions', () => {
    const policy = new DefaultRuntimePolicy()
    const grants = policy.kernelCapabilities(context({ capabilities: ['fs', 'email'] }))
    assert.deepEqual(grants, [{ name: 'fs' }, { name: 'email' }])
  })
})

describe('DefaultRuntimePolicy.productRules', () => {
  it('contributes only trusted product configuration', () => {
    const policy = new DefaultRuntimePolicy()
    const prompt = policy.productRules(promptContext(), context({ productRules: 'Trusted product rule.' }))
    assert.equal(prompt, 'Trusted product rule.')
    assert.doesNotMatch(prompt, /Be helpful|Bot|host\./)
  })

  it('omits the capabilities section when none are granted', () => {
    const policy = new DefaultRuntimePolicy()
    const prompt = policy.productRules(promptContext({ capabilities: [] }))
    assert.doesNotMatch(prompt, /# Granted capabilities/)
  })
})

describe('DefaultRuntimePolicy.dynamicContextItems', () => {
  it('omits dynamic context when no memory snapshot is available', () => {
    const policy = new DefaultRuntimePolicy()
    assert.deepEqual(policy.dynamicContextItems(context()), [])
  })
})

describe('DefaultRuntimePolicy.turnInputItems', () => {
  it('does not duplicate the authoritative trigger in session history', () => {
    const policy = new DefaultRuntimePolicy()
    const trigger = message({ ref: 'm1', body: 'trigger body' })
    const other = message({ ref: 'm2', body: 'unrelated' })
    const items = policy.turnInputItems(context({ messages: [other, trigger] }), true)
    assert.deepEqual(items, [])
  })

  it('renders the last 20 messages when there is no history yet', () => {
    const policy = new DefaultRuntimePolicy()
    const messages = Array.from({ length: 25 }, (_, i) => message({ ref: `m${i}`, body: `body ${i}` }))
    const items = policy.turnInputItems(context({ messages }), false)
    assert.equal(items.length, 1)
    const content = (items[0] as { content: string }).content
    assert.doesNotMatch(content, /body 4\n/)
    assert.match(content, /body 24/)
  })

  it('returns empty when there is history but the trigger message is missing', () => {
    const policy = new DefaultRuntimePolicy()
    const items = policy.turnInputItems(context({ messages: [message({ ref: 'other' })] }), true)
    assert.deepEqual(items, [])
  })
})

describe('DefaultRuntimePolicy.validateAssistantText', () => {
  const policy = new DefaultRuntimePolicy()

  it('accepts plain user-visible text', () => {
    assert.equal(policy.validateAssistantText('Here is your answer.'), null)
  })

  it('allows markup when it is visible answer data', () => {
    assert.equal(policy.validateAssistantText('<thinking>example data</thinking>'), null)
  })

  it('allows explanatory host SDK examples', () => {
    assert.equal(policy.validateAssistantText('Example only: host.fs.read(path="x")'), null)
  })

  it('allows ipython code fences in an answer', () => {
    const text = '```ipython\nprint(1)\n```'
    assert.equal(policy.validateAssistantText(text), null)
  })
})
