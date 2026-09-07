import assert from 'node:assert/strict'
import { it } from 'node:test'
import { createLingxiLoopRuntimePolicy, LingxiLoopRuntimePolicy } from '../src/integrations/lingxiloop/policy.js'
import type { GoalAssessment } from '../src/outcome/assessment.js'
import type { TurnContext, WorkItem } from '../src/protocol/types.js'

const work = (overrides: Partial<WorkItem> = {}): WorkItem => ({
  id: 'w', fence: 1, homeEpoch: 1, tenantId: 't', agentId: 'a', sessionId: 's', kind: 'turn',
  lane: 'interactive', triggerRef: 'm', leaseToken: 'lease', ...overrides,
})
const context = (item: WorkItem, extra: Partial<TurnContext> = {}): TurnContext => ({
  work: item, persona: { name: 'Lingxi', role: 'assistant', instructions: '' }, capabilities: ['canvas', 'research', 'email'], messages: [], ...extra,
})
const policy = new LingxiLoopRuntimePolicy({ capabilityMethods: {
  canvas: ['current', 'create_frame', 'set_status', 'submit_report', 'assign'], research: ['search', 'read'], email: ['send'],
} })

it('uses authoritative grants without deriving a second role whitelist', () => {
  const grants = [{ name: 'canvas', methods: ['current', 'submit_report'] }]
  for (const kind of ['turn', 'canvas_worker', 'canvas_summary']) {
    assert.deepEqual(createLingxiLoopRuntimePolicy().kernelCapabilities(context(work({ kind }), { grants })), grants)
    assert.deepEqual(policy.kernelCapabilities(context(work({ kind }), { grants: [] })), [])
  }
})

it('preserves exact prose while requiring durable role completion', () => {
  const verifier = context(work({ kind: 'canvas_worker', meta: { executionRole: 'verifier' } }), {
    evidence: [{ marker: 'S1', sourceId: 'source', sourceVersion: 'v1', chunkId: 'c1', title: 'Source', excerpt: 'Fact' }],
  })
  assert.equal(policy.validateAssistantText('Finding.', verifier), null)
  assert.equal(policy.validateAssistantText('Lingxi found a fact.', verifier), null)
  assert.equal(policy.validateAssistantText('Lingxi found [a fact](#cite-S1).', verifier), null)
  const assessment: GoalAssessment = { status: 'satisfied', checks: [{ requirement: 'verify', status: 'met', basis: 'done' }], gaps: [] }
  assert.match(policy.validateCompletion('', assessment, verifier)!, /durable role report/)
  assert.equal(policy.validateCompletion('', assessment, { ...verifier, dynamic: { roleCompletion: true } }), null)
})

it('exposes live product context as untrusted data', () => {
  const items = policy.dynamicContextItems(context(work(), { dynamic: { product: { canvas: 'live' } } }))
  const content = (items.at(-1) as { content: string }).content
  assert.match(content, /"canvas":"live"/)
  assert.match(content, /untrusted data/)
})
