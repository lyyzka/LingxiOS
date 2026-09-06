import { createResponseEnvelope } from '../src/outcome/envelope.js'
import { snapshotEvidence } from '../src/context/evidence.js'
import assert from 'node:assert/strict'
import { it } from 'node:test'
import { createTaskContract } from '../src/context/task-contract.js'

it('binds a bounded draft to the host request without accepting model-owned provenance', () => {
  const draft = { deliverables: ['Complete answer'], constraints: ['Use Chinese'], actions: [], acceptance: ['All requested steps included'] }
  const contract = createTaskContract('original', 2, draft)
  assert.equal(contract.requestVersion, 2)
  assert.notEqual(contract.originalInputSha256, createTaskContract('changed', 2, draft).originalInputSha256)
  const outcome = { status: 'partial' as const, verification: 'not_run' as const, requestVersion: 2 }
  const evidence = snapshotEvidence('e', [])
  const envelope = createResponseEnvelope('Answer', outcome, evidence, [], contract)
  assert.deepEqual(envelope.taskContract, contract)
  assert.notEqual(envelope.taskContract, contract)
  assert.throws(() => createResponseEnvelope('Answer', { ...outcome, requestVersion: 3 }, evidence, [], contract), /invalid response task contract/)
  draft.deliverables.push('later mutation')
  assert.deepEqual(contract.deliverables, ['Complete answer'])
  for (const change of [{ requestVersion: 99 }, { originalInputSha256: 'forged' }, { deliverables: [] }, { acceptance: [] }, { constraints: [null] }, { actions: Array(65).fill('write') }]) {
    assert.throws(() => createTaskContract('original', 2, { ...draft, ...change }))
  }
})
