import { test } from 'node:test'
import assert from 'node:assert/strict'
import { CorrectionBudget } from '../src/runtime/corrections.js'

test('reworded errors and timestamps do not reset the bound; new resource versions do', () => {
  const budget = new CorrectionBudget()
  for (let n = 1; n <= 3; n++) {
    assert.equal(budget.consume('kernel_error', 'same operation: missing file'), n < 3)
    assert.equal(budget.rediagnose, n >= 2)
  }
  assert.equal(budget.consume('kernel_error', 'different wording: denied at a new timestamp'), false)
  budget.observe('new result')
  assert.equal(budget.consume('kernel_error', 'same operation: missing file'), true)
  budget.observe('new result') // Repeating the observation does not manufacture progress.
  for (let n = 2; n <= 3; n++) assert.equal(budget.consume('kernel_error', 'same operation: missing file'), n < 3)
  budget.observe({ observedAt: '2026-01-01T00:00:00Z', resource: 'document:1', revision: 'v1' })
  budget.consume('kernel_error', 'failed')
  budget.observe({ observedAt: '2026-01-02T00:00:00Z', resource: 'document:1', revision: 'v1' })
  assert.equal(budget.snapshot().count, 1)
  budget.observe({ resource: 'document:1', revision: 'v2' })
  assert.equal(budget.snapshot().count, 0)
})
