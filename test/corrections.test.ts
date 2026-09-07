import { test } from 'node:test'
import assert from 'node:assert/strict'
import { CorrectionBudget } from '../src/runtime/corrections.js'

test('stalls only after rediagnosis and six identical failures; new evidence resets', () => {
  const budget = new CorrectionBudget()
  for (let n = 1; n <= 6; n++) {
    assert.equal(budget.consume('kernel_error', 'same operation: missing file'), n < 6)
    assert.equal(budget.rediagnose, n >= 3)
  }
  assert.equal(budget.consume('kernel_error', 'different operation: denied'), true)
  budget.observe('new result')
  assert.equal(budget.consume('kernel_error', 'same operation: missing file'), true)
  budget.observe('new result') // Repeating the observation does not manufacture progress.
  for (let n = 2; n <= 6; n++) assert.equal(budget.consume('kernel_error', 'same operation: missing file'), n < 6)
})
