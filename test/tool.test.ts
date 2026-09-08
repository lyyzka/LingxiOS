import assert from 'node:assert/strict'
import { describe, it } from 'node:test'
import { boundedToolOutput, parseIPythonArguments } from '../src/runtime/tool.js'

describe('parseIPythonArguments', () => {
  it('accepts a strict single-code object', () => {
    assert.deepEqual(parseIPythonArguments('{"code":"print(1)"}'), { code: 'print(1)' })
  })

  it('rejects non-JSON', () => {
    assert.throws(() => parseIPythonArguments('print(1)'), /strict JSON/)
  })

  it('rejects extra properties', () => {
    assert.throws(() => parseIPythonArguments('{"code":"x","lang":"py"}'), /exactly one property/)
  })

  it('rejects missing, empty, and non-string code', () => {
    assert.throws(() => parseIPythonArguments('{}'), /exactly one property/)
    assert.throws(() => parseIPythonArguments('{"code":"  "}'), /non-empty string/)
    assert.throws(() => parseIPythonArguments('{"code":42}'), /non-empty string/)
    assert.throws(() => parseIPythonArguments('["code"]'), /JSON object/)
  })
})

describe('boundedToolOutput', () => {
  it('keeps escaped previews and observation references within the same hard output bound', () => {
    const refs = Array.from({ length: 64 }, (_, index) => ({ actionKey: `key-${index}` + 'k'.repeat(100), sha256: 'a'.repeat(64), characters: 20_000 }))
    const output = boundedToolOutput({ blob: '\\"😀'.repeat(5000) }, 1000, refs)
    assert.ok(output.length <= 1000)
    const parsed = JSON.parse(output)
    assert.ok(parsed.omittedReferences > 0)
    assert.equal(parsed.observations[0].sha256, refs[0]!.sha256)
  })
  it('passes small payloads through verbatim', () => {
    assert.equal(boundedToolOutput({ a: 1 }), '{"a":1}')
  })

  it('truncates oversized payloads into valid JSON', () => {
    const output = boundedToolOutput({ blob: 'x'.repeat(20_000) }, 1_000)
    const parsed = JSON.parse(output) as { truncated: boolean; preview: string }
    assert.equal(parsed.truncated, true)
    assert.ok(output.length <= 1_000 + 40)
  })

  it('handles undefined without corrupting JSON', () => {
    assert.equal(boundedToolOutput(undefined), 'null')
  })
})
