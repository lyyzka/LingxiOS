#!/usr/bin/env node
import { open } from 'node:fs/promises'
import { createHash } from 'node:crypto'
import { gradeResources } from '../eval/index.js'

try {
  const args = process.argv.slice(2)
  if (args.length !== 2 || args[0] !== '--observations') throw new Error('usage: lingxios-eval --observations case.json')
  const file = await open(args[1]!, 'r')
  let bytes: Buffer
  try {
    const buffer = Buffer.alloc(8 * 1024 * 1024 + 1)
    let length = 0
    while (length < buffer.length) {
      const result = await file.read(buffer, length, buffer.length - length, null)
      if (!result.bytesRead) break
      length += result.bytesRead
    }
    if (length === buffer.length) throw new Error('observation file exceeds 8 MiB')
    bytes = buffer.subarray(0, length)
  } finally { await file.close() }
  const input = JSON.parse(bytes.toString('utf8'))
  if (!input || typeof input !== 'object' || typeof input.originalInput !== 'string' || !input.originalInput.trim()) throw new Error('originalInput is required')
  const findings = gradeResources(input.requestVersion, input.expectations, input.observations)
  const status = findings.some(item => item.status === 'fail') ? 'fail'
    : !findings.length || findings.some(item => item.status === 'not_observed') ? 'not_observed' : 'pass'
  console.log(JSON.stringify({ version: 1, mode: 'recorded_observations', scope: 'resource_postconditions',
    originalInputSha256: createHash('sha256').update(input.originalInput).digest('hex'),
    inputSha256: createHash('sha256').update(bytes).digest('hex'), requestVersion: input.requestVersion,
    status, findings, semanticQuality: 'not_assessed', safetyCoverage: 'not_assessed' }, null, 2))
  process.exitCode = status === 'pass' ? 0 : 1
} catch {
  // Input may contain private user text; never echo parse errors or file contents.
  console.error('Evaluation failed: use --observations with a valid case JSON (maximum 8 MiB).')
  process.exitCode = 2
}
