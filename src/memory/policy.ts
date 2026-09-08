import { abortable } from '../deadline.js'
import type { MemoryScope } from './store.js'
import { MEMORY_BODY_BYTES } from './text.js'

export interface MemoryWriteInput {
  signal?: AbortSignal
  scope: Readonly<MemoryScope>
  principalId: string
  sourceWorkId: string
  origin: 'explicit' | 'synthesized' | 'evolved'
  kind: string
  body: string
}
export type MemoryWriteDecision = { action: 'allow' } | { action: 'redact'; body: string } | { action: 'reject'; code: string }
export type MemoryWritePolicy = (input: Readonly<MemoryWriteInput>) => MemoryWriteDecision | Promise<MemoryWriteDecision>
export class MemoryContentRejected extends Error {}

/** Shared final boundary for explicit notes, synthesis and evolution candidates. */
export async function memoryWriteBody(input: MemoryWriteInput, policy?: MemoryWritePolicy, external?: AbortSignal): Promise<string> {
  const signal = AbortSignal.any([AbortSignal.timeout(10_000), ...external ? [external] : [], ...input.signal ? [input.signal] : []])
  signal.throwIfAborted()
  const decision = policy ? await abortable(Promise.resolve(policy(Object.freeze({ ...input, signal, scope: Object.freeze({ ...input.scope }) }))), signal) : { action: 'allow' as const }
  if (!decision || !['allow', 'redact', 'reject'].includes(decision.action)) throw new Error('invalid memory write policy decision')
  if (decision.action === 'reject') throw new MemoryContentRejected('memory write rejected by content policy')
  const body = decision.action === 'redact' ? decision.body : input.body
  if (typeof body !== 'string' || !body.trim() || Buffer.byteLength(body) > MEMORY_BODY_BYTES) throw new Error('invalid memory body after content policy')
  // Obvious credentials are a baseline, not a general privacy classifier. Product rules add domain restrictions.
  if (/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|\bBearer\s+[A-Za-z0-9._~+\/-]{12,}|\b(?:password|passwd|api[_-]?key|access[_-]?token)\s*[:=]\s*\S+/i.test(body)) {
    throw new MemoryContentRejected('memory write rejected by credential policy')
  }
  return body.trim()
}
