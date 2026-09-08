import type { MemoryScope } from './store.js'

export interface MemoryWriteInput {
  scope: Readonly<MemoryScope>
  principalId: string
  sourceWorkId: string
  origin: 'explicit' | 'synthesized' | 'evolved'
  kind: string
  body: string
}
export type MemoryWriteDecision = { action: 'allow' } | { action: 'redact'; body: string } | { action: 'reject'; code: string }
export type MemoryWritePolicy = (input: Readonly<MemoryWriteInput>) => MemoryWriteDecision | Promise<MemoryWriteDecision>

/** Shared final boundary for explicit notes, synthesis and evolution candidates. */
export async function memoryWriteBody(input: MemoryWriteInput, policy?: MemoryWritePolicy): Promise<string> {
  const decision = policy ? await policy(Object.freeze({ ...input, scope: Object.freeze({ ...input.scope }) })) : { action: 'allow' as const }
  if (!decision || !['allow', 'redact', 'reject'].includes(decision.action)) throw new Error('invalid memory write policy decision')
  if (decision.action === 'reject') throw new Error('memory write rejected by content policy')
  const body = decision.action === 'redact' ? decision.body : input.body
  if (typeof body !== 'string' || !body.trim() || body.length > (input.origin === 'synthesized' ? 500 : 2000)) throw new Error('invalid memory body after content policy')
  // Obvious credentials are a baseline, not a general privacy classifier. Product rules add domain restrictions.
  if (/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|\bBearer\s+[A-Za-z0-9._~+\/-]{12,}|\b(?:password|passwd|api[_-]?key|access[_-]?token)\s*[:=]\s*\S+/i.test(body)) {
    throw new Error('memory write rejected by credential policy')
  }
  return body.trim()
}
