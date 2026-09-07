import { compileContext, fingerprint, type ContextBlock } from '../context/compiler.js'
import type { PromptContext, TurnContext } from '../protocol/types.js'
import type { RuntimePolicy } from '../runtime/policy.js'

export const PROMPT_CONTRACT_VERSION = 'prompt-v3.1'

/** Gather trusted configuration once per hop; persisted instructions are never an input. */
export function buildPromptContext(context: TurnContext, policy: RuntimePolicy, epoch: number,
  contractVersion = PROMPT_CONTRACT_VERSION, assembledAt = new Date().toISOString()): PromptContext {
  const candidate = context.promptContextCandidate ?? { version: 3, epoch: 0, assembledAt: '', systemInstructions: '',
    persona: context.persona, capabilities: context.capabilities, sourceVersions: {} }
  const grants = policy.kernelCapabilities(context)
  const blocks: ContextBlock[] = [
    { source: 'product:rules', version: contractVersion, trust: 'product', truncated: false, cache: 'prefix',
      content: policy.productRules(candidate, context) },
    { source: 'runtime:authorization', version: fingerprint(grants), trust: 'platform', truncated: false, cache: 'dynamic',
      content: JSON.stringify({ grants }) },
    { source: 'persona', version: fingerprint(context.persona), trust: 'preference', truncated: false, content: JSON.stringify(context.persona) },
  ]
  const compiled = compileContext(blocks)
  // Rebuild from live inputs: resume, compaction and revocation cannot reuse stale grants.
  const sourceVersions = { ...candidate.sourceVersions, promptContract: contractVersion }
  return {
    version: 3, epoch, assembledAt, sourceVersions, blocks,
    persona: structuredClone(context.persona), capabilities: [...context.capabilities],
    fingerprint: fingerprint({ compiled: compiled.fingerprint, schemas: context.tools ?? [], sourceVersions }),
    systemInstructions: compiled.instructions, manifest: compiled.manifest,
  }
}
