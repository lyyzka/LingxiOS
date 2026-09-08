import { compileContext, fingerprint, textSha256, type ContextBlock } from '../context/compiler.js'
import type { PromptContext, TurnContext } from '../protocol/types.js'
import type { RuntimePolicy } from '../runtime/policy.js'
import { executionSnapshot, type ExecutionSnapshot } from '../runtime/execution-policy.js'

export const PROMPT_CONTRACT_VERSION = 'prompt-v3.2'
const compiled = new WeakMap<RuntimePolicy, Map<string, PromptContext>>()

/** Gather trusted configuration once per hop; persisted instructions are never an input. */
export function buildPromptContext(context: TurnContext, policy: RuntimePolicy, epoch: number,
  contractVersion = PROMPT_CONTRACT_VERSION, assembledAt?: string,
  execution: ExecutionSnapshot = executionSnapshot(context, policy), cache = true): PromptContext {
  const candidate = context.promptContextCandidate ?? { version: 3, epoch: 0, assembledAt: '', systemInstructions: '',
    persona: context.persona, capabilities: context.capabilities, sourceVersions: {} }
  const { grants, codeExecution, mode } = execution
  const available = (actions: string[]) => actions.every(action => execution.tools.some(tool => tool.action === action))
  const harness = context.harness
  const blocks: ContextBlock[] = [
    { source: 'product:rules', version: contractVersion, trust: 'product', truncated: false, cache: 'prefix',
      content: policy.productRules(candidate, context) },
    ...(harness ? harness.rules.filter(rule => available(rule.actions)).map(rule => ({ source: `harness:${rule.id}`, version: harness.hash,
      trust: 'product' as const, truncated: false, cache: 'prefix' as const, content: rule.content })) : []),
    ...(codeExecution === 'enabled' ? [{ source: 'runtime:python', version: '1', trust: 'platform' as const, truncated: false, cache: 'prefix' as const,
      content: 'Use ipython for authorized Python execution. Attach actual session files with attach_file("relative/path"); historical filenames do not prove current contents.' }] : []),
    { source: 'runtime:authorization', version: execution.hash, trust: 'platform', truncated: false, cache: 'dynamic',
      content: JSON.stringify({ mode, grants, codeExecution }) },
    { source: 'persona', version: fingerprint(context.persona), trust: 'preference', truncated: false, content: JSON.stringify(context.persona) },
    ...(harness && mode !== 'chat' ? [{ source: 'authored:index', version: harness.hash, trust: 'observation' as const, truncated: false,
      content: JSON.stringify({ skills: harness.skills.filter(skill => available(skill.actions)), presentations: harness.presentations.filter(item => available(item.actions)) }) }] : []),
  ]
  const sourceVersions = { ...candidate.sourceVersions, promptContract: contractVersion, ...(harness ? { harness: harness.hash } : {}) }
  // Resolve policy and grants above on EVERY call; only identical rendered inputs may reuse compilation.
  const inputs = JSON.stringify({ tenantId: context.work.tenantId, principalId: context.work.principalId,
    agentId: context.work.agentId, blocks, schemas: execution.tools, sourceVersions })
  const key = textSha256(inputs)
  let entries = compiled.get(policy)
  const previous = cache ? entries?.get(key) : undefined
  if (previous) return { ...structuredClone(previous), epoch, assembledAt: assembledAt ?? previous.assembledAt }
  const result = compileContext(blocks)
  const prompt: PromptContext = {
    version: 3, epoch, assembledAt: assembledAt ?? new Date().toISOString(), sourceVersions, blocks,
    persona: structuredClone(context.persona), capabilities: grants.map(grant => grant.name),
    fingerprint: fingerprint({ compiled: result.fingerprint, schemas: execution.tools, sourceVersions }),
    systemInstructions: result.instructions, manifest: result.manifest,
  }
  if (cache && Buffer.byteLength(inputs) <= 65_536) {
    if (!entries) { entries = new Map(); compiled.set(policy, entries) }
    if (entries.size >= 64) entries.delete(entries.keys().next().value!)
    entries.set(key, structuredClone(prompt))
  }
  return prompt
}
