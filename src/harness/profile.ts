import { canonicalJson, textSha256 } from '../context/compiler.js'
import type { ToolDefinition } from '../tools/definition.js'
import { toolContractHash } from '../tools/contracts.js'
import { skillIndex, type SkillDefinition, type SkillIndex } from '../skills/definition.js'
import type { PresentationDefinition } from '../presentation/definition.js'
import type { HarnessMode } from '../runtime/execution-policy.js'

export interface CapabilityDefinition {
  id: string
  dependsOn?: string[]
  rules?: string
  tools: readonly ToolDefinition[]
  skills?: readonly SkillDefinition[]
  presentations?: readonly PresentationDefinition[]
}
export interface HarnessProfile {
  id: string
  version: string
  mode: HarnessMode
  rules?: string
  capabilities: readonly CapabilityDefinition[]
}
export interface HarnessContext {
  id: string
  version: string
  hash: string
  rules: Array<{ id: string; content: string; actions: string[] }>
  skills: SkillIndex[]
  presentations: Array<Pick<PresentationDefinition, 'type' | 'version' | 'description' | 'actions'>>
}

/** A deterministic configuration bundle, not another execution engine. */
export function assembleHarness(profile: HarnessProfile) {
  if (!profile.id || !profile.version || !['chat','read','execute'].includes(profile.mode)) throw new Error('invalid harness profile')
  const capabilities = new Map(profile.capabilities.map(item => [item.id, item]))
  if (capabilities.size !== profile.capabilities.length) throw new Error('duplicate capability id')
  const ordered: CapabilityDefinition[] = [], visiting = new Set<string>(), visited = new Set<string>()
  function visit(id: string) {
    const item = capabilities.get(id)
    if (!item) throw new Error(`missing capability dependency: ${id}`)
    if (visiting.has(id)) throw new Error(`cyclic capability dependency: ${id}`)
    if (visited.has(id)) return
    visiting.add(id)
    for (const dependency of [...item.dependsOn ?? []].sort()) visit(dependency)
    visiting.delete(id); visited.add(id); ordered.push(item)
  }
  for (const id of [...capabilities.keys()].sort()) visit(id)
  const tools = ordered.flatMap(item => [...item.tools]), skills = ordered.flatMap(item => [...item.skills ?? []]), presentations = ordered.flatMap(item => [...item.presentations ?? []])
  if (new Set(tools.map(item => item.action)).size !== tools.length || new Set(skills.map(item => item.name)).size !== skills.length
    || new Set(presentations.map(item => item.type)).size !== presentations.length) throw new Error('duplicate harness definition')
  for (const item of [...skills, ...presentations]) if (item.actions.some(action => !tools.some(tool => tool.action === action))) throw new Error('definition requires an unavailable action')
  const context = { id: profile.id, version: profile.version,
    rules: [...(profile.rules ? [{ id: profile.id, content: profile.rules, actions: [] }] : []),
      ...ordered.filter(item => item.rules).map(item => ({ id: item.id, content: item.rules!, actions: item.tools.map(tool => tool.action) }))],
    skills: skills.map(skillIndex), presentations: presentations.map(({ type, version, description, actions }) => ({ type, version, description, actions })) }
  const hash = textSha256(canonicalJson({ ...context, mode: profile.mode, tools: tools.map(toolContractHash) }))
  return { tools, skills, presentations, context: { ...context, hash } satisfies HarnessContext }
}
