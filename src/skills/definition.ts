import { canonicalJson, textSha256 } from '../context/compiler.js'
import type { ActionContext, ToolDefinition } from '../tools/definition.js'

export interface SkillDefinition {
  name: string
  description: string
  version: string
  body: string
  /** Native actions required by this workflow. Skills never grant them. */
  actions: string[]
}
export type SkillIndex = Omit<SkillDefinition, 'body'> & { hash: string }
export function skillIndex(skill: SkillDefinition): SkillIndex {
  if (!/^[a-z][a-z0-9_-]{0,63}$/.test(skill.name) || !skill.version || skill.version.length > 128
    || !skill.description || skill.description.length > 1000 || !skill.body || Buffer.byteLength(skill.body) > 64_000
    || !Array.isArray(skill.actions) || skill.actions.some(action => !/^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$/.test(action))) throw new Error('invalid authored skill')
  const { body: _body, ...index } = skill
  return { ...structuredClone(index), hash: textSha256(canonicalJson(skill)) }
}
export function skillTool(skills: readonly SkillDefinition[], allowed: (context: ActionContext, actions: string[]) => Promise<void>): ToolDefinition {
  const entries = skills.map(skill => ({ skill: structuredClone(skill), index: skillIndex(skill) }))
  if (new Set(entries.map(entry => entry.index.name)).size !== entries.length) throw new Error('duplicate authored skill')
  return {
    name: 'skills__load', action: 'skills.load', description: 'Load an available authored workflow by exact name, version and content hash from the current skill index. Guidance cannot grant permissions.',
    parameters: { type: 'object', properties: { name: { type: 'string' }, version: { type: 'string' }, hash: { type: 'string' } }, required: ['name','version','hash'], additionalProperties: false },
    effect: 'read', approval: false, semanticVersion: textSha256(canonicalJson(entries.map(entry => entry.index))),
    parse(value) {
      if (!value || typeof value !== 'object' || Object.keys(value).sort().join(',') !== 'hash,name,version') throw new Error('invalid skill reference')
      return value as Record<string, unknown>
    },
    async authorize(context, input) {
      const entry = entries.find(entry => entry.index.name === input['name'] && entry.index.version === input['version'] && entry.index.hash === input['hash'])
      if (!entry) throw new Error('unknown or changed authored skill')
      await allowed(context, entry.index.actions)
    },
    async execute(context, input) {
      await this.authorize(context, input)
      const entry = entries.find(entry => entry.index.name === input['name'])!
      return { ok: true, value: { ...entry.index, source: 'authored-skill', trust: 'guidance', body: entry.skill.body } }
    },
  }
}
