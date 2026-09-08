import { canonicalJson, textSha256 } from '../context/compiler.js'
import type { ActionContext, ToolDefinition } from '../tools/definition.js'

export interface PresentationDefinition {
  type: string
  version: string
  description: string
  actions: string[]
  authorize(context: ActionContext, reference: string): Promise<void>
  resolve(context: ActionContext, reference: string): Promise<{ fields: Record<string, unknown>; sources: Array<{ ref: string; version: string; observedAt: string }> } | null>
}
export interface TrustedPresentation {
  type: string
  version: string
  reference: string
  annotation?: string
  fields: Record<string, unknown>
  sources: Array<{ ref: string; version: string; observedAt: string }>
  hash: string
}
export function presentationTool(definitions: readonly PresentationDefinition[], allowed: (context: ActionContext, actions: string[]) => Promise<void>): ToolDefinition {
  if (new Set(definitions.map(item => item.type)).size !== definitions.length) throw new Error('duplicate presentation type')
  for (const item of definitions) if (!/^[a-z][a-z0-9_-]{0,63}$/.test(item.type) || !item.version || !item.description) throw new Error('invalid presentation definition')
  const find = (input: Record<string, unknown>) => {
    const definition = definitions.find(item => item.type === input['type'])
    if (!definition) throw new Error('unknown presentation type')
    return definition
  }
  const authorize = async (context: ActionContext, input: Record<string, unknown>) => {
    const definition = find(input)
    await allowed(context, definition.actions)
    await definition.authorize(context, String(input['reference']))
  }
  return {
    name: 'presentation__render', action: 'presentation.render', description: 'Select a presentation type and source reference. The server fills all authoritative fields. Annotation is explanatory text only; missing data fails.',
    parameters: { type: 'object', properties: { type: { type: 'string' }, reference: { type: 'string', maxLength: 2000 }, annotation: { type: 'string', maxLength: 2000 } }, required: ['type','reference'], additionalProperties: false },
    effect: 'read', approval: false, semanticVersion: textSha256(canonicalJson(definitions.map(({ type, version, actions }) => ({ type, version, actions })))),
    parse(value) {
      if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('invalid presentation selection')
      const input = value as Record<string, unknown>
      if (Object.keys(input).some(key => !['type','reference','annotation'].includes(key)) || typeof input['type'] !== 'string'
        || typeof input['reference'] !== 'string' || !input['reference'] || input['reference'].length > 2000
        || input['annotation'] !== undefined && (typeof input['annotation'] !== 'string' || input['annotation'].length > 2000)) throw new Error('presentation accepts references and annotations only')
      find(input)
      return input
    }, authorize,
    async execute(context, input) {
      await authorize(context, input)
      const definition = find(input), data = await definition.resolve(context, String(input['reference']))
      if (!data || !data.fields || typeof data.fields !== 'object' || Array.isArray(data.fields) || !data.sources?.length || data.sources.length > 32
        || data.sources.some(source => !source.ref || !source.version || !Number.isFinite(Date.parse(source.observedAt)))
        || Buffer.byteLength(JSON.stringify(data)) > 64_000) throw new Error('presentation source is unavailable or invalid')
      const value = { type: definition.type, version: definition.version, reference: String(input['reference']),
        ...(input['annotation'] === undefined ? {} : { annotation: String(input['annotation']) }), fields: data.fields, sources: data.sources }
      return { ok: true, value: { presentation: { ...value, hash: textSha256(canonicalJson(value)) } } }
    },
    async verify(context, input) { await authorize(context, input); return { status: 'passed', evidence: { scope: 'presentation_source_authorized' } } },
  }
}
