import type { ExecutionStep } from '../control-plane/steps.js'
import type { CapabilityGrant } from '../protocol/types.js'
import type { ActionContext, ToolDefinition } from './definition.js'
import { grantedTools, type ToolDefinition as ToolSpecification } from './catalog.js'
import { permitsTool } from '../runtime/execution-policy.js'

export function toolSpecification({ name, action, description, parameters, effect, approval, readback, semanticVersion, observation, preconditions, deferred }: ToolSpecification): ToolSpecification {
  return { name, action, description, parameters, effect, approval, ...(readback ? { readback } : {}), ...(semanticVersion ? { semanticVersion } : {}),
    ...(observation ? { observation } : {}), ...(preconditions ? { preconditions } : {}), ...(deferred ? { deferred } : {}) }
}
export function discoveryTool(tools: readonly ToolDefinition[], grants: (context: ActionContext) => Promise<CapabilityGrant[]>): ToolDefinition {
  return {
    name: 'catalog__discover', action: 'catalog.discover', description: 'Find currently authorized tools and load their full schemas. Use an empty query to page through the directory; returned tools become available on the next hop.',
    parameters: { type: 'object', properties: { query: { type: 'string', maxLength: 128 }, cursor: { type: 'integer', minimum: 0 } }, required: ['query','cursor'], additionalProperties: false },
    effect: 'read', approval: false,
    parse(value) {
      if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('invalid discovery query')
      const input = value as Record<string, unknown>
      if (Object.keys(input).sort().join(',') !== 'cursor,query' || typeof input['query'] !== 'string' || input['query'].length > 128
        || !Number.isSafeInteger(input['cursor']) || Number(input['cursor']) < 0 || Number(input['cursor']) > 100_000) throw new Error('invalid discovery query')
      return input
    }, async authorize(context) {
      const prior = await context.database.query('SELECT result FROM lingxios.agent_action_ledger WHERE idempotency_key=$1', [context.action.idempotencyKey])
      const value = (prior.rows[0]?.['result'] as { value?: { tools?: ToolSpecification[] } } | undefined)?.value
      const available = grantedTools(tools, await grants(context)).filter(tool => permitsTool(context.work, tool))
      if (value?.tools?.some(tool => !available.some(current => current.action === tool.action))) throw new Error('discovered tool permission was revoked')
    },
    async execute(context, input) {
      const query = String(input['query']).toLowerCase(), cursor = Number(input['cursor'])
      const available = grantedTools(tools, await grants(context)).filter(tool => permitsTool(context.work, tool)
        && (tool.name + ' ' + tool.description).toLowerCase().includes(query)).sort((a, b) => a.name.localeCompare(b.name))
      const selected: ToolSpecification[] = []
      for (const tool of available.slice(cursor, cursor + 8)) {
        const next = toolSpecification(tool)
        if (JSON.stringify([...selected, next]).length > 12_000) { if (!selected.length) throw new Error('tool schema exceeds discovery budget'); break }
        selected.push(next)
      }
      const next = cursor + selected.length
      return { ok: true, value: { tools: selected, next: next < available.length ? next : null } }
    },
  }
}

export function exposedTools(tools: readonly ToolSpecification[], steps: readonly ExecutionStep[], discovered: readonly string[] = []): ToolSpecification[] {
  const names = new Set(discovered)
  for (const step of steps) {
    if (step.kind !== 'catalog__discover' || !step.output) continue
    let output
    try { output = JSON.parse(step.output) } catch { continue }
    for (const receipt of output.receipts ?? []) if (receipt.action === 'catalog.discover' && receipt.result?.ok) {
      for (const tool of receipt.result.value?.tools ?? []) if (typeof tool.name === 'string') names.add(tool.name)
    }
  }
  return tools.filter(tool => !tool.deferred || names.has(tool.name))
}
