import type { ActionContext, ToolDefinition } from '../tools/definition.js'
import { digest, identifier } from './access.js'
import { graphNodes } from './graphs.js'
import { validateStateUpdate } from './state.js'
import type { GraphInput, SharedStateChange, SharedStateResult } from './types.js'

function inputObject(value: unknown, keys: string[]) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).some(key => !keys.includes(key))) throw new Error('invalid collaboration tool input')
  return value as Record<string, unknown>
}
const id = { type: 'string', minLength: 1, maxLength: 1000 }
const authorize = async (_context: ActionContext) => {}

/** Registered by the runtime; availability still requires the graph/shared_state capability. */
export function collaborationTools(): ToolDefinition[] {
  const tools: ToolDefinition[] = [{
    name: 'graph__start', action: 'graph.start', description: 'Create a bounded multi-Agent DAG and wait for its branches. Child results remain internal; the parent verifies and replies.',
    effect: 'transaction', approval: false, parameters: { type: 'object', additionalProperties: false,
      properties: { id, nodes: { type: 'array', minItems: 1, maxItems: 64, items: { type: 'object', additionalProperties: false,
        properties: { id, agentId: id, text: { type: 'string', minLength: 1, maxLength: 100000 }, dependsOn: { type: 'array', items: id, maxItems: 64 } }, required: ['id', 'agentId', 'text'] } } }, required: ['id', 'nodes'] },
    parse(value) { const input = inputObject(value, ['id', 'nodes']); graphNodes(input as unknown as GraphInput); return input }, authorize,
    async execute(context, input) {
      const value = await context.enqueueGraph(input as unknown as GraphInput)
      return { ok: true, value, directive: await context.waitForChildren(value.nodes.map(node => node.workId)) }
    },
    async verify(context, input, value) {
      const graph = await context.readGraph(String(input['id']))
      const expected = (value as { nodes?: Array<{ id: string; workId: string }> } | null)?.nodes
      return { status: graph && expected && digest(graph.nodes.map(node => [node.id, node.workId]).sort()) === digest(expected.map(node => [node.id, node.workId]).sort()) ? 'passed' : 'inconclusive',
        evidence: { resource: `graph:${input['id']}`, scope: 'durable_graph_creation' } }
    },
  }, {
    name: 'graph__read', action: 'graph.read', description: 'Read the current results and failures of a graph belonging to this request.', effect: 'read', approval: false,
    parameters: { type: 'object', properties: { id }, required: ['id'], additionalProperties: false },
    parse(value) { const input = inputObject(value, ['id']); identifier(input['id']); return input }, authorize,
    async execute(context, input) { return { ok: true, value: await context.readGraph(String(input['id'])) } },
  }]
  for (const method of ['create', 'read', 'update'] as const) tools.push({
    name: `shared_state__${method}`, action: `shared_state.${method}`,
    description: method === 'update' ? 'Atomically update top-level fields using their observed versions. Conflicts require rereading and a new operation; nested values and arrays are replaced whole.'
      : method === 'read' ? 'Read structured shared state and field versions within this conversation and audience.' : 'Create empty structured state for this work audience.',
    effect: method === 'read' ? 'read' : 'transaction', approval: false,
    parameters: { type: 'object', additionalProperties: false, properties: { id, ...(method === 'update' ? { changes: { type: 'array', minItems: 1, maxItems: 64,
      items: { type: 'object', properties: { field: { type: 'string' }, expectedVersion: { type: 'integer', minimum: 0 }, value: {}, delete: { type: 'boolean' } }, required: ['field', 'expectedVersion'] } } } : {}) },
      required: method === 'update' ? ['id', 'changes'] : ['id'] },
    parse(value) {
      const input = inputObject(value, method === 'update' ? ['id', 'changes'] : ['id']); identifier(input['id'])
      if (method === 'update') validateStateUpdate({ operationId: 'validation', changes: input['changes'] as SharedStateChange[] })
      return input
    },
    async authorize(context) { if (!context.work.conversation) throw new Error('shared state requires an IM conversation') },
    async execute(context, input) {
      const id = String(input['id'])
      const value = method === 'read' ? await context.readSharedState(id) : method === 'create'
        ? await context.createSharedState(id, { visibility: 'participants', participantIds: context.work.conversation!.audience.participantIds })
        : await context.updateSharedState(id, input['changes'] as SharedStateChange[])
      return { ok: true, value }
    },
    ...(method === 'read' ? {} : { async verify(context: ActionContext, input: Record<string, unknown>, value: unknown) {
      const current = await context.readSharedState(String(input['id']))
      if (method === 'create') return { status: current ? 'passed' as const : 'inconclusive' as const, evidence: { resource: `state:${input['id']}`, scope: 'state_exists' } }
      const result = value as SharedStateResult
      const fields = (input['changes'] as SharedStateChange[]).map(change => change.field)
      const mismatches = fields.filter(field => !result?.ok || !current || digest(current.fields[field] ?? null) !== digest(result.state.fields[field] ?? null))
      return { status: mismatches.length ? 'inconclusive' as const : 'passed' as const, evidence: { resource: `state:${input['id']}`, fields, mismatches } }
    } }),
  })
  return tools
}
