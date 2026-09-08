import { canonicalJson, textSha256 } from '../context/compiler.js'
import type { ToolDefinition } from './catalog.js'
import { NoEffectError, type ActionContext, type ToolDefinition as NativeTool } from './definition.js'
import type { ActionIntent } from '../control-plane/stores.js'
import type { HostActionResult } from '../protocol/types.js'

/** Hosts bump semanticVersion when parser, authorization, preview, execution or verifier semantics change. */
export function toolContractHash(tool: ToolDefinition): string {
  return textSha256(canonicalJson({ action: tool.action, name: tool.name, parameters: tool.parameters,
    effect: tool.effect, approval: tool.approval, semanticVersion: tool.semanticVersion ?? '1',
    readback: tool.readback ?? null, observation: tool.observation ?? null, preconditions: tool.preconditions ?? null }))
}

export async function assertToolContract(tool: ToolDefinition, context: ActionContext): Promise<void> {
  const { rows } = await context.database.query('SELECT intent FROM lingxios.agent_action_intents WHERE idempotency_key=$1', [context.action.idempotencyKey])
  const intent = rows[0]?.['intent'] as ActionIntent | undefined
  if (!intent || intent.toolContractHash !== toolContractHash(tool)) throw new NoEffectError('Tool contract is missing or changed; settle the old intent before executing', 'tool_contract_changed')
}

/** Evidence is an actual persisted read in this principal's current request, never an ID supplied on trust. */
export async function assertObservation(tool: NativeTool, context: ActionContext, input: Record<string, unknown>, tools: ReadonlyMap<string, NativeTool>) {
  if (!tool.preconditions) return
  const required = await tool.observationRequirement!(context, input).catch(() => { throw new NoEffectError('Current resource version is unavailable', 'observation_required') })
  if (!required || ![required.actionKey, required.resourceId, required.currentVersion].every(value => typeof value === 'string' && value.length > 0 && value.length <= 4000)) {
    throw new NoEffectError('Full resource observation and current version are required', 'observation_required')
  }
  const { rows } = await context.database.query(`SELECT i.intent,COALESCE(resolved.result,r.result) AS result
    FROM lingxios.agent_action_intents i LEFT JOIN lingxios.agent_action_ledger r USING(idempotency_key)
    LEFT JOIN LATERAL (SELECT resolution->'result' AS result FROM lingxios.agent_action_resolutions
      WHERE idempotency_key=i.idempotency_key ORDER BY resolution_seq DESC LIMIT 1) resolved ON TRUE
    WHERE i.idempotency_key=$1`, [required.actionKey])
  const intent = rows[0]?.['intent'] as ActionIntent | undefined, result = rows[0]?.['result'] as HostActionResult | undefined
  const read = tools.get(tool.preconditions.readAction)
  if (!intent || intent.workId !== context.work.id || intent.tenantId !== context.work.tenantId
    || intent.principalId !== (context.work.principalId ?? null) || intent.agentId !== context.work.agentId
    || intent.sessionId !== context.work.sessionId || intent.threadId !== (context.work.threadId ?? null)
    || intent.requestVersion !== context.requestVersion || intent.action.action !== tool.preconditions.readAction
    || !read || intent.toolContractHash !== toolContractHash(read) || !result?.ok || result.approval || result.directive || result.executionState === 'unknown'
    || !result.observations?.some(item => item.resourceType === tool.preconditions!.resourceType && item.resourceId === required.resourceId
      && item.version === required.currentVersion && item.completeness === 'full')) {
    throw new NoEffectError('Full resource observation is missing, stale or outside this request', 'observation_required')
  }
  await read.authorize({ ...context, action: intent.action }, read.parse(intent.action.args))
    .catch(() => { throw new NoEffectError('Resource observation permission was revoked', 'observation_required') })
}

export async function observeResult(tool: NativeTool, context: ActionContext, input: Record<string, unknown>, result: HostActionResult): Promise<HostActionResult> {
  const { observations: _unbound, ...value } = result
  if (!tool.observation || !result.ok || result.approval || result.directive || result.executionState === 'unknown') return value
  const resources = await tool.observe!(context, input, result.value)
  if (!Array.isArray(resources) || resources.length > 64 || resources.some(item => !item
    || ![item.resourceId, item.version].every(field => typeof field === 'string' && field.length > 0 && field.length <= 4000))) {
    throw new NoEffectError('Native read returned invalid resource observations')
  }
  return { ...value, observations: resources.map(item => ({ resourceType: tool.observation!.resourceType,
    resourceId: item.resourceId, version: item.version, completeness: tool.observation!.completeness })) }
}
