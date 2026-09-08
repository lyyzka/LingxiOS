import { canonicalJson, textSha256 } from './compiler.js'
import type { ActionIntent } from '../control-plane/stores.js'
import type { HostActionResult } from '../protocol/types.js'
import type { ActionContext, ToolDefinition } from '../tools/definition.js'
import { toolContractHash } from '../tools/contracts.js'

export interface ObservationRef { actionKey: string; sha256: string; characters: number }
export function observationRef(actionKey: string, value: unknown): ObservationRef {
  const serialized = canonicalJson(value)
  return { actionKey, sha256: textSha256(serialized), characters: serialized.length }
}
export function receiptReferences(receipts: readonly { action: string; idempotencyKey: string; result: HostActionResult }[]): ObservationRef[] {
  return receipts.filter(receipt => receipt.result.ok && receipt.result.value !== undefined && !receipt.action.startsWith('task.') && receipt.action !== 'catalog.discover').slice(0, 64)
    .map(receipt => receipt.action === 'observations.read'
      ? (({ actionKey, sha256, characters }) => ({ actionKey, sha256, characters }))(receipt.result.value as ObservationRef)
      : observationRef(receipt.idempotencyKey, receipt.result.value))
}

/** Full results already live in the action ledger; these references share its retention and deletion. */
export function observationTool(tools: readonly ToolDefinition[], allowed: (context: ActionContext, actions: string[]) => Promise<void>): ToolDefinition {
  const read = async (context: ActionContext, input: Record<string, unknown>) => {
    const { rows } = await context.database.query(`SELECT i.intent,COALESCE(resolved.result,r.result) AS result FROM lingxios.agent_action_intents i
      LEFT JOIN lingxios.agent_action_ledger r USING(idempotency_key)
      LEFT JOIN LATERAL (SELECT resolution->'result' AS result FROM lingxios.agent_action_resolutions
        WHERE idempotency_key=i.idempotency_key ORDER BY resolution_seq DESC LIMIT 1) resolved ON TRUE WHERE i.idempotency_key=$1`, [input['actionKey']])
    const intent = rows[0]?.['intent'] as ActionIntent | undefined, result = rows[0]?.['result'] as HostActionResult | undefined
    const source = tools.find(tool => tool.action === intent?.action.action)
    if (!intent || intent.workId !== context.work.id || intent.tenantId !== context.work.tenantId || intent.principalId !== (context.work.principalId ?? null)
      || intent.agentId !== context.work.agentId || intent.sessionId !== context.work.sessionId || intent.threadId !== (context.work.threadId ?? null)
      || intent.requestVersion !== context.requestVersion || !source || intent.toolContractHash !== toolContractHash(source)
      || !result?.ok || result.approval || result.directive || result.executionState === 'unknown') throw new Error('observation expired or unavailable in this request')
    await allowed(context, [source.action])
    await source.authorize({ ...context, action: intent.action }, source.parse(intent.action.args))
    const serialized = canonicalJson(result.value)
    if (textSha256(serialized) !== input['sha256']) throw new Error('observation hash mismatch')
    return { serialized, ref: observationRef(intent.action.idempotencyKey, result.value) }
  }
  return {
    name: 'observations__read', action: 'observations.read', description: 'Read a range of a full persisted tool value by observation reference and SHA-256. Ranges use UTF-16 character offsets, at most 8000 characters. Rechecks current source authorization.',
    parameters: { type: 'object', properties: { actionKey: { type: 'string' }, sha256: { type: 'string' }, start: { type: 'integer', minimum: 0 }, length: { type: 'integer', minimum: 1, maximum: 8000 } }, required: ['actionKey','sha256','start','length'], additionalProperties: false },
    effect: 'read', approval: false,
    parse(value) {
      if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('invalid observation range')
      const input = value as Record<string, unknown>
      if (Object.keys(input).sort().join(',') !== 'actionKey,length,sha256,start' || typeof input['actionKey'] !== 'string' || input['actionKey'].length > 4000
        || typeof input['sha256'] !== 'string' || !/^[a-f0-9]{64}$/.test(input['sha256']) || !Number.isSafeInteger(input['start']) || Number(input['start']) < 0
        || !Number.isSafeInteger(input['length']) || Number(input['length']) < 1 || Number(input['length']) > 8000) throw new Error('invalid observation range')
      return input
    },
    async authorize(context, input) { await read(context, input) },
    async execute(context, input) {
      const { serialized, ref } = await read(context, input)
      const start = Number(input['start']), end = Math.min(serialized.length, start + Number(input['length']))
      if (start > serialized.length) throw new Error('observation range exceeds its length')
      return { ok: true, value: { ...ref, start, end,
        text: serialized.slice(start, end), next: end < serialized.length ? end : null } }
    },
  }
}
