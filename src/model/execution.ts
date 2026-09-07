import { setTimeout as delay } from 'node:timers/promises'
import { ModelBudgetExceededError, ModelDriverError, errorMessage } from '../errors.js'
import type { HostPort } from '../host/port.js'
import type { RunEvent, WorkItem } from '../protocol/types.js'
import type { ModelDriver, ModelUsage } from './driver.js'

export interface ModelCallObservation {
  callId: string
  logicalCallId?: string
  purpose: 'agent-turn' | 'structured' | 'compaction' | 'embedding'
  workId: string
  tenantId: string
  agentId: string
  sessionId: string
  threadId?: string
  principalId?: string
  model: string
  usage?: ModelUsage
  latencyMs: number
  status: 'succeeded' | 'failed'
  error?: string
}
export type ModelCallObserver = (observation: ModelCallObservation) => Promise<void>
export interface RootModelBudgetOptions {
  maxModelCalls?: number
  maxTokens?: number
  maxCostMicros?: number
  /** Cumulative execution time across attempts and children; excludes parked waits. */
  wallClockMs?: number
  inputCostMicrosPerMillion?: number
  outputCostMicrosPerMillion?: number
}
export const DEFAULT_MODEL_BUDGET: Required<RootModelBudgetOptions> = {
  maxModelCalls: 128, maxTokens: 1_000_000, maxCostMicros: 10_000_000,
  wallClockMs: 1_800_000, inputCostMicrosPerMillion: 0, outputCostMicrosPerMillion: 0,
}

/** One provider attempt boundary shared by generation, reviews, compaction and embeddings. */
export function modelExecution(host: Pick<HostPort, 'reserveModelCall' | 'recordModelUsage'>,
  model: Pick<ModelDriver, 'modelId' | 'maxOutputTokens' | 'toolDefinitionTokens'>, work: WorkItem,
  limits: Required<RootModelBudgetOptions>, observer?: ModelCallObserver,
  emit?: (event: Omit<RunEvent, 'runId' | 'seq'>) => Promise<unknown>, namespace = 'model') {
  let sequence = 0, calls = 0, tokens = 0, cost = 0
  const started = Date.now()
  const invoke = async <T extends { model?: string; usage: ModelUsage }>(purpose: ModelCallObservation['purpose'],
    request: { signal?: AbortSignal | undefined; input?: unknown }, operation: (signal: AbortSignal) => Promise<T>): Promise<T> => {
    const logicalCallId = `${work.id}:${work.fence}:${namespace}:${++sequence}`
    const input = Buffer.byteLength(JSON.stringify(request)) + (model.toolDefinitionTokens ?? 0)
    const output = model.maxOutputTokens ?? 8192
    const reservedCost = Math.ceil((input * limits.inputCostMicrosPerMillion + output * limits.outputCostMicrosPerMillion) / 1_000_000)
    for (let attempt = 1; ; attempt++) {
      request.signal?.throwIfAborted()
      if (calls >= limits.maxModelCalls || tokens + input + output > limits.maxTokens
        || cost + reservedCost > limits.maxCostMicros || Date.now() - started >= limits.wallClockMs) {
        throw new ModelBudgetExceededError('root work model budget exhausted')
      }
      const callId = attempt === 1 ? logicalCallId : `${logicalCallId}:retry:${attempt}`
      const reservation = await host.reserveModelCall?.(work, callId, {
        ...limits, maxExecutionMs: limits.wallClockMs,
        deadlineAt: new Date(Date.now() + limits.wallClockMs).toISOString(),
        reservedTokens: input + output, reservedInputTokens: input, reservedOutputTokens: output, reservedCostMicros: reservedCost,
      })
      if (reservation && !reservation.allowed) throw new ModelBudgetExceededError('root work model budget exhausted')
      const remaining = reservation ? Date.parse(reservation.deadlineAt) - Date.now() : limits.wallClockMs - (Date.now() - started)
      if (remaining <= 0) throw new ModelBudgetExceededError('root work execution time exhausted')
      calls++; tokens += input + output; cost += reservedCost
      const signal = AbortSignal.any([...(request.signal ? [request.signal] : []), AbortSignal.timeout(Math.min(2_147_483_647, remaining))])
      const began = Date.now()
      await emit?.({
        kind: 'model.request.started', stage: 'started', visibility: 'internal',
        data: { callId, logicalCallId, purpose, model: model.modelId ?? 'unknown' } })
      let result: T | undefined, failure: unknown
      try { result = await operation(signal) } catch (error) { failure = error }
      const usage = result?.usage
      const inputTokens = usage?.available ? usage.inputTokens : input
      const outputTokens = usage?.available ? usage.outputTokens : output
      const costMicros = Math.ceil((inputTokens * limits.inputCostMicrosPerMillion + outputTokens * limits.outputCostMicrosPerMillion) / 1_000_000)
      tokens += inputTokens + outputTokens - input - output; cost += costMicros - reservedCost
      const observation: ModelCallObservation = {
        callId, ...(attempt > 1 ? { logicalCallId } : {}), purpose,
        workId: work.id, tenantId: work.tenantId, agentId: work.agentId, sessionId: work.sessionId,
        ...(work.threadId !== undefined ? { threadId: work.threadId } : {}), ...(work.principalId ? { principalId: work.principalId } : {}),
        model: result?.model ?? model.modelId ?? 'unknown', ...(usage ? { usage } : {}),
        latencyMs: Date.now() - began, status: result ? 'succeeded' : 'failed',
        ...(failure ? { error: errorMessage(failure) } : {}),
      }
      // A completed provider request must be settled even after cancellation or lease loss.
      await host.recordModelUsage?.(work, callId, { inputTokens, outputTokens, costMicros }, observation)
      if (!host.recordModelUsage) await observer?.(observation)
      // Usage remains durable if the old lease can no longer emit telemetry.
      await emit?.({ kind: 'model.request.finished', stage: result ? 'completed' : 'failed', visibility: 'internal', data: { ...observation } })
      if (result) return { ...result, callId, logicalCallId }
      if (!(failure instanceof ModelDriverError) || attempt >= 3 || signal.aborted
        || failure.diagnostics.kind === 'protocol'
        || failure.diagnostics.status !== undefined && failure.diagnostics.status !== 429 && failure.diagnostics.status < 500) throw failure
      await delay(Math.min(2000, 250 * 2 ** (attempt - 1)), undefined, { signal: request.signal })
    }
  }
  return { invoke, nextCallId: () => `${work.id}:${work.fence}:${namespace}:${sequence + 1}` }
}

export function executionModel(host: Pick<HostPort, 'reserveModelCall' | 'recordModelUsage'>, source: ModelDriver, work: WorkItem,
  limits: Required<RootModelBudgetOptions>, observer?: ModelCallObserver,
  emit?: (event: Omit<RunEvent, 'runId' | 'seq'>) => Promise<unknown>): ModelDriver {
  const model = source.singleAttempt?.() ?? source
  const { invoke, nextCallId } = modelExecution(host, model, work, limits, observer, emit)
  return {
    nextCallId,
    ...(model.modelId === undefined ? {} : { modelId: model.modelId }),
    ...(model.configurationFingerprint === undefined ? {} : { configurationFingerprint: model.configurationFingerprint }),
    ...(model.contextWindowTokens === undefined ? {} : { contextWindowTokens: model.contextWindowTokens }),
    ...(model.maxOutputTokens === undefined ? {} : { maxOutputTokens: model.maxOutputTokens }),
    ...(model.toolDefinitionTokens === undefined ? {} : { toolDefinitionTokens: model.toolDefinitionTokens }),
    run: request => invoke('agent-turn', request, signal => model.run({ ...request, signal })),
    structured: request => invoke('structured', request, signal => model.structured({ ...request, signal })),
    compact: request => invoke('compaction', request, signal => model.compact({ ...request, signal })),
  }
}
