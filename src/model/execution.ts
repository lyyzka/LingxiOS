import { COMPACTION_PROMPT, textSha256, type PromptManifest } from '../context/compiler.js'
import { setTimeout as delay } from 'node:timers/promises'
import { createHash } from 'node:crypto'
import { ModelBudgetExceededError, ModelDriverError, errorMessage } from '../errors.js'
import type { HostPort } from '../host/port.js'
import type { RunEvent, WorkItem } from '../protocol/types.js'
import type { ModelDriver, ModelUsage } from './driver.js'
import { abortable } from '../deadline.js'
import { fitsModel, inputTokens, modelProfile } from './profile.js'

export interface ModelPricing {
  version: string
  currency: 'USD'
  inputMicrosPerMillion: number
  outputMicrosPerMillion: number
}
export function modelPricing(limits: Pick<Required<RootModelBudgetOptions>, 'inputCostMicrosPerMillion' | 'outputCostMicrosPerMillion'>): ModelPricing {
  return { version: createHash('sha256').update(JSON.stringify([limits.inputCostMicrosPerMillion, limits.outputCostMicrosPerMillion])).digest('hex'),
    currency: 'USD', inputMicrosPerMillion: limits.inputCostMicrosPerMillion, outputMicrosPerMillion: limits.outputCostMicrosPerMillion }
}

export interface ModelCallObservation {
  cost?: { amountMicros: number; usage: 'measured' | 'estimated'; pricing: ModelPricing }
  prompt?: PromptManifest
  instructionsSha256?: string
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
export type ModelCallObserver = (observation: ModelCallObservation, context?: import('../control-plane/outbox.js').DeliveryContext) => Promise<void>
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
  model: Pick<ModelDriver, 'modelId' | 'maxOutputTokens' | 'maxThinkingTokens' | 'toolDefinitionTokens' | 'countTokens'>, work: WorkItem,
  limits: Required<RootModelBudgetOptions>,
  emit?: (event: Omit<RunEvent, 'runId' | 'seq'>) => Promise<unknown>, namespace = 'model') {
  let sequence = 0, calls = 0, tokens = 0, cost = 0
  const started = Date.now()
  const invoke = async <T extends { model?: string; usage: ModelUsage }>(purpose: ModelCallObservation['purpose'],
    request: { signal?: AbortSignal | undefined; input?: unknown; instructions?: string; prompt?: PromptManifest }, operation: (signal: AbortSignal, callId: string, diagnostics: Record<string, unknown>) => Promise<T>, callModel = model): Promise<T> => {
    const instructionsSha256 = request.instructions === undefined ? undefined : textSha256(request.instructions)
    if (request.prompt && request.prompt.instructionsSha256 !== instructionsSha256) throw new Error('prompt manifest does not match model instructions')
    const logicalCallId = `${work.id}:${work.fence}:${namespace}:${++sequence}`
    const { prompt: _prompt, ...providerRequest } = request
    const input = inputTokens(callModel, providerRequest) + (callModel.toolDefinitionTokens ?? 0)
    const output = (callModel.maxOutputTokens ?? 8192) + (callModel.maxThinkingTokens ?? 0)
    const reservedCost = Math.ceil((input * limits.inputCostMicrosPerMillion + output * limits.outputCostMicrosPerMillion) / 1_000_000)
    for (let attempt = 1; ; attempt++) {
      request.signal?.throwIfAborted()
      if (calls >= limits.maxModelCalls || tokens + input + output > limits.maxTokens
        || cost + reservedCost > limits.maxCostMicros || Date.now() - started >= limits.wallClockMs) {
        throw new ModelBudgetExceededError('root work model budget exhausted')
      }
      const callId = attempt === 1 ? logicalCallId : `${logicalCallId}:retry:${attempt}`
      const reservation = await host.reserveModelCall(work, callId, {
        ...limits, maxExecutionMs: limits.wallClockMs,
        deadlineAt: new Date(Date.now() + limits.wallClockMs).toISOString(),
        reservedTokens: input + output, reservedInputTokens: input, reservedOutputTokens: output, reservedCostMicros: reservedCost,
        pricing: modelPricing(limits),
      })
      if (!reservation.allowed) throw new ModelBudgetExceededError('root work model budget exhausted')
      const remaining = Date.parse(reservation.deadlineAt) - Date.now()
      if (remaining <= 0) throw new ModelBudgetExceededError('root work execution time exhausted')
      calls++; tokens += input + output; cost += reservedCost
      const signal = AbortSignal.any([...(request.signal ? [request.signal] : []), AbortSignal.timeout(Math.min(2_147_483_647, remaining))])
      const began = Date.now()
      await emit?.({
        kind: 'model.request.started', stage: 'started', visibility: 'internal',
        data: { callId, logicalCallId, purpose, model: callModel.modelId ?? 'unknown',
          ...(request.prompt ? { prompt: request.prompt } : {}), ...(instructionsSha256 ? { instructionsSha256 } : {}) } })
      let result: T | undefined, failure: unknown
      const diagnostics: Record<string, unknown> = {}
      try { result = await abortable(operation(signal, callId, diagnostics), signal) } catch (error) { failure = error }
      const usage = result?.usage
      const inputTokens = usage?.available ? usage.inputTokens : input
      const outputTokens = usage?.available ? usage.outputTokens : output
      const costMicros = Math.ceil((inputTokens * limits.inputCostMicrosPerMillion + outputTokens * limits.outputCostMicrosPerMillion) / 1_000_000)
      tokens += inputTokens + outputTokens - input - output; cost += costMicros - reservedCost
      const observation: ModelCallObservation = {
        ...(request.prompt ? { prompt: request.prompt } : {}), ...(instructionsSha256 ? { instructionsSha256 } : {}),
        callId, ...(attempt > 1 ? { logicalCallId } : {}), purpose,
        workId: work.id, tenantId: work.tenantId, agentId: work.agentId, sessionId: work.sessionId,
        ...(work.threadId !== undefined ? { threadId: work.threadId } : {}), ...(work.principalId ? { principalId: work.principalId } : {}),
        model: result?.model ?? callModel.modelId ?? 'unknown', usage: usage?.available ? usage : { available: false, inputTokens, outputTokens },
        cost: { amountMicros: costMicros, usage: usage?.available ? 'measured' : 'estimated', pricing: modelPricing(limits) },
        latencyMs: Date.now() - began, status: result ? 'succeeded' : 'failed',
        ...(failure ? { error: errorMessage(failure) } : {}),
      }
      // A completed provider request must be settled even after cancellation or lease loss.
      await host.recordModelUsage(work, callId, { inputTokens, outputTokens, costMicros }, observation)
      // Usage remains durable if the old lease can no longer emit telemetry.
      await emit?.({ kind: 'model.request.finished', stage: result ? 'completed' : 'failed', visibility: 'internal', data: { ...observation } })
      if (result) return { ...result, callId, logicalCallId, ...(Object.keys(diagnostics).length ? {
        diagnostics: { ...('diagnostics' in result ? result.diagnostics as Record<string, unknown> : {}), ...diagnostics },
      } : {}) }
      if (!(failure instanceof ModelDriverError) || attempt >= 3 || signal.aborted
        || failure.diagnostics.kind === 'protocol'
        || failure.diagnostics.status !== undefined && failure.diagnostics.status !== 429 && failure.diagnostics.status < 500) throw failure
      await delay(Math.min(2000, 250 * 2 ** (attempt - 1)), undefined, { signal: request.signal })
    }
  }
  return { invoke, nextCallId: () => `${work.id}:${work.fence}:${namespace}:${sequence + 1}` }
}

export function executionModel(host: Pick<HostPort, 'reserveModelCall' | 'recordModelUsage'>, source: ModelDriver, work: WorkItem,
  limits: Required<RootModelBudgetOptions>,
  emit?: (event: Omit<RunEvent, 'runId' | 'seq'>) => Promise<unknown>): ModelDriver {
  const model = source.singleAttempt?.() ?? source
  const { invoke, nextCallId } = modelExecution(host, model, work, limits, emit)
  const check = (request: unknown) => {
    if (!fitsModel(model, request)) throw new Error('model call exceeds its context budget; original input was not truncated')
  }
  return {
    ...(model.previewFormat ? { previewFormat: model.previewFormat } : {}),
    nextCallId,
    ...(model.modelId === undefined ? {} : { modelId: model.modelId }),
    ...(model.configurationFingerprint === undefined ? {} : { configurationFingerprint: model.configurationFingerprint }),
    profile: modelProfile(model),
    ...(model.countTokens ? { countTokens: model.countTokens.bind(model) } : {}),
    ...(model.contextWindowTokens === undefined ? {} : { contextWindowTokens: model.contextWindowTokens }),
    ...(model.maxOutputTokens === undefined ? {} : { maxOutputTokens: model.maxOutputTokens }),
    ...(model.maxThinkingTokens === undefined ? {} : { maxThinkingTokens: model.maxThinkingTokens }),
    ...(model.toolDefinitionTokens === undefined ? {} : { toolDefinitionTokens: model.toolDefinitionTokens }),
    run: request => {
      if (request.purpose === 'approval-explanation' && (request.tools?.length || request.codeExecution !== 'disabled')) {
        throw new Error('approval explanations cannot execute tools')
      }
      check(request)
      return invoke('agent-turn', request, (signal, callId, diagnostics) => {
        request.onAttempt?.(callId)
        const began = performance.now()
        let firstTextMs: number | undefined
        let active = true
        const result = model.run({ ...request, signal, onTextDelta: delta => {
            if (!active || signal.aborted) return
            if (delta) firstTextMs ??= performance.now() - began
            request.onTextDelta?.(delta)
          } })
        const finished = () => {
          active = false
          diagnostics['providerDurationMs'] = performance.now() - began
          if (firstTextMs !== undefined) diagnostics['providerFirstContentMs'] = firstTextMs
        }
        void result.then(finished, finished)
        return result
      })
    },
    structured: request => {
      check(request)
      return invoke('structured', request, signal => model.structured({ ...request, signal }))
    },
    compact: request => {
      const compiled = { ...request, instructions: COMPACTION_PROMPT.instructions, prompt: COMPACTION_PROMPT.manifest }
      check(compiled)
      return invoke('compaction', compiled, signal => model.compact({ ...compiled, signal }))
    },
  }
}
