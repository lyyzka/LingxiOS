import type { ModelDriver } from './driver.js'

export type ModelPurpose = 'execute' | 'content-review' | 'compaction' | 'memory-synthesis' | 'approval-explanation'

/** Describes configured, tested provider features; API compatibility is not a capability test. */
export interface ModelProfile {
  id: string
  contextWindowTokens: number
  maxOutputTokens: number
  maxThinkingTokens: number
  toolCalls: boolean
  jsonObject: boolean
  parallelTools: boolean
}

export function modelProfile(model: ModelDriver): ModelProfile {
  return model.profile ?? { id: model.modelId ?? 'unknown', contextWindowTokens: model.contextWindowTokens ?? 128_000,
    maxOutputTokens: model.maxOutputTokens ?? 8192, maxThinkingTokens: model.maxThinkingTokens ?? 0,
    toolCalls: true, jsonObject: true, parallelTools: true }
}

/** The default remains a conservative UTF-8 bound. Hosts can supply a tested tokenizer. */
export function inputTokens(model: Pick<ModelDriver, 'countTokens'>, input: unknown): number {
  const text = JSON.stringify(input)
  const count = model.countTokens ? model.countTokens(text) : Buffer.byteLength(text)
  if (!Number.isSafeInteger(count) || count < 0) throw new Error('invalid model token estimate')
  return count
}

export function fitsModel(model: ModelDriver, input: unknown): boolean {
  const profile = modelProfile(model)
  return inputTokens(model, input) + profile.maxOutputTokens + profile.maxThinkingTokens + 512 <= Math.floor(profile.contextWindowTokens * 0.9)
}
