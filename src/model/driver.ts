/**
 * Model driver port. The runtime speaks only this interface; concrete
 * providers (OpenAI-compatible, Anthropic, test fakes) live behind it.
 */
import type { ModelItem } from '../protocol/types.js'

export interface ModelUsage {
  available: boolean
  inputTokens: number
  outputTokens: number
}

export interface ModelTurnResult {
  callId?: string
  /** Structured final response; the runtime validates it against the captured request. */
  finalCandidate?: string
  /** Items to append to session history (assistant text and/or tool calls). */
  output: ModelItem[]
  /** Full assistant text of this turn ('' when the turn is a tool call). */
  text: string
  model?: string
  usage: ModelUsage
  diagnostics?: Record<string, unknown>
}

export interface ModelTurnRequest {
  tools?: readonly import('../tools/catalog.js').ToolDefinition[]
  instructions: string
  items: readonly ModelItem[]
  signal?: AbortSignal | undefined
  /** Called as assistant text streams; used for latency, not for delivery. */
  onTextDelta?: ((delta: string) => void) | undefined
}

export interface StructuredCallRequest {
  instructions: string
  input: unknown
  signal?: AbortSignal | undefined
}

export interface StructuredCallResult {
  value: unknown
  model: string
  usage: ModelUsage
}

export interface CompactionRequest {
  instructions: string
  items: readonly ModelItem[]
  signal?: AbortSignal | undefined
}

export interface CompactionResult {
  value: string
  model: string
  usage: ModelUsage
}

export interface ModelDriver {
  /** Runtime owns retries so each outbound request reserves and settles its own budget. */
  singleAttempt?(): ModelDriver
  nextCallId?(): string
  readonly modelId?: string
  /** Hash of non-secret provider parameters needed to identify a replay configuration. */
  readonly configurationFingerprint?: string
  readonly contextWindowTokens?: number
  readonly maxOutputTokens?: number
  readonly toolDefinitionTokens?: number
  /** One agent-loop turn: one Python call or a structured final candidate. */
  run(request: ModelTurnRequest): Promise<ModelTurnResult>
  /** One-shot JSON-mode call for auxiliary pipelines. */
  structured(request: StructuredCallRequest): Promise<StructuredCallResult>
  /** Summarize `items` into a continuity summary for context compaction. */
  compact(request: CompactionRequest): Promise<CompactionResult>
}
