/**
 * HttpHostClient — the worker-side HostPort implementation over the control
 * plane's HTTP API.
 *
 * Retry discipline: only idempotent-by-construction calls are retried
 * (everything here is: lease proofs make mutations fenced, events dedupe on
 * seq, actions dedupe on idempotency key). Lease-invalid responses (409) are
 * surfaced as {@link LeaseLostError} so the runtime can stop cleanly.
 */
import { randomUUID } from 'node:crypto'
import { abortable } from '../deadline.js'
import { AgentOSError, LeaseLostError, errorMessage } from '../errors.js'
import type {
  AssistantMessage, HeartbeatResult, HostAction, HostActionResult,
  RunEvent, SessionRecord, TurnContext, WorkCompletion, WorkItem,
} from '../protocol/types.js'
import type { HostPort } from './port.js'
import type { ModelBudgetLimits, ModelBudgetReservation } from '../control-plane/stores.js'

export interface HttpHostClientOptions {
  baseUrl: string
  serviceToken: string
  workerId: string
  workKinds?: readonly string[]
  requestTimeoutMs?: number
  maxAttempts?: number
  retryBaseMs?: number
  maxResponseBytes?: number
  fetchImpl?: typeof fetch
  sleep?: (ms: number) => Promise<void>
}

export class HostRequestError extends AgentOSError {
  constructor(readonly status: number, message: string, readonly responseCode?: string) {
    super('host_request', message)
  }
}

export class HttpHostClient implements HostPort {
  async prepareMemoryReview(work: WorkItem,action: HostAction,signal?: AbortSignal) {
    return this.request<import('../memory/types.js').MemoryReviewRequest|null>('POST',`/v5/work/${encodeURIComponent(work.id)}/memory-review`,{...this.proof(work),action},signal)
  }
  async recordMemoryReview(work: WorkItem,action: HostAction,hash: string,review: import('../memory/types.js').MemoryReview,signal?: AbortSignal) {
    await this.request('POST',`/v5/work/${encodeURIComponent(work.id)}/memory-review-result`,{...this.proof(work),action,hash,review},signal)
  }
  async verifyCandidate(work: WorkItem, candidate: import('../outcome/verification.js').Candidate, signal?: AbortSignal): Promise<import('../outcome/verification.js').CandidateVerification> {
    return this.request('POST', `/v5/work/${encodeURIComponent(work.id)}/verify`, { ...this.proof(work), candidate }, signal)
  }
  async saveStep(work: WorkItem, step: import('../control-plane/steps.js').ExecutionStep, signal?: AbortSignal): Promise<void> {
    await this.request('POST', `/v5/work/${encodeURIComponent(work.id)}/checkpoint`, { ...this.proof(work), step }, signal)
  }
  lastContactAt = 0
  private readonly baseUrl: string
  private readonly timeoutMs: number
  private readonly maxAttempts: number
  private readonly retryBaseMs: number
  private readonly maxResponseBytes: number
  private readonly fetchImpl: typeof fetch
  private readonly sleep: (ms: number) => Promise<void>

  constructor(private readonly options: HttpHostClientOptions) {
    this.baseUrl = options.baseUrl.replace(/\/+$/, '')
    this.timeoutMs = options.requestTimeoutMs ?? 30_000
    this.maxAttempts = options.maxAttempts ?? 3
    this.retryBaseMs = options.retryBaseMs ?? 250
    this.maxResponseBytes = options.maxResponseBytes ?? 24 * 1024 * 1024
    this.fetchImpl = options.fetchImpl ?? fetch
    this.sleep = options.sleep ?? ((ms) => new Promise((resolveSleep) => setTimeout(resolveSleep, ms)))
  }

  private async request<T>(method: string, path: string, body?: unknown, signal?: AbortSignal): Promise<T> {
    signal = AbortSignal.any([AbortSignal.timeout(this.timeoutMs),...signal ? [signal] : []])
    let lastError: unknown
    for (let attempt = 1; attempt <= this.maxAttempts; attempt++) {
      signal?.throwIfAborted()
      const timeout = AbortSignal.timeout(this.timeoutMs)
      const combined = signal ? AbortSignal.any([signal, timeout]) : timeout
      try {
        const response = await abortable(this.fetchImpl(`${this.baseUrl}${path}`, {
          method,
          headers: {
            authorization: `Bearer ${this.options.serviceToken}`,
            ...(body === undefined ? {} : { 'content-type': 'application/json' }),
          },
          ...(body === undefined ? {} : { body: JSON.stringify(body) }),
          signal: combined,
        }), combined)
        const detail = await readBody(response, this.maxResponseBytes, combined)
        if (response.ok) { this.lastContactAt = Date.now(); return JSON.parse(detail) as T }
        let message = detail
        let responseCode: string | undefined
        try {
          const parsed = JSON.parse(detail) as { error?: unknown; code?: unknown }
          if (typeof parsed.error === 'string') message = parsed.error
          if (typeof parsed.code === 'string') responseCode = parsed.code
        } catch { /* plain-text error body */ }
        if (responseCode === 'lease_lost') throw new LeaseLostError(message || 'lease rejected by control plane')
        if (response.status < 500) throw new HostRequestError(response.status, message || `control plane returned ${response.status}`, responseCode)
        lastError = new HostRequestError(response.status, message)
      } catch (error) {
        if (error instanceof LeaseLostError || (error instanceof HostRequestError && error.status > 0 && error.status < 500)) throw error
        signal?.throwIfAborted()
        lastError = error
        if (attempt < this.maxAttempts) {
          await abortable(this.sleep(this.retryBaseMs * 2 ** (attempt - 1)),signal)
          continue
        }
        break
      }
      if (attempt < this.maxAttempts) {
        await abortable(this.sleep(this.retryBaseMs * 2 ** (attempt - 1)),signal)
        continue
      }
    }
    throw new HostRequestError(0, `control plane unreachable after ${this.maxAttempts} attempts: ${errorMessage(lastError)}`)
  }

  private proof(work: WorkItem): { fence: number; leaseToken: string } {
    return { fence: work.fence, leaseToken: work.leaseToken }
  }

  async claimWork(signal?: AbortSignal): Promise<WorkItem | null> {
    return this.request<WorkItem | null>('POST', '/v5/work/claim', {
      workerId: this.options.workerId, requestId: randomUUID(), workKinds: this.options.workKinds ?? ['turn', 'resume'],
    }, signal)
  }

  async heartbeat(work: WorkItem, signal?: AbortSignal): Promise<HeartbeatResult> {
    return this.request<HeartbeatResult>('POST', `/v5/work/${encodeURIComponent(work.id)}/heartbeat`, this.proof(work), signal)
  }

  async loadContext(work: WorkItem, signal?: AbortSignal): Promise<TurnContext> {
    const query = new URLSearchParams({ fence: String(work.fence), leaseToken: work.leaseToken })
    const context = await this.request<TurnContext>('GET', `/v5/work/${encodeURIComponent(work.id)}/context?${query}`, undefined, signal)
    // The wire strips the lease token from the embedded work item; restore it.
    context.work = { ...context.work, leaseToken: work.leaseToken, homeEpoch: work.homeEpoch }
    return context
  }

  async executeAction(work: WorkItem, action: HostAction, signal?: AbortSignal): Promise<HostActionResult> {
    return this.request<HostActionResult>('POST', `/v5/work/${encodeURIComponent(work.id)}/actions`, {
      ...this.proof(work), action,
    }, signal)
  }

  async reserveModelCall(work: WorkItem, callId: string, limits: ModelBudgetLimits, signal?: AbortSignal): Promise<ModelBudgetReservation> {
    return this.request('POST', `/v5/work/${encodeURIComponent(work.id)}/model-budget`, { ...this.proof(work), callId, limits }, signal)
  }

  async recordModelUsage(work: WorkItem, callId: string, usage: { inputTokens: number; outputTokens: number; costMicros: number }, observation?: import('../model/execution.js').ModelCallObservation, signal?: AbortSignal): Promise<void> {
    await this.request('POST', `/v5/work/${encodeURIComponent(work.id)}/model-usage`, { ...this.proof(work), callId, usage, observation }, signal)
  }

  async recoverCell(work: WorkItem, cellId: string, signal?: AbortSignal) {
    return this.request<Array<{ action: string; idempotencyKey: string; result: HostActionResult }> | null>(
      'POST', `/v5/work/${encodeURIComponent(work.id)}/reconcile`, { ...this.proof(work), cellId }, signal)
  }

  async recoverStep(work: WorkItem, cellId: string, signal?: AbortSignal) {
    return this.request<{ output: string; artifacts: import('../protocol/types.js').KernelArtifact[] } | null>(
      'POST', `/v5/work/${encodeURIComponent(work.id)}/step`, { ...this.proof(work), cellId }, signal)
  }

  async stageArtifact(work: WorkItem, artifact: import('../protocol/types.js').KernelArtifact, bytes: Uint8Array, signal?: AbortSignal): Promise<void> {
    await this.request('POST', `/v5/work/${encodeURIComponent(work.id)}/artifacts`, {
      ...this.proof(work), artifact, contentBase64: Buffer.from(bytes).toString('base64'),
    }, signal)
  }

  async emitEvent(work: WorkItem, event: RunEvent, signal?: AbortSignal): Promise<void> {
    await this.request('POST', `/v5/work/${encodeURIComponent(work.id)}/events`, { ...this.proof(work), event }, signal)
  }

  async loadSession(work: WorkItem, key: string, signal?: AbortSignal): Promise<SessionRecord | null> {
    const payload = await this.request<{ session: SessionRecord | null }>('POST', `/v5/work/${encodeURIComponent(work.id)}/session`, { ...this.proof(work), key }, signal)
    return payload.session
  }

  async saveSession(work: WorkItem, session: SessionRecord, signal?: AbortSignal): Promise<void> {
    const saved = await this.request<{ revision: number }>('PUT', '/v5/sessions', {
      workId: work.id, ...this.proof(work), session,
    }, signal)
    session.revision = saved.revision
  }

  async commitResult(work: WorkItem, message: AssistantMessage, signal?: AbortSignal): Promise<void> {
    await this.request('POST', `/v5/work/${encodeURIComponent(work.id)}/result`, { ...this.proof(work), message }, signal)
  }

  async completeWork(work: WorkItem, completion: WorkCompletion, signal?: AbortSignal): Promise<void> {
    await this.request('POST', `/v5/work/${encodeURIComponent(work.id)}/complete`, { ...this.proof(work), ...completion }, signal)
  }

  async waitWork(work: WorkItem, goalOutcome: import('../protocol/outcome.js').WaitingOutcome, signal?: AbortSignal): Promise<void> {
    await this.request('POST', `/v5/work/${encodeURIComponent(work.id)}/wait`, { ...this.proof(work), goalOutcome }, signal)
  }

  async yieldWork(work: WorkItem, signal?: AbortSignal): Promise<void> {
    await this.request('POST', `/v5/work/${encodeURIComponent(work.id)}/yield`, this.proof(work), signal)
  }
}

async function readBody(response: Response, maxBytes: number, signal: AbortSignal): Promise<string> {
  const declared = Number(response.headers.get('content-length'))
  if (Number.isFinite(declared) && declared > maxBytes) throw new Error(`control plane response exceeds ${maxBytes} bytes`)
  if (!response.body) return ''
  const reader = response.body.getReader()
  const cancel = () => { void reader.cancel(signal.reason).catch(() => {}) }
  signal.addEventListener('abort',cancel,{ once: true })
  const chunks: Uint8Array[] = []
  let size = 0
  try { for (;;) {
    const { done, value } = await abortable(reader.read(),signal)
    if (done) break
    size += value.byteLength
    if (size > maxBytes) {
      void reader.cancel().catch(() => {})
      throw new Error(`control plane response exceeds ${maxBytes} bytes`)
    }
    chunks.push(value)
  } } finally { signal.removeEventListener('abort',cancel); reader.releaseLock() }
  const bytes = new Uint8Array(size)
  let offset = 0
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength }
  return new TextDecoder().decode(bytes)
}
