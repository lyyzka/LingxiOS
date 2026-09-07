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
  async verifyCandidate(work: WorkItem, candidate: import('../outcome/verification.js').Candidate): Promise<import('../outcome/verification.js').CandidateVerification> {
    return this.request('POST', `/v4/work/${encodeURIComponent(work.id)}/verify`, { ...this.proof(work), candidate })
  }
  async saveStep(work: WorkItem, step: import('../control-plane/steps.js').ExecutionStep): Promise<void> {
    await this.request('POST', `/v4/work/${encodeURIComponent(work.id)}/checkpoint`, { ...this.proof(work), step })
  }
  lastContactAt = 0
  lecture(work: WorkItem, command: import('../lecture-deck/transport.js').LectureCommand): Promise<unknown> {
    return this.request('POST', `/v4/work/${encodeURIComponent(work.id)}/lecture`, { ...this.proof(work), command })
  }
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
    let lastError: unknown
    for (let attempt = 1; attempt <= this.maxAttempts; attempt++) {
      signal?.throwIfAborted()
      const timeout = AbortSignal.timeout(this.timeoutMs)
      const combined = signal ? AbortSignal.any([signal, timeout]) : timeout
      try {
        const response = await this.fetchImpl(`${this.baseUrl}${path}`, {
          method,
          headers: {
            authorization: `Bearer ${this.options.serviceToken}`,
            ...(body === undefined ? {} : { 'content-type': 'application/json' }),
          },
          ...(body === undefined ? {} : { body: JSON.stringify(body) }),
          signal: combined,
        })
        const detail = await readBody(response, this.maxResponseBytes)
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
          await this.sleep(this.retryBaseMs * 2 ** (attempt - 1))
          continue
        }
        break
      }
      if (attempt < this.maxAttempts) {
        await this.sleep(this.retryBaseMs * 2 ** (attempt - 1))
        continue
      }
    }
    throw new HostRequestError(0, `control plane unreachable after ${this.maxAttempts} attempts: ${errorMessage(lastError)}`)
  }

  private proof(work: WorkItem): { fence: number; leaseToken: string } {
    return { fence: work.fence, leaseToken: work.leaseToken }
  }

  async claimWork(signal?: AbortSignal): Promise<WorkItem | null> {
    return this.request<WorkItem | null>('POST', '/v4/work/claim', {
      workerId: this.options.workerId, requestId: randomUUID(), workKinds: this.options.workKinds ?? ['turn', 'resume'],
    }, signal)
  }

  async heartbeat(work: WorkItem): Promise<HeartbeatResult> {
    return this.request<HeartbeatResult>('POST', `/v4/work/${encodeURIComponent(work.id)}/heartbeat`, this.proof(work))
  }

  async loadContext(work: WorkItem): Promise<TurnContext> {
    const query = new URLSearchParams({ fence: String(work.fence), leaseToken: work.leaseToken })
    const context = await this.request<TurnContext>('GET', `/v4/work/${encodeURIComponent(work.id)}/context?${query}`)
    // The wire strips the lease token from the embedded work item; restore it.
    context.work = { ...context.work, leaseToken: work.leaseToken, homeEpoch: work.homeEpoch }
    return context
  }

  async executeAction(work: WorkItem, action: HostAction): Promise<HostActionResult> {
    return this.request<HostActionResult>('POST', `/v4/work/${encodeURIComponent(work.id)}/actions`, {
      ...this.proof(work), action,
    })
  }

  async reserveModelCall(work: WorkItem, callId: string, limits: ModelBudgetLimits): Promise<ModelBudgetReservation> {
    return this.request('POST', `/v4/work/${encodeURIComponent(work.id)}/model-budget`, { ...this.proof(work), callId, limits })
  }

  async recordModelUsage(work: WorkItem, callId: string, usage: { inputTokens: number; outputTokens: number; costMicros: number }, observation?: import('../model/execution.js').ModelCallObservation): Promise<void> {
    await this.request('POST', `/v4/work/${encodeURIComponent(work.id)}/model-usage`, { ...this.proof(work), callId, usage, observation })
  }

  async recoverCell(work: WorkItem, cellId: string) {
    return this.request<Array<{ action: string; idempotencyKey: string; result: HostActionResult }> | null>(
      'POST', `/v4/work/${encodeURIComponent(work.id)}/reconcile`, { ...this.proof(work), cellId })
  }

  async recoverStep(work: WorkItem, cellId: string) {
    return this.request<{ output: string; artifacts: import('../protocol/types.js').KernelArtifact[] } | null>(
      'POST', `/v4/work/${encodeURIComponent(work.id)}/step`, { ...this.proof(work), cellId })
  }

  async stageArtifact(work: WorkItem, artifact: import('../protocol/types.js').KernelArtifact, bytes: Uint8Array): Promise<void> {
    await this.request('POST', `/v4/work/${encodeURIComponent(work.id)}/artifacts`, {
      ...this.proof(work), artifact, contentBase64: Buffer.from(bytes).toString('base64'),
    })
  }

  async emitEvent(work: WorkItem, event: RunEvent): Promise<void> {
    await this.request('POST', `/v4/work/${encodeURIComponent(work.id)}/events`, { ...this.proof(work), event })
  }

  async loadSession(work: WorkItem, key: string): Promise<SessionRecord | null> {
    const payload = await this.request<{ session: SessionRecord | null }>('POST', `/v4/work/${encodeURIComponent(work.id)}/session`, { ...this.proof(work), key })
    return payload.session
  }

  async saveSession(work: WorkItem, session: SessionRecord): Promise<void> {
    const saved = await this.request<{ revision: number }>('PUT', '/v4/sessions', {
      workId: work.id, ...this.proof(work), session,
    })
    session.revision = saved.revision
  }

  async commitResult(work: WorkItem, message: AssistantMessage): Promise<void> {
    await this.request('POST', `/v4/work/${encodeURIComponent(work.id)}/result`, { ...this.proof(work), message })
  }

  async completeWork(work: WorkItem, completion: WorkCompletion): Promise<void> {
    await this.request('POST', `/v4/work/${encodeURIComponent(work.id)}/complete`, { ...this.proof(work), ...completion })
  }

  async yieldWork(work: WorkItem): Promise<void> {
    await this.request('POST', `/v4/work/${encodeURIComponent(work.id)}/yield`, this.proof(work))
  }
}

async function readBody(response: Response, maxBytes: number): Promise<string> {
  const declared = Number(response.headers.get('content-length'))
  if (Number.isFinite(declared) && declared > maxBytes) throw new Error(`control plane response exceeds ${maxBytes} bytes`)
  if (!response.body) return ''
  const reader = response.body.getReader()
  const chunks: Uint8Array[] = []
  let size = 0
  for (;;) {
    const { done, value } = await reader.read()
    if (done) break
    size += value.byteLength
    if (size > maxBytes) {
      await reader.cancel()
      throw new Error(`control plane response exceeds ${maxBytes} bytes`)
    }
    chunks.push(value)
  }
  const bytes = new Uint8Array(size)
  let offset = 0
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength }
  return new TextDecoder().decode(bytes)
}
