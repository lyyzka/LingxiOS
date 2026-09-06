/**
 * In-memory store implementation. Serves three purposes: local development,
 * the test suite, and the executable specification of the lease state
 * machine that `pg-store.ts` mirrors in SQL.
 *
 * Single-process only — it relies on JavaScript's run-to-completion semantics
 * for atomicity.
 */
import { createHash, randomBytes, randomUUID } from 'node:crypto'
import { isDeepStrictEqual } from 'node:util'
import { sessionKeyOf, type HostActionResult, type SessionRecord, type WorkCompletion, type WorkItem, WORK_LANE_PRIORITY } from '../protocol/types.js'
import type {
  ActionIntent, ActionLedgerStore, EnqueueResult, EnqueueWorkInput, EventStore, HeartbeatRow,
  LeasedWork, SaveSessionResult, SessionStore, StoredRunEvent, WorkStore, WorkStoreOptions,
  ModelBudgetLimits, ModelBudgetReservation, ModelBudgetStore,
} from './stores.js'

export function hashToken(token: string): string {
  return createHash('sha256').update(token).digest('hex')
}

interface WorkRow {
  goalOutcome?: WorkCompletion['goalOutcome']
  id: string
  fence: number
  tenantId: string
  agentId: string
  sessionId: string
  threadId?: string
  kind: string
  lane: WorkItem['lane']
  triggerRef: string
  principalId?: string
  priority: number
  status: 'queued' | 'leased' | 'completed' | 'failed' | 'cancelled'
  createdAt: string
  availableAt: string
  attempts: number
  preemptions: number
  leaseTokenHash: string | null
  leasedBy: string | null
  leaseExpiresAt: number | null
  cancelRequestedAt: string | null
  preemptRequestedAt: string | null
  steerInputs: Array<{ id: string; text: string; createdAt: string }>
  resultText: string | null
  error: string | null
  meta?: Record<string, unknown>
}

interface SessionRoute {
  workerId: string
  homeEpoch: number
}

interface SessionLease {
  workId: string
  fence: number
  expiresAt: number
}

export class MemoryWorkStore implements WorkStore {
  async ownsBudgetRoot(work: Omit<WorkItem, 'leaseToken'>, rootWorkId: string): Promise<boolean> {
    if (rootWorkId === work.id) return true
    const root = this.rows.get(rootWorkId)
    return Boolean(root && work.meta?.['rootWorkId'] === rootWorkId && typeof work.meta?.['parentWorkId'] === 'string'
      && root.tenantId === work.tenantId && root.principalId === work.principalId)
  }
  async hasPendingChild(parent: Omit<WorkItem, 'leaseToken'>, childId: string, requestVersion: number): Promise<boolean> {
    const child = this.rows.get(childId)
    return Boolean(child && ['queued', 'leased'].includes(child.status) && !child.cancelRequestedAt
      && child.tenantId === parent.tenantId && child.sessionId === parent.sessionId && child.principalId === parent.principalId
      && child.meta?.['parentWorkId'] === parent.id && child.meta?.['parentRequestVersion'] === requestVersion)
  }
  private readonly rows = new Map<string, WorkRow>()
  private readonly routes = new Map<string, SessionRoute>()
  private readonly sessionLeases = new Map<string, SessionLease>()
  private readonly workerLastSeen = new Map<string, number>()
  private readonly claims = new Map<string, { workerId: string; work: WorkItem | null }>()
  private readonly leaseTtlMs: number
  private readonly workerTimeoutMs: number

  constructor(options: WorkStoreOptions = {}, private readonly now: () => number = Date.now) {
    this.leaseTtlMs = options.leaseTtlMs ?? 45_000
    this.workerTimeoutMs = options.workerTimeoutMs ?? 90_000
  }

  async enqueue(input: EnqueueWorkInput): Promise<EnqueueResult> {
    const id = input.id ?? randomUUID()
    const existing = this.rows.get(id)
    if (existing) {
      if (existing.tenantId !== input.tenantId || existing.agentId !== input.agentId
        || existing.sessionId !== input.sessionId || existing.threadId !== input.threadId
        || existing.principalId !== input.principalId || existing.kind !== input.kind
        || existing.lane !== input.lane || existing.triggerRef !== input.triggerRef
        || existing.priority !== (input.priority ?? 0)
        || !isDeepStrictEqual(existing.meta, input.meta)) {
        throw new Error('work identity reused with a different request or principal')
      }
      return { id, deduplicated: true }
    }
    const nowIso = new Date(this.now()).toISOString()
    this.rows.set(id, {
      id,
      fence: 0,
      tenantId: input.tenantId,
      agentId: input.agentId,
      sessionId: input.sessionId,
      ...(input.threadId !== undefined ? { threadId: input.threadId } : {}),
      kind: input.kind,
      lane: input.lane,
      triggerRef: input.triggerRef,
      ...(input.principalId ? { principalId: input.principalId } : {}),
      priority: input.priority ?? 0,
      status: 'queued',
      createdAt: nowIso,
      availableAt: input.availableAt ?? nowIso,
      attempts: 0,
      preemptions: 0,
      leaseTokenHash: null,
      leasedBy: null,
      leaseExpiresAt: null,
      cancelRequestedAt: null,
      preemptRequestedAt: null,
      steerInputs: [],
      resultText: null,
      error: null,
      ...(input.meta ? { meta: structuredClone(input.meta) } : {}),
    })
    return { id, deduplicated: false }
  }

  private sessionKey(row: WorkRow): string {
    return sessionKeyOf(row)
  }

  private workerAlive(workerId: string, now: number): boolean {
    const seen = this.workerLastSeen.get(workerId)
    return seen !== undefined && now - seen < this.workerTimeoutMs
  }

  async claim(workerId: string, requestId?: string): Promise<WorkItem | null> {
    if (requestId) {
      const prior = this.claims.get(requestId)
      if (prior) {
        if (prior.workerId !== workerId) throw new Error('claim request identity reused by another worker')
        return structuredClone(prior.work)
      }
      if (this.claims.size >= 10_000) this.claims.delete(this.claims.keys().next().value!)
    }
    const now = this.now()
    this.workerLastSeen.set(workerId, now)
    for (const [key, lease] of this.sessionLeases) {
      if (lease.expiresAt <= now) this.sessionLeases.delete(key)
    }
    const candidates = [...this.rows.values()]
      .filter((row) => {
        if ((row.kind === 'memory_synthesis' || row.kind === 'memory_index') && row.attempts >= 3) return false
        const claimable = row.status === 'queued'
          || (row.status === 'leased' && (row.leaseExpiresAt ?? 0) <= now)
        if (!claimable || row.cancelRequestedAt !== null) return false
        if (Date.parse(row.availableAt) > now) return false
        const sessionKey = this.sessionKey(row)
        if (this.sessionLeases.has(sessionKey)) return false
        const route = this.routes.get(sessionKey)
        if (route && route.workerId !== workerId && this.workerAlive(route.workerId, now)) return false
        return true
      })
      .sort((a, b) =>
        (WORK_LANE_PRIORITY[b.lane] - WORK_LANE_PRIORITY[a.lane])
        || (b.priority - a.priority)
        || (Date.parse(a.createdAt) - Date.parse(b.createdAt)))
    const row = candidates[0]
    if (!row) {
      if (requestId) this.claims.set(requestId, { workerId, work: null })
      return null
    }

    const sessionKey = this.sessionKey(row)
    const existingRoute = this.routes.get(sessionKey)
    let homeEpoch: number
    if (!existingRoute) {
      homeEpoch = 1
      this.routes.set(sessionKey, { workerId, homeEpoch })
    } else if (existingRoute.workerId === workerId) {
      homeEpoch = existingRoute.homeEpoch
    } else {
      // Taking over from a dead worker: the old filesystem home is suspect.
      homeEpoch = existingRoute.homeEpoch + 1
      this.routes.set(sessionKey, { workerId, homeEpoch })
    }

    const token = randomBytes(32).toString('base64url')
    row.status = 'leased'
    row.fence += 1
    row.leaseTokenHash = hashToken(token)
    row.leasedBy = workerId
    row.leaseExpiresAt = now + this.leaseTtlMs
    row.attempts += 1
    this.sessionLeases.set(sessionKey, { workId: row.id, fence: row.fence, expiresAt: now + this.leaseTtlMs })
    const work = this.toWorkItem(row, token, homeEpoch)
    if (requestId) this.claims.set(requestId, { workerId, work: structuredClone(work) })
    return work
  }

  private toWorkItem(row: WorkRow, leaseToken: string, homeEpoch?: number): WorkItem {
    return {
      id: row.id,
      fence: row.fence,
      homeEpoch: homeEpoch ?? this.routes.get(this.sessionKey(row))?.homeEpoch ?? 1,
      tenantId: row.tenantId,
      agentId: row.agentId,
      sessionId: row.sessionId,
      ...(row.threadId !== undefined ? { threadId: row.threadId } : {}),
      kind: row.kind,
      lane: row.lane,
      triggerRef: row.triggerRef,
      ...(row.principalId ? { principalId: row.principalId } : {}),
      createdAt: row.createdAt,
      availableAt: row.availableAt,
      attempts: row.attempts,
      preemptions: row.preemptions,
      leaseToken,
      ...(row.meta ? { meta: row.meta } : {}),
    }
  }

  private validLease(id: string, fence: number, leaseTokenHash: string): WorkRow | null {
    const row = this.rows.get(id)
    if (!row || row.status !== 'leased' || row.fence !== fence) return null
    if (row.leaseTokenHash !== leaseTokenHash) return null
    if ((row.leaseExpiresAt ?? 0) <= this.now()) return null
    return row
  }

  async heartbeat(id: string, fence: number, leaseTokenHash: string): Promise<HeartbeatRow | null> {
    const row = this.validLease(id, fence, leaseTokenHash)
    if (!row) return null
    const now = this.now()
    row.leaseExpiresAt = now + this.leaseTtlMs
    if (row.leasedBy) this.workerLastSeen.set(row.leasedBy, now)
    const lease = this.sessionLeases.get(this.sessionKey(row))
    if (lease && lease.workId === id && lease.fence === fence) lease.expiresAt = now + this.leaseTtlMs
    return {
      cancelRequested: row.cancelRequestedAt !== null,
      preemptRequested: row.preemptRequestedAt !== null,
      steer: [...row.steerInputs],
    }
  }

  async getLeased(id: string, fence: number, leaseTokenHash: string): Promise<LeasedWork | null> {
    const row = this.validLease(id, fence, leaseTokenHash)
    if (!row) return null
    const { leaseToken: _omit, ...work } = this.toWorkItem(row, '')
    return { work, status: 'leased', cancelRequested: row.cancelRequestedAt !== null }
  }

  async yieldWork(id: string, fence: number, leaseTokenHash: string): Promise<boolean> {
    const row = this.validLease(id, fence, leaseTokenHash)
    if (!row || row.preemptRequestedAt === null) return false
    row.status = 'queued'
    row.fence += 1
    row.leaseTokenHash = null
    row.leasedBy = null
    row.leaseExpiresAt = null
    row.preemptRequestedAt = null
    row.preemptions += 1
    row.availableAt = new Date(this.now() + 1_000).toISOString()
    this.releaseSessionLease(row, fence)
    return true
  }

  async complete(id: string, fence: number, leaseTokenHash: string, completion: WorkCompletion): Promise<boolean> {
    const row = this.validLease(id, fence, leaseTokenHash)
    if (!row) return false
    if (completion.status === 'completed' && row.cancelRequestedAt !== null) return false
    if (completion.status === 'completed' && typeof row.meta?.['text'] === 'string' && !completion.goalOutcome) return false
    if (completion.status === 'completed' && completion.goalOutcome && completion.goalOutcome.requestVersion !== row.steerInputs.length + 1) return false
    row.status = completion.status
    row.resultText = completion.resultText ?? null
    row.error = completion.error ?? null
    if (completion.goalOutcome) row.goalOutcome = structuredClone(completion.goalOutcome)
    row.leaseTokenHash = null
    row.leaseExpiresAt = null
    this.releaseSessionLease(row, fence)
    return true
  }

  private releaseSessionLease(row: WorkRow, fence: number): void {
    const key = this.sessionKey(row)
    const lease = this.sessionLeases.get(key)
    if (lease && lease.workId === row.id && lease.fence === fence) this.sessionLeases.delete(key)
  }

  async requestCancel(id: string): Promise<boolean> {
    const row = this.rows.get(id)
    if (!row || row.status === 'completed' || row.status === 'failed' || row.status === 'cancelled') return false
    row.cancelRequestedAt = new Date(this.now()).toISOString()
    if (row.status === 'queued') {
      row.status = 'cancelled'
    }
    return true
  }

  async requestPreempt(id: string): Promise<boolean> {
    const row = this.rows.get(id)
    if (!row || row.status !== 'leased') return false
    row.preemptRequestedAt = new Date(this.now()).toISOString()
    return true
  }

  async addSteer(id: string, text: string): Promise<boolean> {
    const row = this.rows.get(id)
    if (!row || row.status !== 'leased') return false
    row.steerInputs.push({ id: randomUUID(), text, createdAt: new Date(this.now()).toISOString() })
    return true
  }

  /** Test helper: inspect a row's durable state. */
  inspect(id: string): { status: string; fence: number; attempts: number; preemptions: number; resultText: string | null; error: string | null; goalOutcome?: WorkCompletion['goalOutcome'] } | null {
    const row = this.rows.get(id)
    if (!row) return null
    return {
      status: row.status, fence: row.fence, attempts: row.attempts,
      preemptions: row.preemptions, resultText: row.resultText, error: row.error,
      ...(row.goalOutcome ? { goalOutcome: structuredClone(row.goalOutcome) } : {}),
    }
  }

  /** Test helper: force-expire the current lease. */
  expireLease(id: string): void {
    const row = this.rows.get(id)
    if (row) {
      row.leaseExpiresAt = 0
      this.sessionLeases.delete(this.sessionKey(row))
    }
  }

  /** Test helper: mark a worker dead. */
  markWorkerDead(workerId: string): void {
    this.workerLastSeen.set(workerId, 0)
  }
}

export class MemorySessionStore implements SessionStore {
  private readonly sessions = new Map<string, SessionRecord>()
  private readonly requests = new Map<string, NonNullable<SessionRecord['request']>>()

  async get(key: string, workId?: string): Promise<SessionRecord | null> {
    const stored = this.sessions.get(key)
    if (!stored) return null
    const request = workId ? this.requests.get(workId) : stored.request
    const { request: _activeRequest, ...base } = stored
    return structuredClone({ ...base, ...(request ? { request } : {}) })
  }

  async save(session: SessionRecord): Promise<SaveSessionResult> {
    const stored = this.sessions.get(session.key)
    const currentRevision = stored?.revision ?? 0
    if (stored ? session.revision !== currentRevision : session.revision !== 0) {
      return { ok: false, conflict: true }
    }
    const next = structuredClone(session)
    next.revision = currentRevision + 1
    this.sessions.set(session.key, next)
    if (session.request) this.requests.set(session.request.workId, structuredClone(session.request))
    return { ok: true, revision: next.revision }
  }
}

export class MemoryEventStore implements EventStore {
  private readonly byRun = new Map<string, Map<number, StoredRunEvent>>()

  async append(event: StoredRunEvent): Promise<boolean> {
    let run = this.byRun.get(event.runId)
    if (!run) {
      run = new Map()
      this.byRun.set(event.runId, run)
    }
    if (run.has(event.seq)) return false
    run.set(event.seq, structuredClone(event))
    return true
  }

  async listRange(runId: string, fromSeqExclusive: number, toSeqInclusive: number, kinds?: readonly string[]): Promise<StoredRunEvent[]> {
    const run = this.byRun.get(runId)
    if (!run) return []
    return [...run.values()]
      .filter((event) => event.seq > fromSeqExclusive && event.seq <= toSeqInclusive
        && (!kinds || kinds.includes(event.kind)))
      .sort((a, b) => a.seq - b.seq)
      .map((event) => structuredClone(event))
  }
}

export class MemoryActionLedger implements ActionLedgerStore {
  async hasWait(workId: string, requestVersion: number, wait: { approvalId: string } | { question: string }): Promise<boolean> {
    for (const [key, intent] of this.intentDetails) {
      if (intent.workId !== workId || intent.requestVersion !== requestVersion) continue
      const result = this.effectiveResult(key)
      if (!result || result.executionState === 'unknown') continue
      if ('approvalId' in wait ? result.approval?.status === 'PENDING' && result.approval.id === wait.approvalId
        : intent.action.action === 'task.ask' && result.ok && result.directive?.type === 'defer'
          && result.directive.reason === 'user' && result.directive.data?.['question'] === wait.question) return true
    }
    return false
  }
  async unsettled(workId: string) {
    const pending: Array<{ actionKey: string; action: string; state: 'unknown' | 'awaiting_approval' }> = []
    for (const [actionKey, intent] of this.intentDetails) {
      if (intent.workId !== workId || intent.action.action.startsWith('task.')) continue
      const result = this.effectiveResult(actionKey)
      const state = result?.approval?.status === 'PENDING' ? 'awaiting_approval' : !result || result.executionState === 'unknown' ? 'unknown' : undefined
      if (state) pending.push({ actionKey, action: intent.action.action, state })
      if (pending.length === 65) break
    }
    return pending
  }
  private readonly results = new Map<string, HostActionResult>()
  private readonly intents = new Map<string, string>()
  private readonly intentDetails = new Map<string, ActionIntent>()
  private readonly resolutions = new Map<string, import('./stores.js').ActionResolution[]>()
  private readonly resolutionIds = new Map<string, import('./stores.js').ActionResolution>()

  private effectiveResult(key: string): HostActionResult | undefined {
    return this.resolutions.get(key)?.at(-1)?.result ?? this.results.get(key)
  }

  async reserve(idempotencyKey: string, fingerprint: string, intent: ActionIntent): Promise<'started' | 'existing'> {
    if (!intent || intent.action?.idempotencyKey !== idempotencyKey) throw new Error('action intent is required and must match its key')
    const existing = this.intents.get(idempotencyKey)
    if (existing !== undefined) {
      if (existing !== fingerprint) throw new Error('action identity reused with different parameters or authorization')
      return 'existing'
    }
    this.intents.set(idempotencyKey, fingerprint)
    this.intentDetails.set(idempotencyKey, structuredClone(intent))
    return 'started'
  }

  async findIntent(idempotencyKey: string): Promise<ActionIntent | null> {
    return structuredClone(this.intentDetails.get(idempotencyKey) ?? null)
  }

  async find(idempotencyKey: string): Promise<HostActionResult | null> {
    const stored = this.effectiveResult(idempotencyKey)
    return stored ? structuredClone(stored) : null
  }

  async listCell(workId: string, cellId: string, requestVersion: number | null) {
    return [...this.intentDetails.entries()]
      .filter(([, intent]) => intent.workId === workId && intent.action.cellId === cellId
        && intent.requestVersion === requestVersion)
      .sort(([, left], [, right]) => left.action.callIndex - right.action.callIndex)
      .map(([key, intent]) => ({ intent: structuredClone(intent), result: structuredClone(this.effectiveResult(key) ?? null) }))
  }

  async record(idempotencyKey: string, result: HostActionResult): Promise<HostActionResult> {
    if (!this.intents.has(idempotencyKey)) throw new Error('action intent is required before recording a receipt')
    const existing = this.results.get(idempotencyKey)
    if (existing) return structuredClone(existing)
    this.results.set(idempotencyKey, structuredClone(result))
    return result
  }

  async recordResolution(resolution: import('./stores.js').ActionResolution): Promise<'recorded' | 'existing'> {
    if (!this.intents.has(resolution.actionKey)) throw new Error('action intent is required before reconciliation')
    const prior = this.resolutionIds.get(resolution.id)
    if (prior) {
      if (!isDeepStrictEqual(prior, resolution)) throw new Error('resolution identity reused with different evidence')
      return 'existing'
    }
    const saved = structuredClone(resolution)
    this.resolutionIds.set(saved.id, saved)
    this.resolutions.set(saved.actionKey, [...(this.resolutions.get(saved.actionKey) ?? []), saved])
    return 'recorded'
  }
}

export class MemoryModelBudgetStore implements ModelBudgetStore {
  private readonly budgets = new Map<string, ModelBudgetLimits & { calls: Set<string>; recorded: Set<string>; tokens: number; costMicros: number }>()

  async reserve(rootWorkId: string, callId: string, limits: ModelBudgetLimits): Promise<ModelBudgetReservation> {
    let budget = this.budgets.get(rootWorkId)
    if (!budget) {
      budget = { ...limits, calls: new Set(), recorded: new Set(), tokens: 0, costMicros: 0 }
      this.budgets.set(rootWorkId, budget)
    }
    const existing = budget.calls.has(callId)
    const allowed = existing || (Date.now() < Date.parse(budget.deadlineAt)
      && budget.calls.size < budget.maxModelCalls && budget.tokens < budget.maxTokens && budget.costMicros < budget.maxCostMicros)
    if (allowed && !existing) budget.calls.add(callId)
    return { allowed, remainingCalls: Math.max(0, budget.maxModelCalls - budget.calls.size),
      remainingTokens: Math.max(0, budget.maxTokens - budget.tokens),
      remainingCostMicros: Math.max(0, budget.maxCostMicros - budget.costMicros), deadlineAt: budget.deadlineAt }
  }

  async record(rootWorkId: string, callId: string, inputTokens: number, outputTokens: number, costMicros: number): Promise<void> {
    const budget = this.budgets.get(rootWorkId)
    if (!budget || !budget.calls.has(callId)) throw new Error('model call budget reservation is missing')
    if (budget.recorded.has(callId)) return
    budget.recorded.add(callId)
    budget.tokens += inputTokens + outputTokens
    budget.costMicros += costMicros
  }
}
