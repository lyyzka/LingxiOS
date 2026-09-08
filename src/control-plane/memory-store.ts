import { workStatusOf, type WorkStatus } from '../protocol/types.js'
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
  status: WorkStatus
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
  private readonly attempts = new Map<string, Omit<WorkItem, 'leaseToken'>>()
  async getAttempt(id: string, fence: number, leaseTokenHash: string) {
    return structuredClone(this.attempts.get(JSON.stringify([id, fence, leaseTokenHash])) ?? null)
  }
  async children(parent: Omit<WorkItem, 'leaseToken'>) {
    return [...this.rows.values()].filter(row => row.meta?.['parentWorkId'] === parent.id
      && row.tenantId === parent.tenantId && row.principalId === parent.principalId).slice(0, 64)
      .map(row => ({ id: row.id, status: row.status, resultText: null, goalOutcome: row.goalOutcome ?? null }))
  }
  async ownsBudgetRoot(work: Omit<WorkItem, 'leaseToken'>, rootWorkId: string): Promise<boolean> {
    if (rootWorkId === work.id) return true
    if (work.meta?.['rootWorkId'] !== rootWorkId) return false
    const seen = new Set([work.id])
    let parentId = work.meta?.['parentWorkId']
    for (let depth = 0; typeof parentId === 'string' && depth < 64; depth++) {
      if (seen.has(parentId)) return false
      seen.add(parentId)
      const parent = this.rows.get(parentId)
      if (!parent || parent.tenantId !== work.tenantId || parent.principalId !== work.principalId) return false
      if (parent.id === rootWorkId) return true
      parentId = parent.meta?.['parentWorkId']
    }
    return false
  }
  async hasChild(parent: Omit<WorkItem, 'leaseToken'>, childId: string, requestVersion: number): Promise<boolean> {
    const child = this.rows.get(childId)
    return Boolean(child && child.tenantId === parent.tenantId && child.principalId === parent.principalId
      && child.meta?.['parentWorkId'] === parent.id && child.meta?.['parentRequestVersion'] === requestVersion)
  }
  private readonly rows = new Map<string, WorkRow>()
  private readonly routes = new Map<string, SessionRoute>()
  private readonly sessionLeases = new Map<string, SessionLease>()
  private readonly workerLastSeen = new Map<string, number>()
  private readonly claims = new Map<string, { workerId: string; work: WorkItem | null; kinds: string[] | null }>()
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

  async claim(workerId: string, requestId?: string, workKinds?: readonly string[]): Promise<WorkItem | null> {
    const kinds = workKinds ? [...new Set(workKinds)].sort() : null
    if (requestId) {
      const prior = this.claims.get(requestId)
      if (prior) {
        if (prior.workerId !== workerId) throw new Error('claim request identity reused by another worker')
        if (!isDeepStrictEqual(prior.kinds, kinds)) throw new Error('claim request task types changed')
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
        if (kinds && !kinds.includes(row.kind)) return false
        if (['memory_synthesis','memory_index','memory_evaluation'].includes(row.kind) && row.attempts >= 3) return false
        const claimable = row.status === 'queued'
          || (row.status === 'leased' && (row.leaseExpiresAt ?? 0) <= now)
        if (!claimable || row.cancelRequestedAt !== null) return false
        if (Array.isArray(row.meta?.['dependsOn']) && row.meta['dependsOn'].some(id => {
          const dependency = this.rows.get(String(id))
          return !dependency || dependency.tenantId !== row.tenantId || dependency.principalId !== row.principalId
            || ['queued','leased','waiting'].includes(dependency.status)
        })) return false
        if (Date.parse(row.availableAt) > now) return false
        const sessionKey = this.sessionKey(row)
        if (this.sessionLeases.has(sessionKey)) return false
        return true
      })
      .sort((a, b) =>
        (WORK_LANE_PRIORITY[b.lane] + Math.floor(Math.max(0, now - Date.parse(b.availableAt)) / 60_000)
          - WORK_LANE_PRIORITY[a.lane] - Math.floor(Math.max(0, now - Date.parse(a.availableAt)) / 60_000))
        || (b.priority - a.priority)
        || (Number(this.routes.get(this.sessionKey(b))?.workerId === workerId) - Number(this.routes.get(this.sessionKey(a))?.workerId === workerId))
        || (Date.parse(a.createdAt) - Date.parse(b.createdAt)))
    const row = candidates[0]
    if (!row) {
      if (requestId) this.claims.set(requestId, { workerId, work: null, kinds })
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
      // Another worker must restore from committed snapshots, not the previous home.
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
    const { leaseToken: _token, ...issued } = work
    this.attempts.set(JSON.stringify([work.id, work.fence, row.leaseTokenHash]), structuredClone(issued))
    if (requestId) this.claims.set(requestId, { workerId, work: structuredClone(work), kinds })
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
    const status = workStatusOf(completion)
    if (status === 'waiting') throw new Error('waiting requires wait')
    return this.settle(id, fence, leaseTokenHash, status, completion)
  }

  async wait(id: string, fence: number, leaseTokenHash: string, goalOutcome: import('../protocol/outcome.js').WaitingOutcome): Promise<boolean> {
    return this.settle(id, fence, leaseTokenHash, 'waiting', { goalOutcome })
  }

  private async settle(id: string, fence: number, leaseTokenHash: string, status: WorkRow['status'], completion: Omit<WorkCompletion, 'status'>): Promise<boolean> {
    const row = this.validLease(id, fence, leaseTokenHash)
    if (!row) return false
    if (!['failed','cancelled'].includes(status) && row.cancelRequestedAt !== null) return false
    if (!['failed','cancelled'].includes(status) && typeof row.meta?.['text'] === 'string' && !completion.goalOutcome) return false
    if (!['failed','cancelled'].includes(status) && completion.goalOutcome && completion.goalOutcome.requestVersion !== row.steerInputs.length + 1) return false
    row.status = status
    row.error = completion.error ?? null
    if (completion.goalOutcome) row.goalOutcome = structuredClone(completion.goalOutcome)
    row.leaseTokenHash = null
    row.leaseExpiresAt = null
    this.releaseSessionLease(row, fence)
    if (row.status !== 'waiting') for (const child of this.rows.values()) {
      if (child.meta?.['parentWorkId'] === row.id && child.tenantId === row.tenantId && child.principalId === row.principalId) await this.requestCancel(child.id)
    }
    return true
  }

  private releaseSessionLease(row: WorkRow, fence: number): void {
    const key = this.sessionKey(row)
    const lease = this.sessionLeases.get(key)
    if (lease && lease.workId === row.id && lease.fence === fence) this.sessionLeases.delete(key)
  }

  async requestCancel(id: string): Promise<boolean> {
    const row = this.rows.get(id)
    if (!row || !['queued','leased','waiting'].includes(row.status)) return false
    const descendants = new Set([id])
    for (let depth = 0; depth < 64; depth++) {
      let added = false
      for (const child of this.rows.values()) {
        if (child.tenantId === row.tenantId && child.principalId === row.principalId
          && descendants.has(String(child.meta?.['parentWorkId'])) && !descendants.has(child.id)) {
          descendants.add(child.id); added = true
        }
      }
      if (!added) break
    }
    for (const target of descendants) {
      const child = this.rows.get(target)!
      if (!['queued','leased','waiting'].includes(child.status)) continue
      child.cancelRequestedAt = new Date(this.now()).toISOString()
      if (child.status !== 'leased') {
        child.status = 'cancelled'
        child.goalOutcome = { status: 'blocked', verification: 'not_run', requestVersion: child.steerInputs.length + 1, gaps: ['Cancelled by user'] }
      }
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
    if (!row || !['queued','leased','waiting'].includes(row.status) || row.cancelRequestedAt) return false
    row.steerInputs.push({ id: randomUUID(), text, createdAt: new Date(this.now()).toISOString() })
    if (row.status === 'waiting') row.status = 'queued'
    delete row.goalOutcome
    for (const child of this.rows.values()) if (child.meta?.['parentWorkId'] === id
      && child.tenantId === row.tenantId && child.principalId === row.principalId) await this.requestCancel(child.id)
    return true
  }

  /** Test helper: inspect a row's durable state. */
  inspect(id: string): { status: string; fence: number; attempts: number; preemptions: number; resultText: string | null; error: string | null; goalOutcome?: WorkCompletion['goalOutcome'] } | null {
    const row = this.rows.get(id)
    if (!row) return null
    return {
      status: row.status, fence: row.fence, attempts: row.attempts,
      preemptions: row.preemptions, resultText: null, error: row.error,
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
  async artifacts(workId: string) {
    return [...this.intentDetails].filter(([, intent]) => intent.workId === workId)
      .flatMap(([key]) => { const result = this.effectiveResult(key); return result?.ok ? structuredClone(result.artifacts ?? []) : [] })
  }
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
  async hasSuccessfulAction(workId: string, requestVersion: number, actions: readonly string[]): Promise<boolean> {
    const allowed = new Set(actions)
    if (!allowed.size) return false
    for (const [key, intent] of this.intentDetails) {
      if (intent.workId !== workId || intent.requestVersion !== requestVersion || !allowed.has(intent.action.action)) continue
      const result = this.effectiveResult(key)
      if (result?.ok === true && result.executionState !== 'unknown' && result.approval === undefined) return true
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
    if (existing && !existing.approval) return structuredClone(existing)
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
  private readonly budgets = new Map<string, ModelBudgetLimits & { calls: Map<string, [number, number]>; recorded: Set<string>; tokens: number; costMicros: number }>()

  async reserve(rootWorkId: string, callId: string, limits: ModelBudgetLimits): Promise<ModelBudgetReservation> {
    let budget = this.budgets.get(rootWorkId)
    if (!budget) {
      budget = { ...limits, calls: new Map(), recorded: new Set(), tokens: 0, costMicros: 0 }
      this.budgets.set(rootWorkId, budget)
    }
    const existing = budget.calls.has(callId)
    const allowed = existing || (Date.now() < Date.parse(budget.deadlineAt)
      && budget.calls.size < budget.maxModelCalls && budget.tokens + (limits.reservedTokens ?? 0) <= budget.maxTokens && budget.costMicros + (limits.reservedCostMicros ?? 0) <= budget.maxCostMicros)
    if (allowed && !existing) {
      budget.calls.set(callId, [limits.reservedTokens ?? 0, limits.reservedCostMicros ?? 0])
      budget.tokens += limits.reservedTokens ?? 0
      budget.costMicros += limits.reservedCostMicros ?? 0
    }
    return { allowed, remainingCalls: Math.max(0, budget.maxModelCalls - budget.calls.size),
      remainingTokens: Math.max(0, budget.maxTokens - budget.tokens),
      remainingCostMicros: Math.max(0, budget.maxCostMicros - budget.costMicros), deadlineAt: budget.deadlineAt }
  }

  async record(rootWorkId: string, callId: string, inputTokens: number, outputTokens: number, costMicros: number): Promise<void> {
    const budget = this.budgets.get(rootWorkId)
    if (!budget || !budget.calls.has(callId)) throw new Error('model call budget reservation is missing')
    if (budget.recorded.has(callId)) return
    budget.recorded.add(callId)
    budget.tokens += inputTokens + outputTokens - budget.calls.get(callId)![0]
    budget.costMicros += costMicros - budget.calls.get(callId)![1]
  }
}
