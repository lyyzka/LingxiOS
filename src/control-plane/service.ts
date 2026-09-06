import { appendResearchEvidence } from '../context/research-evidence.js'
import { appendResourceCheck } from '../context/resource-checks.js'
import { createTaskContract } from '../context/task-contract.js'
import { snapshotAttachments } from '../context/attachments.js'
/**
 * ControlPlaneService — transport-independent control-plane logic.
 *
 * The HTTP layer (`http-server.ts`) is a thin adapter over this class, so the
 * full behavior is unit-testable without sockets. Responsibilities:
 *
 * - lease validation on every worker call;
 * - authoritative capability enforcement on host actions (the kernel-side
 *   allowlist is advisory; this check is the security boundary);
 * - the host-action idempotency ledger;
 * - run-event envelope validation, attempt-range fencing, and dedupe;
 * - stream-integrity verification of final assistant messages.
 */
import { createHash } from 'node:crypto'
import { isDeepStrictEqual } from 'node:util'
import { snapshotEvidence } from '../context/evidence.js'
import { createResponseEnvelope, snapshotArtifacts } from '../outcome/envelope.js'
import { parseFinalCandidate } from '../outcome/assessment.js'
import type { KernelArtifact } from '../protocol/types.js'
import { errorMessage } from '../errors.js'
import { nullLogger, type Logger } from '../logging.js'
import type { MetricsRegistry } from '../metrics.js'
import { RUN_SEQUENCE_SPAN } from '../protocol/constants.js'
import type {
  AssistantMessage, HeartbeatResult, HostAction, HostActionResult,
  RunEvent, SessionRecord, TurnContext, WorkCompletion, WorkItem,
} from '../protocol/types.js'
import { sessionKeyOf, actionKeyOf } from '../protocol/types.js'
import { isGoalOutcome, type GoalOutcome } from '../protocol/outcome.js'
import type {
  ActionExecutor, ActionLedgerStore, ActionResolution, ArtifactStager, CapabilityResolver, ContextProvider,
  DeliveryPort, EventStore, ModelBudgetLimits, ModelBudgetStore, SessionStore, WorkStore,
} from './stores.js'
import { isModelItem } from './stores.js'

export interface ControlPlaneDeps {
  lecture?: (work: WorkItem, command: import('../lecture-deck/transport.js').LectureCommand) => Promise<unknown>
  work: WorkStore
  sessions: SessionStore
  events: EventStore
  actions: ActionLedgerStore
  modelBudgets?: ModelBudgetStore
  contextProvider: ContextProvider
  actionExecutor: ActionExecutor
  capabilityResolver: CapabilityResolver
  delivery: DeliveryPort
  artifactStager?: ArtifactStager
  logger?: Logger
  metrics?: MetricsRegistry
}

export class ControlPlaneError extends Error {
  constructor(readonly status: number, message: string, readonly code?: string) {
    super(message)
    this.name = 'ControlPlaneError'
  }
}

export interface LeaseProof {
  id: string
  fence: number
  leaseToken: string
}

function hashToken(token: string): string {
  return createHash('sha256').update(token).digest('hex')
}

const RUN_STAGES = new Set(['started', 'delta', 'completed', 'failed', 'cancelled'])

function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`
  if (value && typeof value === 'object') {
    const record = value as Record<string, unknown>
    return `{${Object.keys(record).sort().map((key) => `${JSON.stringify(key)}:${canonicalJson(record[key])}`).join(',')}}`
  }
  return JSON.stringify(value) ?? 'null'
}

export function actionFingerprint(work: Pick<WorkItem, 'tenantId' | 'principalId' | 'agentId' | 'sessionId'>, action: Pick<HostAction, 'action' | 'args'>): string {
  return createHash('sha256').update(canonicalJson({
    tenantId: work.tenantId, principalId: work.principalId ?? null, agentId: work.agentId,
    sessionId: work.sessionId, action: action.action, args: action.args,
  })).digest('hex')
}

export class ControlPlaneService {
  private readonly logger: Logger

  constructor(private readonly deps: ControlPlaneDeps) {
    this.logger = deps.logger ?? nullLogger
  }

  async lecture(proof: LeaseProof, command: import('../lecture-deck/transport.js').LectureCommand): Promise<unknown> {
    const work = await this.requireLease(proof, { rejectCancelled: true })
    if (!this.deps.lecture) throw new ControlPlaneError(501, 'lecture capability is not configured')
    return this.deps.lecture({ ...work, leaseToken: proof.leaseToken }, command)
  }

  async reserveModelCall(proof: LeaseProof, callId: string, limits: ModelBudgetLimits) {
    const work = await this.requireLease(proof, { rejectCancelled: true })
    if (!this.deps.modelBudgets) throw new ControlPlaneError(501, 'durable model budgets are unavailable')
    const rootWorkId = typeof work.meta?.['rootWorkId'] === 'string' ? work.meta['rootWorkId'] : work.id
    if (!await this.deps.work.ownsBudgetRoot(work, rootWorkId)) throw new ControlPlaneError(409, 'model budget root is outside this work lineage')
    if (!callId || callId.length > 256 || !Number.isSafeInteger(limits?.maxModelCalls) || limits.maxModelCalls < 1
      || !Number.isSafeInteger(limits.maxTokens) || limits.maxTokens < 1
      || !Number.isSafeInteger(limits.maxCostMicros) || limits.maxCostMicros < 1
      || !Number.isFinite(Date.parse(limits.deadlineAt))) throw new ControlPlaneError(400, 'invalid model budget reservation')
    if ([limits.reservedTokens ?? 0, limits.reservedCostMicros ?? 0].some(value => !Number.isSafeInteger(value) || value < 0)) throw new ControlPlaneError(400, 'invalid reserved model resources')
    return this.deps.modelBudgets.reserve(rootWorkId, callId, limits)
  }

  async recordModelUsage(proof: LeaseProof, callId: string, usage: { inputTokens: number; outputTokens: number; costMicros: number }): Promise<void> {
    const work = await this.requireLease(proof, { rejectCancelled: true })
    if (!this.deps.modelBudgets) throw new ControlPlaneError(501, 'durable model budgets are unavailable')
    const rootWorkId = typeof work.meta?.['rootWorkId'] === 'string' ? work.meta['rootWorkId'] : work.id
    if (!await this.deps.work.ownsBudgetRoot(work, rootWorkId)) throw new ControlPlaneError(409, 'model budget root is outside this work lineage')
    if (!callId || callId.length > 256 || [usage?.inputTokens, usage?.outputTokens, usage?.costMicros]
      .some(value => !Number.isSafeInteger(value) || value < 0)) throw new ControlPlaneError(400, 'invalid model usage')
    await this.deps.modelBudgets.record(rootWorkId, callId, usage.inputTokens, usage.outputTokens, usage.costMicros)
  }

  /** Internal/operator API: append authoritative evidence settling an uncertain action. */
  async resolveAction(resolution: ActionResolution, scope: {
    tenantId: string; agentId: string; sessionId: string; principalId: string; threadId?: string
  }): Promise<'recorded' | 'existing'> {
    if (!resolution || typeof resolution !== 'object'
      || !/^[A-Za-z0-9._:-]{1,160}$/.test(resolution.id)
      || typeof resolution.actionKey !== 'string' || !resolution.actionKey
      || typeof resolution.resolvedBy !== 'string' || !resolution.resolvedBy.trim()
      || !resolution.evidence || typeof resolution.evidence !== 'object' || Array.isArray(resolution.evidence)
      || !resolution.result || typeof resolution.result.ok !== 'boolean'
      || resolution.result.executionState === 'unknown'
      || JSON.stringify(resolution.evidence).length > 65_536) {
      throw new ControlPlaneError(400, 'invalid action reconciliation resolution')
    }
    const intent = await this.deps.actions.findIntent(resolution.actionKey)
    if (!intent) {
      throw new ControlPlaneError(404, 'action intent not found')
    }
    if (!scope?.principalId?.trim() || intent.tenantId !== scope.tenantId || intent.agentId !== scope.agentId
      || intent.sessionId !== scope.sessionId || intent.principalId !== scope.principalId
      || intent.threadId !== (scope.threadId ?? null)) {
      throw new ControlPlaneError(404, 'action intent not found')
    }
    return this.deps.actions.recordResolution(structuredClone(resolution))
  }

  // -------------------------------------------------------------------------
  // Work lifecycle
  // -------------------------------------------------------------------------

  async enqueue(input: Parameters<WorkStore['enqueue']>[0]): Promise<{ id: string; deduplicated: boolean }> {
    for (const [field, value] of Object.entries({
      tenantId: input.tenantId, agentId: input.agentId, sessionId: input.sessionId,
      kind: input.kind, lane: input.lane, triggerRef: input.triggerRef,
    })) {
      if (typeof value !== 'string' || !value.trim()) {
        throw new ControlPlaneError(400, `enqueue requires a non-empty ${field}`)
      }
    }
    const result = await this.deps.work.enqueue(input)
    this.deps.metrics?.counter('agentos_work_enqueued_total', 'Work items enqueued').inc({ kind: input.kind, lane: input.lane })
    return result
  }

  async claim(workerId: string, requestId?: string, workKinds?: readonly string[]): Promise<WorkItem | null> {
    if (typeof workerId !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(workerId)) {
      throw new ControlPlaneError(400, 'workerId must be 1-128 safe identifier characters')
    }
    if (requestId !== undefined && !/^[A-Za-z0-9_-]{16,128}$/.test(requestId)) {
      throw new ControlPlaneError(400, 'requestId must be a 16-128 character identifier')
    }
    if (workKinds && (workKinds.length < 1 || workKinds.length > 64 || workKinds.some(kind => typeof kind !== 'string' || !/^[a-z][a-z0-9_]{0,63}$/.test(kind)))) throw new ControlPlaneError(400, 'invalid worker task types')
    const work = await this.deps.work.claim(workerId, requestId, workKinds)
    if (work) {
      this.deps.metrics?.counter('agentos_work_claimed_total', 'Work items claimed').inc({ lane: work.lane })
    }
    return work
  }

  private async requireLease(proof: LeaseProof, options: { rejectCancelled?: boolean } = {}): Promise<Omit<WorkItem, 'leaseToken'>> {
    if (!proof.id || !Number.isSafeInteger(proof.fence) || !proof.leaseToken) {
      throw new ControlPlaneError(400, 'work lease proof required')
    }
    const leased = await this.deps.work.getLeased(proof.id, proof.fence, hashToken(proof.leaseToken))
    if (!leased) throw new ControlPlaneError(409, 'work lease lost or expired', 'lease_lost')
    if (options.rejectCancelled && leased.cancelRequested) {
      throw new ControlPlaneError(409, 'work is cancelled; no further actions are permitted')
    }
    return leased.work
  }

  async heartbeat(proof: LeaseProof): Promise<HeartbeatResult> {
    const row = await this.deps.work.heartbeat(proof.id, proof.fence, hashToken(proof.leaseToken))
    if (!row) return { ok: false }
    return {
      ok: true,
      cancelRequested: row.cancelRequested,
      preemptRequested: row.preemptRequested,
      steer: row.steer,
    }
  }

  async yieldWork(proof: LeaseProof): Promise<void> {
    const ok = await this.deps.work.yieldWork(proof.id, proof.fence, hashToken(proof.leaseToken))
    if (!ok) {
      await this.requireLease(proof)
      throw new ControlPlaneError(409, 'work is no longer yieldable', 'work_state_conflict')
    }
  }

  async complete(proof: LeaseProof, completion: WorkCompletion): Promise<void> {
    if (completion.goalOutcome !== undefined && !isGoalOutcome(completion.goalOutcome)) {
      throw new ControlPlaneError(400, 'invalid goal outcome')
    }
    if (completion.goalOutcome?.verification === 'passed') {
      throw new ControlPlaneError(409, 'goal verification requires authoritative acceptance evidence')
    }
    if (!['completed', 'failed', 'cancelled'].includes(completion.status)) {
      throw new ControlPlaneError(400, 'invalid completion status')
    }
    const work = await this.requireLease(proof)
    if (completion.status === 'completed' && typeof work.meta?.['text'] === 'string' && !completion.goalOutcome) {
      throw new ControlPlaneError(400, 'goal outcome is required for a captured request')
    }
    if (completion.goalOutcome) {
      const request = (await this.getSession(proof, sessionKeyOf(work)))?.request
      const version = request?.workId === work.id ? request.revisions.length + 1 : 1
      if (completion.goalOutcome.requestVersion !== version) throw new ControlPlaneError(409, 'goal outcome request version mismatch')
      await this.validateWait(work, completion.goalOutcome)
      if (completion.goalOutcome.status === 'satisfied' || completion.goalOutcome.status === 'delegated') {
        const committed = await this.deps.delivery.getMessage?.(work)
        if (!committed?.envelope.assessment || !isDeepStrictEqual(committed.envelope.goalOutcome, completion.goalOutcome)
          || committed.body !== completion.resultText) throw new ControlPlaneError(409, 'goal outcome requires the committed assessed response')
      }
    }
    const ok = await this.deps.work.complete(proof.id, proof.fence, hashToken(proof.leaseToken), completion)
    if (!ok) {
      await this.requireLease(proof)
      throw new ControlPlaneError(409, 'work cannot complete in its current state', 'work_state_conflict')
    }
    this.deps.metrics?.counter('agentos_work_completed_total', 'Work attempts finished').inc({ status: completion.status })
  }

  async requestCancel(id: string): Promise<boolean> { return this.deps.work.requestCancel(id) }

  private async validateWait(work: Omit<WorkItem, 'leaseToken'>, outcome: GoalOutcome) {
    if (outcome.status === 'awaiting_approval') {
      if (!await this.deps.actions.hasWait(work.id, outcome.requestVersion, { approvalId: outcome.approvalId })) {
        throw new ControlPlaneError(409, 'approval wait requires its current durable pending receipt')
      }
    } else if (outcome.status === 'awaiting_input') {
      if (!outcome.question || !await this.deps.actions.hasWait(work.id, outcome.requestVersion, { question: outcome.question })) {
        throw new ControlPlaneError(409, 'input wait requires its current durable question receipt')
      }
    }
  }
  async requestPreempt(id: string): Promise<boolean> { return this.deps.work.requestPreempt(id) }
  async addSteer(id: string, text: string): Promise<boolean> {
    if (typeof text !== 'string' || !text.trim() || text.length > 8_000) {
      throw new ControlPlaneError(400, 'steer text must be a non-empty string of at most 8000 characters')
    }
    return this.deps.work.addSteer(id, text)
  }

  // -------------------------------------------------------------------------
  // Context
  // -------------------------------------------------------------------------

  async loadContext(proof: LeaseProof): Promise<TurnContext> {
    const work = await this.requireLease(proof)
    const context = await this.deps.contextProvider.loadContext(work)
    if (typeof work.meta?.['text'] === 'string') {
      context.capabilities = [...new Set([...context.capabilities, 'task'])]
      if (context.promptContextCandidate) context.promptContextCandidate = { ...context.promptContextCandidate, capabilities: context.capabilities }
    }
    const priorArtifacts = new Map<string, KernelArtifact>()
    if (work.fence > 1) {
      const events = await this.deps.events.listRange(work.id, 0, (work.fence - 1) * RUN_SEQUENCE_SPAN, ['ipython.completed'])
      for (const event of events) {
        for (const artifact of snapshotArtifacts(event.data['artifacts'] as KernelArtifact[])) {
          priorArtifacts.set(artifact.path, artifact)
          if (priorArtifacts.size > 512) throw new ControlPlaneError(409, 'prior artifact inventory exceeds the per-run limit')
        }
      }
    }
    return { work: { ...work, leaseToken: proof.leaseToken }, ...context, priorArtifacts: [...priorArtifacts.values()] }
  }

  // -------------------------------------------------------------------------
  // Host actions: grant enforcement + idempotency ledger
  // -------------------------------------------------------------------------

  async executeAction(proof: LeaseProof, action: HostAction): Promise<HostActionResult> {
    const work = await this.requireLease(proof, { rejectCancelled: true })
    if (
      !action || typeof action !== 'object'
      || typeof action.action !== 'string'
      || typeof action.idempotencyKey !== 'string'
      || !action.idempotencyKey
      || action.runId !== work.id
      || typeof action.cellId !== 'string' || !action.cellId
      || !Number.isSafeInteger(action.callIndex) || action.callIndex < 0
      || action.idempotencyKey !== actionKeyOf(action)
      || !action.args || typeof action.args !== 'object' || Array.isArray(action.args)
    ) {
      throw new ControlPlaneError(400, 'invalid host action envelope')
    }
    const [namespace, method, ...rest] = action.action.split('.')
    if (!namespace || !method || rest.length > 0) {
      throw new ControlPlaneError(400, 'host action must be <namespace>.<method>')
    }

    // Authoritative capability check. The kernel-side allowlist only shapes
    // what the model can conveniently express; this is the boundary.
    const grants = await this.deps.capabilityResolver.resolve(work)
    const grant = namespace === 'task' && typeof work.meta?.['text'] === 'string' ? grants.find(candidate => candidate.name === 'task') ?? { name: 'task', methods: ['contract', 'ask', 'check_receipt', 'check_resource', 'inspect'] } : grants.find((candidate) => candidate.name === namespace)
    if (!grant || (grant.methods && !grant.methods.includes(method))) {
      this.deps.metrics?.counter('agentos_actions_denied_total', 'Host actions denied by grant').inc({ namespace })
      return { ok: false, error: `capability denied: ${action.action} is not granted to this work item` }
    }
    if (action.action === 'task.check_receipt' || action.action === 'task.check_resource') {
      const target = action.args['action']
      const [targetNamespace, targetMethod, ...extra] = typeof target === 'string' ? target.split('.') : []
      const targetGrant = grants.find(candidate => candidate.name === targetNamespace)
      if (!targetMethod || extra.length || targetNamespace === 'task' || !targetGrant
        || (targetGrant.methods && !targetGrant.methods.includes(targetMethod))) {
        return { ok: false, error: 'capability denied: receipt action is not granted to this work item' }
      }
    }

    let requestVersion: number | null = null
    if (typeof work.meta?.['text'] === 'string') {
      const request = (await this.getSession(proof, sessionKeyOf(work)))?.request
      const current = await this.heartbeat(proof)
      if (!current.ok) {
        await this.requireLease(proof)
        throw new ControlPlaneError(409, 'work lease lost before action', 'lease_lost')
      }
      if (current.cancelRequested) throw new ControlPlaneError(409, 'work is cancelled; no further actions are permitted', 'work_cancelled')
      if (!request || request.workId !== work.id || !isDeepStrictEqual(request.revisions, current.steer ?? [])) {
        return { ok: false, error: 'request snapshot is stale or missing; process the latest user revisions before acting' }
      }
      requestVersion = request.revisions.length + 1
    }

    const fingerprint = actionFingerprint(work, action)
    const reservation = await this.deps.actions.reserve(action.idempotencyKey, fingerprint, {
      workId: work.id, tenantId: work.tenantId, principalId: work.principalId ?? null, agentId: work.agentId,
      sessionId: work.sessionId, threadId: work.threadId ?? null, requestVersion, action: structuredClone(action),
    })
    const replayed = await this.deps.actions.find(action.idempotencyKey)
    if (replayed) {
      this.deps.metrics?.counter('agentos_actions_replayed_total', 'Host actions served from the ledger').inc({ namespace })
      return replayed
    }
    if (reservation === 'existing') {
      return { ok: false, executionState: 'unknown', error: 'action intent exists without a receipt; reconcile before retrying' }
    }
    let result: HostActionResult
    try {
      if (action.action === 'task.inspect') {
        if (Object.keys(action.args).length) throw new Error('task.inspect accepts no arguments')
        const pending = await this.deps.actions.unsettled(work.id)
        result = { ok: true, value: { requestVersion, pending: pending.slice(0, 64), truncated: pending.length > 64 } }
      } else if (action.action === 'task.contract') {
        const request = (await this.getSession(proof, sessionKeyOf(work)))?.request
        if (!request || request.workId !== work.id) throw new Error('task contract requires the current request snapshot')
        const contract = createTaskContract(request.originalText, request.revisions.length + 1, action.args)
        result = { ok: true, value: { status: 'draft_validated', requestVersion: contract.requestVersion }, directive: { type: 'task_contract', data: { ...contract } } }
      } else if (action.action === 'task.check_receipt') {
        const { idempotencyKey, action: expectedAction, expected } = action.args
        if (Object.keys(action.args).length !== 3 || typeof idempotencyKey !== 'string' || !idempotencyKey
          || typeof expectedAction !== 'string' || !expectedAction || expectedAction.startsWith('task.')
          || expected === undefined) throw new Error('task.check_receipt requires idempotencyKey, a business action and its complete expected value')
        const intent = await this.deps.actions.findIntent(idempotencyKey)
        if (!intent || intent.workId !== work.id || intent.tenantId !== work.tenantId
          || intent.principalId !== (work.principalId ?? null) || intent.agentId !== work.agentId
          || intent.sessionId !== work.sessionId || intent.threadId !== (work.threadId ?? null)
          || intent.requestVersion !== requestVersion || intent.action.action !== expectedAction) {
          throw new Error('receipt is unavailable for this request version and action')
        }
        const receipt = await this.deps.actions.find(idempotencyKey)
        const observed = receipt?.ok === true && receipt.executionState !== 'unknown'
          && receipt.directive === undefined && receipt.value !== undefined
        result = { ok: true, value: { scope: 'recorded_action_result', requestVersion, idempotencyKey,
          action: expectedAction, status: !observed ? 'not_observed' : isDeepStrictEqual(receipt.value, expected) ? 'pass' : 'fail',
          ...(observed ? { observed: receipt.value } : {}),
          limitation: 'This checks a recorded action result, not current resource state or overall goal completion.' } }
      } else if (action.action === 'task.check_resource') {
        const { action: readAction, args, expected } = action.args
        if (!this.deps.actionExecutor.readResource) throw new Error('resource readback is unavailable')
        if (Object.keys(action.args).length !== 3 || typeof readAction !== 'string'
          || !args || typeof args !== 'object' || Array.isArray(args)
          || !expected || typeof expected !== 'object' || Array.isArray(expected)) {
          throw new Error('task.check_resource requires a read action, args and expected fields')
        }
        const fields = Object.entries(expected)
        if (!fields.length || fields.length > 16 || JSON.stringify(expected).length > 16_384
          || fields.some(([key]) => !key || key.length > 256 || ['__proto__', 'prototype', 'constructor'].includes(key))) {
          throw new Error('expected must contain 1-16 resource fields within 16384 characters')
        }
        const resource = await this.deps.actionExecutor.readResource(work,
          { ...action, action: readAction, args: args as Record<string, unknown> })
        const observed = resource && typeof resource === 'object' && !Array.isArray(resource)
          && fields.every(([key]) => Object.hasOwn(resource, key))
          ? Object.fromEntries(fields.map(([key]) => [key, (resource as Record<string, unknown>)[key]])) : undefined
        if (observed && JSON.stringify(observed).length > 65_536) throw new Error('observed resource fields exceed the 65536 character limit')
        result = { ok: true, value: { scope: 'observed_resource_fields', requestVersion, action: readAction, args, expected,
          observedAt: new Date().toISOString(), status: observed === undefined ? 'not_observed'
            : fields.every(([key, value]) => isDeepStrictEqual(observed[key], value)) ? 'pass' : 'fail',
          ...(observed ? { observed } : {}),
          limitation: 'Only these fields at observation time were checked. Replaying this receipt does not refresh it; absence does not prove deletion or overall goal completion.' } }
      } else if (action.action === 'task.ask') {
        const question = action.args['question']
        if (Object.keys(action.args).length !== 1 || typeof question !== 'string' || !question.trim() || question.length > 4_000) throw new Error('task.ask requires one non-empty question of at most 4000 characters')
        result = { ok: true, value: { question }, directive: { type: 'defer', reason: 'user', data: { question } } }
      } else result = await this.deps.actionExecutor.execute(work, action)
    } catch (error) {
      result = { ok: false, ...(namespace === 'task' ? {} : { executionState: 'unknown' as const }), error: errorMessage(error) }
    }
    const recorded = await this.deps.actions.record(action.idempotencyKey, result)
    this.deps.metrics?.counter('agentos_actions_executed_total', 'Host actions executed').inc({
      namespace, ok: String(recorded.ok),
    })
    return recorded
  }

  async recoverCell(proof: LeaseProof, cellId: string): Promise<Array<{
    action: string; idempotencyKey: string; result: HostActionResult
  }> | null> {
    const work = await this.requireLease(proof, { rejectCancelled: true })
    if (!cellId || cellId.length > 512) throw new ControlPlaneError(400, 'invalid cellId')
    const request = typeof work.meta?.['text'] === 'string'
      ? (await this.getSession(proof, sessionKeyOf(work)))?.request : undefined
    const requestVersion = request?.workId === work.id ? request.revisions.length + 1 : null
    const records = await this.deps.actions.listCell(work.id, cellId, requestVersion)
    if (!records.length || records.length > 100) return null
    return records.map(({ intent, result }, index) => {
      if (intent.tenantId !== work.tenantId || intent.principalId !== (work.principalId ?? null)
        || intent.agentId !== work.agentId || intent.sessionId !== work.sessionId
        || intent.threadId !== (work.threadId ?? null) || intent.action.callIndex !== index) {
        throw new ControlPlaneError(409, 'cell action history is inconsistent', 'reconciliation_conflict')
      }
      return {
        action: intent.action.action,
        idempotencyKey: intent.action.idempotencyKey,
        result: result ?? { ok: false, executionState: 'unknown', error: 'action intent has no receipt; reconciliation required' },
      }
    })
  }

  async recoverStep(proof: LeaseProof, cellId: string): Promise<{ output: string; artifacts: KernelArtifact[] } | null> {
    const work = await this.requireLease(proof, { rejectCancelled: true })
    if (!cellId || cellId.length > 512) throw new ControlPlaneError(400, 'invalid cellId')
    const request = (await this.getSession(proof, sessionKeyOf(work)))?.request
    const requestVersion = request?.workId === work.id ? request.revisions.length + 1 : null
    if (requestVersion === null || work.fence <= 1) return null
    const events = await this.deps.events.listRange(work.id, 0, (work.fence - 1) * RUN_SEQUENCE_SPAN, ['ipython.completed'])
    const matches = events.filter(event => event.data['callId'] === cellId && event.data['requestVersion'] === requestVersion)
    if (matches.length !== 1) return null
    const output = matches[0]!.data['output'], artifacts = matches[0]!.data['artifacts']
    if (typeof output !== 'string' || !Array.isArray(artifacts)) return null
    return { output, artifacts: snapshotArtifacts(artifacts as KernelArtifact[]) }
  }

  async stageArtifact(proof: LeaseProof, artifact: KernelArtifact, contentBase64: string): Promise<void> {
    const work = await this.requireLease(proof, { rejectCancelled: true })
    if (!this.deps.artifactStager) throw new ControlPlaneError(501, 'artifact upload is unavailable')
    let checked: KernelArtifact
    try { checked = snapshotArtifacts([artifact])[0]! }
    catch { throw new ControlPlaneError(400, 'invalid artifact metadata') }
    if (typeof contentBase64 !== 'string' || contentBase64.length > 22_369_624) {
      throw new ControlPlaneError(413, 'artifact upload exceeds 16 MiB')
    }
    const bytes = Buffer.from(contentBase64, 'base64')
    if (bytes.length !== checked.size || bytes.length > 16 * 1024 * 1024
      || bytes.toString('base64') !== contentBase64
      || createHash('sha256').update(bytes).digest('hex') !== checked.sha256.toLowerCase()) {
      throw new ControlPlaneError(409, 'artifact content does not match its metadata', 'artifact_mismatch')
    }
    await this.deps.artifactStager.stage(work, checked, bytes)
  }

  // -------------------------------------------------------------------------
  // Run events
  // -------------------------------------------------------------------------

  async recordEvent(proof: LeaseProof, event: RunEvent): Promise<void> {
    const work = await this.requireLease(proof)
    if (
      !event || typeof event !== 'object'
      || event.runId !== work.id
      || !Number.isSafeInteger(event.seq)
      || typeof event.kind !== 'string' || !event.kind || event.kind.length > 160
      || !RUN_STAGES.has(event.stage)
      || (event.visibility !== 'user' && event.visibility !== 'internal')
      || !event.data || typeof event.data !== 'object' || Array.isArray(event.data)
    ) {
      throw new ControlPlaneError(400, 'invalid run event envelope')
    }
    // Attempt-range fencing: an event must land inside its own attempt's
    // sequence range so retried attempts can never shadow one another.
    const rangeStart = Math.max(0, work.fence - 1) * RUN_SEQUENCE_SPAN
    const rangeEnd = work.fence * RUN_SEQUENCE_SPAN
    if (event.seq <= rangeStart || event.seq > rangeEnd) {
      throw new ControlPlaneError(400, `event seq ${event.seq} is outside this attempt's range (${rangeStart}, ${rangeEnd}]`)
    }
    if (event.kind === 'ipython.completed') {
      try { snapshotArtifacts(event.data['artifacts'] as KernelArtifact[]) }
      catch { throw new ControlPlaneError(400, 'invalid kernel artifact event') }
    }
    const inserted = await this.deps.events.append({
      ...event,
      tenantId: work.tenantId,
      agentId: work.agentId,
      recordedAt: new Date().toISOString(),
    }, { workId: work.id, fence: proof.fence, leaseTokenHash: hashToken(proof.leaseToken) })
    if (!inserted) {
      await this.requireLease(proof)
      return // duplicate delivery of an already-recorded event
    }
    this.deps.metrics?.counter('agentos_events_recorded_total', 'Run events recorded').inc({ kind: event.kind })
    try {
      await this.deps.delivery.onEvent(work, event)
    } catch (error) {
      // Delivery is best-effort fan-out; the durable ledger already has the
      // event, so a delivery hiccup must not fail the worker's call.
      this.logger.warn('event delivery failed', { runId: event.runId, seq: event.seq, error: errorMessage(error) })
    }
  }

  // -------------------------------------------------------------------------
  // Final messages: stream-integrity verification
  // -------------------------------------------------------------------------

  async commitMessage(proof: LeaseProof, message: AssistantMessage): Promise<void> {
    const work = await this.requireLease(proof, { rejectCancelled: true })
    if (
      !message || typeof message !== 'object'
      || message.version !== 2
      || 'data' in message
      || message.runId !== work.id
      || message.agentId !== work.agentId
      || message.sessionId !== work.sessionId
      || message.threadId !== work.threadId
      || typeof message.body !== 'string' || !message.body.trim()
    ) {
      throw new ControlPlaneError(409, 'assistant message is missing its stream identity or body')
    }
    if (!message.envelope) {
      throw new ControlPlaneError(409, 'response envelope is required')
    }
    const rangeStart = Math.max(0, work.fence - 1) * RUN_SEQUENCE_SPAN
    const rangeEnd = work.fence * RUN_SEQUENCE_SPAN
    const streamEvents = await this.deps.events.listRange(
      work.id, rangeStart, rangeEnd, ['model.delta', 'model.completed', 'ipython.completed', 'response.assessed'],
    )
    const streamed = streamEvents
      .filter((event) => event.kind === 'model.delta'
        && event.data['partType'] === 'text' && typeof event.data['delta'] === 'string')
      .map((event) => String(event.data['delta']))
      .join('')
      .trim()
    const completedTurns = streamEvents.filter((event) => event.kind === 'model.completed')
    if (!streamed || completedTurns.length === 0 || streamed !== message.body.trim()) {
      throw new ControlPlaneError(409, 'assistant final message does not match its durably streamed deltas')
    }
    if (message.envelope.goalOutcome?.verification === 'passed') {
      throw new ControlPlaneError(409, 'goal verification requires authoritative acceptance evidence')
    }
    const session = await this.getSession(proof, sessionKeyOf(work))
    if (!session?.request?.evidence) throw new ControlPlaneError(409, 'response requires a saved request and evidence snapshot')
    if (session.request.workId !== work.id) throw new ControlPlaneError(409, 'response request belongs to another work item')
    const evidence = session.request.evidence
    const artifacts = streamEvents.filter((event) => event.kind === 'ipython.completed')
      .flatMap((event) => Array.isArray(event.data['artifacts']) ? event.data['artifacts'] as KernelArtifact[] : [])
    if (message.envelope.requestVersion !== session.request.revisions.length + 1) {
      throw new ControlPlaneError(409, 'response request version is stale')
    }
    if (message.envelope.goalOutcome.status === 'delegated'
      && !await this.deps.work.hasPendingChild(work, message.envelope.goalOutcome.taskRef, message.envelope.requestVersion)) {
      throw new ControlPlaneError(409, 'delegated task is not pending for this request')
    }
    await this.validateWait(work, message.envelope.goalOutcome)
    let expected
    try {
      const assessment = message.envelope.assessment
      if (assessment) {
        parseFinalCandidate(JSON.stringify({ body: message.body, ...assessment }), session.request)
        if (!streamEvents.some(event => event.kind === 'response.assessed' && event.data['body'] === message.body
          && isDeepStrictEqual(event.data['assessment'], assessment)
          && isDeepStrictEqual(event.data['goalOutcome'], message.envelope.goalOutcome))) throw new Error('missing durable assessment')
      }
      if (message.envelope.goalOutcome.status === 'delegated' && (assessment?.status !== 'delegated'
        || assessment.taskRef !== message.envelope.goalOutcome.taskRef)) throw new Error('missing delegated self-assessment')
      if (message.envelope.goalOutcome.status === 'satisfied') {
        if (assessment?.status !== 'satisfied') throw new Error('missing satisfied self-assessment')
        if ((await this.deps.actions.unsettled(work.id)).length) throw new Error('business actions remain unresolved')
        const latest = new Map<string, unknown>()
        for (const record of session.request.resourceChecks ?? []) {
          const value = record.result.value as Record<string, unknown>
          if (value['requestVersion'] === message.envelope.requestVersion) {
            latest.set(JSON.stringify([value['action'], value['args'], value['expected']]), value['status'])
          }
        }
        if ([...latest.values()].some(status => status !== 'pass')) throw new Error('known resource postconditions remain unresolved')
      }
      expected = createResponseEnvelope(message.body, message.envelope.goalOutcome, evidence, artifacts, session.request.contract, session.request.resourceChecks, assessment)
    } catch {
      throw new ControlPlaneError(409, 'response does not match its evidence or artifact records')
    }
    if (!isDeepStrictEqual(expected, message.envelope)) {
      throw new ControlPlaneError(409, 'response envelope is inconsistent with its durable records')
    }
    const current = await this.heartbeat(proof)
    if (!current.ok) {
      await this.requireLease(proof)
      throw new ControlPlaneError(409, 'work lease lost before delivery', 'lease_lost')
    }
    if (current.cancelRequested) throw new ControlPlaneError(409, 'work is cancelled; response cannot be delivered', 'work_cancelled')
    if (message.envelope.requestVersion !== (current.steer?.length ?? 0) + 1) throw new ControlPlaneError(409, 'response request version is stale')
    await this.deps.delivery.deliverMessage(work, message)
    this.deps.metrics?.counter('agentos_messages_delivered_total', 'Final assistant messages delivered').inc()
  }

  // -------------------------------------------------------------------------
  // Sessions
  // -------------------------------------------------------------------------

  async getSession(proof: LeaseProof, key: string): Promise<SessionRecord | null> {
    const work = await this.requireLease(proof)
    if (key !== sessionKeyOf(work)) throw new ControlPlaneError(403, 'session is outside this work lease')
    const session = await this.deps.sessions.get(key, work.id)
    if (session && (session.tenantId !== work.tenantId || session.agentId !== work.agentId
      || session.sessionId !== work.sessionId || session.threadId !== work.threadId)) {
      throw new ControlPlaneError(409, 'stored session identity mismatch')
    }
    return session
  }

  async saveSession(proof: LeaseProof, session: SessionRecord): Promise<{ revision: number }> {
    const work = await this.requireLease(proof)
    const expectedKey = sessionKeyOf(work)
    if (
      !session || typeof session !== 'object'
      || session.key !== expectedKey
      || session.tenantId !== work.tenantId || session.agentId !== work.agentId
      || session.sessionId !== work.sessionId || session.threadId !== work.threadId
      || !Array.isArray(session.history)
      || !session.history.every(isModelItem)
      || !Number.isSafeInteger(session.revision) || session.revision < 0
      || !Number.isSafeInteger(session.compactionEpoch) || session.compactionEpoch < 0
      || !Array.isArray(session.appliedWorkIds)
      || !session.appliedWorkIds.every((id) => typeof id === 'string')
      || (typeof work.meta?.['text'] === 'string' && !session.request)
      || (session.request !== undefined && (
        !session.request || session.request.version !== 1 || session.request.workId !== work.id
        || session.request.tenantId !== work.tenantId || session.request.sessionId !== work.sessionId
        || typeof session.request.authorId !== 'string' || typeof session.request.sourceRef !== 'string'
        || session.request.sourceRef !== work.triggerRef
        || (work.principalId !== undefined && session.request.authorId !== work.principalId)
        || (typeof work.meta?.['text'] === 'string' && (work.meta?.['delegation']
          ? session.request.delegatedAssignment !== work.meta['text'] : session.request.originalText !== work.meta['text']))
        || typeof session.request.originalText !== 'string' || !Array.isArray(session.request.revisions)
        || !session.request.revisions.every((item) => item && typeof item.id === 'string'
          && typeof item.text === 'string' && typeof item.createdAt === 'string')
      ))
    ) {
      throw new ControlPlaneError(400, 'invalid session record for this work lease')
    }
    if (session.request?.contract) {
      const contract = session.request.contract
      try {
        const expected = createTaskContract(session.request.originalText, session.request.revisions.length + 1, {
          deliverables: contract.deliverables, constraints: contract.constraints,
          actions: contract.actions, acceptance: contract.acceptance,
        })
        if (!isDeepStrictEqual(contract, expected)) throw new Error('contract provenance mismatch')
      } catch { throw new ControlPlaneError(400, 'invalid request task contract') }
    }
    if (session.request) {
      try {
        if (!isDeepStrictEqual(snapshotAttachments(session.request.attachments), snapshotAttachments(work.meta?.['attachments'] ?? []))) throw new Error('attachment mismatch')
        const attachmentCount = session.request.revisions.reduce((count, revision) => count + snapshotAttachments(revision.attachments ?? []).length,
          session.request.attachments.length)
        if (attachmentCount > 20) throw new Error('request attachment limit reached')
      } catch { throw new ControlPlaneError(400, 'invalid request attachments or mismatch with the original input') }
    }
    if (session.request) {
      try {
        if (!session.request.evidence || session.request.evidence.version !== 1) throw new Error('invalid evidence version')
        snapshotEvidence(session.request.evidence.id, session.request.evidence.items)
      } catch {
        throw new ControlPlaneError(400, 'invalid request evidence snapshot')
      }
    }
    if (session.request?.revisions.length) {
      const current = await this.deps.work.heartbeat(proof.id, proof.fence, hashToken(proof.leaseToken))
      if (!current) throw new ControlPlaneError(409, 'work lease lost before saving revisions')
      if (!session.request.revisions.every((revision, index) => isDeepStrictEqual(revision, current.steer[index]))) {
        throw new ControlPlaneError(400, 'request revisions do not match persisted human steering')
      }
    }
    const previous = (await this.deps.sessions.get(expectedKey, work.id))?.request
    const previousChecks = previous?.workId === work.id ? previous.resourceChecks ?? [] : []
    const nextChecks = session.request?.resourceChecks ?? []
    if (!Array.isArray(nextChecks) || nextChecks.length > 64
      || !isDeepStrictEqual(nextChecks.slice(0, previousChecks.length), previousChecks)) {
      throw new ControlPlaneError(409, 'resource observations cannot be rewritten')
    }
    if (nextChecks.length > previousChecks.length) {
      if (previous?.workId !== work.id) throw new ControlPlaneError(409, 'resource observations require a saved request')
      let expected = previousChecks
      for (const record of nextChecks.slice(previousChecks.length)) {
        const key = record?.actionKey
        const intent = typeof key === 'string' ? await this.deps.actions.findIntent(key) : null
        if (!intent || intent.workId !== work.id || intent.tenantId !== work.tenantId
          || intent.principalId !== (work.principalId ?? null) || intent.agentId !== work.agentId
          || intent.sessionId !== work.sessionId || intent.threadId !== (work.threadId ?? null)
          || intent.requestVersion !== session.request!.revisions.length + 1 || intent.action.action !== 'task.check_resource') {
          throw new ControlPlaneError(409, 'resource observation lacks a current scoped read intent')
        }
        const result = await this.deps.actions.find(key)
        if (!result) throw new ControlPlaneError(409, 'resource observation lacks a recorded result')
        try { expected = appendResourceCheck(expected, key, result, session.request!.revisions.length + 1) }
        catch { throw new ControlPlaneError(409, 'invalid resource observation') }
      }
      if (!isDeepStrictEqual(expected, nextChecks)) throw new ControlPlaneError(409, 'resource observations do not match recorded reads')
    }
    if (previous?.workId === work.id) {
      const next = session.request
      if (!next || next.originalText !== previous.originalText || next.authorId !== previous.authorId
        || next.sourceRef !== previous.sourceRef

        || !previous.revisions.every((revision, index) => isDeepStrictEqual(revision, next.revisions[index]))) {
        throw new ControlPlaneError(409, 'acquired request and evidence snapshots cannot be rewritten')
      }
    }
    if ((previous?.workId !== work.id || !previous.evidence) && session.request?.evidence?.items.some(item => item.actionKey !== undefined)) {
      throw new ControlPlaneError(409, 'research evidence requires a saved initial snapshot')
    }
    if (previous?.workId === work.id && previous.evidence && !isDeepStrictEqual(previous.evidence, session.request?.evidence)) {
      const next = session.request?.evidence
      if (!next || next.items.length <= previous.evidence.items.length
        || !isDeepStrictEqual(next.items.slice(0, previous.evidence.items.length), previous.evidence.items)) throw new ControlPlaneError(409, 'acquired evidence cannot be rewritten')
      let expected = previous.evidence
      for (const item of next.items.slice(previous.evidence.items.length)) {
        const key = item.actionKey
        const intent = key ? await this.deps.actions.findIntent(key) : null
        if (!key || !intent || intent.workId !== work.id || intent.tenantId !== work.tenantId
          || intent.principalId !== (work.principalId ?? null) || intent.agentId !== work.agentId
          || intent.sessionId !== work.sessionId || intent.threadId !== (work.threadId ?? null)
          || intent.requestVersion !== session.request!.revisions.length + 1 || intent.action.action !== 'research.read') throw new ControlPlaneError(409, 'evidence lacks a current research read intent')
        const result = await this.deps.actions.find(key)
        if (!result) throw new ControlPlaneError(409, 'evidence lacks a recorded research read')
        expected = appendResearchEvidence(expected, key, result)
      }
      if (!isDeepStrictEqual(expected, next)) throw new ControlPlaneError(409, 'evidence does not match recorded research reads')
    }
    const saved = await this.deps.sessions.save(session, {
      workId: work.id, fence: proof.fence, leaseTokenHash: hashToken(proof.leaseToken),
    })
    if (!saved.ok) {
      await this.requireLease(proof)
      const current = await this.deps.sessions.get(session.key, work.id)
      if (current && current.revision === session.revision + 1
        && isDeepStrictEqual(current, { ...session, revision: current.revision })) {
        return { revision: current.revision }
      }
      throw new ControlPlaneError(409, 'session revision conflict', 'session_conflict')
    }
    return { revision: saved.revision }
  }
}
