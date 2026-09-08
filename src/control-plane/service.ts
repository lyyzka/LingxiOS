import { abortable } from '../deadline.js'
import { candidateHash, type Candidate, type CandidateVerification } from '../outcome/verification.js'
import { grantedTools, TASK_TOOLS, parseTaskArgs, type ToolDefinition } from '../tools/catalog.js'
import { readRequestAttachment } from '../context/request.js'
import { NoEffectError } from '../tools/definition.js'
import { permitsTool, executionMode } from '../runtime/execution-policy.js'
import { canonicalJson } from '../context/compiler.js'
import { toolContractHash } from '../tools/contracts.js'
import { snapshotObligations } from '../outcome/obligations.js'
import { modelPricing } from '../model/execution.js'
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
import { businessActionDeliveryGap, requiresReview, validateCompletion } from '../outcome/completion.js'
import type { KernelArtifact } from '../protocol/types.js'
import { errorMessage } from '../errors.js'
import { nullLogger, type Logger } from '../logging.js'
import { LATENCY_BUCKETS, type MetricsRegistry } from '../metrics.js'
import { RUN_SEQUENCE_SPAN } from '../protocol/constants.js'
import type {
  AssistantMessage, HeartbeatResult, HostAction, HostActionResult,
  RunEvent, SessionRecord, TurnContext, WorkCompletion, WorkItem,
} from '../protocol/types.js'
import { sessionKeyOf, actionKeyOf } from '../protocol/types.js'
import { isGoalOutcome, isWaitingOutcome, type GoalOutcome, type WaitingOutcome } from '../protocol/outcome.js'
import type {
  ActionExecutor, ActionLedgerStore, ActionResolution, ArtifactStager, CapabilityResolver, ContextProvider,
  DeliveryPort, EventStore, ModelBudgetLimits, ModelBudgetStore, SessionStore, WorkStore,
} from './stores.js'
import { isModelItem } from './stores.js'

export interface ControlPlaneDeps {
  contextSnapshot?: (work: Omit<WorkItem, 'leaseToken'>) => Promise<{
    session: SessionRecord | null; steps: import('./steps.js').ExecutionStep[]; requestVersion: number
  }>
  authorizeWork?: (work: Omit<WorkItem, 'leaseToken'>) => Promise<void>
  memory?: {
    prepareReview(work: Omit<WorkItem,'leaseToken'>,action: HostAction): Promise<import('../memory/types.js').MemoryReviewRequest|null>
    recordReview(work: Omit<WorkItem,'leaseToken'>,action: HostAction,hash: string,review: import('../memory/types.js').MemoryReview): Promise<void>
  }
  modelBudget?: Required<import('../model/execution.js').RootModelBudgetOptions>
  verifyCandidate?: (work: Omit<WorkItem, 'leaseToken'>, candidate: Candidate, signal?: AbortSignal) => Promise<CandidateVerification>
  tools?: readonly ToolDefinition[]
  steps: import('./steps.js').StepStore
  work: WorkStore
  sessions: SessionStore
  events: EventStore
  actions: ActionLedgerStore
  modelBudgets: ModelBudgetStore
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

export function actionFingerprint(work: Pick<WorkItem, 'tenantId' | 'principalId' | 'agentId' | 'sessionId'>, action: Pick<HostAction, 'action' | 'args'>): string {
  return createHash('sha256').update(canonicalJson({
    tenantId: work.tenantId, principalId: work.principalId ?? null, agentId: work.agentId,
    sessionId: work.sessionId, action: action.action, args: action.args,
  })).digest('hex')
}

export class ControlPlaneService {
  private async memoryReviewWork(proof: LeaseProof,action: HostAction) {
    const work=await this.requireLease(proof,{rejectCancelled:true})
    const grants=await this.deps.capabilityResolver.resolve(work)
    const tool=grantedTools(this.deps.tools ?? [],grants).find(item=>item.action===action?.action)
    if (!this.deps.memory || !tool || !permitsTool(work,tool)) throw new ControlPlaneError(403,'memory action is unavailable')
    return work
  }
  async prepareMemoryReview(proof: LeaseProof,action: HostAction) {
    const work=await this.memoryReviewWork(proof,action)
    const intent=await this.deps.actions.findIntent(action.idempotencyKey)
    if (intent && intent.workId===work.id && isDeepStrictEqual(intent.action,action) && await this.deps.actions.find(action.idempotencyKey)) return null
    return this.deps.memory!.prepareReview(work,action)
  }
  async recordMemoryReview(proof: LeaseProof,action: HostAction,hash: string,review: import('../memory/types.js').MemoryReview) {
    const work=await this.memoryReviewWork(proof,action)
    if (typeof hash!=='string' || !/^[a-f0-9]{64}$/.test(hash)) throw new ControlPlaneError(400,'invalid memory review hash')
    await this.deps.memory!.recordReview(work,action,hash,review)
  }
  async verifyCandidate(proof: LeaseProof, candidate: Candidate, signal?: AbortSignal): Promise<CandidateVerification> {
    const work = await this.requireLease(proof, { rejectCancelled: true })
    if (!candidate || typeof candidate.body !== 'string' || candidate.body.length > 100_000
      || !Number.isSafeInteger(candidate.requestVersion) || candidate.requestVersion < 1) throw new ControlPlaneError(400, 'invalid candidate')
    candidate = { ...candidate, artifacts: snapshotArtifacts(candidate.artifacts) }
    const session = await this.getSession(proof, sessionKeyOf(work))
    if (!session?.request || session.request.workId !== work.id || candidate.requestVersion !== session.request.revisions.length + 1) throw new ControlPlaneError(409, 'candidate request version is stale')
    const facts = snapshotArtifacts([...(await this.deps.steps.list(work.id)).flatMap(step => step.artifacts),
      ...await this.deps.actions.artifacts(work.id)])
    if (candidate.artifacts.some(artifact => !facts.some(fact => isDeepStrictEqual(fact, artifact)))) throw new ControlPlaneError(409, 'candidate artifact has no execution record')
    if (!this.deps.verifyCandidate) return { requestVersion: candidate.requestVersion, candidateHash: candidateHash(candidate),
      records: [{ checker: 'availability', status: 'inconclusive', evidence: { reason: 'No authoritative checker is configured' } }] }
    return this.deps.verifyCandidate(work, candidate, signal)
  }

  async saveStep(proof: LeaseProof, step: import('./steps.js').ExecutionStep): Promise<void> {
    const work = await this.requireLease(proof, { rejectCancelled: true })
    if (!step || typeof step.id !== 'string' || !step.id || step.id.length > 512
      || !Number.isSafeInteger(step.requestVersion) || step.requestVersion < 1 || typeof step.kind !== 'string'
      || !step.input || typeof step.input !== 'object' || Array.isArray(step.input)
      || JSON.stringify(step.input).length > 1_000_000 || step.output !== undefined && (typeof step.output !== 'string' || step.output.length > 1_000_000)) throw new ControlPlaneError(400, 'invalid execution step')
    snapshotArtifacts(step.artifacts)
    if (step.kind === 'runtime.checkpoint') {
      const state = step.input
      if (typeof state['last'] !== 'string' || !Number.isSafeInteger(state['count']) || Number(state['count']) < 0 || Number(state['count']) > 6
        || state['protocolRepairs'] !== undefined && (!Number.isSafeInteger(state['protocolRepairs']) || Number(state['protocolRepairs']) < 0 || Number(state['protocolRepairs']) > 3)
        || !Array.isArray(state['observations']) || state['observations'].length > 2048
        || state['observations'].some(value => typeof value !== 'string' || !/^[a-f0-9]{64}$/.test(value))) throw new ControlPlaneError(400, 'invalid progress checkpoint')
    }
    await this.deps.steps.save({ workId: work.id, fence: proof.fence, leaseTokenHash: hashToken(proof.leaseToken) }, step)
  }
  private readonly logger: Logger

  constructor(private readonly deps: ControlPlaneDeps) {
    this.logger = deps.logger ?? nullLogger
  }

  async reserveModelCall(proof: LeaseProof, callId: string, limits: ModelBudgetLimits) {
    const work = await this.requireLease(proof, { rejectCancelled: true })
    await this.deps.authorizeWork?.(work)
    const rootWorkId = typeof work.meta?.['rootWorkId'] === 'string' ? work.meta['rootWorkId'] : work.id
    if (!await this.deps.work.ownsBudgetRoot(work, rootWorkId)) throw new ControlPlaneError(409, 'model budget root is outside this work lineage')
    if (!callId || callId.length > 256 || !Number.isSafeInteger(limits?.maxModelCalls) || limits.maxModelCalls < 1
      || !Number.isSafeInteger(limits.maxTokens) || limits.maxTokens < 1
      || !Number.isSafeInteger(limits.maxCostMicros) || limits.maxCostMicros < 1
      || !Number.isFinite(Date.parse(limits.deadlineAt))) throw new ControlPlaneError(400, 'invalid model budget reservation')
    if ([limits.reservedTokens ?? 0, limits.reservedCostMicros ?? 0].some(value => !Number.isSafeInteger(value) || value < 0)) throw new ControlPlaneError(400, 'invalid reserved model resources')
    if (limits.maxExecutionMs !== undefined && (!Number.isSafeInteger(limits.maxExecutionMs) || limits.maxExecutionMs < 1)) throw new ControlPlaneError(400, 'invalid execution budget')
    const policy = this.deps.modelBudget
    if (policy) {
      const input = limits.reservedInputTokens, output = limits.reservedOutputTokens
      if (![input, output].every(value => Number.isSafeInteger(value) && Number(value) >= 0)
        || input! + output! !== limits.reservedTokens) throw new ControlPlaneError(400, 'reservation requires input and output token bounds')
      limits = { ...limits, maxModelCalls: Math.min(limits.maxModelCalls, policy.maxModelCalls),
        pricing: modelPricing(policy),
        maxTokens: Math.min(limits.maxTokens, policy.maxTokens), maxCostMicros: Math.min(limits.maxCostMicros, policy.maxCostMicros),
        maxExecutionMs: Math.min(limits.maxExecutionMs ?? policy.wallClockMs, policy.wallClockMs),
        reservedCostMicros: Math.ceil((input! * policy.inputCostMicrosPerMillion + output! * policy.outputCostMicrosPerMillion) / 1_000_000) }
    }
    return this.deps.modelBudgets.reserve(rootWorkId, callId, limits,
      { workId: work.id, fence: proof.fence, leaseTokenHash: hashToken(proof.leaseToken) })
  }

  async recordModelUsage(proof: LeaseProof, callId: string, usage: { inputTokens: number; outputTokens: number; costMicros: number }, observation?: import('../model/execution.js').ModelCallObservation): Promise<void> {
    const work = await this.deps.work.getAttempt(proof.id, proof.fence, hashToken(proof.leaseToken)) ?? await this.requireLease(proof)
    const rootWorkId = typeof work.meta?.['rootWorkId'] === 'string' ? work.meta['rootWorkId'] : work.id
    if (!await this.deps.work.ownsBudgetRoot(work, rootWorkId)) throw new ControlPlaneError(409, 'model budget root is outside this work lineage')
    if (!callId || callId.length > 256 || [usage?.inputTokens, usage?.outputTokens, usage?.costMicros]
      .some(value => !Number.isSafeInteger(value) || value < 0)) throw new ControlPlaneError(400, 'invalid model usage')
    if (observation && (observation.callId !== callId || observation.workId !== work.id || observation.tenantId !== work.tenantId
      || observation.agentId !== work.agentId || observation.sessionId !== work.sessionId || observation.principalId !== work.principalId
      || observation.threadId !== work.threadId || !['agent-turn','structured','compaction','embedding'].includes(observation.purpose)
      || !['succeeded','failed'].includes(observation.status) || !Number.isFinite(observation.latencyMs)
      || observation.latencyMs < 0 || typeof observation.model !== 'string')) throw new ControlPlaneError(400, 'invalid model observation identity')
    const policy = this.deps.modelBudget
    const costMicros = policy ? Math.ceil((usage.inputTokens * policy.inputCostMicrosPerMillion
      + usage.outputTokens * policy.outputCostMicrosPerMillion) / 1_000_000) : usage.costMicros
    await this.deps.modelBudgets.record(rootWorkId, callId, usage.inputTokens, usage.outputTokens, costMicros,
      { workId: work.id, fence: proof.fence, leaseTokenHash: hashToken(proof.leaseToken) }, observation)
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

  private async reconcileNative(work: Omit<WorkItem, 'leaseToken'>, action: HostAction,
    options: import('./stores.js').ActionExecutionOptions): Promise<HostActionResult | null> {
    let result: HostActionResult | null | undefined
    try { result = await this.deps.actionExecutor.reconcile?.(work, action, options) }
    catch (error) {
      this.logger.warn('action reconciliation unavailable', { runId: work.id, attempt: work.fence, actionKey: action.idempotencyKey,
        requestVersion: options.requestVersion, action: action.action, error: errorMessage(error) })
      this.deps.metrics?.counter('agentos_action_recovery_total', 'Actions settled by native readback').inc({ outcome: 'unavailable' })
      return null
    }
    if (!result || result.approval || result.executionState === 'unknown' || !result.ok && result.executionState !== 'no_effect') return null
    result = { ...result, executionState: result.ok ? 'succeeded' : 'no_effect' }
    await this.deps.actions.recordResolution({ id: 'native:' + createHash('sha256').update(JSON.stringify([action.idempotencyKey,result])).digest('hex'),
      actionKey: action.idempotencyKey, result, resolvedBy: `native:${action.action}`,
      evidence: { source: 'native_reconciliation', action: action.action } })
    this.deps.metrics?.counter('agentos_action_recovery_total', 'Actions settled by native readback').inc({ outcome: result.ok ? 'succeeded' : 'no_effect' })
    return result
  }

  /** Read back uncertain effects under the newly issued lease before restoring the session. */
  async reconcilePending(proof: LeaseProof, external?: AbortSignal) {
    const work = await this.requireLease(proof, { rejectCancelled: true })
    const unresolved = await this.deps.actions.unsettled(work.id)
    if (unresolved.length > 64) throw new ControlPlaneError(409, 'action reconciliation exceeds 64 pending effects')
    const signal = AbortSignal.any([AbortSignal.timeout(10_000), ...external ? [external] : []]), deadlineAt = new Date(Date.now() + 10_000).toISOString()
    for (const pending of unresolved) {
      if (pending.state !== 'unknown') continue
      const intent = await this.deps.actions.findIntent(pending.actionKey)
      if (!intent || intent.workId !== work.id) continue
      signal.throwIfAborted()
      await abortable(this.reconcileNative(work, intent.action, { requestVersion: intent.requestVersion, signal, deadlineAt }), signal)
    }
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

  async claim(workerId: string, requestId?: string, workKinds?: readonly string[], lanes?: readonly WorkItem['lane'][], executionClass?: import('../protocol/types.js').ExecutionClass): Promise<WorkItem | null> {
    if (typeof workerId !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(workerId)) {
      throw new ControlPlaneError(400, 'workerId must be 1-128 safe identifier characters')
    }
    if (requestId !== undefined && !/^[A-Za-z0-9_-]{16,128}$/.test(requestId)) {
      throw new ControlPlaneError(400, 'requestId must be a 16-128 character identifier')
    }
    if (workKinds && (workKinds.length < 1 || workKinds.length > 64 || workKinds.some(kind => typeof kind !== 'string' || !/^[a-z][a-z0-9_]{0,63}$/.test(kind)))) throw new ControlPlaneError(400, 'invalid worker task types')
    if (lanes && (!Array.isArray(lanes) || !lanes.length || lanes.length > 4
      || lanes.some(lane => !['interactive','approval','collaboration','background'].includes(lane)))) throw new ControlPlaneError(400, 'invalid worker lanes')
    if (executionClass !== undefined && !['conversation', 'operation'].includes(executionClass)) throw new ControlPlaneError(400, 'invalid execution class')
    const work = await this.deps.work.claim(workerId, requestId, workKinds, lanes, executionClass)
    if (work) {
      this.deps.metrics?.counter('agentos_work_claimed_total', 'Work items claimed').inc({ lane: work.lane })
      if (work.availableAt) this.deps.metrics?.histogram('agentos_queue_wait_seconds', 'Eligible queue wait at claim', LATENCY_BUCKETS)
        .observe(Math.max(0,(Date.now()-Date.parse(work.availableAt))/1000), { lane: work.lane })
    }
    return work
  }

  async requireLease(proof: LeaseProof, options: { rejectCancelled?: boolean } = {}): Promise<Omit<WorkItem, 'leaseToken'>> {
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
    if (isWaitingOutcome(completion.goalOutcome)) throw new ControlPlaneError(400, 'waiting requires waitWork')
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
      if (completion.goalOutcome.status === 'satisfied') {
        const committed = await this.deps.delivery.getMessage?.(work)
        if (!committed || !isDeepStrictEqual(committed.envelope.goalOutcome, completion.goalOutcome)) throw new ControlPlaneError(409, 'goal outcome requires the committed assessed response')
        if (request) validateCompletion(committed.body, committed.envelope.assessment, request, completion.goalOutcome, [])
      }
    }
    const ok = await this.deps.work.complete(proof.id, proof.fence, hashToken(proof.leaseToken), completion)
    if (!ok) {
      await this.requireLease(proof)
      throw new ControlPlaneError(409, 'work cannot complete in its current state', 'work_state_conflict')
    }
    this.deps.metrics?.counter('agentos_work_completed_total', 'Work attempts finished').inc({ status: completion.status })
  }

  async waitWork(proof: LeaseProof, outcome: WaitingOutcome): Promise<void> {
    if (!isGoalOutcome(outcome) || !isWaitingOutcome(outcome) || outcome.verification !== 'not_run') throw new ControlPlaneError(400, 'invalid waiting outcome')
    const work = await this.requireLease(proof, { rejectCancelled: true })
    const request = (await this.getSession(proof, sessionKeyOf(work)))?.request
    if (!request || request.workId !== work.id || request.revisions.length + 1 !== outcome.requestVersion) throw new ControlPlaneError(409, 'wait request version mismatch')
    await this.validateWait(work, outcome)
    if (!await this.deps.work.wait(proof.id, proof.fence, hashToken(proof.leaseToken), outcome)) throw new ControlPlaneError(409, 'work cannot wait in its current state', 'work_state_conflict')
    this.deps.metrics?.counter('agentos_work_waiting_total', 'Work released while awaiting an input or child').inc({ reason: outcome.status })
  }

  async requestCancel(id: string): Promise<boolean> { return this.deps.work.requestCancel(id) }

  private async validateWait(work: Omit<WorkItem, 'leaseToken'>, outcome: GoalOutcome) {
    if (outcome.status === 'delegated') {
      if (!await this.deps.work.hasChild(work, outcome.taskRef, outcome.requestVersion)) throw new ControlPlaneError(409, 'wait requires a child of this request')
    } else if (outcome.status === 'awaiting_approval') {
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

  async loadContext(proof: LeaseProof, includeSession = false, signal?: AbortSignal): Promise<TurnContext> {
    const work = await this.requireLease(proof)
    await this.deps.authorizeWork?.(work)
    const [context, snapshot, grants, dependencies] = await Promise.all([
      this.deps.contextProvider.loadContext(work, signal),
      this.deps.contextSnapshot?.(work) ?? (async () => {
        const [session, steps] = await Promise.all([this.deps.sessions.get(sessionKeyOf(work), work.id), this.deps.steps.list(work.id)])
        return { session, steps, requestVersion: (session?.request?.revisions.length ?? 0) + 1 }
      })(),
      this.deps.capabilityResolver.resolve(work),
      this.deps.work.children(work),
    ])
    if (typeof work.meta?.['text'] === 'string') {
      context.capabilities = [...new Set([...context.capabilities, 'task'])]
      if (context.promptContextCandidate) context.promptContextCandidate = { ...context.promptContextCandidate, capabilities: context.capabilities }
    }
    const { steps, requestVersion } = snapshot
    const checkpoint = steps.findLast(step => step.kind === 'runtime.checkpoint' && step.requestVersion === requestVersion)
    const priorArtifacts = new Map<string, KernelArtifact>()
    if (work.fence > 1) {
      for (const step of steps) {
        for (const artifact of snapshotArtifacts(step.artifacts)) {
          priorArtifacts.set(artifact.path, artifact)
          if (priorArtifacts.size > 512) throw new ControlPlaneError(409, 'prior artifact inventory exceeds the per-run limit')
        }
      }
      for (const artifact of snapshotArtifacts(await this.deps.actions.artifacts(work.id))) priorArtifacts.set(artifact.path, artifact)
    }
    if (typeof work.meta?.['text'] === 'string' && !grants.some(grant => grant.name === 'task')) grants.push({ name: 'task', methods: TASK_TOOLS.map(tool => tool.action.split('.')[1]!) })
    await this.requireLease(proof)
    return { work: { ...work, leaseToken: proof.leaseToken }, ...context,
      snapshotVersion: { fence: work.fence, requestVersion, sessionRevision: snapshot.session?.revision ?? 0 },
      ...(includeSession ? { session: snapshot.session } : {}),
      executionSteps: steps, ...(checkpoint ? { executionCheckpoint: checkpoint.input as unknown as import('../runtime/corrections.js').ProgressCheckpoint } : {}), grants, dependencies, tools: grantedTools(this.deps.tools ?? TASK_TOOLS, grants).filter(tool => permitsTool(work, tool)), priorArtifacts: [...priorArtifacts.values()] }
  }

  // -------------------------------------------------------------------------
  // Host actions: grant enforcement + idempotency ledger
  // -------------------------------------------------------------------------

  async executeAction(proof: LeaseProof, action: HostAction, signal?: AbortSignal): Promise<HostActionResult> {
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

    const reject = async (code: string, error: string): Promise<HostActionResult> => {
      const prior = await this.deps.actions.find(action.idempotencyKey)
      const result: HostActionResult = { ok: false, executionState: prior?.approval ? 'no_effect' : 'rejected', code, error }
      return prior?.approval ? this.deps.actions.record(action.idempotencyKey, result) : result
    }
    // Authoritative capability check. The kernel-side allowlist only shapes
    // what the model can conveniently express; this is the boundary.
    const grants = await this.deps.capabilityResolver.resolve(work)
    const grant = namespace === 'task' && typeof work.meta?.['text'] === 'string' ? grants.find(candidate => candidate.name === 'task')
      ?? { name: 'task', methods: TASK_TOOLS.map(tool => tool.action.split('.')[1]!) } : grants.find((candidate) => candidate.name === namespace)
    if (!grant || (grant.methods && !grant.methods.includes(method))) {
      this.deps.metrics?.counter('agentos_actions_denied_total', 'Host actions denied by grant').inc({ namespace })
      return reject('forbidden', `capability denied: ${action.action} is not granted to this work item`)
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

    const current = await this.heartbeat(proof)
    if (!current.ok) throw new ControlPlaneError(409, 'work lease lost before action', 'lease_lost')
    if (current.cancelRequested) throw new ControlPlaneError(409, 'work is cancelled; no further actions are permitted', 'work_cancelled')
    let requestVersion = (current.steer?.length ?? 0) + 1
    if (typeof work.meta?.['text'] === 'string') {
      const request = (await this.getSession(proof, sessionKeyOf(work)))?.request
      if (!request || request.workId !== work.id || !isDeepStrictEqual(request.revisions, current.steer ?? [])) {
        return { ok: false, error: 'request snapshot is stale or missing; process the latest user revisions before acting' }
      }
      requestVersion = request.revisions.length + 1
    }

    const tool = (this.deps.tools ?? TASK_TOOLS).find(tool => tool.action === action.action)
    const contractHash = tool ? toolContractHash(tool) : undefined
    const existingIntent = await this.deps.actions.findIntent(action.idempotencyKey)
    if (existingIntent && (existingIntent.workId !== work.id || existingIntent.requestVersion !== requestVersion
      || existingIntent.tenantId !== work.tenantId || existingIntent.principalId !== (work.principalId ?? null)
      || existingIntent.agentId !== work.agentId || existingIntent.sessionId !== work.sessionId || existingIntent.threadId !== (work.threadId ?? null)
      || !isDeepStrictEqual(existingIntent.action, action))) throw new ControlPlaneError(409, 'action identity mismatch; reconcile before replay')
    if (action.action !== 'task.inspect' && (tool ? !permitsTool(work, tool) : executionMode(work) !== 'execute')) {
      return reject('mode_forbidden', `execution mode ${executionMode(work)} does not permit ${action.action}`)
    }
    const timeoutMs = tool?.execution?.timeoutMs ?? 30_000
    const options = { requestVersion, deadlineAt: new Date(Date.now() + timeoutMs).toISOString(),
      signal: AbortSignal.any([AbortSignal.timeout(timeoutMs), ...(signal ? [signal] : [])]) }
    if (tool && Object.keys(action.args).some(key => !Object.hasOwn(tool.parameters.properties, key))) return { ok: false, executionState: 'rejected', code: 'invalid_arguments', error: 'unknown tool argument' }
    if (namespace === 'task') {
      try { parseTaskArgs(action.action, action.args) }
      catch (error) { return reject('invalid_arguments', errorMessage(error)) }
    }
    if (namespace !== 'task') {
      try { await this.deps.actionExecutor.prepare(work, action, options) }
      catch (error) { return reject(error instanceof NoEffectError ? error.code : 'invalid_arguments', errorMessage(error)) }
    }
    if (existingIntent && existingIntent.toolContractHash !== contractHash) {
      const prior = await this.deps.actions.find(action.idempotencyKey)
      if (prior && !prior.approval && prior.executionState !== 'unknown') return prior
      if (prior?.approval) return reject('tool_contract_changed', 'Tool semantics changed; create a new preview and approval')
      return { ok: false, executionState: 'unknown', code: 'tool_contract_changed', error: 'Tool semantics changed; settle the existing intent before continuing' }
    }
    if (tool?.effect !== 'read' && namespace !== 'task') {
      const pending = await this.deps.actions.unsettled(work.id)
      if (pending.some(item => item.actionKey !== action.idempotencyKey)) return {
        ok: false, executionState: 'unknown', error: 'Prior actions require approval or reconciliation before starting another write',
      }
    }
    const fingerprint = actionFingerprint(work, action)
    const reservation = await this.deps.actions.reserve(action.idempotencyKey, fingerprint, {
      workId: work.id, tenantId: work.tenantId, principalId: work.principalId ?? null, agentId: work.agentId,
      sessionId: work.sessionId, threadId: work.threadId ?? null, requestVersion, action: structuredClone(action),
      ...(contractHash ? { toolContractHash: contractHash } : {}),
    })
    const replayed = await this.deps.actions.find(action.idempotencyKey)
    if (replayed && !replayed.approval) {
      this.deps.metrics?.counter('agentos_actions_replayed_total', 'Host actions served from the ledger').inc({ namespace })
      return replayed.executionState === 'unknown' ? await this.reconcileNative(work, action, options) ?? replayed : replayed
    }
    if (reservation === 'existing' && (!tool || tool.effect === 'uncertain')) {
      return await this.reconcileNative(work, action, options) ?? { ok: false, executionState: 'unknown', error: 'action intent exists without a receipt; reconcile before retrying' }
    }
    let result: HostActionResult
    try {
      if (action.action === 'task.inspect') {
        for (const pending of (await this.deps.actions.unsettled(work.id)).slice(0, 64)) {
          const intent = await this.deps.actions.findIntent(pending.actionKey)
          const receipt = await this.deps.actions.find(pending.actionKey)
          const allowed = intent && grants.find(grant => grant.name === intent.action.action.split('.')[0])
          if (intent && !receipt?.approval && allowed && (!allowed.methods || allowed.methods.includes(intent.action.action.split('.')[1]!))
            && intent.principalId === (work.principalId ?? null) && intent.tenantId === work.tenantId && intent.sessionId === work.sessionId
            && intent.agentId === work.agentId && intent.threadId === (work.threadId ?? null)) {
            await this.reconcileNative(work, intent.action, { ...options, requestVersion: intent.requestVersion })
          }
        }
        const pending = await this.deps.actions.unsettled(work.id)
        const sideEffects = (this.deps.tools ?? TASK_TOOLS).filter(candidate => !candidate.action.startsWith('task.') && !candidate.action.startsWith('graph.') && candidate.effect !== 'read').map(candidate => candidate.action)
        const completedBusinessAction = await this.deps.actions.hasSuccessfulAction(work.id, requestVersion, sideEffects)
        result = { ok: true, value: { requestVersion, pending: pending.slice(0, 64), truncated: pending.length > 64, completedBusinessAction } }
      } else if (action.action === 'task.read_attachment') {
        const request = (await this.getSession(proof, sessionKeyOf(work)))?.request
        if (!request || request.workId !== work.id) throw new Error('attachment read requires the current request snapshot')
        result = { ok: true, value: readRequestAttachment(request, action.args) }
      } else if (action.action === 'task.contract') {
        const request = (await this.getSession(proof, sessionKeyOf(work)))?.request
        if (!request || request.workId !== work.id) throw new Error('task contract requires the current request snapshot')
        const contract = createTaskContract(request.originalText, request.revisions.length + 1, action.args)
        result = { ok: true, value: { status: 'draft_validated', requestVersion: contract.requestVersion }, directive: { type: 'task_contract', data: { ...contract } } }
      } else if (action.action === 'task.check_receipt') {
        const { idempotencyKey, action: expectedAction, expected } = action.args
        const intent = await this.deps.actions.findIntent(idempotencyKey as string)
        if (!intent || intent.workId !== work.id || intent.tenantId !== work.tenantId
          || intent.principalId !== (work.principalId ?? null) || intent.agentId !== work.agentId
          || intent.sessionId !== work.sessionId || intent.threadId !== (work.threadId ?? null)
          || intent.requestVersion !== requestVersion || intent.action.action !== expectedAction) {
          throw new Error('receipt is unavailable for this request version and action')
        }
        const receipt = await this.deps.actions.find(idempotencyKey as string)
        const observed = receipt?.ok === true && receipt.executionState !== 'unknown'
          && receipt.directive === undefined && receipt.value !== undefined
        result = { ok: true, value: { scope: 'recorded_action_result', requestVersion, idempotencyKey,
          action: expectedAction, status: !observed ? 'not_observed' : isDeepStrictEqual(receipt.value, expected) ? 'pass' : 'fail',
          ...(observed ? { observed: receipt.value } : {}),
          limitation: 'This checks a recorded action result, not current resource state or overall goal completion.' } }
      } else if (action.action === 'task.check_resource') {
        const { action: readAction, args, expected } = action.args
        if (!this.deps.actionExecutor.readResource) throw new Error('resource readback is unavailable')
        const fields = Object.entries(expected as Record<string, unknown>)
        const resource = await this.deps.actionExecutor.readResource(work,
          { ...action, action: readAction as string, args: args as Record<string, unknown> }, options)
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
        result = { ok: true, value: { question }, directive: { type: 'defer', reason: 'user', data: { question } } }
      } else result = await this.deps.actionExecutor.execute(work, action, options)
    } catch (error) {
      result = { ok: false, executionState: namespace === 'task' || tool?.effect === 'read' || error instanceof NoEffectError ? 'no_effect' : 'unknown',
        ...(error instanceof NoEffectError ? { code: error.code } : {}), error: errorMessage(error) }
    }
    const recorded = await this.deps.actions.record(action.idempotencyKey, result)
    this.deps.metrics?.counter('agentos_actions_executed_total', 'Host actions executed').inc({
      namespace, ok: String(recorded.ok),
    })
    return recorded
  }

  async recoverCell(proof: LeaseProof, cellId: string, signal?: AbortSignal): Promise<Array<{
    action: string; idempotencyKey: string; result: HostActionResult
  }> | null> {
    const work = await this.requireLease(proof, { rejectCancelled: true })
    if (!cellId || cellId.length > 512) throw new ControlPlaneError(400, 'invalid cellId')
    const request = typeof work.meta?.['text'] === 'string'
      ? (await this.getSession(proof, sessionKeyOf(work)))?.request : undefined
    const requestVersion = request?.workId === work.id ? request.revisions.length + 1 : ((await this.heartbeat(proof)).steer?.length ?? 0) + 1
    const records = await this.deps.actions.listCell(work.id, cellId, requestVersion)
    if (!records.length || records.length > 100) return null
    const recovered: Array<{ action: string; idempotencyKey: string; result: HostActionResult }> = []
    for (const [index, { intent, result }] of records.entries()) {
      if (intent.tenantId !== work.tenantId || intent.principalId !== (work.principalId ?? null)
        || intent.agentId !== work.agentId || intent.sessionId !== work.sessionId
        || intent.threadId !== (work.threadId ?? null) || intent.action.callIndex !== index) {
        throw new ControlPlaneError(409, 'cell action history is inconsistent', 'reconciliation_conflict')
      }
      const tool = (this.deps.tools ?? TASK_TOOLS).find(tool => tool.action === intent.action.action)
      const safe = tool && ['read', 'transaction', 'idempotent'].includes(tool.effect)
      recovered.push({
        action: intent.action.action,
        idempotencyKey: intent.action.idempotencyKey,
        result: result ?? (safe ? await this.executeAction(proof, intent.action, signal)
          : { ok: false, executionState: 'unknown', error: 'action intent has no receipt; reconciliation required' }),
      })
    }
    return recovered
  }

  async recoverStep(proof: LeaseProof, cellId: string): Promise<{ output: string; artifacts: KernelArtifact[] } | null> {
    const work = await this.requireLease(proof, { rejectCancelled: true })
    if (!cellId || cellId.length > 512) throw new ControlPlaneError(400, 'invalid cellId')
    const request = (await this.getSession(proof, sessionKeyOf(work)))?.request
    const requestVersion = request?.workId === work.id ? request.revisions.length + 1 : null
    if (requestVersion === null || work.fence <= 1) return null
    const step = await this.deps.steps.get(work.id, cellId, requestVersion)
    return step?.output === undefined ? null : { output: step.output, artifacts: snapshotArtifacts(step.artifacts) }
  }

  async stageArtifactStream(proof: LeaseProof, artifact: KernelArtifact, content: AsyncIterable<Uint8Array>, signal?: AbortSignal): Promise<void> {
    const work = await this.requireLease(proof, { rejectCancelled: true })
    let checked: KernelArtifact
    try { checked = snapshotArtifacts([artifact])[0]! }
    catch { throw new ControlPlaneError(400, 'invalid artifact metadata') }
    if (checked.size > 16 * 1024 * 1024) throw new ControlPlaneError(413, 'artifact upload exceeds 16 MiB')
    if (this.deps.artifactStager?.stream) await this.deps.artifactStager.stream(work, checked, content, signal)
    else {
      // Compatibility for embedded stagers; the HTTP adapter reserves the full upload byte budget first.
      const bytes = Buffer.alloc(checked.size)
      let size = 0
      for await (const chunk of content) {
        signal?.throwIfAborted()
        if (size + chunk.byteLength > bytes.length) throw new ControlPlaneError(413, 'artifact content exceeds metadata')
        bytes.set(chunk, size); size += chunk.byteLength
      }
      await this.stageArtifact(proof, checked, bytes.subarray(0, size), signal)
    }
    await this.requireLease(proof, { rejectCancelled: true })
  }

  async stageArtifact(proof: LeaseProof, artifact: KernelArtifact, content: string | Uint8Array, signal?: AbortSignal): Promise<void> {
    const work = await this.requireLease(proof, { rejectCancelled: true })
    if (!this.deps.artifactStager) throw new ControlPlaneError(501, 'artifact upload is unavailable')
    let checked: KernelArtifact
    try { checked = snapshotArtifacts([artifact])[0]! }
    catch { throw new ControlPlaneError(400, 'invalid artifact metadata') }
    if (!(typeof content === 'string' || content instanceof Uint8Array)
      || content.length > (typeof content === 'string' ? 22_369_624 : 16 * 1024 * 1024)) {
      throw new ControlPlaneError(413, 'artifact upload exceeds 16 MiB')
    }
    const bytes = typeof content === 'string' ? Buffer.from(content, 'base64') : Buffer.from(content.buffer, content.byteOffset, content.byteLength)
    if (bytes.length !== checked.size || bytes.length > 16 * 1024 * 1024
      || typeof content === 'string' && bytes.toString('base64') !== content
      || createHash('sha256').update(bytes).digest('hex') !== checked.sha256.toLowerCase()) {
      throw new ControlPlaneError(409, 'artifact content does not match its metadata', 'artifact_mismatch')
    }
    await this.deps.artifactStager.stage(work, checked, bytes, signal)
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
    }, { workId: work.id, fence: proof.fence, leaseTokenHash: hashToken(proof.leaseToken) }, work)
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

  async commitResult(proof: LeaseProof, message: AssistantMessage): Promise<void> {
    if (!await this.deps.work.getLeased(proof.id, proof.fence, hashToken(proof.leaseToken))) {
      const issued = await this.deps.work.getAttempt(proof.id, proof.fence, hashToken(proof.leaseToken))
      if (issued) {
        const prior = await this.deps.delivery.getMessage?.(issued)
        if (prior && isDeepStrictEqual(prior, message)) return
      }
    }
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
    if (message.envelope.goalOutcome?.verification === 'passed') {
      throw new ControlPlaneError(409, 'goal verification requires authoritative acceptance evidence')
    }
    const session = await this.getSession(proof, sessionKeyOf(work))
    if (!session?.request?.evidence) throw new ControlPlaneError(409, 'response requires a saved request and evidence snapshot')
    if (session.request.workId !== work.id) throw new ControlPlaneError(409, 'response request belongs to another work item')
    const evidence = session.request.evidence
    const steps = await this.deps.steps.list(work.id)
    const recordedArtifacts = snapshotArtifacts(steps.flatMap(step => step.artifacts))
    const artifacts = snapshotArtifacts(message.envelope.artifacts)
    if (artifacts.some(artifact => !recordedArtifacts.some(recorded => isDeepStrictEqual(recorded, artifact)))) {
      throw new ControlPlaneError(409, 'response artifact has no durable execution record')
    }
    if (message.envelope.requestVersion !== session.request.revisions.length + 1) {
      throw new ControlPlaneError(409, 'response request version is stale')
    }
    const latestRequest = await this.heartbeat(proof)
    if (message.envelope.requestVersion !== (latestRequest.steer?.length ?? 0) + 1) throw new ControlPlaneError(409, 'response request version is stale')
    if (message.envelope.goalOutcome.status === 'delegated'
      && !await this.deps.work.hasChild(work, message.envelope.goalOutcome.taskRef, message.envelope.requestVersion)) {
      throw new ControlPlaneError(409, 'delegated task is not pending for this request')
    }
    await this.validateWait(work, message.envelope.goalOutcome)
    let expected
    try {
      const assessment = message.envelope.assessment
      const gaps: string[] = []
      const sideEffects = (this.deps.tools ?? TASK_TOOLS).filter(tool => !tool.action.startsWith('task.') && !tool.action.startsWith('graph.') && tool.effect !== 'read').map(tool => tool.action)
      const actionGap = businessActionDeliveryGap(session.request,
        await this.deps.actions.hasSuccessfulAction(work.id, message.envelope.requestVersion, sideEffects), artifacts.length > 0)
      if (actionGap) gaps.push(actionGap)
      if (requiresReview(session.request, steps, this.deps.tools ?? TASK_TOOLS, artifacts)
        || (await this.deps.work.children(work)).length) {
        const hash = candidateHash({ body: message.body, requestVersion: message.envelope.requestVersion, artifacts })
        const review = steps.findLast(step => step.kind === 'runtime.review' && step.requestVersion === message.envelope.requestVersion
          && step.input['workId'] === work.id && step.input['candidateHash'] === hash)
        const checked = review?.output ? JSON.parse(review.output) : undefined
        if (!checked || checked.error || checked.workId !== work.id || checked.candidateHash !== hash
          || checked.requestVersion !== message.envelope.requestVersion || !Array.isArray(checked.missing) || checked.missing.length) gaps.push('Complex candidate lacks an independent review for this request version and content')
      }
      validateCompletion(message.body, assessment, session.request, message.envelope.goalOutcome, gaps)
      if (message.envelope.goalOutcome.status === 'delegated' && (assessment?.status !== 'delegated'
        || assessment.taskRef !== message.envelope.goalOutcome.taskRef)) throw new Error('missing delegated self-assessment')
      if (message.envelope.goalOutcome.status === 'satisfied') {
        if (assessment && assessment.status !== 'satisfied') throw new Error('conflicting self-assessment')
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
    await this.deps.authorizeWork?.(work)
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
    await this.deps.authorizeWork?.(work)
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
        || !isDeepStrictEqual(session.request.conversation, work.conversation)
        || typeof session.request.authorId !== 'string' || typeof session.request.sourceRef !== 'string'
        || session.request.sourceRef !== work.triggerRef
        || (work.principalId !== undefined && session.request.authorId !== work.principalId)
        || (typeof work.meta?.['text'] === 'string' && (work.meta?.['delegation']
          ? session.request.delegatedAssignment !== work.meta['text'] : session.request.originalText !== work.meta['text']))
        || typeof session.request.originalText !== 'string' || !Array.isArray(session.request.revisions)
        || (session.request.codeExecution !== undefined && !['enabled','disabled'].includes(session.request.codeExecution))
        || (session.request.deliveryMode !== undefined && !['auto','text','action'].includes(session.request.deliveryMode))
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
      const delegation = work.meta?.['delegation'] as { parentRequest?: import('../context/request.js').RequestSnapshot } | undefined
      const parent = delegation?.parentRequest
      if (parent ? session.request.originalText !== parent.originalText
        || !isDeepStrictEqual(session.request.inheritedRevisions ?? [], [...(parent.inheritedRevisions ?? []), ...parent.revisions])
        : Boolean(session.request.inheritedRevisions?.length)) {
        throw new ControlPlaneError(400, 'inherited human requirements differ from the durable delegation')
      }
      const configuredCode = work.meta?.['codeExecution']
      const expectedCode = parent?.codeExecution === 'disabled' || configuredCode === 'disabled' ? 'disabled'
        : configuredCode === 'enabled' || parent?.codeExecution === 'enabled' ? 'enabled' : undefined
      const configuredDelivery = work.meta?.['deliveryMode']
      if (session.request.codeExecution !== expectedCode
        || session.request.deliveryMode !== configuredDelivery) {
        throw new ControlPlaneError(400, 'request execution or delivery policy differs from trusted work metadata')
      }
      const parentMode = parent?.mode
      const expectedMode = parentMode === 'chat' || parentMode === 'read' && executionMode(work) === 'execute'
        ? parentMode : work.meta?.['mode']
      if (session.request.mode !== expectedMode) throw new ControlPlaneError(400, 'request mode differs from trusted work metadata')
      if (!isDeepStrictEqual(snapshotObligations(session.request.obligations ?? []), snapshotObligations(work.meta?.['obligations'] ?? []))) {
        throw new ControlPlaneError(400, 'delivery obligations differ from trusted work metadata')
      }
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
        || next.codeExecution !== previous.codeExecution || next.deliveryMode !== previous.deliveryMode || next.mode !== previous.mode
        || !isDeepStrictEqual(next.obligations ?? [], previous.obligations ?? [])

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
