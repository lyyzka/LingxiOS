import { DEFAULT_MODEL_BUDGET } from '../model/execution.js'
import { flushOutbox } from '../control-plane/outbox.js'
import { resumeDependents } from '../control-plane/dependencies.js'
import { candidateHash } from '../outcome/verification.js'
import { candidateActions, inspectActions } from '../outcome/action-check.js'
import { workStatusOf } from '../protocol/types.js'
import { ControlPlaneServer } from '../control-plane/http-server.js'
import { checkStorage } from './storage.js'
import { captureMemoryEvidence, retryMemorySynthesis } from '../memory/evidence.js'
import { registerProcessors } from '../worker/processors.js'
import { readArtifact, persistArtifacts, stageArtifact, inspectArtifacts } from './artifacts.js'
import { resolve } from 'node:path'
import { snapshotAttachments, type RequestAttachment } from '../context/attachments.js'
import { recoverWait } from './recover-wait.js'
import { continueInput, type InputContinuation } from './input.js'
import { createHash, randomUUID } from 'node:crypto'
import { ConfigError } from '../errors.js'
import { ControlPlaneService } from '../control-plane/service.js'
import { withTransaction, PgWorkStore, PgSessionStore, PgEventStore, PgActionLedger, PgModelBudgetStore, type SqlPool } from '../control-plane/pg-store.js'
import type { HostPort } from '../host/port.js'
import { KernelManager, type KernelHostBridge, type KernelManagerOptions, type ManagedKernelExecutor } from '../kernel/manager.js'
import { DEFAULT_MODEL, OpenAIChatDriver } from '../model/openai.js'
import { AgentRuntime } from '../runtime/runtime.js'
import { AgentWorker } from '../worker/worker.js'
import type { AssistantMessage, PromptContext, WorkItem, RunEvent } from '../protocol/types.js'
import { sessionKeyOf } from '../protocol/types.js'
import { RUN_SEQUENCE_SPAN } from '../protocol/constants.js'
import type { GoalOutcome } from '../protocol/outcome.js'
import type { ControlPlaneDeps } from '../control-plane/service.js'
import type { ActionResolution, ContextProvider } from '../control-plane/stores.js'
import type { RuntimePolicy } from '../runtime/policy.js'
import type { ModelCallObserver } from '../runtime/runtime.js'
import { type LectureDeckService } from '../lecture-deck/service.js'
import { createLectureDeckApp } from '../lecture-deck/app.js'
import { lectureControl } from '../lecture-deck/control.js'
import { kernelIsolation } from '../kernel/isolation.js'
import { mkdir, open, unlink } from 'node:fs/promises'
import { createLogger, type Logger } from '../logging.js'
import { MetricsRegistry } from '../metrics.js'
import { loadModelBudget } from '../config.js'
import { maintainStorage } from './maintenance.js'
import { PgStepStore } from '../control-plane/steps.js'

export interface LingxiOSOptions {
  logger?: Logger
  metrics?: MetricsRegistry
  database: SqlPool
  model?: { id?: string; apiKey: string; baseUrl?: string; reasoningEffort?: 'high' | 'max'; maxOutputTokens?: number; contextWindowTokens?: number }
  persona?: PromptContext['persona']
  /** Trusted product context loaded for each execution attempt. */
  contextProvider?: ContextProvider
  /** Product policy used by local runtime execution. */
  policy?: RuntimePolicy
  /** Opt in to storing full normalized model payloads in internal events. */
  recordModelPayloads?: boolean
  modelTrace?: import('../runtime/runtime.js').ModelTracePolicy
  /** Root-work limits shared by retries and delegated children. */
  modelBudget?: import('../runtime/runtime.js').RootModelBudgetOptions
  /** Required for products that own billing, quota and model-call observability. */
  onModelCall?: ModelCallObserver
  /** Optional native professional HTML lecture-deck processor. */
  lectureDeck?: LectureDeckService
  kernel?: Omit<KernelManagerOptions, 'runnerPath' | 'logger' | 'maxKernels'>
  /** Required in production for untrusted model-authored code. */
  kernelFactory?: (bridge: KernelHostBridge) => ManagedKernelExecutor
  /** Explicit opt-in for trusted model code using the local process backend in production. */
  trustProcessKernel?: boolean
  worker?: { id?: string; concurrency?: number; shutdownGraceMs?: number; pollIdleMs?: number; healthPort?: number }
}

export interface RequestInput {
  id?: string
  sourceRef?: string
  tenantId: string
  agentId: string
  sessionId: string
  principalId: string
  text: string
  authorName?: string
  threadId?: string
  attachments?: RequestAttachment[]
}

export interface DelegatedRequestInput extends RequestInput {
  delegation: {
    parentWorkId: string; rootWorkId: string; parentRequestVersion: number
    instructionAuthorId: string; parentRequest: import('../context/request.js').RequestSnapshot
  }
}

export interface MessageIdentity {
  runId: string
  tenantId: string
  agentId: string
  sessionId: string
}

export type ActionResolutionInput = ActionResolution & Pick<RequestInput,
  'tenantId' | 'agentId' | 'sessionId' | 'principalId' | 'threadId'>

/** Trusted server entry point. Product ingress must authenticate the principal. */
export async function createLingxiOS(options: LingxiOSOptions) {
  return assembleApp(options)
}

/** Package-internal assembly hook; never exposed as consumer configuration. */
export async function assembleApp(options: LingxiOSOptions, integration?: Pick<ControlPlaneDeps, 'contextProvider' | 'capabilityResolver' | 'actionExecutor' | 'delivery'> & { tools?: readonly import('../tools/catalog.js').ToolDefinition[]; backgroundJobs?: Record<string, () => Promise<unknown>> }) {
  if (!options.database?.query || !options.database.connect) throw new ConfigError('a PostgreSQL pool is required')
  if (options.lectureDeck && !options.lectureDeck.dependencies.publisher.read) throw new ConfigError('lecture publisher must support reading committed artifacts for recovery')
  if (options.model && ((options.model.id !== undefined && !options.model.id.trim()) || !options.model.apiKey?.trim())) throw new ConfigError('model apiKey is required and any explicit model id must be non-empty')
  const concurrency = options.worker?.concurrency ?? 2
  if (!Number.isSafeInteger(concurrency) || concurrency < 1 || concurrency > 1_024) throw new ConfigError('worker concurrency must be 1-1024')
  await checkStorage(options.database)
  const logger = options.logger ?? createLogger()
  const metrics = options.metrics ?? new MetricsRegistry()
  const homesRoot = resolve(options.kernel?.homesRoot ?? process.env['AGENT_OS_HOMES_ROOT'] ?? '.agent-os/homes')
  const modelBudget = { ...DEFAULT_MODEL_BUDGET, ...loadModelBudget(), ...options.modelBudget }
  if (process.env['NODE_ENV'] === 'production' && !(modelBudget.inputCostMicrosPerMillion > 0 || modelBudget.outputCostMicrosPerMillion > 0)) {
    throw new ConfigError('production requires configured model token prices')
  }
  metrics.gauge('agentos_cost_budget_enabled', 'Configured prices make the cost budget effective').set(
    modelBudget.inputCostMicrosPerMillion! > 0 || modelBudget.outputCostMicrosPerMillion! > 0 ? 1 : 0)
  let lastContactAt = 0
  const persona = options.persona ?? { name: 'Assistant', role: 'assistant', instructions: 'Follow the current user request. Clearly distinguish verified results from remaining work.' }
  const workerId = options.worker?.id ?? `lingxios-${randomUUID()}`
  const sessions = new PgSessionStore(options.database)
  async function flushDeliveries() {
    if (!integration) return
    await flushOutbox(options.database, 'agent_delivery_outbox', row => integration.delivery.deliverMessage(
      row['work'] as Omit<WorkItem, 'leaseToken'>, row['message'] as AssistantMessage))
  }
  async function flushEvents() {
    if (!integration) return
    await flushOutbox(options.database, 'agent_run_events', row => integration.delivery.onEvent(
      row['delivery_work'] as Omit<WorkItem, 'leaseToken'>, { runId: String(row['run_id']), seq: Number(row['seq']),
        kind: String(row['kind']), stage: row['stage'] as RunEvent['stage'], visibility: row['visibility'] as RunEvent['visibility'],
        data: row['data'] as RunEvent['data'] }))
  }
  async function flushModelUsage() {
    if (!options.onModelCall) return
    await flushOutbox(options.database, 'agent_model_budget_calls', row => options.onModelCall!(
      row['observation'] as import('../model/execution.js').ModelCallObservation))
  }

  const service = new ControlPlaneService({
    ...(integration?.tools ? { tools: integration.tools } : {}),
    steps: new PgStepStore(options.database),
    verifyCandidate: async (work, candidate) => {
      const records = [...await inspectArtifacts(homesRoot, work, candidate.artifacts),
        ...await inspectActions(options.database, work, candidate.requestVersion, integration?.tools ?? [], integration?.actionExecutor)]
      const hash = candidateHash(candidate)
      await withTransaction(options.database, async client => {
        const current = await client.query(`SELECT id FROM lingxios.agent_work_items WHERE id=$1 AND fence=$2
          AND status='leased' AND lease_expires_at>NOW() AND cancel_requested_at IS NULL
          AND jsonb_array_length(steer_inputs)+1=$3 FOR UPDATE`, [work.id,work.fence,candidate.requestVersion])
        if (!current.rows.length) throw new Error('candidate request changed while checking files')
        for (const record of records) await client.query(`INSERT INTO lingxios.agent_verifications
          (work_id,request_version,candidate_hash,checker,status,evidence) VALUES($1,$2,$3,$4,$5,$6::jsonb)
          ON CONFLICT(work_id,request_version,candidate_hash,checker) DO UPDATE SET
            status=EXCLUDED.status,evidence=EXCLUDED.evidence,observed_at=NOW()`,
          [work.id,candidate.requestVersion,hash,record.checker,record.status,JSON.stringify(record.evidence)])
      })
      return { requestVersion: candidate.requestVersion, candidateHash: hash, records }
    },
    logger, metrics,
    ...(options.lectureDeck ? { lecture: lectureControl(options.database, options.lectureDeck) } : {}),
    work: new PgWorkStore(options.database), sessions,
    events: new PgEventStore(options.database, Boolean(integration)), actions: new PgActionLedger(options.database),
    modelBudgets: new PgModelBudgetStore(options.database), modelBudget,
    artifactStager: { stage: (work, artifact, bytes) => stageArtifact(
      resolve(options.kernel?.homesRoot ?? process.env['AGENT_OS_HOMES_ROOT'] ?? '.agent-os/homes'), work, artifact, bytes) },
    contextProvider: integration?.contextProvider ?? options.contextProvider ?? { loadContext: async (work) => {
      const text = work.meta?.['text']
      if (typeof text !== 'string' || !work.principalId) throw new Error('request text and principal are missing')
      return {
        persona, capabilities: [],
        messages: [{ ref: work.triggerRef, authorId: work.principalId, authorName: String(work.meta?.['authorName'] ?? 'User'), authorKind: 'human', body: text, createdAt: work.createdAt ?? '' }],
        promptContextCandidate: { version: 2, epoch: 0, assembledAt: '', systemInstructions: '', persona, capabilities: [], sourceVersions: { persona: JSON.stringify(persona) } },
      }
    } },
    capabilityResolver: integration?.capabilityResolver ?? { resolve: async () => [] },
    actionExecutor: integration?.actionExecutor ?? { execute: async () => ({ ok: false, error: 'no product capabilities are granted by the default application' }) },
    delivery: {
      getMessage: async work => {
        const result = await options.database.query('SELECT message FROM lingxios.agent_messages WHERE run_id=$1 AND tenant_id=$2 AND agent_id=$3 AND session_id=$4',
          [work.id, work.tenantId, work.agentId, work.sessionId])
        return (result.rows[0]?.['message'] as AssistantMessage | undefined) ?? null
      },
      onEvent: async () => { background('events', flushEvents) },
      deliverMessage: async (work, message) => {
        const recordMemory = integration && (await integration.capabilityResolver.resolve(work)).some(grant => grant.name === 'memory')
        await persistArtifacts(resolve(options.kernel?.homesRoot ?? process.env['AGENT_OS_HOMES_ROOT'] ?? '.agent-os/homes'), work, message)
        await withTransaction(options.database, async client => {
          const current = await client.query(
            `SELECT id FROM lingxios.agent_work_items
              WHERE id=$1 AND fence=$2 AND status='leased' AND lease_expires_at>NOW()
                AND cancel_requested_at IS NULL
                AND ($3::integer IS NULL OR jsonb_array_length(steer_inputs)+1=$3)
              FOR UPDATE`, [work.id, work.fence, message.envelope.requestVersion])
          if (!current.rows.length) throw new Error('request changed or lease expired before message persistence')
          const { rows } = await client.query(
            `INSERT INTO lingxios.agent_messages (run_id, tenant_id, agent_id, session_id, message, home_epoch)
             VALUES ($1,$2,$3,$4,$5::jsonb,$6)
             ON CONFLICT (run_id) DO UPDATE SET message=EXCLUDED.message,home_epoch=EXCLUDED.home_epoch,committed_at=NOW()
             WHERE (lingxios.agent_messages.message=EXCLUDED.message OR lingxios.agent_messages.message->'envelope'->'goalOutcome'->>'status' IN ('delegated','awaiting_input','awaiting_approval'))
               AND lingxios.agent_messages.tenant_id=EXCLUDED.tenant_id
               AND lingxios.agent_messages.agent_id=EXCLUDED.agent_id
               AND lingxios.agent_messages.session_id=EXCLUDED.session_id
             RETURNING run_id`, [work.id, work.tenantId, work.agentId, work.sessionId, JSON.stringify(message), work.homeEpoch],
          )
          if (rows.length !== 1) throw new Error('a different response is already committed for this run')
          if (message.envelope.goalOutcome.status === 'satisfied') {
            const hash = candidateHash({ body: message.body, requestVersion: message.envelope.requestVersion, artifacts: message.envelope.artifacts })
            const checks = await client.query(`SELECT checker,status FROM lingxios.agent_verifications
              WHERE work_id=$1 AND request_version=$2 AND candidate_hash=$3`, [work.id,message.envelope.requestVersion,hash])
            if (message.envelope.artifacts.some(artifact => !checks.rows.some(check => check['checker'] === `artifact:${artifact.path}` && check['status'] === 'passed'))) throw new Error('candidate files require current authoritative checks')
            const actions = await candidateActions(client, work.id, message.envelope.requestVersion, integration?.tools ?? [])
            if (actions.length > 1024 || actions.some(action => !checks.rows.some(check => check['checker'] === `action:${action.key}` && check['status'] === 'passed'))) throw new Error('candidate writes require current authoritative checks')
            const unresolved = await client.query(`SELECT 1 FROM lingxios.agent_action_intents intent
              LEFT JOIN lingxios.agent_action_ledger receipt USING(idempotency_key)
              LEFT JOIN LATERAL (SELECT resolution->'result' AS result FROM lingxios.agent_action_resolutions
                WHERE idempotency_key=intent.idempotency_key ORDER BY resolution_seq DESC LIMIT 1) resolved ON TRUE
              WHERE intent.intent->>'workId'=$1 AND (COALESCE(resolved.result,receipt.result) IS NULL
                OR COALESCE(resolved.result,receipt.result)->>'executionState'='unknown'
                OR COALESCE(resolved.result,receipt.result)->'approval'->>'status'='PENDING') LIMIT 1`, [work.id])
            if (unresolved.rows.length) throw new Error('unresolved effects prevent successful completion')
          }
          await client.query(`INSERT INTO lingxios.agent_results(work_id,candidate_hash,request_version,fence,message)
            VALUES($1,$2,$3,$4,$5::jsonb) ON CONFLICT DO NOTHING`,
            [work.id,createHash('sha256').update(JSON.stringify(message)).digest('hex'),message.envelope.requestVersion,work.fence,JSON.stringify(message)])
          if (recordMemory && !['awaiting_input','awaiting_approval','delegated'].includes(message.envelope.goalOutcome.status)) await captureMemoryEvidence(client, work, message)
          const status = workStatusOf({ status: 'completed', goalOutcome: message.envelope.goalOutcome })
          await client.query(`UPDATE lingxios.agent_work_items SET status=$3,result_text=$4,goal_outcome=$5::jsonb,
            lease_token_hash=NULL,lease_expires_at=NULL,finished_at=CASE WHEN $3='waiting' THEN NULL ELSE NOW() END,
            last_progress_at=NOW(),updated_at=NOW() WHERE id=$1 AND fence=$2`,
            [work.id, work.fence, status, message.body, JSON.stringify(message.envelope.goalOutcome)])
          await client.query('DELETE FROM lingxios.agent_os_session_leases WHERE work_id=$1 AND fence=$2', [work.id, work.fence])
          if (integration) await client.query('INSERT INTO lingxios.agent_delivery_outbox(run_id,work) VALUES($1,$2::jsonb) ON CONFLICT(run_id) DO UPDATE SET work=EXCLUDED.work,delivered_at=NULL,available_at=NOW(),claim_token=NULL,attempts=0', [work.id, JSON.stringify(work)])
        })
      },
    },
  })
  async function claimWork(claimingWorkerId: string, requestId?: string, workKinds?: readonly string[]) {
    const work = await service.claim(claimingWorkerId, requestId, workKinds)
    lastContactAt = Date.now()
    if (!work) return null
    if (await recoverWait(options.database, service, work)) return null
    return work
  }
  const host: HostPort = {
    verifyCandidate: (work, candidate) => service.verifyCandidate(work, candidate),
    saveStep: (work, step) => service.saveStep(work, step),
    lecture: (work, command) => service.lecture(work, command),
    claimWork: () => claimWork(workerId, undefined, local?.runtime.workKinds), heartbeat: async (work) => { const result = await service.heartbeat(work); lastContactAt = Date.now(); return result },
    loadContext: (work) => service.loadContext(work), executeAction: (work, action) => service.executeAction(work, action),
    reserveModelCall: (work, callId, limits) => service.reserveModelCall(work, callId, limits),
    recordModelUsage: (work, callId, usage, observation) => service.recordModelUsage(work, callId, usage, observation),
    recoverCell: (work, cellId) => service.recoverCell(work, cellId),
    recoverStep: (work, cellId) => service.recoverStep(work, cellId),
    stageArtifact: (work, artifact, bytes) => stageArtifact(
      resolve(options.kernel?.homesRoot ?? process.env['AGENT_OS_HOMES_ROOT'] ?? '.agent-os/homes'), work, artifact, bytes),
    emitEvent: (work, event) => service.recordEvent(work, event), loadSession: (work, key) => service.getSession(work, key),
    saveSession: async (work, session) => { session.revision = (await service.saveSession(work, session)).revision },
    commitResult: (work, message) => service.commitResult(work, message), completeWork: (work, completion) => service.complete(work, completion),
    yieldWork: (work) => service.yieldWork(work),
  }
  const jobs = new Map<string, Promise<unknown>>()
  const background = (name: string, operation: () => Promise<unknown>) => {
    if (jobs.has(name) || stopped) return
    const running = operation().catch(error => logger.warn(`${name} failed`, { error: error instanceof Error ? error.message : String(error) }))
      .finally(() => { jobs.delete(name) })
    jobs.set(name, running)
  }
  const deliveryTimer = setInterval(() => {
    background('delivery', () => flushDeliveries())
    background('events', flushEvents)
    background('billing', () => flushModelUsage())
    background('dependencies', () => resumeDependents(options.database))
    for (const [name, operation] of Object.entries(integration?.backgroundJobs ?? {})) background(name, operation)
    if (integration) background('memory synthesis', () => retryMemorySynthesis(options.database))
  }, 1_000)
  const maintenanceTimer = setInterval(() => {
    background('storage maintenance', () => maintainStorage(options.database, homesRoot))
  }, 60_000)
  deliveryTimer.unref(); maintenanceTimer.unref()
  let stopped = false
  let stopPromise: Promise<void> | undefined
  let listening: Promise<number> | undefined
  let local: { runtime: AgentRuntime; worker: AgentWorker } | undefined
  function localExecution() {
    if (stopped) throw new Error('application has stopped')
    if (local) return local
    if (!options.model) throw new ConfigError('model configuration is required for local execution')
    const model = new OpenAIChatDriver(options.model.id ?? DEFAULT_MODEL.id, options.model)
    const bridge: KernelHostBridge = { execute: (work, action) => service.executeAction(work, action) }
    const kernels = options.kernelFactory?.(bridge) ?? new KernelManager(bridge, { ...options.kernel, maxKernels: concurrency,
      isolation: kernelIsolation(options.kernel?.isolation ?? process.env['AGENT_OS_KERNEL_ISOLATION'], process.env['NODE_ENV'] === 'production', options.trustProcessKernel) })
    const runtime = new AgentRuntime(host, model, kernels, {
      logger,
      ...(options.policy ? { policy: options.policy } : {}), recordModelPayloads: options.recordModelPayloads ?? false,
      ...(options.modelTrace ? { modelTrace: options.modelTrace } : {}),
      rootModelBudget: modelBudget,
      ...(options.onModelCall ? { onModelCall: options.onModelCall } : {}),
    })
    registerProcessors(runtime, options.lectureDeck)
    const worker = new AgentWorker({ host, runtime, kernels, workerId, maxConcurrentRuns: concurrency,
      logger, metrics, lastContactAt: () => lastContactAt,
      shutdownGraceMs: options.worker?.shutdownGraceMs ?? 20_000,
      ...(options.worker?.pollIdleMs === undefined ? {} : { pollIdleMs: options.worker.pollIdleMs }),
      ...(options.worker?.healthPort === undefined ? {} : { healthPort: options.worker.healthPort }),
    })
    local = { runtime, worker }
    return local
  }
  let controlPlane: ControlPlaneServer | undefined
  return {
    metrics: () => metrics.expose(),
    maintenance: () => maintainStorage(options.database, homesRoot),
    lectures: options.lectureDeck ? createLectureDeckApp(options.database, options.lectureDeck) : undefined,
    /** Authenticate and authorize the caller before using this server API. */
    readArtifact: (identity: MessageIdentity & Pick<RequestInput, 'principalId' | 'threadId'>, path: string) =>
      readArtifact(options.database, resolve(options.kernel?.homesRoot ?? process.env['AGENT_OS_HOMES_ROOT'] ?? '.agent-os/homes'), identity, path),
    continueInput: (input: InputContinuation) => continueInput(options.database, input),
    resolveAction: async (input: ActionResolutionInput) => {
      const { tenantId, agentId, sessionId, principalId, threadId, ...resolution } = input
      const recorded = await service.resolveAction(resolution, { tenantId, agentId, sessionId, principalId, ...(threadId ? { threadId } : {}) })
      await options.database.query(`WITH target AS (
        SELECT intent->>'workId' AS work_id,(intent->>'requestVersion')::integer AS request_version
        FROM lingxios.agent_action_intents WHERE idempotency_key=$1), unsettled AS (
        SELECT intent.intent->>'workId' AS work_id FROM lingxios.agent_action_intents intent
        LEFT JOIN lingxios.agent_action_ledger receipt USING(idempotency_key)
        LEFT JOIN LATERAL (SELECT resolution->'result' AS result FROM lingxios.agent_action_resolutions
          WHERE idempotency_key=intent.idempotency_key ORDER BY resolution_seq DESC LIMIT 1) resolved ON TRUE
        WHERE intent.intent->>'workId'=(SELECT work_id FROM target)
          AND (COALESCE(resolved.result,receipt.result) IS NULL
            OR COALESCE(resolved.result,receipt.result)->>'executionState'='unknown'
            OR COALESCE(resolved.result,receipt.result)->'approval'->>'status'='PENDING') LIMIT 1)
      UPDATE lingxios.agent_work_items work SET status='queued',available_at=NOW(),finished_at=NULL,
        goal_outcome=NULL,error=NULL,updated_at=NOW() FROM target
      WHERE work.id=target.work_id AND work.tenant_id=$2 AND work.agent_id=$3 AND work.session_id=$4
        AND work.principal_id=$5 AND work.thread_id IS NOT DISTINCT FROM $6 AND work.status='failed'
        AND (work.goal_outcome->>'requestVersion')::integer=target.request_version
        AND NOT EXISTS(SELECT 1 FROM unsettled)
        AND NOT EXISTS(SELECT 1 FROM lingxios.agent_os_session_leases lease
          WHERE lease.session_key=('[' || to_json(work.tenant_id)::text || ',' || to_json(work.agent_id)::text || ','
            || to_json(work.session_id)::text || ',' || COALESCE(to_json(work.thread_id)::text, 'null') || ']')
            AND lease.expires_at>NOW())`,
      [resolution.actionKey, tenantId, agentId, sessionId, principalId, threadId ?? null])
      return recorded
    },
    /** Trusted server boundary: use the authenticated original principal. */
    async cancel(identity: MessageIdentity & Pick<RequestInput, 'principalId' | 'threadId'>): Promise<boolean> {
      if (!identity || !identity.principalId?.trim()) throw new Error('authenticated principalId is required')
      const { rows } = await options.database.query(`SELECT id FROM lingxios.agent_work_items
        WHERE id=$1 AND tenant_id=$2 AND agent_id=$3 AND session_id=$4 AND principal_id=$5
          AND thread_id IS NOT DISTINCT FROM $6`, [identity.runId, identity.tenantId, identity.agentId, identity.sessionId, identity.principalId, identity.threadId ?? null])
      return rows.length === 1 && service.requestCancel(identity.runId)
    },
    async enqueue(input: RequestInput) {
      if (typeof input.text !== 'string' || !input.text.trim() || typeof input.principalId !== 'string' || !input.principalId.trim()) {
        throw new Error('non-empty request text and authenticated principalId are required')
      }
      return service.enqueue({ ...input, kind: 'turn', lane: 'interactive', triggerRef: input.sourceRef ?? input.id ?? randomUUID(),
        meta: { text: input.text, authorName: input.authorName ?? 'User', attachments: snapshotAttachments(input.attachments ?? []) },
      })
    },
    async enqueueDelegated(input: DelegatedRequestInput) {
      const { delegation, ...request } = input
      if (!delegation || delegation.parentRequest.workId !== delegation.parentWorkId
        || delegation.parentRequest.revisions.length + 1 !== delegation.parentRequestVersion) throw new Error('invalid delegated request')
      return service.enqueue({ ...request, kind: 'turn', lane: 'collaboration', triggerRef: request.sourceRef ?? request.id ?? randomUUID(),
        meta: { text: request.text, authorName: request.authorName ?? 'Agent', attachments: snapshotAttachments(request.attachments ?? []),
          parentWorkId: delegation.parentWorkId, rootWorkId: delegation.rootWorkId,
          parentRequestVersion: delegation.parentRequestVersion, delegation: { ...delegation, assignment: request.text } },
      })
    },
    async runNext() {
      const { runtime, worker } = localExecution()
      await worker.check()
      const work = await host.claimWork()
      if (!work) return false
      await runtime.runWork(work)
      return true
    },
    async readMessage(identity: MessageIdentity): Promise<AssistantMessage | null> {
      const { rows } = await options.database.query(
        'SELECT message FROM lingxios.agent_messages WHERE run_id=$1 AND tenant_id=$2 AND agent_id=$3 AND session_id=$4',
        [identity.runId, identity.tenantId, identity.agentId, identity.sessionId],
      )
      return (rows[0]?.['message'] as AssistantMessage | undefined) ?? null
    },
    async readOutcome(identity: MessageIdentity): Promise<GoalOutcome | null> {
      const { rows } = await options.database.query(
        'SELECT goal_outcome FROM lingxios.agent_work_items WHERE id=$1 AND tenant_id=$2 AND agent_id=$3 AND session_id=$4',
        [identity.runId, identity.tenantId, identity.agentId, identity.sessionId],
      )
      return (rows[0]?.['goal_outcome'] as GoalOutcome | undefined) ?? null
    },
    async readEvents(identity: MessageIdentity, afterSeq = 0): Promise<{ events: RunEvent[]; nextSeq: number }> {
      if (!Number.isSafeInteger(afterSeq) || afterSeq < 0) throw new Error('event cursor must be a non-negative safe integer')
      const { rows } = await options.database.query(`SELECT event.* FROM lingxios.agent_run_events event
        JOIN lingxios.agent_work_items work ON work.id=event.run_id
          AND work.tenant_id=event.tenant_id AND work.agent_id=event.agent_id
        WHERE work.id=$1 AND work.tenant_id=$2 AND work.agent_id=$3 AND work.session_id=$4
          AND event.visibility='user' AND event.seq>$5 ORDER BY event.seq LIMIT 100`,
      [identity.runId, identity.tenantId, identity.agentId, identity.sessionId, afterSeq])
      const events = rows.map(row => ({ runId: String(row['run_id']), seq: Number(row['seq']), kind: String(row['kind']),
        stage: row['stage'] as RunEvent['stage'], visibility: 'user' as const, data: row['data'] as Record<string, unknown> }))
      return { events, nextSeq: events.at(-1)?.seq ?? afterSeq }
    },
    async readDelivery(identity: MessageIdentity): Promise<'pending' | 'delivered' | 'not_observed' | null> {
      const { rows } = await options.database.query(
        `SELECT CASE WHEN outbox.run_id IS NULL THEN 'not_observed'
           WHEN outbox.delivered_at IS NULL THEN 'pending' ELSE 'delivered' END AS state
         FROM lingxios.agent_messages message
         LEFT JOIN lingxios.agent_delivery_outbox outbox ON outbox.run_id=message.run_id
         WHERE message.run_id=$1 AND message.tenant_id=$2 AND message.agent_id=$3 AND message.session_id=$4`,
        [identity.runId, identity.tenantId, identity.agentId, identity.sessionId])
      return rows[0]?.['state'] as 'pending' | 'delivered' | 'not_observed' | undefined ?? null
    },
    async listenControlPlane(input: { serviceToken: string; port: number; host?: string }) {
      if (stopped) throw new Error('application has stopped')
      if (controlPlane) throw new Error('control plane is already listening or starting')
      if (!input.serviceToken?.trim()) throw new ConfigError('control plane service token is required')
      if (!Number.isSafeInteger(input.port) || input.port < 0 || input.port > 65535) throw new ConfigError('control plane port must be 0-65535')
      const server = new ControlPlaneServer({ service, claimWork, serviceToken: input.serviceToken, logger, metrics,
        ready: async () => {
          if (stopped) return false
          await checkStorage(options.database)
          await mkdir(homesRoot, { recursive: true, mode: 0o700 })
          const path = resolve(homesRoot, `.ready-${randomUUID()}`)
          const file = await open(path, 'wx', 0o600)
          try { await file.writeFile('ready'); await file.sync() } finally { await file.close(); await unlink(path) }
          const { rows } = await options.database.query(`SELECT COUNT(*)::integer AS count FROM lingxios.agent_delivery_outbox WHERE delivered_at IS NULL`)
          metrics.gauge('agentos_delivery_pending', 'Pending message deliveries').set(Number(rows[0]?.['count'] ?? 0))
          return !stopped
        } })
      controlPlane = server
      try {
        listening = server.listen(input.port, input.host ?? '127.0.0.1')
        return await listening
      } catch (error) {
        controlPlane = undefined
        throw error
      }
    },
    start: async () => localExecution().worker.start(),
    stop() {
      stopped = true
      clearInterval(deliveryTimer); clearInterval(maintenanceTimer)
      stopPromise ??= (async () => {
        await local?.worker.stop()
        await Promise.race([Promise.allSettled([...jobs.values()]), new Promise(resolve => { const timer = setTimeout(resolve, 5_000); timer.unref() })])
        // The listen caller receives startup errors; shutdown still releases any listener.
        await listening?.catch(() => {})
        if (controlPlane) {
          await controlPlane.close()
          controlPlane = undefined
        }
      })()
      return stopPromise
    },
  }
}
