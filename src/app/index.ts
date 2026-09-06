import { ControlPlaneServer } from '../control-plane/http-server.js'
import { checkStorage } from './storage.js'
import { captureMemoryEvidence, retryMemorySynthesis } from '../memory/evidence.js'
import { memorySynthesisProcessor, memoryIndexProcessor } from '../memory/processor.js'
import { readArtifact, persistArtifacts, stageArtifact } from './artifacts.js'
import { resolve } from 'node:path'
import { snapshotAttachments, type RequestAttachment } from '../context/attachments.js'
import { recoverWait } from './recover-wait.js'
import { continueInput, type InputContinuation } from './input.js'
import { randomUUID } from 'node:crypto'
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
import { lectureDeckProcessor, type LectureDeckService } from '../lecture-deck/service.js'
import { contentHash } from '../lecture-deck/contracts.js'

export interface LingxiOSOptions {
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
  kernel?: Pick<KernelManagerOptions, 'pythonCommand' | 'homesRoot' | 'startupTimeoutMs' | 'executionTimeoutMs' | 'hostActionTimeoutMs' | 'maxOutputChars' | 'allowNetwork' | 'isolation'>
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

export interface LectureRequestInput extends Omit<RequestInput, 'text' | 'attachments' | 'authorName'> { request: unknown }
export interface LectureOperationInput extends Omit<LectureRequestInput, 'request' | 'id'> {
  deckId: string; operation: 'retry' | 'revise'; idempotencyKey: string; request?: unknown
}

export type ActionResolutionInput = ActionResolution & Pick<RequestInput,
  'tenantId' | 'agentId' | 'sessionId' | 'principalId' | 'threadId'>

/** Trusted server entry point. Product ingress must authenticate the principal. */
export async function createLingxiOS(options: LingxiOSOptions) {
  return assembleApp(options)
}

/** Package-internal assembly hook; never exposed as consumer configuration. */
export async function assembleApp(options: LingxiOSOptions, integration?: Pick<ControlPlaneDeps, 'contextProvider' | 'capabilityResolver' | 'actionExecutor' | 'delivery'> & { beforeClaim?(): Promise<void> }) {
  if (!options.database?.query || !options.database.connect) throw new ConfigError('a PostgreSQL pool is required')
  if (options.model && ((options.model.id !== undefined && !options.model.id.trim()) || !options.model.apiKey?.trim())) throw new ConfigError('model apiKey is required and any explicit model id must be non-empty')
  const concurrency = options.worker?.concurrency ?? 2
  if (!Number.isSafeInteger(concurrency) || concurrency < 1 || concurrency > 1_024) throw new ConfigError('worker concurrency must be 1-1024')
  await checkStorage(options.database)
  const persona = options.persona ?? { name: 'Assistant', role: 'assistant', instructions: 'Follow the current user request. Clearly distinguish verified results from remaining work.' }
  const workerId = options.worker?.id ?? `lingxios-${randomUUID()}`
  const sessions = new PgSessionStore(options.database)
  async function flushDeliveries(runId?: string) {
    if (!integration) return
    const claimToken = randomUUID()
    const { rows } = await options.database.query(
      `WITH candidate AS (
         SELECT outbox.run_id,messages.message
         FROM lingxios.agent_delivery_outbox outbox
         JOIN lingxios.agent_messages messages ON messages.run_id=outbox.run_id
         JOIN lingxios.agent_work_items current ON current.id=outbox.run_id
        WHERE outbox.delivered_at IS NULL AND outbox.available_at <= NOW()
          AND ($1::text IS NULL OR outbox.run_id=$1)
          AND current.cancel_requested_at IS NULL
          AND (messages.message->'envelope'->>'requestVersion')::integer=jsonb_array_length(current.steer_inputs)+1
        ORDER BY outbox.available_at,messages.committed_at LIMIT 1 FOR UPDATE OF outbox SKIP LOCKED)
       UPDATE lingxios.agent_delivery_outbox outbox
          SET claim_token=$2,available_at=NOW()+INTERVAL '60 seconds',attempts=LEAST(outbox.attempts+1,30)
         FROM candidate WHERE outbox.run_id=candidate.run_id
       RETURNING outbox.run_id,outbox.work,candidate.message`, [runId ?? null, claimToken])
    for (const row of rows) {
      // Native delivery uses a stable message ID; sends exceeding the claim window may overlap.
      try {
        await integration.delivery.deliverMessage(row['work'] as Omit<WorkItem, 'leaseToken'>, row['message'] as AssistantMessage)
        await options.database.query('UPDATE lingxios.agent_delivery_outbox SET delivered_at=NOW(),claim_token=NULL WHERE run_id=$1 AND claim_token=$2', [row['run_id'], claimToken])
      } catch {
        await options.database.query(`UPDATE lingxios.agent_delivery_outbox SET claim_token=NULL,
          available_at=NOW()+LEAST(300,5*power(2,LEAST(attempts-1,6)))*INTERVAL '1 second'
          WHERE run_id=$1 AND claim_token=$2`, [row['run_id'], claimToken])
        // The committed answer survives transport failure; the outbox owns retry.
      }
    }
  }

  const service = new ControlPlaneService({
    work: new PgWorkStore(options.database), sessions,
    events: new PgEventStore(options.database), actions: new PgActionLedger(options.database),
    modelBudgets: new PgModelBudgetStore(options.database),
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
      onEvent: async (work, event) => { await integration?.delivery.onEvent(work, event) },
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
             ON CONFLICT (run_id) DO UPDATE SET message=EXCLUDED.message
             WHERE lingxios.agent_messages.message=EXCLUDED.message
               AND lingxios.agent_messages.tenant_id=EXCLUDED.tenant_id
               AND lingxios.agent_messages.agent_id=EXCLUDED.agent_id
               AND lingxios.agent_messages.session_id=EXCLUDED.session_id
               AND lingxios.agent_messages.home_epoch=EXCLUDED.home_epoch
             RETURNING run_id`, [work.id, work.tenantId, work.agentId, work.sessionId, JSON.stringify(message), work.homeEpoch],
          )
          if (rows.length !== 1) throw new Error('a different response is already committed for this run')
          if (recordMemory) await captureMemoryEvidence(client, work, message)
          if (integration) await client.query('INSERT INTO lingxios.agent_delivery_outbox(run_id,work) VALUES($1,$2::jsonb) ON CONFLICT(run_id) DO NOTHING', [work.id, JSON.stringify(work)])
        })
        await flushDeliveries(work.id)
      },
    },
  })
  async function claimWork(claimingWorkerId: string, requestId?: string) {
    await flushDeliveries()
    if (integration) await retryMemorySynthesis(options.database)
    await integration?.beforeClaim?.()
    const work = await service.claim(claimingWorkerId, requestId)
    if (!work) return null
    const { rows } = await options.database.query(
      'SELECT message FROM lingxios.agent_messages WHERE run_id=$1 AND tenant_id=$2 AND agent_id=$3 AND session_id=$4',
      [work.id, work.tenantId, work.agentId, work.sessionId])
    const committed = rows[0]?.['message'] as AssistantMessage | undefined
    if (committed) {
      if (!committed.envelope) throw new Error('committed response envelope is missing')
      // A durable response is the recovery checkpoint; do not regenerate it.
      const session = await service.getSession(work, sessionKeyOf(work))
      if (!session?.request || session.request.workId !== work.id
        || session.request.revisions.length + 1 !== committed.envelope.requestVersion) throw new Error('committed response request snapshot is stale or missing')
      const last = session.history.at(-1)
      if (!last || !('role' in last) || last.role !== 'assistant' || last.content !== committed.body) {
        session.history.push({ role: 'assistant', content: committed.body })
        await service.saveSession(work, session)
      }
      await service.recordEvent(work, {
        runId: work.id, seq: (work.fence - 1) * RUN_SEQUENCE_SPAN + 1,
        kind: 'response.recovered', stage: 'completed', visibility: 'internal',
        data: { requestVersion: committed.envelope.requestVersion, evidenceSnapshotId: committed.envelope.evidenceSnapshotId },
      })
      await service.complete(work, { status: 'completed', resultText: committed.body, goalOutcome: committed.envelope.goalOutcome })
      return null
    }
    if (await recoverWait(options.database, service, work)) return null
    return work
  }
  const host: HostPort = {
    claimWork: () => claimWork(workerId), heartbeat: (work) => service.heartbeat(work),
    loadContext: (work) => service.loadContext(work), executeAction: (work, action) => service.executeAction(work, action),
    reserveModelCall: (work, callId, limits) => service.reserveModelCall(work, callId, limits),
    recordModelUsage: (work, callId, usage) => service.recordModelUsage(work, callId, usage),
    recoverCell: (work, cellId) => service.recoverCell(work, cellId),
    recoverStep: (work, cellId) => service.recoverStep(work, cellId),
    stageArtifact: (work, artifact, bytes) => stageArtifact(
      resolve(options.kernel?.homesRoot ?? process.env['AGENT_OS_HOMES_ROOT'] ?? '.agent-os/homes'), work, artifact, bytes),
    emitEvent: (work, event) => service.recordEvent(work, event), loadSession: (work, key) => service.getSession(work, key),
    saveSession: async (work, session) => { session.revision = (await service.saveSession(work, session)).revision },
    commitMessage: (work, message) => service.commitMessage(work, message), completeWork: (work, completion) => service.complete(work, completion),
    yieldWork: (work) => service.yieldWork(work),
  }
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
    if (!options.kernelFactory && options.kernel?.isolation !== 'bubblewrap' && process.env['NODE_ENV'] === 'production' && options.trustProcessKernel !== true) {
      throw new ConfigError('production local execution requires an OS-isolated kernelFactory; trustProcessKernel is only for trusted model code')
    }
    const kernels = options.kernelFactory?.(bridge) ?? new KernelManager(bridge, { ...options.kernel, maxKernels: concurrency })
    const runtime = new AgentRuntime(host, model, kernels, {
      ...(options.policy ? { policy: options.policy } : {}), recordModelPayloads: options.recordModelPayloads ?? false,
      ...(options.modelTrace ? { modelTrace: options.modelTrace } : {}),
      ...(options.modelBudget ? { rootModelBudget: options.modelBudget } : {}),
      ...(options.onModelCall ? { onModelCall: options.onModelCall } : {}),
    })
    runtime.registerProcessor('memory_synthesis', memorySynthesisProcessor)
    runtime.registerProcessor('memory_index', memoryIndexProcessor)
    runtime.registerProcessor('teacher_digest', 'conversation')
    runtime.registerProcessor('routine', 'conversation')
    runtime.registerProcessor('mission_coordinator', 'conversation')
    if (options.lectureDeck) runtime.registerProcessor('lecture_deck', lectureDeckProcessor(options.lectureDeck))
    const worker = new AgentWorker({ host, runtime, kernels, workerId, maxConcurrentRuns: concurrency,
      shutdownGraceMs: options.worker?.shutdownGraceMs ?? 20_000,
      ...(options.worker?.pollIdleMs === undefined ? {} : { pollIdleMs: options.worker.pollIdleMs }),
      ...(options.worker?.healthPort === undefined ? {} : { healthPort: options.worker.healthPort }),
    })
    local = { runtime, worker }
    return local
  }
  let controlPlane: ControlPlaneServer | undefined
  return {
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
      const { rows } = await options.database.query(`UPDATE lingxios.agent_work_items
        SET cancel_requested_at=NOW(),status=CASE WHEN status='leased' THEN status ELSE 'cancelled' END,
          finished_at=CASE WHEN status='leased' THEN finished_at ELSE NOW() END,
          goal_outcome=jsonb_build_object('status','blocked','verification','inconclusive','requestVersion',jsonb_array_length(steer_inputs)+1,
            'gaps',jsonb_build_array('Execution was cancelled')),updated_at=NOW()
        WHERE id=$1 AND tenant_id=$2 AND agent_id=$3 AND session_id=$4 AND principal_id=$5
          AND thread_id IS NOT DISTINCT FROM $6 AND cancel_requested_at IS NULL
          AND (status IN ('queued','leased') OR (status='completed' AND goal_outcome->>'status' IN ('awaiting_input','awaiting_approval')))
        RETURNING id`, [identity.runId, identity.tenantId, identity.agentId, identity.sessionId, identity.principalId, identity.threadId ?? null])
      return rows.length === 1
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
    async enqueueLecture(input: LectureRequestInput) {
      if (!options.lectureDeck) throw new Error('native lecture-deck capability is not configured')
      if (!input?.principalId?.trim()) throw new Error('authenticated principalId is required')
      const requestId = input.id ?? randomUUID()
      const deckId = `deck_${contentHash([input.tenantId, input.principalId, requestId]).slice(0, 32)}`
      const deck = await options.lectureDeck.begin({ tenantId: input.tenantId, principalId: input.principalId }, input.request, deckId)
      const workId = `lecture_${deck.id}_r${deck.revision}`
      const queued = await service.enqueue({ id: workId, tenantId: input.tenantId, agentId: input.agentId, sessionId: input.sessionId,
        ...(input.threadId ? { threadId: input.threadId } : {}), principalId: input.principalId, kind: 'lecture_deck', lane: 'background',
        triggerRef: input.sourceRef ?? requestId, meta: { operation: 'run', deckId: deck.id } })
      return { ...queued, deckId: deck.id, revision: deck.revision, status: deck.status }
    },
    async enqueueLectureOperation(input: LectureOperationInput) {
      if (!options.lectureDeck) throw new Error('native lecture-deck capability is not configured')
      if (!input?.principalId?.trim() || !input.idempotencyKey?.trim()) throw new Error('authenticated principalId and idempotencyKey are required')
      const deck = await options.lectureDeck.get({ tenantId: input.tenantId, principalId: input.principalId }, input.deckId)
      const workId = `lecture_${contentHash([deck.id, deck.revision, input.operation, input.idempotencyKey]).slice(0, 48)}`
      const queued = await service.enqueue({ id: workId, tenantId: input.tenantId, agentId: input.agentId, sessionId: input.sessionId,
        ...(input.threadId ? { threadId: input.threadId } : {}), principalId: input.principalId, kind: 'lecture_deck', lane: 'background',
        triggerRef: input.sourceRef ?? input.idempotencyKey, meta: { operation: input.operation, deckId: deck.id, ...(input.request === undefined ? {} : { request: input.request }) } })
      return { ...queued, deckId: deck.id, revision: deck.revision, status: deck.status }
    },
    async readLecture(input: { tenantId: string; principalId: string; deckId: string }) {
      if (!options.lectureDeck) throw new Error('native lecture-deck capability is not configured')
      return options.lectureDeck.get({ tenantId: input.tenantId, principalId: input.principalId }, input.deckId)
    },
    async readLectureHtml(input: { tenantId: string; principalId: string; deckId: string }) {
      if (!options.lectureDeck) throw new Error('native lecture-deck capability is not configured')
      return options.lectureDeck.readHtml({ tenantId: input.tenantId, principalId: input.principalId }, input.deckId)
    },
    async runNext() {
      const { runtime } = localExecution()
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
      const server = new ControlPlaneServer({ service, claimWork, serviceToken: input.serviceToken })
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
      stopPromise ??= (async () => {
        await local?.worker.stop()
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
