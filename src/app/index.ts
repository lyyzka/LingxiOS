import { DEFAULT_MODEL_BUDGET } from '../model/execution.js'
import { abortable } from '../deadline.js'
import type { VerificationRecord } from '../outcome/verification.js'
import { flushOutbox } from '../control-plane/outbox.js'
import { resumeDependents } from '../control-plane/dependencies.js'
import { candidateHash } from '../outcome/verification.js'
import { candidateActions, inspectActions } from '../outcome/action-check.js'
import { workStatusOf } from '../protocol/types.js'
import { ControlPlaneServer } from '../control-plane/http-server.js'
import { checkStorage } from './storage.js'
import { captureMemoryEvidence, retryMemorySynthesis, scheduleMemoryReflection } from '../memory/evidence.js'
import { readArtifact, persistArtifacts, stageArtifact, inspectArtifacts, createNativeArtifact } from './artifacts.js'
import { resolve } from 'node:path'
import { snapshotAttachments, type RequestAttachment } from '../context/attachments.js'
import { recoverWait } from './recover-wait.js'
import { continueInput, type InputContinuation } from './input.js'
import { createHash, randomUUID } from 'node:crypto'
import { ConfigError } from '../errors.js'
import { ControlPlaneService } from '../control-plane/service.js'
import { withTransaction, workItemFromRow, PgWorkStore, PgSessionStore, PgEventStore, PgActionLedger, PgModelBudgetStore, type SqlPool, type SqlQueryable } from '../control-plane/pg-store.js'
import type { HostPort } from '../host/port.js'
import type { AssistantMessage, CodeExecutionMode, PromptContext, WorkItem, RunEvent } from '../protocol/types.js'
import type { DeliveryMode } from '../context/request.js'
import { executionMode, type HarnessMode } from '../runtime/execution-policy.js'
import { sessionKeyOf } from '../protocol/types.js'
import { RUN_SEQUENCE_SPAN } from '../protocol/constants.js'
import type { GoalOutcome } from '../protocol/outcome.js'
import type { ActionResolution, ContextProvider } from '../control-plane/stores.js'
import type { ModelCallObserver } from '../runtime/runtime.js'
import { mkdir, open, unlink } from 'node:fs/promises'
import { createLogger, type Logger } from '../logging.js'
import { MetricsRegistry } from '../metrics.js'
import { loadModelBudget } from '../config.js'
import { maintainStorage } from './maintenance.js'
import { PgStepStore } from '../control-plane/steps.js'
import { toolExecutor } from '../tools/executor.js'
import { TASK_TOOLS } from '../tools/catalog.js'
import type { ToolDefinition } from '../tools/definition.js'
import { readApproval, decideApproval, resumeDecidedApprovals, executeDecidedApprovals, type ApprovalLookup, type ApprovalDecision } from '../control-plane/approvals.js'
import { readRun, readRunState, reviseRun, cancelRun, cancelDescendants, enqueueWork, type RunIdentity, type JobInput } from './jobs.js'
import { createMemoryRuntime, type MemoryOptions } from '../memory/runtime.js'
import type { MemoryScope } from '../memory/store.js'
import { authorizedScopes, identityOf } from '../memory/access.js'
import { inspectObligations, snapshotObligations, type DeliveryObligation } from '../outcome/obligations.js'
import { sweepQueuedWork } from '../control-plane/scheduler.js'
import { readDiagnostics, refreshMetrics, listRuns, readOperations, retryDelivery, type RunListQuery } from './diagnostics.js'
import { freezeEvolutionBenchmark, rollbackEvolution, type EvolutionBenchmark } from '../memory/evolution.js'
import { deadlinePool } from '../control-plane/deadline-pool.js'
import { assembleHarness, type HarnessProfile } from '../harness/profile.js'
import { skillIndex, skillTool, type SkillDefinition } from '../skills/definition.js'
import { presentationTool, type PresentationDefinition, type TrustedPresentation } from '../presentation/definition.js'
import { observationTool } from '../context/observations.js'
import { discoveryTool, toolSpecification } from '../tools/discovery.js'
import { grantedTools } from '../tools/catalog.js'
import { permitsTool } from '../runtime/execution-policy.js'
import { canonicalJson, textSha256 } from '../context/compiler.js'
import { toolContractHash } from '../tools/contracts.js'

export interface LingxiOSOptions {
  harness?: HarnessProfile
  skills?: readonly SkillDefinition[]
  presentations?: readonly PresentationDefinition[]
  memory?: MemoryOptions
  tools?: readonly ToolDefinition[]
  delivery?: import('../control-plane/stores.js').DeliveryPort
  capabilityResolver?: import('../control-plane/stores.js').CapabilityResolver
  logger?: Logger
  metrics?: MetricsRegistry
  database: SqlPool
  /** Shared artifact storage root, also configured on workers. */
  homesRoot?: string
  persona?: PromptContext['persona']
  /** Trusted product context loaded for each execution attempt. */
  contextProvider?: ContextProvider
  /** Native acceptance checks, using live business records rather than model assertions. */
  verifyRun?: (context: RunVerificationContext) => Promise<VerificationRecord[]>
  /** Root-work limits shared by retries and delegated children. */
  modelBudget?: import('../runtime/runtime.js').RootModelBudgetOptions
  /** Required for products that own billing, quota and model-call observability. */
  onModelCall?: ModelCallObserver

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
  /** Trusted product classification; disabled removes the Python execution surface end to end. */
  codeExecution?: CodeExecutionMode
  /** Modes narrow current grants; chat has no tools and read allows only native reads. */
  mode?: HarnessMode
  obligations?: DeliveryObligation[]
  /** Trusted completion obligation; action requires durable successful business-action evidence. */
  deliveryMode?: DeliveryMode
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

export interface RunVerificationContext {
  candidate: import('../outcome/verification.js').Candidate
  work: Omit<WorkItem, 'leaseToken'>
  requestVersion: number
  database: SqlQueryable
  signal: AbortSignal
  deadlineAt: string
}

function requestPolicyMeta(input: Pick<RequestInput, 'codeExecution' | 'deliveryMode' | 'mode' | 'obligations'>): {
  codeExecution?: CodeExecutionMode; deliveryMode?: DeliveryMode; mode?: HarnessMode; obligations?: DeliveryObligation[]
} {
  if (input.mode !== undefined) executionMode({ meta: { mode: input.mode } })
  if (input.codeExecution !== undefined && !['enabled','disabled'].includes(input.codeExecution)) throw new Error('invalid codeExecution policy')
  if (input.deliveryMode !== undefined && !['auto','text','action'].includes(input.deliveryMode)) throw new Error('invalid deliveryMode policy')
  return {
    ...(input.mode ? { mode: input.mode } : {}),
    ...(input.obligations ? { obligations: snapshotObligations(input.obligations) } : {}),
    ...(input.codeExecution ? { codeExecution: input.codeExecution } : {}),
    ...(input.deliveryMode ? { deliveryMode: input.deliveryMode } : {}),
  }
}

/** Trusted server entry point. Product ingress authenticates the principal; workers execute separately. */
export async function createLingxiOS(options: LingxiOSOptions) {
  if (!options.database?.query || !options.database.connect) throw new ConfigError('a PostgreSQL pool is required')
  const shutdown = new AbortController()
  options = { ...options, database: deadlinePool(options.database,shutdown.signal) }
  await checkStorage(options.database)
  const logger = options.logger ?? createLogger()
  const metrics = options.metrics ?? new MetricsRegistry()
  const homesRoot = resolve(options.homesRoot ?? process.env['AGENT_OS_HOMES_ROOT'] ?? '.agent-os/homes')
  const modelBudget = { ...DEFAULT_MODEL_BUDGET, ...loadModelBudget(), ...options.modelBudget }
  if ((process.env['NODE_ENV'] === 'production' || options.modelBudget?.maxCostMicros !== undefined || process.env['AGENT_OS_MAX_COST_MICROS'] !== undefined)
    && !(modelBudget.inputCostMicrosPerMillion > 0 || modelBudget.outputCostMicrosPerMillion > 0)) {
    throw new ConfigError('production or an explicit monetary budget requires configured model token prices')
  }
  metrics.gauge('agentos_cost_budget_enabled', 'Configured prices make the cost budget effective').set(
    modelBudget.inputCostMicrosPerMillion! > 0 || modelBudget.outputCostMicrosPerMillion! > 0 ? 1 : 0)
  const persona = options.persona ?? { name: 'Assistant', role: 'assistant', instructions: 'Follow the current user request. Clearly distinguish verified results from remaining work.' }
  const contextProvider: ContextProvider = options.contextProvider ?? { loadContext: async (work) => {
      const text = work.meta?.['text']
      if (typeof text !== 'string' || !work.principalId) throw new Error('request text and principal are missing')
      return {
        persona, capabilities: [],
        messages: [{ ref: work.triggerRef, authorId: work.principalId, authorName: String(work.meta?.['authorName'] ?? 'User'), authorKind: 'human', body: text, createdAt: work.createdAt ?? '' }],
        promptContextCandidate: { version: 3, epoch: 0, assembledAt: '', systemInstructions: '', persona, capabilities: [], sourceVersions: { persona: JSON.stringify(persona) } },
      }
    } }
  const memory = options.memory ? createMemoryRuntime(options.database, options.memory, options.modelBudget) : undefined
  const harness = options.harness ? assembleHarness(options.harness) : undefined
  const businessTools = [...options.tools ?? [], ...harness?.tools ?? []]
  const skills = [...options.skills ?? [], ...harness?.skills ?? []], presentations = [...options.presentations ?? [], ...harness?.presentations ?? []]
  const capabilities = options.capabilityResolver ?? { resolve: async () => [...new Set(businessTools.map(tool => tool.action.split('.')[0]!))]
    .map(name => ({ name, methods: businessTools.filter(tool => tool.action.startsWith(name + '.')).map(tool => tool.action.split('.')[1]!) })) }
  const allowed = async (context: import('../tools/definition.js').ActionContext, actions: string[]) => {
    const tools = grantedTools(businessTools, await capabilities.resolve(context.work)).filter(tool => permitsTool(context.work, tool))
    if (actions.some(action => !tools.some(tool => tool.action === action))) throw new Error('required capability was revoked or is unavailable in this mode')
  }
  const extensionTools = [...(skills.length ? [skillTool(skills, allowed)] : []), ...(presentations.length ? [presentationTool(presentations, allowed)] : [])]
  const readableTools = [...businessTools, ...extensionTools]
  const helperTools = [...extensionTools, ...(readableTools.length ? [observationTool(readableTools, async (context, actions) => {
    if (actions.every(action => businessTools.some(tool => tool.action === action))) await allowed(context, actions)
  })] : []), ...(businessTools.some(tool => tool.deferred) ? [discoveryTool(businessTools, context => capabilities.resolve(context.work))] : [])]
  const definitions = [...businessTools, ...helperTools, ...memory?.tools ?? []]
  const behavior = harness?.context ?? (skills.length || presentations.length ? { id: 'authored', version: '1', hash: '', rules: [], skills: [], presentations: [] } : undefined)
  if (behavior) {
    behavior.skills = skills.map(skillIndex)
    behavior.presentations = presentations.map(({ type, version, description, actions }) => ({ type, version, description, actions }))
    behavior.hash = textSha256(canonicalJson({ ...behavior, tools: definitions.map(toolContractHash) }))
  }
  const policyMeta = (input: Pick<RequestInput, 'mode' | 'codeExecution' | 'deliveryMode' | 'obligations'>) => {
    const mode = input.mode ?? options.harness?.mode
    if (options.harness && ['chat','read','execute'].indexOf(mode!) > ['chat','read','execute'].indexOf(options.harness.mode)) throw new Error('request cannot widen its harness mode')
    return { ...requestPolicyMeta({ ...input, ...(mode ? { mode } : {}) }), ...(behavior ? { harnessHash: behavior.hash } : {}) }
  }
  const integration = {
    tools: [...TASK_TOOLS, ...definitions.map(toolSpecification)],
    actionExecutor: toolExecutor(options.database, definitions, (work, input) => createNativeArtifact(
      resolve(options.homesRoot ?? process.env['AGENT_OS_HOMES_ROOT'] ?? '.agent-os/homes'), work, input), options.memory),
    capabilityResolver: { async resolve(work: Omit<WorkItem, 'leaseToken'>) {
      if (memory && ['memory_synthesis','memory_index','memory_evaluation'].includes(work.kind)) return [{ name: work.kind,
        methods: memory.tools.filter(tool => tool.action.startsWith(work.kind + '.')).map(tool => tool.action.split('.')[1]!) }]
      return [...await capabilities.resolve(work), ...helperTools.map(tool => ({ name: tool.action.split('.')[0]!, methods: [tool.action.split('.')[1]!] })),
        ...memory?[{name:'memory',methods:memory.tools.filter(tool=>tool.action.startsWith('memory.')).map(tool=>tool.action.split('.')[1]!)}]:[]]
    } },
    contextProvider: { async loadContext(work: Omit<WorkItem, 'leaseToken'>) {
      if (behavior && !['memory_synthesis','memory_index','memory_evaluation'].includes(work.kind) && work.meta?.['harnessHash'] !== behavior.hash) throw new Error('harness version mismatch; resume with the pinned deployment or drain the old run')
      const context = { ...await contextProvider.loadContext(work), ...(behavior ? { harness: structuredClone(behavior) } : {}) }
      let discoveredTools: string[] | undefined
      if (businessTools.some(tool => tool.deferred)) {
        const discovered = await options.database.query(`SELECT DISTINCT tool->>'name' AS name FROM lingxios.agent_action_intents i
          JOIN lingxios.agent_action_ledger r USING(idempotency_key)
          JOIN lingxios.agent_work_items w ON w.id=i.intent->>'workId'
          CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(r.result->'value'->'tools')='array' THEN r.result->'value'->'tools' ELSE '[]'::jsonb END) tool
          WHERE w.id=$1 AND i.intent->'action'->>'action'='catalog.discover' AND r.result->>'ok'='true'
            AND (i.intent->>'requestVersion')::integer=jsonb_array_length(w.steer_inputs)+1 LIMIT 1025`, [work.id])
        if (discovered.rows.length > 1024) throw new Error('too many materialized tool schemas')
        discoveredTools = discovered.rows.map(row => String(row['name']))
      }
      return { ...context, ...(discoveredTools ? { discoveredTools } : {}),
        ...(memory && !['memory_synthesis','memory_index','memory_evaluation'].includes(work.kind) ? { memory: await memory.context(work) } : {}) }
    } },
    ...(options.delivery ? { delivery: options.delivery } : {}),
  }
  const sessions = new PgSessionStore(options.database)
  async function flushDeliveries() {
    if (!integration?.delivery) return
    await flushOutbox(options.database, 'agent_delivery_outbox', (row, context) => {
      const { leaseToken: _token, ...work } = workItemFromRow(row['delivery_work'] as Record<string, unknown>, '', Number(row['home_epoch']))
      return integration.delivery!.deliverMessage(work, row['message'] as AssistantMessage,
        { ...context, commit: { resultId: String(row['result_id']), fence: Number(row['result_fence']) } })
    }, { signal: shutdown.signal })
  }
  async function flushEvents() {
    if (!integration?.delivery) return
    await flushOutbox(options.database, 'agent_run_events', (row, context) => integration.delivery!.onEvent(
      row['delivery_work'] as Omit<WorkItem, 'leaseToken'>, { runId: String(row['run_id']), seq: Number(row['seq']),
        kind: String(row['kind']), stage: row['stage'] as RunEvent['stage'], visibility: row['visibility'] as RunEvent['visibility'],
        data: row['data'] as RunEvent['data'] }, context), { signal: shutdown.signal })
  }
  async function flushModelUsage() {
    if (!options.onModelCall) return
    await flushOutbox(options.database, 'agent_model_budget_calls', (row, context) => options.onModelCall!(
      row['observation'] as import('../model/execution.js').ModelCallObservation, context), { signal: shutdown.signal })
  }

  async function verifyNative(work: Omit<WorkItem, 'leaseToken'>, candidate: import('../outcome/verification.js').Candidate, database: SqlQueryable) {
    if (!options.verifyRun) return []
    const deadlineAt = new Date(Date.now() + 10_000).toISOString(), signal = AbortSignal.any([shutdown.signal,AbortSignal.timeout(10_000)])
    let active = true
    try {
      const records = await abortable(options.verifyRun({ work, candidate: structuredClone(candidate), requestVersion: candidate.requestVersion, signal, deadlineAt, database: {
        query: (sql, params) => { signal.throwIfAborted(); if (!active) throw new Error('verification ended'); return database.query(sql, params) },
      } }), signal)
      if (records.length > 64 || records.some(record => !record.checker.startsWith('product:')
        || !['passed','failed','inconclusive'].includes(record.status)) || new Set(records.map(record => record.checker)).size !== records.length) {
        throw new Error('native verification returned invalid acceptance records')
      }
      return records
    } finally { active = false }
  }

  const service = new ControlPlaneService({
    ...memory ? { memory } : {},
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
        records.push(...await verifyNative(work, candidate, client))
        records.push(...await inspectObligations(client, work, candidate, integration.tools, records))
        for (const record of records) await client.query(`INSERT INTO lingxios.agent_verifications
          (work_id,request_version,candidate_hash,checker,status,evidence) VALUES($1,$2,$3,$4,$5,$6::jsonb)
          ON CONFLICT(work_id,request_version,candidate_hash,checker) DO UPDATE SET
            status=EXCLUDED.status,evidence=EXCLUDED.evidence,observed_at=NOW()`,
          [work.id,candidate.requestVersion,hash,record.checker,record.status,JSON.stringify(record.evidence)])
      })
      return { requestVersion: candidate.requestVersion, candidateHash: hash, records }
    },
    logger, metrics,
    work: new PgWorkStore(options.database), sessions,
    events: new PgEventStore(options.database, Boolean(integration?.delivery)), actions: new PgActionLedger(options.database),
    modelBudgets: new PgModelBudgetStore(options.database), modelBudget,
    artifactStager: { stage: (work, artifact, bytes) => stageArtifact(
      resolve(options.homesRoot ?? process.env['AGENT_OS_HOMES_ROOT'] ?? '.agent-os/homes'), work, artifact, bytes) },
    contextProvider: integration.contextProvider,
    capabilityResolver: integration.capabilityResolver,
    actionExecutor: integration.actionExecutor,
    delivery: {
      getMessage: async work => {
        const result = await options.database.query(`SELECT result.message FROM lingxios.agent_work_items work JOIN lingxios.agent_results result ON result.id=work.result_id
          WHERE work.id=$1 AND work.tenant_id=$2 AND work.agent_id=$3 AND work.session_id=$4`,
          [work.id, work.tenantId, work.agentId, work.sessionId])
        return (result.rows[0]?.['message'] as AssistantMessage | undefined) ?? null
      },
      onEvent: async () => { background('events', flushEvents) },
      deliverMessage: async (work, message) => {
        const recordMemory = options.memory && !['memory_synthesis','memory_index','memory_evaluation'].includes(work.kind)
        await persistArtifacts(resolve(options.homesRoot ?? process.env['AGENT_OS_HOMES_ROOT'] ?? '.agent-os/homes'), work, message)
        await withTransaction(options.database, async client => {
          const current = await client.query(
            `SELECT id FROM lingxios.agent_work_items
              WHERE id=$1 AND fence=$2 AND status='leased' AND lease_expires_at>NOW()
                AND cancel_requested_at IS NULL
                AND ($3::integer IS NULL OR jsonb_array_length(steer_inputs)+1=$3)
              FOR UPDATE`, [work.id, work.fence, message.envelope.requestVersion])
          if (!current.rows.length) throw new Error('request changed or lease expired before message persistence')
          if (presentations.length) {
            const rendered = await client.query(`SELECT i.intent,r.result FROM lingxios.agent_action_intents i JOIN lingxios.agent_action_ledger r USING(idempotency_key)
              WHERE i.intent->>'workId'=$1 AND (i.intent->>'requestVersion')::integer=$2 AND i.intent->'action'->>'action'='presentation.render'
                AND r.result->>'ok'='true' ORDER BY i.recorded_at,i.idempotency_key LIMIT 17`, [work.id, message.envelope.requestVersion])
            if (rendered.rows.length > 16) throw new Error('at most 16 presentation components may be committed')
            const components: TrustedPresentation[] = []
            for (const row of rendered.rows) {
              const intent = row['intent'] as import('../control-plane/stores.js').ActionIntent
              if (intent.toolContractHash !== toolContractHash(helperTools.find(tool => tool.action === 'presentation.render')!)) throw new Error('presentation contract changed before commit')
              await integration.actionExecutor.prepare(work, intent.action, { requestVersion: message.envelope.requestVersion,
                signal: AbortSignal.any([shutdown.signal, AbortSignal.timeout(10_000)]), deadlineAt: new Date(Date.now() + 10_000).toISOString() })
              components.push(structuredClone((row['result'] as { value: { presentation: TrustedPresentation } }).value.presentation))
            }
            if (components.length) message = { ...message, envelope: { ...message.envelope, presentations: components } }
          }
          const resultId = 'result:' + createHash('sha256').update(JSON.stringify([work.id, message])).digest('hex')
          const previous = await client.query(`SELECT result.id,result.message FROM lingxios.agent_work_items work
            JOIN lingxios.agent_results result ON result.id=work.result_id WHERE work.id=$1`, [work.id])
          const prior = previous.rows[0]
          if (prior && prior['id'] !== resultId && !['delegated','awaiting_input','awaiting_approval','blocked','partial']
            .includes((prior['message'] as AssistantMessage).envelope.goalOutcome.status)) throw new Error('a different response is already committed for this run')
          await client.query(`INSERT INTO lingxios.agent_results(id,work_id,request_version,fence,home_epoch,message)
            VALUES($1,$2,$3,$4,$5,$6::jsonb) ON CONFLICT(id) DO NOTHING`,
          [resultId,work.id,message.envelope.requestVersion,work.fence,work.homeEpoch,JSON.stringify(message)])
          if (message.envelope.goalOutcome.status === 'satisfied') {
            const nativeChecks = await verifyNative(work, { body: message.body, requestVersion: message.envelope.requestVersion, artifacts: message.envelope.artifacts }, client)
            if (nativeChecks.some(record => record.status !== 'passed')) {
              throw new Error('native acceptance changed before successful completion')
            }
            const hash = candidateHash({ body: message.body, requestVersion: message.envelope.requestVersion, artifacts: message.envelope.artifacts })
            const checks = await client.query(`SELECT checker,status FROM lingxios.agent_verifications
              WHERE work_id=$1 AND request_version=$2 AND candidate_hash=$3`, [work.id,message.envelope.requestVersion,hash])
            const obligations = await inspectObligations(client, work, { body: message.body, requestVersion: message.envelope.requestVersion,
              artifacts: message.envelope.artifacts }, integration.tools, [
              ...checks.rows.filter(row => !String(row['checker']).startsWith('product:')).map(row => ({ checker: String(row['checker']), status: row['status'] as VerificationRecord['status'], evidence: {} })), ...nativeChecks])
            if (obligations.some(record => record.status !== 'passed')) throw new Error('delivery obligations are unfulfilled or no longer current')
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
          if (recordMemory && !['awaiting_input','awaiting_approval','delegated'].includes(message.envelope.goalOutcome.status)) await captureMemoryEvidence(client, work, message,
            await authorizedScopes(options.memory!,identityOf(work),client),options.memory!.writePolicy)
          const status = workStatusOf({ status: 'completed', goalOutcome: message.envelope.goalOutcome })
          await client.query(`UPDATE lingxios.agent_work_items SET status=$3,result_id=$4,goal_outcome=$5::jsonb,
            lease_token_hash=NULL,lease_expires_at=NULL,finished_at=CASE WHEN $3='waiting' THEN NULL ELSE NOW() END,
            last_progress_at=NOW(),updated_at=NOW() WHERE id=$1 AND fence=$2`,
            [work.id, work.fence, status, resultId, JSON.stringify(message.envelope.goalOutcome)])
          await client.query('DELETE FROM lingxios.agent_os_session_leases WHERE work_id=$1 AND fence=$2', [work.id, work.fence])
          if (status !== 'waiting') await cancelDescendants(client, work.id)
          if (integration.delivery) await client.query('INSERT INTO lingxios.agent_delivery_outbox(result_id) VALUES($1) ON CONFLICT DO NOTHING', [resultId])
          await client.query(`INSERT INTO lingxios.agent_run_events(run_id,seq,tenant_id,agent_id,kind,stage,visibility,data,delivery_work)
            SELECT $1,COALESCE(MAX(seq),$2::bigint)+1,$3,$4,'response.committed','completed','user',$5::jsonb,$6::jsonb
            FROM lingxios.agent_run_events WHERE run_id=$1`,
          [work.id,(work.fence-1)*RUN_SEQUENCE_SPAN,work.tenantId,work.agentId,
            JSON.stringify({ resultId,requestVersion: message.envelope.requestVersion }), integration.delivery ? JSON.stringify(work) : null])
        })
      },
    },
  })
  async function claimWork(claimingWorkerId: string, requestId?: string, workKinds?: readonly string[]) {
    const work = await service.claim(claimingWorkerId, requestId, workKinds)
    if (!work) return null
    await service.reconcilePending(work)
    await executeDecidedApprovals(options.database, service, work)
    if (await recoverWait(options.database, service, work)) return null
    return work
  }
  const host: HostPort = {
    prepareMemoryReview: (work,action) => service.prepareMemoryReview(work,action),
    recordMemoryReview: (work,action,hash,review) => service.recordMemoryReview(work,action,hash,review),
    verifyCandidate: (work, candidate) => service.verifyCandidate(work, candidate),
    saveStep: (work, step) => service.saveStep(work, step),
    claimWork: async () => { throw new Error('connect a worker before claiming work') },
    heartbeat: work => service.heartbeat(work),
    loadContext: (work) => service.loadContext(work), executeAction: (work, action, signal) => service.executeAction(work, action, signal),
    reserveModelCall: (work, callId, limits) => service.reserveModelCall(work, callId, limits),
    recordModelUsage: (work, callId, usage, observation) => service.recordModelUsage(work, callId, usage, observation),
    recoverCell: (work, cellId) => service.recoverCell(work, cellId),
    recoverStep: (work, cellId) => service.recoverStep(work, cellId),
    stageArtifact: (work, artifact, bytes) => stageArtifact(
      resolve(options.homesRoot ?? process.env['AGENT_OS_HOMES_ROOT'] ?? '.agent-os/homes'), work, artifact, bytes),
    emitEvent: (work, event) => service.recordEvent(work, event), loadSession: (work, key) => service.getSession(work, key),
    saveSession: async (work, session) => { session.revision = (await service.saveSession(work, session)).revision },
    commitResult: (work, message) => service.commitResult(work, message), completeWork: (work, completion) => service.complete(work, completion),
    waitWork: (work, outcome) => service.waitWork(work, outcome),
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
    background('approvals', () => resumeDecidedApprovals(options.database))
    background('queue fairness', () => sweepQueuedWork(options.database))
    if (memory) background('memory synthesis', async () => {
      await retryMemorySynthesis(options.database)
      await scheduleMemoryReflection(options.database,options.memory!)
    })
  }, 1_000)
  const maintenanceTimer = setInterval(() => {
    background('storage maintenance', () => maintainStorage(options.database, homesRoot))
    background('metrics', () => refreshMetrics(options.database, metrics))
  }, 60_000)
  deliveryTimer.unref(); maintenanceTimer.unref()
  let stopped = false
  let stopPromise: Promise<void> | undefined
  let listening: Promise<number> | undefined
  let controlPlane: ControlPlaneServer | undefined
  return {
    /** An explicit in-process worker connection. Creating a control plane never executes work. */
    connectWorker(input: { workerId: string; workKinds: readonly string[] }): HostPort {
      if (stopped) throw new Error('application has stopped')
      if (!input.workerId.trim()) throw new Error('workerId is required')
      return { ...host, claimWork: async signal => {
        signal?.throwIfAborted()
        if (stopped) throw new Error('application has stopped')
        return claimWork(input.workerId, undefined, input.workKinds)
      } }
    },
    memory: memory?.api,
    metrics: () => metrics.expose(),
    listRuns: (query?: RunListQuery) => listRuns(options.database,query),
    readOperations: () => readOperations(options.database),
    readDiagnostics: (identity: RunIdentity) => readDiagnostics(options.database, identity),
    readRun: (identity: RunIdentity, transaction: SqlQueryable = options.database) => readRun(transaction, identity),
    readRunState: (identity: RunIdentity) => readRunState(options.database,identity),
    freezeEvolutionBenchmark: (tenantId: string, benchmark: EvolutionBenchmark) => freezeEvolutionBenchmark(options.database,tenantId,benchmark),
    /** Trusted administration APIs; the product authorizes the opaque scope before calling. */
    rollbackEvolution: (scope: MemoryScope, activeId: string, expectedVersion: number, targetId: string | null) =>
      withTransaction(options.database, db => rollbackEvolution(db,scope,activeId,expectedVersion,targetId)),
    async readEvolution(scope: MemoryScope) {
      return (await options.database.query(`SELECT m.id,m.kind,m.body,m.status,m.version,m.source_refs,e.benchmark_id,e.verdict,e.summary,e.evaluated_at,
        w.status AS evaluation_status,w.error AS evaluation_error FROM lingxios.agent_memories m
        JOIN lingxios.agent_evolution_evaluations e ON e.tenant_id=m.tenant_id AND e.memory_id=m.id
        LEFT JOIN lingxios.agent_work_items w ON w.id='evaluate:'||m.id AND w.tenant_id=m.tenant_id
        WHERE m.tenant_id=$1 AND m.scope_type=$2 AND m.scope_id=$3 AND m.origin='evolved' ORDER BY m.updated_at DESC,m.id LIMIT 64`,
      [scope.tenantId,scope.scopeType,scope.scopeId])).rows
    },
    revise: (identity: RunIdentity, text: string, transaction?: SqlQueryable, author?: import('../protocol/types.js').SteerInput['author']) => transaction
      ? reviseRun(transaction, identity, text, author) : withTransaction(options.database, db => reviseRun(db, identity, text, author)),
    async enqueueJob(input: JobInput, transaction?: SqlQueryable) {
      if (!input.principalId?.trim() || !input.text?.trim() || input.text.length > 100_000) throw new Error('job requires its authenticated principal and original request text')
      const { mode: _reservedMode, obligations: _reservedObligations, codeExecution: _reservedCodeExecution, deliveryMode: _reservedDeliveryMode, ...jobMeta } = input.meta ?? {}
      const work = { ...input, triggerRef: input.sourceRef ?? input.id ?? randomUUID(),
        meta: { ...jobMeta, ...policyMeta(input), text: input.text, authorName: input.authorName ?? 'User', attachments: snapshotAttachments(input.attachments ?? []) } }
      return transaction ? enqueueWork(transaction, work) : service.enqueue(work)
    },
    readApproval: (identity: ApprovalLookup) => readApproval(options.database, identity),
    decideApproval: (decision: ApprovalDecision) => decideApproval(options.database, decision),
    async reconcileAction(input: RunIdentity & { actionKey: string }) {
      if (!input.principalId?.trim()) throw new Error('authenticated principal is required')
      return withTransaction(options.database, async db => {
        const { rows } = await db.query(`SELECT work.status,work.cancel_requested_at,jsonb_array_length(work.steer_inputs)+1 AS version,
          (intent.intent->>'requestVersion')::integer AS action_version,intent.intent->'action'->>'action' AS action,
          COALESCE(resolved.result,receipt.result) AS result FROM lingxios.agent_action_intents intent
          JOIN lingxios.agent_work_items work ON work.id=intent.intent->>'workId'
          LEFT JOIN lingxios.agent_action_ledger receipt USING(idempotency_key)
          LEFT JOIN LATERAL (SELECT resolution->'result' AS result FROM lingxios.agent_action_resolutions
            WHERE idempotency_key=intent.idempotency_key ORDER BY resolution_seq DESC LIMIT 1) resolved ON TRUE
          WHERE intent.idempotency_key=$1 AND work.id=$2 AND work.tenant_id=$3 AND work.principal_id=$4
            AND work.agent_id=$5 AND work.session_id=$6 AND work.thread_id IS NOT DISTINCT FROM $7 FOR UPDATE OF work`,
          [input.actionKey,input.runId,input.tenantId,input.principalId,input.agentId,input.sessionId,input.threadId ?? null])
        const row = rows[0]
        if (!row) throw new Error('action is outside this principal and run')
        const receipt = row['result'] as import('../protocol/types.js').HostActionResult | null
        if (receipt && receipt.executionState !== 'unknown') return { state: 'settled' as const, result: receipt }
        if (row['cancel_requested_at'] || row['version'] !== row['action_version']) throw new Error('action belongs to a cancelled or revised request')
        if (!options.tools?.some(tool => tool.action === row['action'] && tool.reconcile)) return { state: 'unavailable' as const }
        if (row['status'] === 'leased') return { state: 'busy' as const }
        if (!['queued','waiting','blocked','partial','failed'].includes(String(row['status']))) throw new Error('run is not eligible for reconciliation')
        await db.query(`UPDATE lingxios.agent_work_items SET status='queued',available_at=NOW(),goal_outcome=NULL,error=NULL,
          finished_at=NULL,updated_at=NOW() WHERE id=$1`, [input.runId])
        return { state: 'queued' as const }
      })
    },
    maintenance: () => maintainStorage(options.database, homesRoot),
    /** Authenticate and authorize the caller before using this server API. */
    readArtifact: (identity: MessageIdentity & Pick<RequestInput, 'principalId' | 'threadId'>, path: string) =>
      readArtifact(options.database, resolve(options.homesRoot ?? process.env['AGENT_OS_HOMES_ROOT'] ?? '.agent-os/homes'), identity, path),
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
    async cancel(identity: MessageIdentity & Pick<RequestInput, 'principalId' | 'threadId'>, transaction?: SqlQueryable): Promise<boolean> {
      if (!identity || !identity.principalId?.trim()) throw new Error('authenticated principalId is required')
      return transaction ? cancelRun(transaction, identity) : withTransaction(options.database, db => cancelRun(db, identity))
    },
    async enqueue(input: RequestInput) {
      if (typeof input.text !== 'string' || !input.text.trim() || typeof input.principalId !== 'string' || !input.principalId.trim()) {
        throw new Error('non-empty request text and authenticated principalId are required')
      }
      return service.enqueue({ ...input, kind: 'turn', lane: 'interactive', triggerRef: input.sourceRef ?? input.id ?? randomUUID(),
        meta: { ...policyMeta(input), text: input.text, authorName: input.authorName ?? 'User', attachments: snapshotAttachments(input.attachments ?? []) },
      })
    },
    async enqueueDelegated(input: DelegatedRequestInput) {
      const { delegation, ...request } = input
      if (!delegation || delegation.parentRequest.workId !== delegation.parentWorkId
        || delegation.parentRequest.revisions.length + 1 !== delegation.parentRequestVersion) throw new Error('invalid delegated request')
      const requestPolicy = policyMeta(request)
      if (delegation.parentRequest.codeExecution === 'disabled') requestPolicy.codeExecution = 'disabled'
      const parentMode = delegation.parentRequest.mode
      if (parentMode === 'chat' || parentMode === 'read' && requestPolicy.mode !== 'chat') requestPolicy.mode = parentMode
      return service.enqueue({ ...request, kind: 'turn', lane: 'collaboration', triggerRef: request.sourceRef ?? request.id ?? randomUUID(),
        meta: { ...requestPolicy, text: request.text, authorName: request.authorName ?? 'Agent', attachments: snapshotAttachments(request.attachments ?? []),
          parentWorkId: delegation.parentWorkId, rootWorkId: delegation.rootWorkId,
          parentRequestVersion: delegation.parentRequestVersion, delegation: { ...delegation, assignment: request.text } },
      })
    },
    async readMessage(identity: MessageIdentity): Promise<AssistantMessage | null> {
      const { rows } = await options.database.query(
        `SELECT result.message FROM lingxios.agent_work_items work JOIN lingxios.agent_results result ON result.id=work.result_id
          WHERE work.id=$1 AND work.tenant_id=$2 AND work.agent_id=$3 AND work.session_id=$4`,
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
    async readUsage(identity: MessageIdentity) {
      const { rows } = await options.database.query(`SELECT
        (SELECT COALESCE(MAX(seq),0) FROM lingxios.agent_run_events WHERE run_id=work.id) AS last_seq,
        COUNT(calls.call_id)::integer AS calls,COUNT(calls.call_id) FILTER (WHERE calls.observation IS NULL)::integer AS pending_calls,
        COUNT(calls.call_id) FILTER (WHERE calls.observation->'cost'->>'usage'='estimated')::integer AS estimated_calls,
        COALESCE(SUM(calls.input_tokens),0) AS input_tokens,COALESCE(SUM(calls.output_tokens),0) AS output_tokens,
        COALESCE(SUM(COALESCE(calls.cost_micros,calls.reserved_cost_micros)),0) AS cost_micros
        FROM lingxios.agent_work_items work LEFT JOIN lingxios.agent_model_budget_calls calls ON calls.work_id=work.id
        WHERE work.id=$1 AND work.tenant_id=$2 AND work.agent_id=$3 AND work.session_id=$4 GROUP BY work.id`,
        [identity.runId,identity.tenantId,identity.agentId,identity.sessionId])
      const row = rows[0]
      return row ? { lastSeq: Number(row['last_seq']), calls: Number(row['calls']), pendingCalls: Number(row['pending_calls']),
        estimatedCalls: Number(row['estimated_calls']), inputTokens: Number(row['input_tokens']), outputTokens: Number(row['output_tokens']), costMicros: Number(row['cost_micros']) } : null
    },
    async readDelivery(identity: MessageIdentity): Promise<'pending' | 'delivered' | 'failed' | 'not_observed' | null> {
      const { rows } = await options.database.query(
        `SELECT CASE WHEN outbox.result_id IS NULL THEN 'not_observed'
           WHEN outbox.failed_at IS NOT NULL THEN 'failed' WHEN outbox.delivered_at IS NULL THEN 'pending' ELSE 'delivered' END AS state
         FROM lingxios.agent_work_items work
         LEFT JOIN lingxios.agent_delivery_outbox outbox ON outbox.result_id=work.result_id
         WHERE work.id=$1 AND work.tenant_id=$2 AND work.agent_id=$3 AND work.session_id=$4 AND work.result_id IS NOT NULL`,
        [identity.runId, identity.tenantId, identity.agentId, identity.sessionId])
      return rows[0]?.['state'] as 'pending' | 'delivered' | 'failed' | 'not_observed' | undefined ?? null
    },
    retryDelivery: (identity: RunIdentity, channel: 'message' | 'events' | 'usage' = 'message') => retryDelivery(options.database,identity,channel),
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
    stop() {
      stopped = true
      shutdown.abort(new Error('control plane stopped'))
      clearInterval(deliveryTimer); clearInterval(maintenanceTimer)
      stopPromise ??= (async () => {
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
