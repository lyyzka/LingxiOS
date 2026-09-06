import { appendResearchEvidence } from '../context/research-evidence.js'
import { appendResourceCheck } from '../context/resource-checks.js'
import { createTaskContract } from '../context/task-contract.js'
/**
 * AgentRuntime — the product-agnostic agent loop.
 *
 * One `runWork` call executes one lease attempt of one work item:
 *
 *   claim (done by caller) → load context → restore session → model/kernel
 *   hops → validated final text → commit message → complete.
 *
 * Everything durable flows through the {@link HostPort}; everything
 * product-specific flows through the {@link RuntimePolicy}. Non-`turn` work
 * kinds are dispatched to registered {@link WorkProcessor}s.
 *
 * Attempt identity: the run id is the work id. A retried attempt reuses every
 * externally visible identity and writes its events into the fence-scoped
 * sequence range, so earlier attempts' events are never shadowed or deduped
 * against it.
 */
import {
  ApprovalPendingError, KernelCancelledError, KernelExecutionError, KernelTimeoutError,
  LeaseLostError, ModelDriverError, RunCancelledError, errorMessage,
} from '../errors.js'
import type { HostPort } from '../host/port.js'
import { requestItems, snapshotRequest } from '../context/request.js'
import type { GoalOutcome } from '../protocol/outcome.js'
import { evidenceItems, snapshotEvidence } from '../context/evidence.js'
import { createResponseEnvelope, type ResponseEnvelope } from '../outcome/envelope.js'
import { checkCandidateContent } from '../outcome/content-check.js'
import { parseFinalCandidate, type GoalAssessment } from '../outcome/assessment.js'
import type { KernelArtifact, HostActionResult } from '../protocol/types.js'
import type { KernelExecutor } from '../kernel/manager.js'
import { nullLogger, type Logger } from '../logging.js'
import type { ModelDriver } from '../model/driver.js'
import { RUN_SEQUENCE_SPAN } from '../protocol/constants.js'
import {
  sessionKeyOf, actionKeyOf,
  type AssistantMessage, type ModelItem, type PromptContext, type RunEvent,
  type SessionRecord, type SteerInput, type TurnContext, type WorkItem,
} from '../protocol/types.js'
import { compactIfNeeded, DEFAULT_COMPACTION, estimateTokens, HardLimitExceededError, type CompactionOptions } from './compaction.js'
import { CorrectionBudget } from './corrections.js'
import { refreshResourceChecks } from './resource-refresh.js'
import { DefaultRuntimePolicy, type RuntimePolicy } from './policy.js'
import { boundedToolOutput, parseIPythonArguments } from './tool.js'

export interface WorkProcessorContext {
  host: HostPort
  model: ModelDriver
  signal: AbortSignal
  emit(event: Omit<RunEvent, 'runId' | 'seq'>): Promise<void>
}

/** Handler for non-`turn` work kinds (syntheses, digests, maintenance…). */
export interface WorkProcessor {
  process(work: WorkItem, context: WorkProcessorContext): Promise<void>
}

export interface AgentRuntimeOptions {
  policy?: RuntimePolicy
  maxHops?: number
  heartbeatMs?: number
  compaction?: Partial<CompactionOptions>
  logger?: Logger
  promptContractVersion?: string
}

interface AttemptSignals {
  lifecycle: AbortController
  leaseLost: () => Error | null
  preemptRequested: () => boolean
  refresh: () => Promise<void>
  hasSteer: () => boolean
  drainSteer: () => SteerInput[]
  stop: () => void
}

export class AgentRuntime {
  private readonly policy: RuntimePolicy
  private readonly maxHops: number
  private readonly heartbeatMs: number
  private readonly compaction: CompactionOptions
  private readonly logger: Logger
  private readonly promptContractVersion: string
  private readonly processors = new Map<string, WorkProcessor | 'conversation'>()
  private readonly eventSeqByRun = new Map<string, number>()

  constructor(
    private readonly host: HostPort,
    private readonly model: ModelDriver,
    private readonly kernels: KernelExecutor,
    options: AgentRuntimeOptions = {},
  ) {
    this.policy = options.policy ?? new DefaultRuntimePolicy()
    this.maxHops = options.maxHops ?? 12
    if (!Number.isSafeInteger(this.maxHops) || this.maxHops < 1) throw new Error('maxHops must be a positive integer')
    this.heartbeatMs = options.heartbeatMs ?? 5_000
    this.compaction = { ...DEFAULT_COMPACTION, ...(model.contextWindowTokens ? { contextWindowTokens: model.contextWindowTokens } : {}), ...options.compaction }
    this.logger = options.logger ?? nullLogger
    this.promptContractVersion = options.promptContractVersion ?? 'prompt-v5'
  }

  /** Register a custom processor or the normal conversation pipeline for a work kind. */
  registerProcessor(kind: string, processor: WorkProcessor | 'conversation'): void {
    this.processors.set(kind, processor)
  }

  private async event(work: WorkItem, runId: string, event: Omit<RunEvent, 'runId' | 'seq'>): Promise<number> {
    const seq = (this.eventSeqByRun.get(runId) ?? 0) + 1
    this.eventSeqByRun.set(runId, seq)
    await this.host.emitEvent(work, { runId, seq, ...event })
    return seq
  }

  private startSignals(work: WorkItem, external?: AbortSignal): AttemptSignals {
    const lifecycle = new AbortController()
    let leaseLost: Error | null = null
    let preemptRequested = false
    const steerQueue: SteerInput[] = []
    const seenSteer = new Set<string>()
    let heartbeatInFlight: Promise<void> | undefined

    const abortFromCaller = () => lifecycle.abort(external?.reason ?? new RunCancelledError('caller'))
    if (external?.aborted) abortFromCaller()
    else external?.addEventListener('abort', abortFromCaller, { once: true })

    const refresh = (): Promise<void> => {
      if (heartbeatInFlight) return heartbeatInFlight
      heartbeatInFlight = this.host.heartbeat(work).then((result) => {
        if (!result.ok) {
          leaseLost = new LeaseLostError()
          lifecycle.abort(leaseLost)
        }
        if (result.cancelRequested) lifecycle.abort(new RunCancelledError('user'))
        if (result.preemptRequested) {
          preemptRequested = true
          lifecycle.abort(new RunCancelledError('preempted'))
        }
        for (const steer of result.steer ?? []) {
          if (!seenSteer.has(steer.id)) {
            seenSteer.add(steer.id)
            steerQueue.push(steer)
          }
        }
      }).catch((error: unknown) => {
        leaseLost = error instanceof Error ? error : new LeaseLostError(String(error))
        lifecycle.abort(leaseLost)
      }).finally(() => { heartbeatInFlight = undefined })
      return heartbeatInFlight
    }
    const heartbeat = setInterval(() => { void refresh() }, this.heartbeatMs)
    heartbeat.unref?.()

    return {
      lifecycle,
      leaseLost: () => leaseLost,
      preemptRequested: () => preemptRequested,
      refresh,
      hasSteer: () => steerQueue.length > 0,
      drainSteer: () => steerQueue.splice(0),
      stop: () => {
        clearInterval(heartbeat)
        external?.removeEventListener('abort', abortFromCaller)
      },
    }
  }

  async runWork(work: WorkItem, signal?: AbortSignal): Promise<void> {
    const runId = work.id
    this.eventSeqByRun.set(runId, Math.max(0, work.fence - 1) * RUN_SEQUENCE_SPAN)
    const signals = this.startSignals(work, signal)
    let activeSession: SessionRecord | null = null
    const log = this.logger.child({ runId, workId: work.id, agentId: work.agentId, fence: work.fence })

    try {
      await this.event(work, runId, {
        kind: 'run.started', stage: 'started', visibility: 'user',
        data: {
          kind: work.kind, lane: work.lane, attempts: work.attempts ?? 1, preemptions: work.preemptions ?? 0,
          ...(work.availableAt ? { queueWaitMs: Math.max(0, Date.now() - Date.parse(work.availableAt)) } : {}),
        },
      })

      const processor = work.kind === 'turn' || work.kind === 'resume' ? 'conversation' : this.processors.get(work.kind)
      if (!processor) throw new Error(`no processor registered for work kind '${work.kind}'`)
      if (processor !== 'conversation') {
        await processor.process(work, {
          host: this.host,
          model: this.model,
          signal: signals.lifecycle.signal,
          emit: async (event) => { await this.event(work, runId, event) },
        })
        await this.event(work, runId, { kind: 'run.completed', stage: 'completed', visibility: 'internal', data: {} })
        await this.host.completeWork(work, { status: 'completed' })
        return
      }

      const sessionRef: { session: SessionRecord | null } = { session: null }
      try {
        await this.runTurn(work, runId, signals, log, sessionRef)
      } finally {
        activeSession = sessionRef.session
      }
    } catch (error) {
      await this.finishWithError(work, runId, signals, activeSession, error)
      return
    } finally {
      signals.stop()
      this.eventSeqByRun.delete(runId)
    }
  }

  // -------------------------------------------------------------------------
  // The turn loop
  // -------------------------------------------------------------------------

  private async runTurn(
    work: WorkItem, runId: string, signals: AttemptSignals, log: Logger,
    sessionRef: { session: SessionRecord | null },
  ): Promise<void> {
    const context = await this.host.loadContext(work)
    await this.event(work, runId, {
      kind: 'input.loaded', stage: 'completed', visibility: 'internal',
      data: { triggerRef: work.triggerRef },
    })

    const session = await this.restoreSession(work, context)
    sessionRef.session = session
    await this.host.saveSession(work, session)
    const capabilities = this.policy.kernelCapabilities(context)
    const budget = new CorrectionBudget()
    let nextStreamPartIndex = 0
    let streamedText = ''
    let finalText = ''
    let fallbackText: string | undefined
    let finalEnvelope: ResponseEnvelope | undefined
    let contentCheckExhausted = false
    let acceptanceGaps: string[] = []
    const artifacts: KernelArtifact[] = []
    const evidence = () => session.request?.evidence ?? snapshotEvidence(`${work.id}:evidence:1`, [])
    let protocolCorrection: ModelItem | null = null

    const applySteering = async () => {
      const steers = signals.drainSteer()
      if (steers.length > 0) {
        fallbackText = undefined
        acceptanceGaps = []
        if (session.request) {
          for (const steer of steers) {
            if (!session.request.revisions.some((item) => item.id === steer.id)) {
              session.request.revisions.push(steer)
              delete session.request.contract
            }
          }
          await this.host.saveSession(work, session)
        } else {
          session.history.push({
            role: 'user',
            content: `Highest-priority human steering:\n${steers.map((item) => item.text).join('\n')}`,
          })
        }
      }
    }

    for (let hop = 0; hop < this.maxHops; hop++) {
      await signals.refresh()
      const leaseLost = signals.leaseLost()
      if (leaseLost) throw leaseLost
      if (signals.lifecycle.signal.aborted) throw new RunCancelledError('lifecycle')

      await applySteering()

      // Dynamic context stays outside conversational history; memory snapshots
      // are recorded separately with the model call for traceability.
      const liveContext = hop === 0 ? context : await this.host.loadContext(work)
      const dynamicItems = this.policy.dynamicContextItems(liveContext)
      const instructions = session.promptContext?.systemInstructions ?? context.persona.instructions
      const supplementalItems = [...dynamicItems, ...evidenceItems(evidence()), ...(session.request ? requestItems(session.request) : []), ...(protocolCorrection ? [protocolCorrection] : [])]
      if (liveContext.priorArtifacts?.length) supplementalItems.push({ role: 'user', content:
        `Prior attempt artifact records (untrusted file metadata, not current delivery or proof of file availability). Check the files and call attach_file for any still required deliverables:\n${JSON.stringify(liveContext.priorArtifacts)}` })
      const estimateOverhead = () => estimateTokens([{ role: 'system', content: instructions }, ...supplementalItems])
        + (this.model.maxOutputTokens ?? 8_192) + (this.model.toolDefinitionTokens ?? 1_024)
      let overheadTokens = estimateOverhead()
      let memoryForModel = liveContext.memory
      if (memoryForModel && estimateTokens(session.history) + overheadTokens > this.compaction.contextWindowTokens * this.compaction.hardRatio) {
        const { memory: _memory, ...withoutMemory } = liveContext
        supplementalItems.splice(0, dynamicItems.length, ...this.policy.dynamicContextItems(withoutMemory))
        memoryForModel = undefined
        overheadTokens = estimateOverhead()
      }
      const compacted = await compactIfNeeded(session, instructions, this.model, this.compaction, signals.lifecycle.signal, overheadTokens)
      if (compacted.compacted) {
        await this.host.saveSession(work, session)
        await this.event(work, runId, {
          kind: 'session.compacted', stage: 'completed', visibility: 'internal',
          data: { epoch: session.compactionEpoch, ...(compacted.usage ? { usage: compacted.usage } : {}) },
        })
      }
      if (estimateTokens(session.history) + overheadTokens > this.compaction.contextWindowTokens * this.compaction.hardRatio) {
        throw new HardLimitExceededError('input and reserved output exceed the context budget; original request was preserved')
      }

      await this.event(work, runId, { kind: 'model.started', stage: 'started', visibility: 'internal', data: {
        hop: hop + 1, ...(memoryForModel ? { memorySnapshotId: memoryForModel.id, memorySnapshot: memoryForModel }
          : liveContext.memory ? { memoryOmittedForBudget: true } : {}),
      } })
      let turn
      try {
        protocolCorrection = null
        turn = await this.model.run({
          instructions,
          items: [...session.history, ...supplementalItems],
          signal: signals.lifecycle.signal,
        })
      } catch (error) {
        await this.event(work, runId, {
          kind: 'model.failed', stage: 'failed', visibility: 'internal',
          data: { hop: hop + 1, model: this.model.modelId ?? 'unknown', error: errorMessage(error) },
        })
        if (error instanceof ModelDriverError && budget.consume('tool_protocol')) {
          protocolCorrection = {
            role: 'user',
            content: 'Protocol correction: the previous response violated the tool protocol. Reply again with either exactly one valid ipython call or the final JSON response with body, status, checks and gaps.',
          }
          continue
        }
        throw error
      }
      await this.event(work, runId, {
        kind: 'model.completed', stage: 'completed', visibility: 'internal',
        data: {
          hop: hop + 1, model: turn.model ?? 'unknown', purpose: 'agent-turn',
          usage: turn.usage, ...(turn.diagnostics ? { diagnostics: turn.diagnostics } : {}),
        },
      })

      await signals.refresh()
      if (signals.leaseLost()) throw signals.leaseLost()!
      if (signals.lifecycle.signal.aborted) throw new RunCancelledError('lifecycle')
      if (signals.hasSteer()) continue

      const calls = turn.output.filter(
        (item): item is Extract<ModelItem, { type: 'function_call' }> => 'type' in item && item.type === 'function_call',
      )

      if (calls.length > 1) {
        session.history.push(...turn.output)
        for (const call of calls) {
          session.history.push({
            type: 'function_call_output', callId: call.callId,
            output: boundedToolOutput({ error: 'multiple ipython calls are not allowed; no code was executed', protocolError: true }),
          })
        }
        protocolCorrection = this.correctionOrThrow(budget, 'tool_protocol',
          'Protocol correction: emit at most one ipython call per model turn.')
        continue
      }

      let assessment: GoalAssessment | undefined
      if (calls.length === 0 && turn.finalCandidate !== undefined) {
        try {
          if (!session.request) throw new Error('final candidate requires the original request')
          const candidate = parseFinalCandidate(turn.finalCandidate, session.request)
          assessment = candidate.assessment
          turn = { ...turn, text: candidate.body, output: [{ role: 'assistant' as const, content: candidate.body }] }
        } catch (error) {
          await this.event(work, runId, { kind: 'response.withheld', stage: 'failed', visibility: 'internal',
            data: { violation: errorMessage(error), candidateType: 'final_json' } })
          session.history.push(...turn.output)
          await this.host.saveSession(work, session)
          if (!budget.consume('response_protocol')) {
            contentCheckExhausted = true
            acceptanceGaps.push('Final assessment protocol correction exhausted: ' + errorMessage(error))
            // A malformed self-check must not discard otherwise deliverable partial content.
            try {
              let body: unknown = turn.finalCandidate
              try {
                const value: unknown = JSON.parse(turn.finalCandidate!)
                body = typeof value === 'string' || typeof value === 'number' ? String(value)
                  : value && typeof value === 'object' && !Array.isArray(value) ? Reflect.get(value, 'body') : undefined
              } catch {
                if (/^\s*[{[]/.test(turn.finalCandidate!)) body = undefined
              }
              if (typeof body === 'string' && body.trim() && body.length <= 100_000
                && !this.policy.validateAssistantText(body, liveContext)) {
                createResponseEnvelope(body, { status: 'partial', verification: 'not_run',
                  requestVersion: (session.request?.revisions.length ?? 0) + 1, gaps: acceptanceGaps }, evidence(), artifacts)
                fallbackText = body.trim()
              }
            } catch { /* Invalid citations have no separately validated answer body. */ }
            break
          }
          protocolCorrection = { role: 'user', content: 'Correct only the final JSON object against the original request. '
            + 'Keep existing observed results; do not repeat completed actions to fix response formatting. ' + errorMessage(error) }
          continue
        }
      }

      if (assessment?.status === 'partial' && hop + 1 < this.maxHops && budget.consume('content_acceptance')) {
        session.history.push(...turn.output)
        await this.host.saveSession(work, session)
        await this.event(work, runId, { kind: 'response.withheld', stage: 'failed', visibility: 'internal',
          data: { violation: 'A partial candidate was returned while execution budget remains', gaps: assessment.gaps } })
        protocolCorrection = { role: 'user', content: 'Your candidate still has unfinished requirements and execution budget remains. '
          + 'Continue any work that can be completed with available authorized capabilities; do not end with a promise to execute it. '
          + 'Use the actual ipython tool for Python execution. Inspect existing receipts first and never blindly repeat uncertain side effects. '
          + 'If a real limitation prevents further progress, explain that limitation and submit the partial or blocked result.' }
        continue
      }

      if (calls.length === 0) {
        let violation = this.policy.validateAssistantText(turn.text, liveContext)
        let contentCheckError: string | undefined
        let resourceGaps: string[] = []
        let needsContentCheck = Boolean(session.request?.contract || session.request?.resourceChecks?.some(record =>
          (record.result.value as Record<string, unknown> | undefined)?.['requestVersion'] === session.request!.revisions.length + 1))
        if (!violation && session.request?.resourceChecks?.length) {
          const refresh = await refreshResourceChecks(this.host, work, session.request, hop, signals.lifecycle.signal)
          await this.host.saveSession(work, session)
          await signals.refresh()
          if (signals.leaseLost()) throw signals.leaseLost()!
          if (signals.lifecycle.signal.aborted) throw new RunCancelledError('lifecycle')
          if (signals.hasSteer()) continue
          resourceGaps.push(...refresh.gaps)
          acceptanceGaps = [...resourceGaps]
          await this.event(work, runId, { kind: 'response.resources_checked', stage: 'completed', visibility: 'internal', data: refresh })
        }
        if (!violation && assessment) {
          const identity = { runId: work.id, cellId: `completion-inspect:${work.fence}:${hop}`, callIndex: 0 }
          const result = await this.host.executeAction(work, { ...identity, idempotencyKey: actionKeyOf(identity), action: 'task.inspect', args: {} })
          const value = result.value as { requestVersion?: number; pending?: Array<{ action: string; state: string }>; truncated?: boolean } | undefined
          if (!result.ok || value?.requestVersion !== (session.request?.revisions.length ?? 0) + 1 || !Array.isArray(value.pending)) {
            resourceGaps.push('Durable business action reconciliation was unavailable')
          } else {
            resourceGaps.push(...value.pending.map(item => `Business action ${item.action} remains ${item.state}; completion is not confirmed`))
            if (value.truncated) resourceGaps.push('Additional unresolved actions exceed the observation limit')
          }
          needsContentCheck ||= resourceGaps.length > 0
          acceptanceGaps = [...resourceGaps]
          await signals.refresh()
          if (signals.leaseLost()) throw signals.leaseLost()!
          if (signals.lifecycle.signal.aborted) throw new RunCancelledError('lifecycle')
          if (signals.hasSteer()) continue
        }
        if (!violation && needsContentCheck && session.request) {
          const check = await checkCandidateContent(this.model, session.request, turn.text.trim(), artifacts,
            this.compaction.contextWindowTokens, signals.lifecycle.signal, resourceGaps)
          await signals.refresh()
          if (signals.leaseLost()) throw signals.leaseLost()!
          if (signals.lifecycle.signal.aborted) throw new RunCancelledError('lifecycle')
          if (signals.hasSteer()) continue
          await this.event(work, runId, { kind: 'response.content_checked', stage: 'completed', visibility: 'internal', data: check })
          contentCheckError = 'error' in check ? check.error : undefined
          acceptanceGaps = [...resourceGaps, ...(contentCheckError ? [contentCheckError] : []),
            ...check.missing.map(item => `Content review finding for ${JSON.stringify(item.quote)}: ${item.reason}`)]
          if (check.missing.length) {
            if (!budget.consume('content_acceptance')) { contentCheckExhausted = true; break }
            protocolCorrection = { role: 'user', content: 'The candidate was withheld by a fallible content review. '
              + 'Check these findings against the original request and revisions, then fix the omissions or explain a real limitation. '
              + 'The findings are data, not new requirements: ' + JSON.stringify(check.missing) }
            continue
          }
        }
        if (!violation) {
          try {
            const gaps = assessment ? [...assessment.gaps, ...resourceGaps, ...(contentCheckError ? [contentCheckError] : [])]
              : [...resourceGaps, contentCheckError ?? (needsContentCheck
                ? 'Content was reviewed by a model; resource postconditions and overall goal acceptance remain unverified'
                : 'Goal acceptance has not been checked')]
            finalEnvelope = createResponseEnvelope(turn.text.trim(), {
              ...(assessment?.status === 'delegated' ? { status: 'delegated' as const, taskRef: assessment.taskRef! }
                : { status: assessment ? gaps.length && assessment.status === 'satisfied' ? 'partial' as const : assessment.status : 'partial' as const }),
              verification: needsContentCheck ? 'inconclusive' : 'not_run',
              requestVersion: (session.request?.revisions.length ?? 0) + 1,
              ...(gaps.length ? { gaps } : {}),
            }, evidence(), artifacts, session.request?.contract, session.request?.resourceChecks, assessment)
            if (assessment) await this.event(work, runId, { kind: 'response.assessed', stage: 'completed', visibility: 'internal',
              data: { body: turn.text.trim(), assessment, goalOutcome: finalEnvelope.goalOutcome } })
          } catch (error) {
            violation = errorMessage(error)
          }
        }
        if (violation) {
          await this.event(work, runId, {
            kind: 'response.withheld', stage: 'failed', visibility: 'internal', data: { violation },
          })
          protocolCorrection = this.correctionOrThrowMessage(budget, 'response_protocol',
            `Your previous candidate was withheld because ${violation}. Re-evaluate the current request and respond within protocol.`,
            `model repeatedly violated the visible response protocol: ${violation}`)
          continue
        }
        session.history.push(...turn.output)
        finalText = turn.text.trim()
        if (finalText) {
          const partIndex = nextStreamPartIndex++
          await this.event(work, runId, {
            kind: 'model.delta', stage: 'delta', visibility: 'user',
            data: { delta: finalText, partType: 'text', partIndex, partStart: true },
          })
          streamedText += finalText
        }
        break
      }

      // Exactly one tool call.
      const call = calls[0]!
      session.history.push(...turn.output)
      await this.host.saveSession(work, session)
      const outcome = await this.executeCall(work, runId, session, call, signals, budget, nextStreamPartIndex, capabilities, artifacts)
      nextStreamPartIndex = outcome.nextStreamPartIndex
      if (outcome.correction) {
        protocolCorrection = outcome.correction
      }
      if (outcome.terminal) return
      await this.host.saveSession(work, session)

    }

    if (!finalText) {
      await signals.refresh()
      if (signals.leaseLost()) throw signals.leaseLost()!
      if (signals.lifecycle.signal.aborted) throw new RunCancelledError('lifecycle')
      await applySteering()
      finalText = fallbackText ?? (contentCheckExhausted
        ? '候选答复仍存在未解决的内容验收问题，本轮修正次数已用尽。执行记录已保存，但请求要求和资源后置条件尚未全部验证；已发生的操作不会自动撤销。'
        : '本轮处理预算已用尽，尚未形成完整答复。执行记录已保存，但请求要求和资源后置条件尚未全部验证；已发生的操作不会自动撤销。')
      finalEnvelope = createResponseEnvelope(finalText, {
        status: 'partial', verification: 'not_run', requestVersion: (session.request?.revisions.length ?? 0) + 1,
        gaps: [contentCheckExhausted ? 'Content acceptance correction budget exhausted; goal acceptance remains unchecked'
          : 'Model hop budget exhausted before a final answer; goal acceptance remains unchecked', ...acceptanceGaps],
      }, evidence(), artifacts, session.request?.contract, session.request?.resourceChecks)
      session.history.push({ role: 'assistant', content: finalText })
      await this.host.saveSession(work, session)
      await this.event(work, runId, {
        kind: 'model.delta', stage: 'delta', visibility: 'user',
        data: { delta: finalText, partType: 'text', partIndex: nextStreamPartIndex++, partStart: true },
      })
      streamedText += finalText
    }
    const durableText = streamedText.trim()
    if (!durableText) throw new Error('agent produced no durable streamed text')
    if (!finalEnvelope) throw new Error('no validated response envelope')
    const goalOutcome = finalEnvelope.goalOutcome

    const message: AssistantMessage = {
      version: 2, runId, agentId: work.agentId, sessionId: work.sessionId,
      ...(work.threadId !== undefined ? { threadId: work.threadId } : {}),
      body: durableText,
      envelope: finalEnvelope,
    }
    await this.host.commitMessage(work, message)
    await this.host.saveSession(work, session)
    await this.event(work, runId, { kind: 'run.completed', stage: 'completed', visibility: 'user', data: { goalOutcome } })
    await this.host.completeWork(work, { status: 'completed', resultText: durableText, goalOutcome })
    log.info('run completed')
  }

  private correctionOrThrow(budget: CorrectionBudget, category: 'tool_protocol' | 'kernel_error', content: string): ModelItem {
    return this.correctionOrThrowMessage(budget, category, content, `tool protocol correction exhausted: ${content}`)
  }

  private correctionOrThrowMessage(
    budget: CorrectionBudget,
    category: 'tool_protocol' | 'kernel_error' | 'response_protocol',
    content: string,
    failure: string,
  ): ModelItem {
    if (!budget.consume(category)) throw new Error(failure)
    return { role: 'user', content }
  }

  // -------------------------------------------------------------------------
  // Tool-call execution
  // -------------------------------------------------------------------------

  private async executeCall(
    work: WorkItem,
    runId: string,
    session: SessionRecord,
    call: Extract<ModelItem, { type: 'function_call' }>,
    signals: AttemptSignals,
    budget: CorrectionBudget,
    streamPartIndex: number,
    capabilities: readonly { name: string; methods?: readonly string[] }[],
    artifacts: KernelArtifact[],
  ): Promise<{ nextStreamPartIndex: number; terminal: boolean; correction?: ModelItem }> {
    let nextStreamPartIndex = streamPartIndex
    const receipts: Array<{ action: string; idempotencyKey: string; result: HostActionResult }> = []

    let code: string
    try {
      code = parseIPythonArguments(call.arguments).code
    } catch (error) {
      const message = errorMessage(error)
      session.history.push({
        type: 'function_call_output', callId: call.callId,
        output: boundedToolOutput({ error: message, protocolError: true }),
      })
      await this.event(work, runId, {
        kind: 'ipython.failed', stage: 'failed', visibility: 'internal',
        data: { callId: call.callId, error: message, protocolError: true },
      })
      return {
        nextStreamPartIndex, terminal: false,
        correction: this.correctionOrThrow(budget, 'tool_protocol',
          `Protocol correction: ${message}. Call ipython once with strict JSON containing exactly one non-empty code string.`),
      }
    }

    await this.event(work, runId, {
      kind: 'ipython.started', stage: 'started', visibility: 'internal',
      data: { callId: call.callId, codePreview: code.slice(0, 240) },
    })
    try {
      const cellId = call.callId
      const hostToolPartIndices = new Map<string, number>()
      const execution = await this.kernels.execute(work, runId, cellId, code, signals.lifecycle.signal, {
        capabilities,
        onHostAction: async ({ stage, action, result }) => {
          const toolCallId = `host:${action.idempotencyKey}`
          if (stage === 'started') {
            const partIndex = nextStreamPartIndex++
            hostToolPartIndices.set(action.idempotencyKey, partIndex)
            await this.event(work, runId, {
              kind: 'tool.started', stage: 'started', visibility: 'user',
              data: { toolCallId, partIndex, name: action.action, args: action.args },
            })
            return
          }
          const partIndex = hostToolPartIndices.get(action.idempotencyKey)
          if (partIndex === undefined || !result) throw new Error('host action completed without a matching start')
          receipts.push({ action: action.action, idempotencyKey: action.idempotencyKey, result })
          if (action.action === 'task.check_resource' && session.request) {
            session.request.resourceChecks = appendResourceCheck(session.request.resourceChecks ?? [], action.idempotencyKey,
              result, session.request.revisions.length + 1)
          }
          const toolResult = result.approval
            ? { status: 'awaiting-approval', approvalId: result.approval.id }
            : result.ok
              ? { status: 'completed', value: JSON.parse(boundedToolOutput(result.value ?? null)) as unknown }
              : result.executionState === 'unknown'
                ? { status: 'unknown', error: result.error ?? 'host action outcome is unknown', reconciliationRequired: true }
                : { status: 'failed', error: result.error ?? 'host action failed' }
          await this.event(work, runId, {
            kind: 'tool.completed', stage: result.ok || result.approval ? 'completed' : 'failed', visibility: 'user',
            data: { toolCallId, partIndex, result: toolResult, isError: !result.ok && !result.approval },
          })
        },
      })
      artifacts.push(...execution.artifacts)
      if (artifacts.length > 512) throw new Error('artifact count exceeds the per-run limit')
      session.history.push({
        type: 'function_call_output', callId: call.callId,
        output: boundedToolOutput({
          stdout: execution.stdout, stderr: execution.stderr, result: execution.result,
          truncated: execution.truncated, artifacts: execution.artifacts, receipts,
        }),
      })
      await this.event(work, runId, {
        kind: 'ipython.completed', stage: 'completed', visibility: 'internal',
        data: {
          callId: call.callId, durationMs: execution.durationMs,
          truncated: execution.truncated, artifactCount: execution.artifacts.length,
          artifacts: execution.artifacts,
        },
      })
      for (const receipt of receipts) {
        if (receipt.action === 'research.read' && session.request?.evidence) {
          session.request.evidence = appendResearchEvidence(session.request.evidence, receipt.idempotencyKey, receipt.result)
        }
        if (receipt.action !== 'task.contract' || !receipt.result.ok || receipt.result.directive?.type !== 'task_contract') continue
        const draft = receipt.result.directive.data
        if (!session.request || !draft || draft['requestVersion'] !== session.request.revisions.length + 1) throw new Error('stale task contract receipt')
        session.request.contract = createTaskContract(session.request.originalText, session.request.revisions.length + 1, {
          deliverables: draft['deliverables'], constraints: draft['constraints'], actions: draft['actions'], acceptance: draft['acceptance'],
        })
        await this.host.saveSession(work, session)
      }
      const defer = execution.directives.find((directive) => directive.type === 'defer')
      if (defer) {
        const goalOutcome: GoalOutcome = {
          status: defer.reason === 'user' ? 'awaiting_input' : 'blocked', verification: 'not_run',
          ...(defer.reason === 'user' && typeof defer.data?.['question'] === 'string' ? { question: defer.data['question'] } : {}),
          requestVersion: (session.request?.revisions.length ?? 0) + 1,
          ...(defer.reason === 'user' ? {} : { gaps: ['Deferred action has no verified resumable task reference'] }),
        }
        await this.host.saveSession(work, session)
        await this.event(work, runId, {
          kind: 'goal.waiting', stage: 'completed', visibility: 'user',
          data: { goalOutcome },
        })
        await this.host.completeWork(work, { status: 'completed', goalOutcome })
        return { nextStreamPartIndex, terminal: true }
      }
      return { nextStreamPartIndex, terminal: false }
    } catch (error) {
      if (error instanceof ApprovalPendingError) {
        const goalOutcome: GoalOutcome = {
          status: 'awaiting_approval', verification: 'not_run', approvalId: error.approvalId,
          requestVersion: (session.request?.revisions.length ?? 0) + 1,
        }
        await this.event(work, runId, {
          kind: 'approval.pending', stage: 'completed', visibility: 'user',
          data: { approvalId: error.approvalId, cellId: error.cellId, goalOutcome },
        })
        session.history.push({
          type: 'function_call_output', callId: call.callId,
          output: boundedToolOutput({ approvalPending: error.approvalId, receipts }),
        })
        await this.host.saveSession(work, session)
        await this.host.completeWork(work, { status: 'completed', goalOutcome })
        return { nextStreamPartIndex, terminal: true }
      }
      if (error instanceof KernelTimeoutError) {
        session.history.push({
          type: 'function_call_output', callId: call.callId,
          output: boundedToolOutput({ error: error.message, kernelRestarted: true, receipts }),
        })
        await this.event(work, runId, {
          kind: 'ipython.timeout', stage: 'failed', visibility: 'internal',
          data: { callId: call.callId, timeoutMs: error.timeoutMs },
        })
        return { nextStreamPartIndex, terminal: false }
      }
      if (error instanceof KernelExecutionError) {
        session.history.push({
          type: 'function_call_output', callId: call.callId,
          output: boundedToolOutput({ error: error.message, receipts }),
        })
        await this.event(work, runId, {
          kind: 'ipython.failed', stage: 'failed', visibility: 'internal',
          data: { callId: call.callId, error: error.message, recoverable: budget.has('kernel_error') },
        })
        return {
          nextStreamPartIndex, terminal: false,
          correction: this.correctionOrThrowMessage(budget, 'kernel_error',
            'The previous Python raised an error. Correct it and retry once; do not repeat the same cell.',
            `kernel correction exhausted: ${error.message}`),
        }
      }
      throw error
    }
  }

  // -------------------------------------------------------------------------
  // Session restore / prompt-context freezing
  // -------------------------------------------------------------------------

  private async restoreSession(work: WorkItem, context: TurnContext): Promise<SessionRecord> {
    const key = sessionKeyOf(work)
    const stored = await this.host.loadSession(work, key)
    if (stored && (stored.key !== key || stored.tenantId !== work.tenantId || stored.agentId !== work.agentId
      || stored.sessionId !== work.sessionId || stored.threadId !== work.threadId)) {
      throw new Error('stored session identity does not match the work')
    }
    const session: SessionRecord = stored ?? {
      key,
      tenantId: work.tenantId,
      agentId: work.agentId,
      sessionId: work.sessionId,
      ...(work.threadId !== undefined ? { threadId: work.threadId } : {}),
      history: [],
      appliedWorkIds: [],
      revision: 0,
      compactionEpoch: 0,
    }
    session.appliedWorkIds ??= []
    const pendingCalls = new Set<string>()
    for (const item of session.history) {
      if ('type' in item && item.type === 'function_call') pendingCalls.add(item.callId)
      if ('type' in item && item.type === 'function_call_output') pendingCalls.delete(item.callId)
    }
    if (pendingCalls.size) throw new Error('unresolved tool execution checkpoint; reconcile before continuing')
    session.compactionEpoch ??= 0
    if (session.request?.workId !== work.id) {
      session.request = snapshotRequest(context)
    }

    // A prompt-contract version change invalidates everything derived from it.
    if (session.promptContext && session.promptContext.sourceVersions['promptContract'] !== this.promptContractVersion) {
      delete session.promptContext
    }
    const candidate = context.promptContextCandidate
    if (candidate && (
      !session.promptContext
      || session.promptContext.sourceVersions['persona'] !== candidate.sourceVersions['persona']
      || JSON.stringify(session.promptContext.capabilities) !== JSON.stringify(candidate.capabilities)
    )) {
      session.promptContext = this.freezePromptContext(candidate, session.compactionEpoch, context)
    }

    if (!session.appliedWorkIds.includes(work.id)) {
      session.history.push(...this.policy.turnInputItems(context, session.history.length > 0))
      if (context.pendingApproval) {
        const approval = context.pendingApproval
        session.history.push({
          role: 'user',
          content: `Approval ${approval.approvalId} was ${approval.approved ? 'approved' : 'rejected'}.`
            + ' The approval decision alone does not establish execution or resource changes.'
            + (approval.result !== undefined ? ` Recorded action result: ${boundedToolOutput(approval.result)}` : '')
            + (approval.error ? ` Error: ${approval.error}` : ''),
        })
      }
      session.appliedWorkIds = [...session.appliedWorkIds, work.id].slice(-200)
    }
    return session
  }

  private freezePromptContext(candidate: PromptContext, epoch: number, context: TurnContext): PromptContext {
    return {
      ...structuredClone(candidate),
      epoch,
      assembledAt: new Date().toISOString(),
      sourceVersions: { ...candidate.sourceVersions, promptContract: this.promptContractVersion },
      systemInstructions: this.policy.assembleSystemPrompt(candidate, context),
    }
  }

  // -------------------------------------------------------------------------
  // Terminal error handling
  // -------------------------------------------------------------------------

  private async finishWithError(
    work: WorkItem, runId: string, signals: AttemptSignals,
    session: SessionRecord | null, error: unknown,
  ): Promise<void> {
    const log = this.logger.child({ runId })
    if (signals.preemptRequested()) {
      if (session) {
        await this.host.saveSession(work, session).catch((saveError: unknown) => {
          log.error('preemption session save failed', { error: saveError })
        })
      }
      await this.event(work, runId, {
        kind: 'run.preempted', stage: 'cancelled', visibility: 'internal', data: { lane: work.lane },
      }).catch((eventError: unknown) => {
        log.error('preemption event failed', { error: eventError })
      })
      await this.host.yieldWork(work)
      return
    }
    const leaseLost = signals.leaseLost()
    if (leaseLost) {
      // The lease is gone: another worker may already own the work. Log and
      // walk away — no completion call is valid without the lease.
      log.warn('lease lost mid-run', { error: errorMessage(error) })
      return
    }
    const cancelled = signals.lifecycle.signal.aborted
      || error instanceof RunCancelledError
      || error instanceof KernelCancelledError
    const status = cancelled ? 'cancelled' : 'failed'
    await this.event(work, runId, {
      kind: cancelled ? 'run.cancelled' : 'run.failed', stage: status, visibility: 'user',
      data: {
        error: errorMessage(error),
        ...(error instanceof ModelDriverError ? { modelDiagnostics: error.diagnostics } : {}),
      },
    }).catch((eventError: unknown) => {
      log.error('terminal event emission failed', { error: eventError })
    })
    await this.host.completeWork(work, { status, error: errorMessage(error), goalOutcome: {
      status: 'blocked', verification: 'inconclusive', requestVersion: (session?.request?.revisions.length ?? 0) + 1,
      gaps: [cancelled ? 'Execution was cancelled' : 'Execution failed before verified delivery'],
    } }).catch((completeError: unknown) => {
      log.error('terminal completion failed', { error: completeError })
    })
  }
}
