/**
 * HostPort — the worker-side view of the control plane. The runtime depends
 * only on this interface; `HttpHostClient` is the production implementation
 * and tests use in-memory fakes.
 */
import type {
  AssistantMessage, HeartbeatResult, HostAction, HostActionResult,
  RunEvent, SessionRecord, TurnContext, WorkCompletion, WorkItem,
} from '../protocol/types.js'
import type { ModelBudgetLimits, ModelBudgetReservation } from '../control-plane/stores.js'

export interface HostPort {
  /** Private worker review channel; it is not exposed through the agent tool catalog or Kernel SDK. */
  prepareMemoryReview?(work: WorkItem,action: HostAction,signal?: AbortSignal): Promise<import('../memory/types.js').MemoryReviewRequest | null>
  recordMemoryReview?(work: WorkItem,action: HostAction,hash: string,review: import('../memory/types.js').MemoryReview,signal?: AbortSignal): Promise<void>
  verifyCandidate(work: WorkItem, candidate: import('../outcome/verification.js').Candidate, signal?: AbortSignal): Promise<import('../outcome/verification.js').CandidateVerification>
  saveStep(work: WorkItem, step: import('../control-plane/steps.js').ExecutionStep, signal?: AbortSignal): Promise<void>
  /** Claim one queued work item, or null when none is available. */
  claimWork(signal?: AbortSignal, lanes?: readonly WorkItem['lane'][]): Promise<WorkItem | null>
  /** Advisory wake cursor. Always scan durable work after wake/timeout/reconnect. */
  waitForWork?(cursor: string | undefined, timeoutMs: number, signal?: AbortSignal): Promise<string>
  /** One bounded, ephemeral stream per leased run; no token persistence or transport retries. */
  streamPreview?(work: WorkItem, frames: AsyncIterable<import('../protocol/preview.js').PreviewFrame>, signal?: AbortSignal): Promise<void>

  /** Renew the lease; also transports cancel/preempt/steer signals back. */
  heartbeat(work: WorkItem, signal?: AbortSignal): Promise<HeartbeatResult>

  /** Load the full turn context for a claimed work item. */
  loadContext(work: WorkItem, signal?: AbortSignal): Promise<TurnContext>
  /** Initial session and journal share a versioned snapshot, avoiding a second restore round trip. */
  loadInitialContext?(work: WorkItem, signal?: AbortSignal): Promise<TurnContext>

  reserveModelCall(work: WorkItem, callId: string, limits: ModelBudgetLimits, signal?: AbortSignal): Promise<ModelBudgetReservation>
  recordModelUsage(work: WorkItem, callId: string, usage: { inputTokens: number; outputTokens: number; costMicros: number }, observation?: import('../model/execution.js').ModelCallObservation, signal?: AbortSignal): Promise<void>

  /** Execute one host action under the work's lease and capability grant. */
  executeAction(work: WorkItem, action: HostAction, signal?: AbortSignal): Promise<HostActionResult>

  /** Rebuild durable host-action observations for an interrupted Python cell. */
  recoverCell(work: WorkItem, cellId: string, signal?: AbortSignal): Promise<Array<{
    action: string; idempotencyKey: string; result: HostActionResult
  }> | null>

  /** Recover a completed cell output journaled before the session checkpoint. */
  recoverStep(work: WorkItem, cellId: string, signal?: AbortSignal): Promise<{ output: string; artifacts: import('../protocol/types.js').KernelArtifact[] } | null>

  /** Upload checked artifact bytes when worker and control plane do not share a filesystem. */
  stageArtifact?(work: WorkItem, artifact: import('../protocol/types.js').KernelArtifact, bytes: Uint8Array, signal?: AbortSignal): Promise<void>

  /** Append one durable run event (also drives user-visible streaming). */
  emitEvent(work: WorkItem, event: RunEvent, signal?: AbortSignal): Promise<void>

  loadSession(work: WorkItem, key: string, signal?: AbortSignal): Promise<SessionRecord | null>
  saveSession(work: WorkItem, session: SessionRecord, signal?: AbortSignal): Promise<void>

  /** Atomically persist the checked result, terminal state and delivery intent. */
  commitResult(work: WorkItem, message: AssistantMessage, signal?: AbortSignal): Promise<void>

  /** Terminal state transition for this attempt. */
  completeWork(work: WorkItem, completion: WorkCompletion, signal?: AbortSignal): Promise<void>
  waitWork(work: WorkItem, outcome: import('../protocol/outcome.js').WaitingOutcome, signal?: AbortSignal): Promise<void>

  /** Requeue preempted work (fence advances; this lease is dead). */
  yieldWork(work: WorkItem, signal?: AbortSignal): Promise<void>
}
