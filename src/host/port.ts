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
  verifyCandidate?(work: WorkItem, candidate: import('../outcome/verification.js').Candidate): Promise<import('../outcome/verification.js').CandidateVerification>
  saveStep?(work: WorkItem, step: import('../control-plane/steps.js').ExecutionStep): Promise<void>
  lecture?(work: WorkItem, command: import('../lecture-deck/transport.js').LectureCommand): Promise<unknown>
  /** Claim one queued work item, or null when none is available. */
  claimWork(signal?: AbortSignal): Promise<WorkItem | null>

  /** Renew the lease; also transports cancel/preempt/steer signals back. */
  heartbeat(work: WorkItem): Promise<HeartbeatResult>

  /** Load the full turn context for a claimed work item. */
  loadContext(work: WorkItem): Promise<TurnContext>

  reserveModelCall?(work: WorkItem, callId: string, limits: ModelBudgetLimits): Promise<ModelBudgetReservation>
  recordModelUsage?(work: WorkItem, callId: string, usage: { inputTokens: number; outputTokens: number; costMicros: number }, observation?: import('../model/execution.js').ModelCallObservation): Promise<void>

  /** Execute one host action under the work's lease and capability grant. */
  executeAction(work: WorkItem, action: HostAction): Promise<HostActionResult>

  /** Rebuild durable host-action observations for an interrupted Python cell. */
  recoverCell?(work: WorkItem, cellId: string): Promise<Array<{
    action: string; idempotencyKey: string; result: HostActionResult
  }> | null>

  /** Recover a completed cell output journaled before the session checkpoint. */
  recoverStep?(work: WorkItem, cellId: string): Promise<{ output: string; artifacts: import('../protocol/types.js').KernelArtifact[] } | null>

  /** Upload checked artifact bytes when worker and control plane do not share a filesystem. */
  stageArtifact?(work: WorkItem, artifact: import('../protocol/types.js').KernelArtifact, bytes: Uint8Array): Promise<void>

  /** Append one durable run event (also drives user-visible streaming). */
  emitEvent(work: WorkItem, event: RunEvent): Promise<void>

  loadSession(work: WorkItem, key: string): Promise<SessionRecord | null>
  saveSession(work: WorkItem, session: SessionRecord): Promise<void>

  /** Atomically persist the checked result, terminal state and delivery intent. */
  commitResult(work: WorkItem, message: AssistantMessage): Promise<void>

  /** Terminal state transition for this attempt. */
  completeWork(work: WorkItem, completion: WorkCompletion): Promise<void>

  /** Requeue preempted work (fence advances; this lease is dead). */
  yieldWork(work: WorkItem): Promise<void>
}
