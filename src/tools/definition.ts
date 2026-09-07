import type { HostAction, HostActionResult, WorkItem } from '../protocol/types.js'
import type { SqlQueryable } from '../control-plane/pg-store.js'
import type { VerificationRecord } from '../outcome/verification.js'
import type { ToolDefinition as ToolSpecification } from './catalog.js'
import type { KernelArtifact } from '../protocol/types.js'

export interface ArtifactInput {
  path: string
  mime: string
  bytes: Uint8Array
  source?: { ref: string; version: string }
}

/** Trusted native context. The principal authorizes; the agent is the operator. */
export interface ActionContext {
  requestSnapshot(): Promise<import('../context/request.js').RequestSnapshot>
  enqueueChild(input: import('../app/jobs.js').ChildInput): Promise<{ id: string; deduplicated: boolean }>
  readChild(id: string): Promise<import('../app/jobs.js').RunSnapshot | null>
  cancelChild(id: string): Promise<boolean>
  reviseChild(id: string, text: string): Promise<boolean>
  work: Omit<WorkItem, 'leaseToken'>
  action: HostAction
  requestVersion: number | null
  /** Exact approved native preview, supplied only after version checks and reauthorization. */
  approvedPreview?: Record<string, unknown>
  database: SqlQueryable
  signal: AbortSignal
  deadlineAt: string
  createArtifact(input: ArtifactInput): Promise<KernelArtifact>
}

export type ActionResult = HostActionResult

/** One native definition owns the schema, permissions and effect semantics. */
export interface ToolDefinition<Input extends Record<string, unknown> = Record<string, unknown>> extends ToolSpecification {
  /** Use the same native schema that produced parameters. Must have no effects. */
  parse(input: unknown): Input
  /** Rechecked before execution, inside the transaction for transactional tools. */
  authorize(context: ActionContext, input: Input): Promise<void>
  execute(context: ActionContext, input: Input): Promise<ActionResult>
  preview?(context: ActionContext, input: Input): Promise<Record<string, unknown>>
  reconcile?(context: ActionContext, input: Input): Promise<ActionResult | null>
  /** Independently authorize the read scope; a deleted resource need not still exist. */
  verify?(context: ActionContext, input: Input, value: unknown): Promise<Omit<VerificationRecord, 'checker'>>
}

/** Only throw this when the native service can prove that nothing was committed. */
export class NoEffectError extends Error {
  constructor(message: string, readonly code = 'native_rejected') {
    super(message)
    this.name = 'NoEffectError'
  }
}
