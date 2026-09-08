/** Product-authenticated identity; administrative reads do not require a worker lease. */
export interface MemoryIdentity {
  tenantId: string
  agentId: string
  principalId: string
  sessionId: string
  threadId?: string
  workId?: string
}

export interface MemoryScope { tenantId: string; scopeType: string; scopeId: string }
export type MemoryLayer = 'core' | 'reference'
export interface MemorySource {
  sourceRef: string
  authorId: string
  workId?: string
  requestVersion?: number
  inputSha256?: string
  actionId?: string
  synthesisWorkId?: string
  observedAt?: string
  confidence?: number
}
export interface MemoryDocument extends MemoryScope {
  id: string
  path: string
  title: string
  description: string
  body: string
  layer: MemoryLayer
  kind: string
  origin: 'explicit' | 'synthesized' | 'evolved'
  locked: boolean
  version: number
  status: 'candidate' | 'active' | 'retired' | 'expired'
  sources: MemorySource[]
  validUntil: string | null
  updatedAt: string
}
export type MemoryEntry = Omit<MemoryDocument, 'body' | 'sources'>
export interface MemoryPage<T> { items: T[]; nextCursor: string | null }
export interface MemoryListQuery { prefix?: string; layer?: MemoryLayer; limit?: number; cursor?: string; includeInactive?: boolean }
export interface MemorySearchQuery { query: string; limit?: number; target?: 'documents' | 'history'; cursor?: string }
export interface MemoryHit extends MemoryEntry { excerpt: string; score: number }
export interface MemoryHistoryHit {
  sourceRunId: string
  sessionId: string
  sourceRef: string
  requestVersion: number
  role: 'user' | 'assistant'
  text: string
  truncated: boolean
  observedAt: string
}
export interface MemorySearchResult extends MemoryPage<MemoryHit | MemoryHistoryHit> {
  retrieval: 'browse' | 'keyword' | 'hybrid' | 'keyword_embedding_unavailable' | 'optional_timeout' | 'optional_deferred'
}
export interface MemoryContent {
  path: string
  title: string
  description: string
  body: string
  layer: MemoryLayer
  kind?: string
  locked?: boolean
  validUntil?: string | null
}
export type MemoryChange = { action: 'create'; content: MemoryContent }
  | { action: 'update'; id: string; expectedVersion: number; content: MemoryContent }
  | { action: 'move'; id: string; expectedVersion: number; path: string }
  | { action: 'expire'; id: string; expectedVersion: number }
  | { action: 'merge'; id: string; expectedVersion: number; content: MemoryContent; from: Array<{ id: string; expectedVersion: number }> }
  | { action: 'delete'; id: string; expectedVersion: number }
export interface MemoryApplyInput { scope: MemoryScope; changes: MemoryChange[]; idempotencyKey: string; sourceRef: string }
export interface MemoryApplyResult { documents: MemoryDocument[]; deleted: string[] }
export interface MemoryVersion { version: number; snapshot: MemoryDocument; replacedAt: string | null }
export interface MemoryRestoreInput { scope: MemoryScope; id: string; expectedVersion: number; version: number; idempotencyKey: string; sourceRef: string }
export interface MemoryDiagnostics {
  coreBytes: number
  budgetTokens: number
  overBudget: boolean
  duplicates: string[][]
  brokenLinks: Array<{ path: string; target: string }>
  expired: string[]
  conflicts: Array<{ id: string; sourceRunIds: string[]; reason: string; memoryIds: string[] }>
  failedReflections: Array<{ id: string; attempts: number; error: string | null }>
  nextCursor: string | null
}
export interface MemorySnapshot {
  id: string
  status: 'available' | 'unavailable'
  core: MemoryDocument[]
  directory: MemoryEntry[]
  recalled: MemoryHit[]
  strategies: Array<Record<string, unknown>>
  omitted: { core: number; directory: number; recalled: number; strategies: number }
  budget: { ratio: number; maxTokens: number }
  retrieval: MemorySearchResult['retrieval'][]
}
export interface MemoryReview {
  approved: boolean
  explicit: boolean
  confidence: number
}
export interface MemoryReviewRequest {
  hash: string
  input: {
    request: { originalText: string; revisions: Array<{ text: string }>; delegated: boolean }
    action: string
    args: Record<string, unknown>
    documents: MemoryDocument[]
  }
}
