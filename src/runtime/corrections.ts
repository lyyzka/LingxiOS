import { createHash } from 'node:crypto'

export interface ProgressCheckpoint { last: string; count: number; observations: string[]; protocolRepairs?: number }

export type CorrectionCategory = 'content_acceptance' | 'tool_protocol' | 'kernel_error' | 'response_protocol'

const incidental = new Set(['observedAt', 'recordedAt', 'timestamp', 'durationMs', 'latencyMs', 'executionId', 'idempotencyKey', 'callId'])
function stableObservation(value: unknown, key = ''): unknown {
  if (Array.isArray(value)) return value.map(item => stableObservation(item))
  if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value)
    .filter(([name]) => !incidental.has(name)).sort(([a], [b]) => a.localeCompare(b)).map(([name, item]) => [name, stableObservation(item, name)]))
  // Resource revisions remain exact. Incidental clock output cannot manufacture progress.
  if (typeof value === 'string' && !/revision|version|sha256/i.test(key)) return value.replace(/\b\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|[+-]\d\d:\d\d)?\b/g, '<time>')
  return value
}

/** Only new observations reset the shared continuation bound, never a reworded error. */
export class CorrectionBudget {
  private last = ''
  private count = 0
  private protocolRepairs = 0
  private observations = new Set<string>()
  constructor(checkpoint?: ProgressCheckpoint) {
    if (checkpoint) { this.last = checkpoint.last; this.count = checkpoint.count; this.observations = new Set(checkpoint.observations); this.protocolRepairs = checkpoint.protocolRepairs ?? 0 }
  }
  snapshot(): ProgressCheckpoint { return { last: this.last, count: this.count, observations: [...this.observations], protocolRepairs: this.protocolRepairs } }
  consume(category: CorrectionCategory, failure = category as string): boolean {
    if (category === 'response_protocol' || category === 'tool_protocol') {
      this.protocolRepairs++
      if (this.protocolRepairs > 2) return false
    }
    const key = createHash('sha256').update(category + ':' + failure).digest('hex')
    this.count = Math.min(6, this.count + 1)
    this.last = key
    return this.count < 3
  }
  observe(value: unknown): void {
    const key = createHash('sha256').update(JSON.stringify(stableObservation(value))).digest('hex')
    if (this.observations.has(key)) return
    this.observations.add(key)
    if (this.observations.size > 2048) this.observations.delete(this.observations.values().next().value!)
    this.last = ''; this.count = 0
  }
  get rediagnose(): boolean { return this.count >= 2 }
  has(category: CorrectionCategory): boolean { return this.count < 2 && (!category.endsWith('protocol') || this.protocolRepairs < 2) }
}

/** Only resource versions, artifact content and action state transitions count as progress. */
export function progressFacts(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(progressFacts)
  if (!value || typeof value !== 'object') return undefined
  return Object.fromEntries(Object.entries(value).flatMap(([key, item]) => {
    if (['version', 'revision', 'sha256', 'ok', 'executionState', 'status', 'action'].includes(key)) return [[key, item]]
    const nested = progressFacts(item)
    return nested && (Array.isArray(nested) ? nested.some(Boolean) : Object.keys(nested).length) ? [[key, nested]] : []
  }))
}
