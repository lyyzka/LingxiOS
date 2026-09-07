import { createHash } from 'node:crypto'

export interface ProgressCheckpoint { last: string; count: number; observations: string[] }

export type CorrectionCategory = 'content_acceptance' | 'tool_protocol' | 'kernel_error' | 'response_protocol'

/** Consecutive identical failures, not a lifetime allowance for unrelated errors. */
export class CorrectionBudget {
  private last = ''
  private count = 0
  private observations = new Set<string>()
  constructor(checkpoint?: ProgressCheckpoint) {
    if (checkpoint) { this.last = checkpoint.last; this.count = checkpoint.count; this.observations = new Set(checkpoint.observations) }
  }
  snapshot(): ProgressCheckpoint { return { last: this.last, count: this.count, observations: [...this.observations] } }
  consume(category: CorrectionCategory, failure = category as string): boolean {
    const key = createHash('sha256').update(category + ':' + failure).digest('hex')
    this.count = this.last === key ? this.count + 1 : 1
    this.last = key
    return this.count < 6
  }
  observe(value: string): void {
    const key = createHash('sha256').update(value).digest('hex')
    if (this.observations.has(key)) return
    this.observations.add(key)
    if (this.observations.size > 2048) this.observations.delete(this.observations.values().next().value!)
    this.last = ''; this.count = 0
  }
  get rediagnose(): boolean { return this.count >= 3 }
  has(_category: CorrectionCategory): boolean { return this.count < 5 }
}
