import type { PreviewFrame } from '../protocol/preview.js'

/** Two coalesced frames at most. Backpressure never queues one promise/HTTP call per token. */
export class PreviewBuffer implements AsyncIterable<PreviewFrame> {
  private pending: PreviewFrame[] = []
  private seq = 0
  private identity = { requestVersion: 1, attemptId: '' }
  private wake: (() => void) | undefined
  private timer: NodeJS.Timeout | undefined
  private closed = false
  private enabled = false
  private first = true
  private bytes = 0

  reset(attemptId: string, requestVersion: number, enabled = true): void {
    if (this.closed) return
    this.identity = { attemptId, requestVersion }
    this.enabled = enabled
    this.first = true
    this.bytes = 0
    this.pending = [{ ...this.identity, seq: ++this.seq, kind: 'reset', text: '' }]
    this.flush()
  }

  push(text: string): void {
    if (!this.enabled || this.closed || !text) return
    this.bytes += text.length
    const last = this.pending.at(-1)
    if (this.bytes > 100_000 || (last?.kind === 'delta' ? last.text.length : 0) + text.length > 16_384) {
      // ponytail: a lagging preview resets; reliable final results recover from the database.
      this.reset(this.identity.attemptId, this.identity.requestVersion, false)
      return
    }
    if (last?.kind === 'delta') last.text += text
    else this.pending.push({ ...this.identity, seq: ++this.seq, kind: 'delta', text })
    if (this.first) { this.first = false; this.flush() }
    else this.timer ??= setTimeout(() => this.flush(), 50)
  }

  private flush(): void {
    if (this.timer) clearTimeout(this.timer)
    this.timer = undefined
    this.wake?.()
    this.wake = undefined
  }

  close(): void { this.closed = true; this.flush() }
  discard(): void { if (this.identity.attemptId) this.reset(this.identity.attemptId, this.identity.requestVersion, false) }

  async *[Symbol.asyncIterator](): AsyncGenerator<PreviewFrame> {
    while (true) {
      if (this.timer || !this.pending.length && !this.closed) await new Promise<void>(resolve => { this.wake = resolve })
      const frames = this.pending.splice(0)
      for (const frame of frames) yield frame
      if (this.closed && !this.pending.length) return
    }
  }
}
