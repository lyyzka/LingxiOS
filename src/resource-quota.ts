/** A slot is held until the operation settles, even if its transport ignores cancellation. */
export class ResourceQuota {
  private active = 0
  private readonly waiters: Array<() => void> = []
  constructor(readonly capacity: number) {
    if (!Number.isSafeInteger(capacity) || capacity < 1 || capacity > 1024) throw new Error('resource capacity must be 1-1024')
  }

  run<T>(operation: () => Promise<T>, signal?: AbortSignal): Promise<T> {
    if (signal?.aborted) return Promise.reject(signal.reason)
    const release = () => {
      const next = this.waiters.shift()
      if (next) next()
      else this.active--
    }
    const start = () => {
      try {
        signal?.throwIfAborted()
        const pending = operation()
        void pending.then(release, release)
        return pending // Preserve an already-completed provider response when cancellation arrives with its usage.
      } catch (error) { release(); return Promise.reject(error) }
    }
    if (this.active >= this.capacity) {
      if (this.waiters.length >= 1024) return Promise.reject(new Error('resource queue is full'))
      return new Promise<void>((resolve, reject) => {
        const ready = () => { signal?.removeEventListener('abort', cancel); resolve() }
        const cancel = () => {
          const index = this.waiters.indexOf(ready)
          if (index >= 0) this.waiters.splice(index, 1)
          reject(signal?.reason)
        }
        this.waiters.push(ready)
        signal?.addEventListener('abort', cancel, { once: true })
      }).then(start)
    }
    this.active++
    return start()
  }
}
