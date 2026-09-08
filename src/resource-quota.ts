import { LATENCY_BUCKETS, type MetricsRegistry } from './metrics.js'

export type ResourcePriority = 'foreground' | 'background'

/** A slot is held until the underlying operation settles, even if it ignores cancellation. */
export class ResourceQuota {
  private active = 0
  private background = 0
  private readonly waiters: Array<{ priority: ResourcePriority; start: () => void }> = []
  constructor(readonly capacity: number, private readonly maxQueued = 1024, private readonly reservedForeground = 0,
    private readonly metrics?: MetricsRegistry, private readonly name = 'resource') {
    if (!Number.isSafeInteger(capacity) || capacity < 1 || capacity > 1024) throw new Error('resource capacity must be 1-1024')
    if (!Number.isSafeInteger(maxQueued) || maxQueued < 0 || maxQueued > 1024) throw new Error('invalid resource queue limit')
    if (!Number.isSafeInteger(reservedForeground) || reservedForeground < 0 || reservedForeground >= capacity) throw new Error('invalid foreground reservation')
  }

  run<T>(operation: () => Promise<T>, signal?: AbortSignal, priority: ResourcePriority = 'foreground'): Promise<T> {
    if (signal?.aborted) return Promise.reject(signal.reason)
    const queuedAt = performance.now()
    const start = () => {
      this.metrics?.histogram('agentos_resource_queue_wait_seconds', 'Resource admission queue wait', LATENCY_BUCKETS)
        .observe((performance.now() - queuedAt) / 1000, { resource: this.name, priority })
      let cancelledAt: number | undefined
      const cancelled = () => { cancelledAt ??= performance.now() }
      signal?.addEventListener('abort', cancelled, { once: true })
      this.active++; if (priority === 'background') this.background++
      const release = () => {
        signal?.removeEventListener('abort', cancelled)
        if (cancelledAt !== undefined) this.metrics?.histogram('agentos_cancel_resource_release_seconds', 'Cancellation to actual resource settlement', LATENCY_BUCKETS)
          .observe((performance.now() - cancelledAt) / 1000, { resource: this.name })
        this.active--; if (priority === 'background') this.background--
        this.drain()
      }
      try {
        signal?.throwIfAborted()
        const pending = operation()
        void pending.then(release, release)
        return pending // Preserve already-completed provider usage when abort arrives in the same turn.
      } catch (error) { release(); return Promise.reject<T>(error) }
    }
    if (this.available(priority) && !this.waiters.length) return start()
    if (this.waiters.length >= this.maxQueued) return Promise.reject(new Error('resource queue is full'))
    return new Promise<T>((resolve, reject) => {
      const entry = { priority, start: () => {
        signal?.removeEventListener('abort', cancel)
        void start().then(resolve, reject)
      } }
      const cancel = () => {
        const index = this.waiters.indexOf(entry)
        if (index >= 0) this.waiters.splice(index, 1)
        signal?.removeEventListener('abort', cancel)
        reject(signal?.reason)
      }
      this.waiters.push(entry); signal?.addEventListener('abort', cancel, { once: true }); this.drain()
    })
  }

  private available(priority: ResourcePriority) {
    return this.active < this.capacity && (priority === 'foreground' || this.background < this.capacity - this.reservedForeground)
  }
  private drain() {
    // FIFO among eligible jobs: background keeps a fair share of non-reserved capacity.
    for (let index = this.waiters.findIndex(item => this.available(item.priority)); index >= 0;
      index = this.waiters.findIndex(item => this.available(item.priority))) this.waiters.splice(index, 1)[0]!.start()
  }
}

/** Reject overload before retaining bytes; callers release in finally after real resource cleanup. */
export class ByteBudget {
  private active = 0
  private readonly tenants = new Map<string, number>()
  constructor(private readonly total = 64 * 1024 * 1024, private readonly perTenant = 32 * 1024 * 1024) {}
  acquire(tenant: string, bytes: number): () => void {
    const used = this.tenants.get(tenant) ?? 0
    if (!Number.isSafeInteger(bytes) || bytes < 0 || this.active + bytes > this.total || used + bytes > this.perTenant) throw new Error('in-flight byte budget exceeded')
    this.active += bytes; this.tenants.set(tenant, used + bytes)
    let released = false
    return () => {
      if (released) return
      released = true; this.active -= bytes
      const remaining = this.tenants.get(tenant)! - bytes
      if (remaining) this.tenants.set(tenant, remaining); else this.tenants.delete(tenant)
    }
  }
}
