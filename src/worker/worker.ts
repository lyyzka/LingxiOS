/**
 * AgentWorker — the long-running worker process body: claim loop with bounded
 * concurrency, health/metrics endpoints, and graceful drain.
 *
 * Separated from `main.ts` so the whole lifecycle is testable in-process.
 */
import http from 'node:http'
import { setTimeout as delay } from 'node:timers/promises'
import type { AddressInfo } from 'node:net'
import { errorMessage } from '../errors.js'
import type { HostPort } from '../host/port.js'
import type { ManagedKernelExecutor } from '../kernel/manager.js'
import { nullLogger, type Logger } from '../logging.js'
import type { MetricsRegistry } from '../metrics.js'
import type { AgentRuntime } from '../runtime/runtime.js'
import { abortable } from '../deadline.js'

export interface AgentWorkerOptions {
  host: Pick<HostPort, 'claimWork' | 'waitForWork'>
  runtime: Pick<AgentRuntime, 'runWork'>
  kernels?: ManagedKernelExecutor
  workerId: string
  maxConcurrentRuns: number
  reservedInteractiveRuns?: number
  shutdownGraceMs: number
  pollIdleMs?: number
  healthPort?: number
  logger?: Logger
  metrics?: MetricsRegistry
  lastContactAt?: () => number
}

export class AgentWorker {
  private readonly active = new Map<string, Promise<void>>()
  private backgroundRuns = 0
  private readonly logger: Logger
  private readonly pollIdleMs: number
  private stopping = false
  private polling: Promise<void> | null = null
  private health: http.Server | null = null
  private started = false
  private lastClaimAt = 0
  private checked = false
  private readonly shutdown = new AbortController()
  private readonly stopPolling = new AbortController()
  private stopResult: Promise<{ timedOut: boolean }> | undefined

  constructor(private readonly options: AgentWorkerOptions) {
    const reserved = options.reservedInteractiveRuns ?? 0
    if (!Number.isSafeInteger(reserved) || reserved < 0 || reserved >= options.maxConcurrentRuns) throw new Error('reserved interactive runs must be below total concurrency')
    this.logger = (options.logger ?? nullLogger).child({ workerId: options.workerId })
    this.pollIdleMs = options.pollIdleMs ?? 750
  }

  get activeRuns(): number { return this.active.size }
  get draining(): boolean { return this.stopping }

  /** Execute one queued item, for isolated evaluations and embedded workers. */
  async runNext(): Promise<boolean> {
    if (this.started || this.stopping) throw new Error('worker has already been started or stopped')
    if (this.active.size >= this.options.maxConcurrentRuns) throw new Error('worker concurrency is full')
    await this.check()
    const work = await abortable(this.options.host.claimWork(this.stopPolling.signal),this.stopPolling.signal)
    if (this.stopping) return false
    if (!work) return false
    const running = this.options.runtime.runWork(work, this.shutdown.signal)
    this.active.set(work.id, running)
    try { await running; return true }
    finally { this.active.delete(work.id) }
  }

  async start(): Promise<{ healthPort: number | null }> {
    if (this.started || this.stopping) throw new Error('worker has already been started or stopped')
    this.started = true
    await this.check()
    let healthPort: number | null = null
    if (this.options.healthPort !== undefined) {
      this.health = http.createServer((req, res) => {
        if (req.url === '/healthz') {
          res.writeHead(200, { 'content-type': 'application/json' })
          res.end(JSON.stringify(this.status()))
          return
        }
        if (req.url === '/readyz') {
          res.writeHead(this.ready ? 200 : 503, { 'content-type': 'application/json' })
          res.end(JSON.stringify(this.status()))
          return
        }
        if (req.url === '/metrics' && this.options.metrics) {
          res.writeHead(200, { 'content-type': 'text/plain; version=0.0.4' })
          res.end(this.options.metrics.expose())
          return
        }
        res.writeHead(404).end()
      })
      healthPort = await new Promise<number>((resolveListen, rejectListen) => {
        this.health!.once('error', rejectListen)
        this.health!.listen(this.options.healthPort, () => {
          resolveListen((this.health!.address() as AddressInfo).port)
        })
      })
    }
    this.polling = this.poll()
    this.logger.info('worker started', { healthPort, maxConcurrentRuns: this.options.maxConcurrentRuns })
    return { healthPort }
  }

  private status(): Record<string, unknown> {
    return {
      ok: this.ready,
      workerId: this.options.workerId,
      activeRuns: this.active.size,
      maxConcurrentRuns: this.options.maxConcurrentRuns,
      draining: this.stopping,
      ...(this.options.kernels ? { kernels: this.options.kernels.size } : {}),
    }
  }

  get ready(): boolean { const contact = this.options.lastContactAt?.() ?? this.lastClaimAt; return this.checked && !this.stopping && contact > 0 && Date.now() - contact < 60_000 }

  async check(): Promise<void> {
    if (this.checked) return
    await this.options.kernels?.check?.()
    this.checked = true
  }

  private async poll(): Promise<void> {
    let wakeCursor: string | undefined
    while (!this.stopping) {
      try {
        if (this.active.size >= this.options.maxConcurrentRuns) {
          await Promise.race(this.active.values())
          continue
        }
        const backgroundLimit = this.options.maxConcurrentRuns - (this.options.reservedInteractiveRuns ?? 0)
        const work = await abortable(this.options.host.claimWork(this.stopPolling.signal,
          this.backgroundRuns >= backgroundLimit ? ['interactive', 'approval'] : undefined),this.stopPolling.signal)
        this.lastClaimAt = Date.now()
        if (this.stopping) return
        if (!work) {
          if (this.options.host.waitForWork) {
            wakeCursor = await this.options.host.waitForWork(wakeCursor, this.pollIdleMs, this.stopPolling.signal)
          } else await this.sleep(this.pollIdleMs)
          continue
        }
        if (this.active.has(work.id)) continue
        const background = !['interactive', 'approval'].includes(work.lane)
        if (background) this.backgroundRuns++
        this.options.metrics?.gauge('agentos_worker_active_runs', 'Runs in flight').set(this.active.size + 1)
        const done = this.options.runtime.runWork(work, this.shutdown.signal)
          .catch((error: unknown) => {
            this.logger.error('work escaped runtime handling', { workId: work.id, error: errorMessage(error) })
          })
          .finally(() => {
            this.active.delete(work.id)
            if (background) this.backgroundRuns--
            this.options.metrics?.gauge('agentos_worker_active_runs', 'Runs in flight').set(this.active.size)
          })
        this.active.set(work.id, done)
      } catch (error) {
        if (this.stopping) return
        this.logger.error('poll failed', { error: errorMessage(error) })
        await this.sleep(2_000)
      }
    }
  }

  private async sleep(ms: number): Promise<void> {
    try {
      await delay(ms, undefined, { ref: false, signal: this.stopPolling.signal })
    } catch (error) {
      if (!this.stopPolling.signal.aborted) throw error
    }
  }

  /**
   * Graceful drain: stop claiming, let in-flight runs finish inside the grace
   * window, then tear everything down. Idempotent.
   */
  stop(): Promise<{ timedOut: boolean }> {
    return this.stopResult ??= this.drain()
  }

  private async drain(): Promise<{ timedOut: boolean }> {
    this.stopping = true
    this.stopPolling.abort()
    let graceTimer: NodeJS.Timeout | undefined
    const timedOut = await Promise.race([
      Promise.allSettled([this.polling, ...this.active.values()]).then(() => false),
      new Promise<true>((resolveTimeout) => {
        graceTimer = setTimeout(() => resolveTimeout(true), this.options.shutdownGraceMs)
        graceTimer.unref?.()
      }),
    ])
    if (graceTimer) clearTimeout(graceTimer)
    if (timedOut) {
      this.shutdown.abort(new Error('worker shutdown grace period expired'))
      this.logger.error('shutdown grace period expired', { graceMs: this.options.shutdownGraceMs })
    }
    this.options.kernels?.close()
    if (this.health) {
      await new Promise<void>((resolveClose) => {
        this.health!.close(() => resolveClose())
        this.health!.closeAllConnections?.()
      })
    }
    this.logger.info('worker stopped', { timedOut })
    return { timedOut }
  }
}
