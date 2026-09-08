import { abortable } from '../deadline.js'
import type { HostPort } from './port.js'
import { LATENCY_BUCKETS, type MetricsRegistry } from '../metrics.js'

// Explicit signal positions also cover methods whose optional filter follows the signal.
const signalIndex: Record<keyof HostPort, number> = { claimWork: 0, recoverWork: 1, waitForWork: 2, streamPreview: 2, heartbeat: 1, loadContext: 1, loadInitialContext: 1, reserveModelCall: 3,
  prepareMemoryReview: 2,recordMemoryReview: 4,
  recordModelUsage: 4, executeAction: 2, recoverCell: 2, recoverStep: 2, stageArtifact: 3, emitEvent: 2,
  loadSession: 2, saveSession: 2, commitResult: 2, completeWork: 2, waitWork: 2, yieldWork: 1, verifyCandidate: 2, saveStep: 2 }

/** Apply the same attempt cancellation to every local/HTTP call, even an uncooperative transport. */
export function deadlineHost(host: HostPort, signal?: AbortSignal, timeoutMs = 30_000, metrics?: MetricsRegistry): HostPort {
  return new Proxy(host, { get(target, property, receiver) {
    const value: unknown = Reflect.get(target,property,receiver)
    if (typeof value !== 'function' || !Object.hasOwn(signalIndex,property)) return value
    return (...args: unknown[]) => {
      const index = signalIndex[property as keyof HostPort]
      const supplied = args[index] as AbortSignal | undefined
      // Billing must settle an already-issued provider call after cancellation.
      const lifecycle = property === 'recordModelUsage' ? undefined : signal
      const bounded = AbortSignal.any([...property === 'streamPreview' ? [] : [AbortSignal.timeout(property === 'recordModelUsage' ? 5000 : timeoutMs)],
        ...lifecycle ? [lifecycle] : [], ...supplied ? [supplied] : []])
      bounded.throwIfAborted()
      args[index] = bounded
      const began = performance.now()
      return abortable(Reflect.apply(value,target,args) as Promise<unknown>,bounded).finally(() => {
        metrics?.histogram('agentos_host_call_seconds', 'Host call latency including transport', LATENCY_BUCKETS)
          .observe((performance.now() - began) / 1000, { operation: String(property) })
        metrics?.counter('agentos_host_calls_total', 'Host calls including checkpoints').inc({ operation: String(property) })
      })
    }
  } })
}
