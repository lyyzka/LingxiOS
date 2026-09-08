import { abortable } from '../deadline.js'
import type { HostPort } from './port.js'

// The final parameter is always the signal, including methods with optional data.
const signalIndex: Record<keyof HostPort, number> = { claimWork: 0, heartbeat: 1, loadContext: 1, reserveModelCall: 3,
  prepareMemoryReview: 2,recordMemoryReview: 4,
  recordModelUsage: 4, executeAction: 2, recoverCell: 2, recoverStep: 2, stageArtifact: 3, emitEvent: 2,
  loadSession: 2, saveSession: 2, commitResult: 2, completeWork: 2, waitWork: 2, yieldWork: 1, verifyCandidate: 2, saveStep: 2 }

/** Apply the same attempt cancellation to every local/HTTP call, even an uncooperative transport. */
export function deadlineHost(host: HostPort, signal?: AbortSignal, timeoutMs = 30_000): HostPort {
  return new Proxy(host, { get(target, property, receiver) {
    const value: unknown = Reflect.get(target,property,receiver)
    if (typeof value !== 'function' || !Object.hasOwn(signalIndex,property)) return value
    return (...args: unknown[]) => {
      const index = signalIndex[property as keyof HostPort]
      const supplied = args[index] as AbortSignal | undefined
      // Billing must settle an already-issued provider call after cancellation.
      const lifecycle = property === 'recordModelUsage' ? undefined : signal
      const bounded = AbortSignal.any([AbortSignal.timeout(property === 'recordModelUsage' ? 5000 : timeoutMs),
        ...lifecycle ? [lifecycle] : [], ...supplied ? [supplied] : []])
      bounded.throwIfAborted()
      args[index] = bounded
      return abortable(Reflect.apply(value,target,args) as Promise<unknown>,bounded)
    }
  } })
}
