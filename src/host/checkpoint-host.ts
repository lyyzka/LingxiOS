import { createHash } from 'node:crypto'
import type { HostPort } from './port.js'
import type { MetricsRegistry } from '../metrics.js'

/** Per-attempt serialization also prevents parallel read tools from racing the session CAS. */
export function checkpointHost(host: HostPort, metrics?: MetricsRegistry): HostPort {
  let tail = Promise.resolve()
  let saved: { identity: string; revision: number; hash: string } | undefined
  return new Proxy(host, { get(target, property, receiver) {
    if (property !== 'saveSession') {
      const value: unknown = Reflect.get(target, property, receiver)
      return typeof value === 'function' ? value.bind(target) : value
    }
    return (...[work, session, signal]: Parameters<HostPort['saveSession']>) => {
      const operation = tail.then(async () => {
        signal?.throwIfAborted()
        const snapshot = structuredClone(session)
        const identity = JSON.stringify([work.id, work.fence, work.leaseToken, snapshot.key])
        const hash = createHash('sha256').update(JSON.stringify({ ...snapshot, revision: 0 })).digest('hex')
        if (saved?.identity === identity && saved.revision === snapshot.revision && saved.hash === hash) {
          metrics?.counter('agentos_session_checkpoints_skipped_total', 'Identical acknowledged session checkpoints omitted').inc()
          return
        }
        await target.saveSession(work, snapshot, signal)
        session.revision = snapshot.revision
        saved = { identity, revision: snapshot.revision, hash }
      })
      tail = operation.catch(() => {})
      return operation
    }
  } })
}
