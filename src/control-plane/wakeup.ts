import { setTimeout as delay } from 'node:timers/promises'
import { abortable } from '../deadline.js'
import type { SqlClient, SqlPool } from './pg-store.js'
import type { Logger } from '../logging.js'

/** Capture the version BEFORE scanning, so a notification during a scan cannot be lost. */
export class Wakeup {
  version = 0
  private readonly waiting = new Set<() => void>()

  notify(): void {
    this.version++
    for (const wake of this.waiting) wake()
  }

  wait(after: number, timeoutMs: number, signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted()
    if (after !== this.version) return Promise.resolve()
    if (this.waiting.size >= 4096) throw new Error('too many wakeup subscribers')
    return new Promise((resolve, reject) => {
      const cleanup = () => { clearTimeout(timer); this.waiting.delete(wake); signal?.removeEventListener('abort', cancel) }
      const wake = () => { cleanup(); resolve() }
      const cancel = () => { cleanup(); reject(signal?.reason) }
      const timer = setTimeout(wake, timeoutMs)
      timer.unref()
      this.waiting.add(wake)
      signal?.addEventListener('abort', cancel, { once: true })
      if (after !== this.version) wake()
      else if (signal?.aborted) cancel()
    })
  }
}

interface NotificationClient extends SqlClient {
  on(event: string, listener: (message?: unknown) => void): unknown
  removeListener(event: string, listener: (message?: unknown) => void): unknown
}

/** One dedicated pool slot; no payloads, writes, or DDL. Polling remains authoritative. */
export async function listenWakeups(pool: SqlPool, notify: (channel: 'work' | 'outbox') => void,
  signal: AbortSignal, logger: Logger): Promise<void> {
  while (!signal.aborted) {
    let client: NotificationClient | undefined
    let disconnected: (() => void) | undefined
    const notification = (message?: unknown) => {
      const channel = message && typeof message === 'object' && 'channel' in message ? message.channel : undefined
      if (channel === 'lingxios_work') notify('work')
      if (channel === 'lingxios_outbox') notify('outbox')
    }
    try {
      const pending = pool.connect()
      const connecting = AbortSignal.any([signal, AbortSignal.timeout(5000)])
      let acquired: SqlClient
      try { acquired = await abortable(pending, connecting) }
      catch (error) {
        void pending.then(late => late.release(new Error('LISTEN acquisition cancelled')), () => {})
        throw error
      }
      if (!('on' in acquired) || typeof acquired.on !== 'function'
        || !('removeListener' in acquired) || typeof acquired.removeListener !== 'function') {
        acquired.release()
        return // In-memory/transaction-only adapters use the periodic scan.
      }
      client = acquired as NotificationClient
      const ended = new Promise<void>(resolve => { disconnected = resolve })
      client.on('notification', notification)
      client.on('error', disconnected!)
      client.on('end', disconnected!)
      await abortable(client.query('LISTEN lingxios_work; LISTEN lingxios_outbox'), connecting)
      notify('work'); notify('outbox') // Initial/reconnect scan AFTER LISTEN commits.
      await abortable(ended, signal)
    } catch {
      if (!signal.aborted) logger.warn('database notification listener unavailable; periodic scans remain active')
    } finally {
      if (client) {
        client.removeListener('notification', notification)
        client.removeListener('error', disconnected!)
        client.removeListener('end', disconnected!)
        // Discard rather than return a LISTEN connection to unrelated transactions.
        client.release(new Error('notification listener closed'))
      }
    }
    if (!signal.aborted) await delay(1000, undefined, { signal, ref: false }).catch(() => {})
  }
}
