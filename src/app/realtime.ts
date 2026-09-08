import { createHash } from 'node:crypto'
import { readRunState, readRunRecord, runStateFromRow, type RunIdentity, type RunState } from './jobs.js'
import { workItemFromRow, type SqlPool } from '../control-plane/pg-store.js'
import { Wakeup } from '../control-plane/wakeup.js'
import { ControlPlaneError, type LeaseProof } from '../control-plane/service.js'
import { authorizeConversationWork } from '../collaboration/access.js'
import { isPreviewFrame, type PreviewFrame, type PreviewSnapshot } from '../protocol/preview.js'
import type { RunEvent, WorkItem } from '../protocol/types.js'
import { abortable } from '../deadline.js'
import { candidateHash } from '../outcome/verification.js'

export type PreviewUpdate = PreviewSnapshot & { kind: 'snapshot' }
  | Omit<PreviewSnapshot, 'draft'> & { kind: 'delta'; delta: string; fromSeq: number }
export type RunStreamEvent = { type: 'event'; event: RunEvent }
  | { type: 'state'; state: RunState; candidateHash?: string }
  | { type: 'preview'; preview: PreviewUpdate }
  | { type: 'reset'; runId: string; reason: 'unavailable' | 'superseded' }

export interface RealtimeOptions {
  /** Trusted host policy: explicitly allow pre-acceptance body drafts for this request. Default is false. */
  allowDraft?: (work: Omit<WorkItem, 'leaseToken'>, requestVersion: number) => boolean | Promise<boolean>
  maxSubscribers?: number
}

interface Entry { wake: Wakeup; snapshot: PreviewSnapshot | null; updatedAt: number; readers: number }

/** Local ephemeral snapshots. A different process/restart explicitly resets; durable replay always works. */
export function createRealtime(database: SqlPool, changed: Wakeup, shutdown: AbortSignal, options: RealtimeOptions = {}) {
  const entries = new Map<string, Entry>()
  const maxSubscribers = options.maxSubscribers ?? 256
  if (!Number.isSafeInteger(maxSubscribers) || maxSubscribers < 1 || maxSubscribers > 4096) throw new Error('invalid realtime subscriber limit')
  let subscribers = 0, uploads = 0
  const owners = new Map<string, { fence: number; token: symbol }>()
  const entryFor = (id: string): Entry => {
    let entry = entries.get(id)
    if (!entry) {
      for (const [key, value] of entries) {
        if (!value.readers && (entries.size >= 256 || Date.now() - value.updatedAt > 60_000)) entries.delete(key)
      }
      if (entries.size >= 256 + maxSubscribers) throw new Error('realtime snapshot capacity exceeded')
      entry = { wake: new Wakeup(), snapshot: null, updatedAt: Date.now(), readers: 0 }
      entries.set(id, entry)
    }
    return entry
  }
  const reset = (id: string) => {
    const entry = entries.get(id)
    if (entry?.snapshot) { entry.snapshot = null; entry.wake.notify() }
  }
  const allowed = async (work: Omit<WorkItem, 'leaseToken'>, version: number): Promise<boolean> => {
    if (!options.allowDraft || work.conversation?.internal) return false
    await authorizeConversationWork(database, work, 'speak')
    return options.allowDraft(work, version)
  }

  const receive = async (proof: LeaseProof, frames: AsyncIterable<PreviewFrame>, signal: AbortSignal) => {
    if (!options.allowDraft) throw new Error('preview is not enabled by the host')
    if (uploads >= 64) throw new Error('too many preview uploads')
    uploads++
    const token = Symbol()
    const resetOwned = () => { if (owners.get(proof.id)?.token === token) reset(proof.id) }
    const leaseHash = createHash('sha256').update(proof.leaseToken).digest('hex')
    try {
      for await (const frame of frames) {
        signal.throwIfAborted()
        if (!isPreviewFrame(frame)) throw new Error('invalid preview frame')
        const { rows } = await database.query(`SELECT *,jsonb_array_length(steer_inputs)+1 AS request_version
          FROM lingxios.agent_work_items WHERE id=$1 AND fence=$2 AND lease_token_hash=$3
            AND status='leased' AND lease_expires_at>NOW() AND cancel_requested_at IS NULL`, [proof.id, proof.fence, leaseHash])
        if (!rows[0]) throw new ControlPlaneError(409, 'preview lease is no longer current', 'lease_lost')
        const owner = owners.get(proof.id)
        if (owner && owner.fence >= proof.fence && owner.token !== token) throw new Error('preview attempt already has an upload')
        owners.set(proof.id, { fence: proof.fence, token })
        const work = workItemFromRow(rows[0], '', 1)
        if (Number(rows[0]['request_version']) !== frame.requestVersion || !await allowed(work, frame.requestVersion)) {
          resetOwned()
          continue
        }
        const entry = entryFor(proof.id), prior = entry.snapshot
        if (prior && (prior.fence > proof.fence || prior.requestVersion > frame.requestVersion
          || prior.fence === proof.fence && prior.seq >= frame.seq)) continue
        if (frame.kind === 'reset') entry.snapshot = { runId: proof.id, fence: proof.fence,
          requestVersion: frame.requestVersion, attemptId: frame.attemptId, seq: frame.seq, draft: '' }
        else if (prior?.fence === proof.fence && prior.requestVersion === frame.requestVersion
          && prior.attemptId === frame.attemptId && frame.seq === prior.seq + 1 && prior.draft.length + frame.text.length <= 100_000) {
          entry.snapshot = { ...prior, seq: frame.seq, draft: prior.draft + frame.text }
        } else { resetOwned(); continue }
        entry.updatedAt = Date.now()
        entry.wake.notify()
      }
    } finally {
      uploads--; resetOwned()
      if (owners.get(proof.id)?.token === token) owners.delete(proof.id)
    }
  }

  const response = async (identity: RunIdentity, input: { lastEventId?: string | null; signal?: AbortSignal } = {}): Promise<Response> => {
    if (!identity.principalId?.trim()) throw new Error('authenticated principalId is required for SSE')
    let after = Number(input.lastEventId ?? 0)
    if (!Number.isSafeInteger(after) || after < 0 || input.lastEventId && !/^\d+$/.test(input.lastEventId)) throw new Error('invalid Last-Event-ID')
    const initial = await readRunState(database, identity)
    if (!initial) throw new Error('run is outside this identity')
    if (subscribers >= maxSubscribers) throw new Error('too many realtime subscribers')
    const entry = entryFor(identity.runId)
    subscribers++
    entry.readers++
    const stopped = new AbortController(), signal = AbortSignal.any([shutdown, stopped.signal, ...input.signal ? [input.signal] : []])
    let released = false
    const cleanup = () => {
      if (released) return
      released = true; subscribers--; entry.readers--
      signal.removeEventListener('abort', cleanup)
    }
    signal.addEventListener('abort', cleanup, { once: true })
    if (signal.aborted) cleanup()
    async function* events(): AsyncGenerator<RunStreamEvent | null> {
      let lastState = '', lastPreview: PreviewSnapshot | null | undefined
      try {
        while (!signal.aborted) {
          const durableVersion = changed.version, previewVersion = entry.wake.version
          // Reauthorize every read, including resumes and ephemeral updates. No cached grants.
          const record = await abortable(readRunRecord(database, identity), signal)
          if (!record) throw new Error('run is no longer available to this identity')
          const state = runStateFromRow(record)
          const stateKey = JSON.stringify(state)
          if (stateKey !== lastState) {
            lastState = stateKey
            yield { type: 'state', state, ...state.message ? { candidateHash: candidateHash({ body: state.message.body,
              requestVersion: state.message.envelope.requestVersion, artifacts: state.message.envelope.artifacts }) } : {} }
          }
          const { rows } = await database.query(`SELECT event.* FROM lingxios.agent_run_events event
            WHERE run_id=$1 AND tenant_id=$2 AND agent_id=$3 AND visibility='user' AND seq>$4
              AND (expires_at IS NULL OR expires_at>NOW()) ORDER BY seq LIMIT 100`, [identity.runId, identity.tenantId, identity.agentId, after])
          for (const row of rows) {
            const event: RunEvent = { runId: identity.runId, seq: Number(row['seq']), kind: String(row['kind']),
              stage: row['stage'] as RunEvent['stage'], visibility: 'user', data: row['data'] as RunEvent['data'] }
            after = event.seq
            yield { type: 'event', event }
          }
          const current = entry.snapshot
          const preview = state.run.status === 'leased' && current?.fence === state.run.fence
            && current.requestVersion === state.run.requestVersion && Date.now() - entry.updatedAt < 60_000
            && await allowed(workItemFromRow(record, '', 1), state.run.requestVersion) ? current : null
          if (!preview && lastPreview !== null) {
            yield { type: 'reset', runId: identity.runId, reason: lastPreview ? 'superseded' : 'unavailable' }
            lastPreview = null
          } else if (preview && (preview.seq !== lastPreview?.seq || preview.fence !== lastPreview?.fence)) {
            if (lastPreview?.attemptId === preview.attemptId && lastPreview.fence === preview.fence
              && lastPreview.requestVersion === preview.requestVersion && preview.draft.startsWith(lastPreview.draft)) {
              const { draft, ...version } = preview
              yield { type: 'preview', preview: { ...version, kind: 'delta', fromSeq: lastPreview.seq, delta: draft.slice(lastPreview.draft.length) } }
            } else yield { type: 'preview', preview: { ...preview, kind: 'snapshot' } }
            lastPreview = preview
          }
          if (rows.length === 100) continue
          if (!['queued','leased'].includes(state.run.status)) return
          const waiting = new AbortController()
          try {
            await Promise.race([changed.wait(durableVersion, 750, AbortSignal.any([signal, waiting.signal])),
              entry.wake.wait(previewVersion, 750, AbortSignal.any([signal, waiting.signal]))])
          } finally { waiting.abort() }
          yield null // SSE keepalive; never counted as body TTFT.
        }
      } finally { cleanup(); stopped.abort() }
    }
    const iterator = events()
    const body = new ReadableStream<Uint8Array>({
      async pull(controller) {
        try {
          const next = await abortable(iterator.next(), signal)
          if (next.done) { cleanup(); controller.close(); return }
          const item = next.value
          const text = item ? `${item.type === 'event' ? `id: ${item.event.seq}\n` : ''}event: ${item.type}\ndata: ${JSON.stringify(item)}\n\n` : ': keepalive\n\n'
          controller.enqueue(new TextEncoder().encode(text))
        } catch (error) { cleanup(); stopped.abort(); controller.error(error); await iterator.return(undefined) }
      },
      async cancel() { cleanup(); stopped.abort(); await iterator.return(undefined) },
    }, { highWaterMark: 1 })
    return new Response(body, { headers: { 'content-type': 'text/event-stream; charset=utf-8',
      'cache-control': 'no-cache, no-transform', 'x-accel-buffering': 'no' } })
  }
  return { allowed, receive, response, reset }
}
