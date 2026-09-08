import type { IncomingMessage } from 'node:http'
import { isPreviewFrame, type PreviewFrame } from '../protocol/preview.js'

/** Bounded NDJSON upload; input chunks and JSON lines may split anywhere. */
export async function* readPreviewFrames(request: IncomingMessage, signal: AbortSignal): AsyncGenerator<PreviewFrame> {
  const decoder = new TextDecoder('utf-8', { fatal: true })
  let pending = '', size = 0
  for await (const chunk of request) {
    signal.throwIfAborted()
    size += (chunk as Buffer).length
    if (size > 16 * 1024 * 1024) throw new Error('preview stream exceeds its byte budget')
    pending += decoder.decode(chunk as Buffer, { stream: true })
    let end: number
    while ((end = pending.indexOf('\n')) >= 0) {
      if (end > 128_000) throw new Error('preview frame is too large')
      const frame: unknown = JSON.parse(pending.slice(0, end))
      pending = pending.slice(end + 1)
      if (!isPreviewFrame(frame)) throw new Error('invalid preview frame')
      yield frame
    }
    if (pending.length > 128_000) throw new Error('preview frame is too large')
  }
  pending += decoder.decode()
  if (pending.trim()) throw new Error('incomplete preview frame')
}

export function previewRequestBody(frames: AsyncIterable<PreviewFrame>, signal: AbortSignal): ReadableStream<Uint8Array> {
  const iterator = frames[Symbol.asyncIterator]()
  return new ReadableStream({
    async pull(controller) {
      signal.throwIfAborted()
      const next = await iterator.next()
      if (next.done) controller.close()
      else controller.enqueue(new TextEncoder().encode(JSON.stringify(next.value) + '\n'))
    },
    async cancel() { await iterator.return?.() },
  }, { highWaterMark: 1 })
}
