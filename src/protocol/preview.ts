/** Ephemeral user-body preview. Its sequence is independent of the durable event cursor. */
export interface PreviewFrame {
  requestVersion: number
  attemptId: string
  seq: number
  kind: 'reset' | 'delta'
  text: string
}

export interface PreviewSnapshot {
  runId: string
  fence: number
  requestVersion: number
  attemptId: string
  seq: number
  draft: string
}

export function isPreviewFrame(value: unknown): value is PreviewFrame {
  if (!value || typeof value !== 'object') return false
  const frame = value as PreviewFrame
  return Number.isSafeInteger(frame.requestVersion) && frame.requestVersion > 0
    && Number.isSafeInteger(frame.seq) && frame.seq > 0
    && typeof frame.attemptId === 'string' && frame.attemptId.length > 0 && frame.attemptId.length <= 256
    && typeof frame.text === 'string' && frame.text.length <= 16_384
    && (frame.kind === 'delta' || frame.kind === 'reset' && frame.text === '')
}
