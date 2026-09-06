import type { AssistantMessage, RunEvent } from '../protocol/types.js'
export { releaseVersions } from '../versions.js'
import { isGoalOutcome, type GoalOutcome } from '../protocol/outcome.js'
import type { CitationAnnotation, ResponseEnvelope } from '../outcome/envelope.js'

export interface RunView {
  runId: string
  lastSeq: number
  draft: string
  message: AssistantMessage | null
  goalOutcome: GoalOutcome | null
}

export function createRunView(runId: string): RunView {
  return { runId, lastSeq: 0, draft: '', message: null, goalOutcome: null }
}

/** Consume ordered, replayable public events. Draft text is never a committed message. */
export function consumeRunEvent(view: RunView, event: RunEvent): RunView {
  if (event.runId !== view.runId || event.visibility !== 'user' || !Number.isSafeInteger(event.seq) || event.seq <= view.lastSeq) return view
  const next = { ...view, lastSeq: event.seq }
  if (event.kind === 'run.started' && !view.message) {
    next.draft = ''
    next.goalOutcome = null
  }
  if (event.kind === 'model.delta' && event.data['partType'] === 'text' && typeof event.data['delta'] === 'string' && !view.message) {
    next.draft += event.data['delta']
  }
  if (!view.message && isGoalOutcome(event.data['goalOutcome'])) {
    next.goalOutcome = structuredClone(event.data['goalOutcome'])
    if (next.goalOutcome.status === 'awaiting_input' || next.goalOutcome.status === 'awaiting_approval') next.draft = ''
  }
  return next
}

/** Call only for messages received from the product's committed-message channel. */
export function consumeAssistantMessage(view: RunView, message: AssistantMessage): RunView {
  if (!message || typeof message !== 'object' || 'data' in message || message.version !== 2 || message.runId !== view.runId || typeof message.body !== 'string' || !message.body.trim()) {
    throw new Error('invalid committed assistant message')
  }
  if (!message.envelope || !isGoalOutcome(message.envelope.goalOutcome)
    || message.envelope.body !== message.body || message.envelope.requestVersion !== message.envelope.goalOutcome.requestVersion) {
    throw new Error('committed message has an inconsistent envelope')
  }
  return { ...view, draft: '', message: structuredClone(message), goalOutcome: structuredClone(message.envelope.goalOutcome) }
}

export type { GoalOutcome } from '../protocol/outcome.js'

export type ResponseSegment = { type: 'text'; text: string } | { type: 'citation'; text: string; annotation: CitationAnnotation }

/** Text values are display data; callers must not insert them as raw HTML. */
export function responseSegments(envelope: ResponseEnvelope): ResponseSegment[] {
  const segments: ResponseSegment[] = []
  let offset = 0
  for (const annotation of envelope.citations) {
    if (!Number.isSafeInteger(annotation.start) || !Number.isSafeInteger(annotation.end)
      || annotation.start < offset || annotation.end <= annotation.start || annotation.end > envelope.body.length) {
      throw new Error('invalid citation span')
    }
    if (annotation.start > offset) segments.push({ type: 'text', text: envelope.body.slice(offset, annotation.start) })
    segments.push({ type: 'citation', text: annotation.text, annotation: structuredClone(annotation) })
    offset = annotation.end
  }
  if (offset < envelope.body.length) segments.push({ type: 'text', text: envelope.body.slice(offset) })
  return segments
}
