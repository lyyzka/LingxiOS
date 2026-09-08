import type { AssistantMessage, RunEvent } from '../protocol/types.js'
export { releaseVersions } from '../versions.js'
export type { ConversationIdentity, ThreadIdentity, Audience, Visibility, IMDeliveryContext, IMDeliveryReceipt } from '../collaboration/types.js'
import { isGoalOutcome, type GoalOutcome } from '../protocol/outcome.js'
import type { CitationAnnotation, ResponseEnvelope } from '../outcome/envelope.js'
import { RUN_SEQUENCE_SPAN } from '../protocol/constants.js'
import type { RunState, DeliveryState } from '../app/jobs.js'
import type { RunStreamEvent, PreviewUpdate } from '../app/realtime.js'
export type { RunStreamEvent, PreviewUpdate } from '../app/realtime.js'
export type { RunState, RunSnapshot, DeliveryState } from '../app/jobs.js'
export type { AssistantMessage, RunEvent } from '../protocol/types.js'
export type { ResponseEnvelope } from '../outcome/envelope.js'
export type { TrustedPresentation } from '../presentation/definition.js'

export interface RunView {
  preview?: { attemptId: string; seq: number; fence: number; requestVersion: number } | null
  previewClosedFence?: number
  runId: string
  lastSeq: number
  draft: string
  message: AssistantMessage | null
  goalOutcome: GoalOutcome | null
  requestVersion: number
  fence: number
  messageFence: number
  resultId: string | null
  lifecycle: RunState['run']['status'] | null
  delivery: DeliveryState | null
}

export function createRunView(runId: string): RunView {
  return { runId, lastSeq: 0, draft: '', message: null, goalOutcome: null, requestVersion: 0, fence: 0,
    messageFence: 0, resultId: null, lifecycle: null, delivery: null }
}

/** Consume ordered, replayable public events. Draft text is never a committed message. */
export function consumeRunEvent(view: RunView, event: RunEvent): RunView {
  if (event.runId !== view.runId || event.visibility !== 'user' || !Number.isSafeInteger(event.seq) || event.seq <= view.lastSeq) return view
  const next = { ...view, lastSeq: event.seq }
  const fence = Math.floor((event.seq - 1) / RUN_SEQUENCE_SPAN) + 1
  if (fence < view.fence) return next
  next.fence = fence
  if (event.kind === 'run.started' && fence > view.messageFence) {
    next.draft = ''
    next.preview = null
    next.goalOutcome = null
    next.lifecycle = 'leased'
  }
  if (event.kind === 'model.delta' && event.data['partType'] === 'text' && typeof event.data['delta'] === 'string' && fence > view.messageFence
    && (typeof event.data['requestVersion'] === 'number' && event.data['requestVersion'] >= view.requestVersion
      || event.data['requestVersion'] === undefined && view.requestVersion <= 1)) {
    next.draft = ((next.preview ? '' : next.draft) + event.data['delta']).slice(-200_000)
    if (typeof event.data['requestVersion'] === 'number') next.requestVersion = event.data['requestVersion']
    next.preview = null
  }
  if (event.kind === 'response.committed') { next.draft = ''; next.preview = null; next.previewClosedFence = fence }
  if (fence > view.messageFence && isGoalOutcome(event.data['goalOutcome']) && event.data['goalOutcome'].requestVersion >= view.requestVersion) {
    next.goalOutcome = structuredClone(event.data['goalOutcome'])
    next.requestVersion = next.goalOutcome.requestVersion
    if (['awaiting_input','awaiting_approval','delegated'].includes(next.goalOutcome.status)) { next.draft = ''; next.lifecycle = 'waiting' }
  }
  if (event.kind === 'run.failed' || event.kind === 'run.cancelled') {
    next.lifecycle = event.kind === 'run.failed' ? 'failed' : 'cancelled'
    next.draft = ''; next.preview = null; next.previewClosedFence = fence
  }
  return next
}

/** Call only for messages received from the product's committed-message channel. */
export function consumeAssistantMessage(view: RunView, message: AssistantMessage, commit: { resultId: string; fence: number }): RunView {
  if (!message || typeof message !== 'object' || 'data' in message || message.version !== 2 || message.runId !== view.runId || typeof message.body !== 'string' || !message.body.trim()) {
    throw new Error('invalid committed assistant message')
  }
  if (!message.envelope || message.envelope.version !== 1 || !isGoalOutcome(message.envelope.goalOutcome)
    || !Array.isArray(message.envelope.citations) || !Array.isArray(message.envelope.artifacts)
    || message.envelope.body !== message.body || message.envelope.requestVersion !== message.envelope.goalOutcome.requestVersion) {
    throw new Error('committed message has an inconsistent envelope')
  }
  if (!commit?.resultId || !Number.isSafeInteger(commit.fence) || commit.fence < 1) throw new Error('committed result identity is required')
  if (view.message && commit.fence === view.messageFence && commit.resultId === view.resultId) return view
  if (message.envelope.requestVersion < view.requestVersion || commit.fence < view.messageFence
    || commit.fence === view.messageFence && view.resultId !== commit.resultId) return view
  const current = commit.fence >= view.fence
  return { ...view, message: structuredClone(message), messageFence: commit.fence, resultId: commit.resultId,
    requestVersion: Math.max(view.requestVersion,message.envelope.requestVersion),
    ...current ? { draft: '', preview: null, previewClosedFence: commit.fence, fence: commit.fence, goalOutcome: structuredClone(message.envelope.goalOutcome),
      lifecycle: message.envelope.goalOutcome.status === 'satisfied' ? 'succeeded' : ['partial','blocked'].includes(message.envelope.goalOutcome.status)
        ? message.envelope.goalOutcome.status as 'partial' | 'blocked' : 'waiting' } : {} }
}

/** Use a consistent public readRunState snapshot after replay and after every control operation. */
export function consumeRunState(view: RunView, state: RunState): RunView {
  const { run } = state
  if (run.id !== view.runId || run.requestVersion < view.requestVersion || run.fence < view.fence) return view
  if (run.fence === view.messageFence && (run.resultFence ?? 0) < view.messageFence) return view
  let next = view
  if (state.message && run.resultId && run.resultFence) next = consumeAssistantMessage(next,state.message,{ resultId: run.resultId, fence: run.resultFence })
  return { ...next, fence: run.fence, requestVersion: run.requestVersion, lifecycle: run.status,
    goalOutcome: run.goalOutcome, delivery: state.delivery,
    ...!['queued','leased'].includes(run.status) ? { previewClosedFence: run.fence } : {},
    ...['queued','leased'].includes(run.status) && run.fence === view.fence && run.requestVersion === view.requestVersion
      ? {} : { draft: '', preview: null } }
}

/** Gaps discard the draft; reconnect the SSE source to obtain a complete snapshot. */
export function consumePreview(view: RunView, update: PreviewUpdate): RunView {
  if (update.runId !== view.runId || update.fence < view.fence || update.fence <= view.messageFence
    || update.fence <= (view.previewClosedFence ?? 0) || update.requestVersion < view.requestVersion
    || !Number.isSafeInteger(update.seq) || update.seq < 1) return view
  const prior = view.preview
  if (prior?.fence === update.fence && update.seq <= prior.seq) return view
  const draft = update.kind === 'snapshot' ? update.draft
    : prior?.attemptId === update.attemptId && prior.fence === update.fence
      && prior.requestVersion === update.requestVersion && prior.seq === update.fromSeq ? view.draft + update.delta : null
  if (draft === null || draft.length > 100_000) return { ...view, draft: '', preview: null }
  return { ...view, draft, fence: update.fence, requestVersion: update.requestVersion,
    preview: { attemptId: update.attemptId, seq: update.seq, fence: update.fence, requestVersion: update.requestVersion } }
}

export function consumeRunStreamEvent(view: RunView, item: RunStreamEvent): RunView {
  switch (item.type) {
    case 'event': return consumeRunEvent(view, item.event)
    case 'state': return consumeRunState(view, item.state)
    case 'preview': return consumePreview(view, item.preview)
    case 'reset': return item.runId === view.runId ? { ...view, draft: '', preview: null } : view
  }
}

export type { GoalOutcome } from '../protocol/outcome.js'

export type ResponseSegment = { type: 'text'; text: string } | { type: 'citation'; text: string; annotation: CitationAnnotation }
  | { type: 'presentation'; component: import('../presentation/definition.js').TrustedPresentation }

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
  for (const component of envelope.presentations ?? []) segments.push({ type: 'presentation', component: structuredClone(component) })
  return segments
}
