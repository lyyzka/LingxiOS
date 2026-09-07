import type { RequestSnapshot } from '../context/request.js'

/** The answering model's explicit self-check, not independent verification. */
export interface GoalAssessment {
  status: 'satisfied' | 'partial' | 'blocked' | 'delegated'
  taskRef?: string
  checks: Array<{ requirement: string; status: 'met' | 'unmet' | 'unknown'; basis: string }>
  gaps: string[]
}

export function parseFinalCandidate(raw: string, request: RequestSnapshot): { body: string; assessment: GoalAssessment } {
  const value = JSON.parse(raw)
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('final response must be a JSON object')
  if (Object.keys(value).filter(key => key !== 'taskRef').sort().join(',') !== 'body,checks,gaps,status') {
    throw new Error('final JSON requires exactly body, status, checks and gaps; taskRef is only for delegation. Execute Python through the actual ipython tool, not a JSON description of a tool call.')
  }
  if (typeof value.body !== 'string' || !value.body.trim() || value.body.length > 100_000) throw new Error('final body must be a non-empty string, including when the answer is a number')
  if (!['satisfied', 'partial', 'blocked', 'delegated'].includes(value.status)) throw new Error('final status must be satisfied, partial, blocked or delegated')
  if (value.status === 'delegated' && (typeof value.taskRef !== 'string' || !value.taskRef.trim() || value.taskRef.length > 240)) throw new Error('delegation requires a taskRef')
  if (value.status !== 'delegated' && value.taskRef != null && value.taskRef !== '') throw new Error('taskRef is only for delegation')
  if (!Array.isArray(value.gaps) || value.gaps.length > 64
    || !value.gaps.every((gap: unknown) => typeof gap === 'string' && gap.trim() && gap.length <= 2000)) throw new Error('gaps must be an array of up to 64 remaining-work strings')
  if (!Array.isArray(value.checks) || !value.checks.length || value.checks.length > 64) throw new Error('checks must contain 1 to 64 requirement/status/basis objects')
  const source = [request.originalText, ...(request.inheritedRevisions ?? []).map(revision => revision.text), ...request.revisions.map(revision => revision.text)]
  for (const check of value.checks) {
    if (!check || typeof check !== 'object' || Array.isArray(check)
      || Object.keys(check).sort().join(',') !== 'basis,requirement,status'
      || typeof check.requirement !== 'string' || !check.requirement.trim() || check.requirement.length > 4000
      || !['met', 'unmet', 'unknown'].includes(check.status)
      || typeof check.basis !== 'string' || !check.basis.trim() || check.basis.length > 4000) throw new Error('final checks must refer to the original request or revisions')
    if (!source.some(text => text.includes(check.requirement))) {
      throw new Error('Each requirement must be copied verbatim from originalText or a revision, without paraphrasing or translating. Non-matching requirement: ' + JSON.stringify(check.requirement.slice(0, 240)))
    }
  }
  const latestRevision = request.revisions.at(-1) ?? request.inheritedRevisions?.at(-1)
  if (latestRevision && !value.checks.some((check: GoalAssessment['checks'][number]) => latestRevision.text.includes(check.requirement))) {
    throw new Error('final self-check must cover the latest user revision')
  }
  if (value.status === 'satisfied' && (value.gaps.length || value.checks.some((check: GoalAssessment['checks'][number]) => check.status !== 'met'))) {
    throw new Error('a satisfied self-check cannot contain unresolved requirements')
  }
  if (value.status !== 'satisfied' && !value.gaps.length) throw new Error('partial or blocked candidates must explain the remaining gap')
  return { body: value.body.trim(), assessment: { status: value.status, checks: value.checks, gaps: value.gaps,
    ...(value.status === 'delegated' ? { taskRef: value.taskRef } : {}) } }
}
