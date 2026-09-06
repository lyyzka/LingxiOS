export type Verification = 'passed' | 'not_run' | 'inconclusive'

export type GoalOutcome = {
  requestVersion: number
  verification: Verification
  gaps?: string[]
  question?: string
} & (
  | { status: 'satisfied' | 'partial' | 'awaiting_input' | 'blocked' }
  | { status: 'awaiting_approval'; approvalId: string }
  | { status: 'delegated'; taskRef: string }
)

export function isGoalOutcome(value: unknown): value is GoalOutcome {
  if (!value || typeof value !== 'object') return false
  const item = value as Record<string, unknown>
  return Number.isSafeInteger(item['requestVersion']) && Number(item['requestVersion']) > 0
    && typeof item['verification'] === 'string' && ['passed', 'not_run', 'inconclusive'].includes(item['verification'])
    && typeof item['status'] === 'string' && ['satisfied', 'partial', 'awaiting_input', 'awaiting_approval', 'delegated', 'blocked'].includes(item['status'])
    && (item['gaps'] === undefined || (Array.isArray(item['gaps']) && item['gaps'].every((gap) => typeof gap === 'string')))
    && (item['question'] === undefined || (item['status'] === 'awaiting_input' && typeof item['question'] === 'string' && item['question'].trim().length > 0 && item['question'].length <= 4_000))
    && (item['status'] !== 'satisfied' || item['gaps'] === undefined || (item['gaps'] as unknown[]).length === 0)
    && (item['status'] !== 'awaiting_approval' || (typeof item['approvalId'] === 'string' && item['approvalId'].trim().length > 0))
    && (item['status'] !== 'delegated' || (typeof item['taskRef'] === 'string' && item['taskRef'].trim().length > 0))
}
