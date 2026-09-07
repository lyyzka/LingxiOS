import type { RequestSnapshot } from '../context/request.js'
import type { ExecutionStep } from '../control-plane/steps.js'
import type { ToolDefinition } from '../tools/catalog.js'
import type { KernelArtifact } from '../protocol/types.js'
import type { GoalOutcome } from '../protocol/outcome.js'
import { parseFinalCandidate, type GoalAssessment } from './assessment.js'

export function requiresReview(request: RequestSnapshot, steps: readonly ExecutionStep[], tools: readonly ToolDefinition[], artifacts: readonly KernelArtifact[] = []): boolean {
  const actions = steps.filter(step => !step.kind.startsWith('runtime.') && !step.kind.startsWith('task__') && step.kind !== 'discover')
  // A missing tool call must not turn an omitted file or write into a simple answer.
  const requestedEffect = request.originalText.split(/[。；;\n]/).some(clause => !/(?:不要|不得|禁止|do not|don't|never)\s*/i.test(clause)
    && /(?:创建|生成|保存|导出|提交|写入).{0,32}(?:文件|文档|报告|幻灯片|邮件)|\b(?:create|save|write|export|attach)\b.{0,40}\b(?:file|document|report|attachment)\b/i.test(clause))
  return Boolean(request.contract || request.delegatedAssignment || artifacts.length || actions.length > 1
    || actions.some(step => step.kind === 'ipython' || tools.find(tool => tool.name === step.kind)?.effect !== 'read')
    || requestedEffect || request.revisions.length || /\n\s*(?:[-*]|\d+[.)、])\s|(?:然后|并且|同时|多步骤|分步|分别|以及)|\b(?:and|multi-step|step by step)\b/i.test(request.originalText))
}

export function validateCompletion(body: string, assessment: GoalAssessment | undefined, request: RequestSnapshot,
  outcome: GoalOutcome, gaps: readonly string[]): void {
  if (assessment) parseFinalCandidate(JSON.stringify({ body, ...assessment }), request)
  if (outcome.status === 'satisfied' && (assessment && assessment.status !== 'satisfied' || gaps.length)) {
    throw new Error('successful completion requires no unresolved verification: ' + gaps.join('; '))
  }
  if (outcome.status === 'delegated' && (assessment?.status !== 'delegated' || assessment.taskRef !== outcome.taskRef)) {
    throw new Error('delegation requires a matching assessed task reference')
  }
}
