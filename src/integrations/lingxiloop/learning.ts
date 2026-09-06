import type { HostAction, WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices } from './service-contracts.js'
import { nativeWork } from './actions.js'

export const LEARNING_METHODS = { list_attempts: ['activityId', 'missionStepId'], get_attempt: ['attemptId'], propose_evaluation: ['attemptId', 'demonstratedLevel', 'confidence', 'rubricResults', 'feedback', 'sourceEvidenceId', 'verifierEvidenceId'], record_attempt: ['activityId', 'missionStepId', 'evidenceClientMsgNos', 'documentIds', 'canvasFrameIds', 'assistance'], draft_knowledge_units: ['knowledgeUnits'], draft_activity: ['title', 'instructions', 'kind', 'evaluationMode', 'targetLevel', 'rubric', 'knowledgeUnitIds', 'dueAt'], start_mission: ['goal', 'successCriteria', 'missionKind', 'sourceClientMsgNo', 'explicit'], update_step: ['missionId', 'stepId', 'status', 'outcome', 'sourceEvidenceId', 'attemptId'], add_steps: ['missionId', 'steps'], finish_planning: ['missionId'], complete_mission: ['missionId'], current: [], get_learner_state: [], list_knowledge_units: [], list_due: [], get_mission: ['missionId'], get_activity: ['activityId'] } as const

function draftText(value: unknown): string {
  if (typeof value !== 'string' || !value.trim() || value.length > 10000) throw new Error('draft text must contain 1 to 10000 characters')
  return value.trim()
}
function draftLevel(value: unknown): number | undefined {
  if (value === undefined) return undefined
  if (!Number.isInteger(value) || Number(value) < 1 || Number(value) > 4) throw new Error('targetLevel must be 1-4')
  return Number(value)
}
function draftIds(value: unknown): string[] | undefined {
  if (value === undefined) return undefined
  if (!Array.isArray(value) || value.length > 100) throw new Error('draft ID lists are limited to 100 items')
  const ids = value.map(draftText)
  if (new Set(ids).size !== ids.length) throw new Error('draft IDs must be unique')
  return ids
}

export async function executeLearning(work: Omit<WorkItem, 'leaseToken'>, action: HostAction, services: Pick<LingxiLoopServices, 'learning' | 'permissionService'>) {
  const api = services.learning
  const method = action.action.slice('learning.'.length)
  if (!api || !action.action.startsWith('learning.') || !Object.hasOwn(LEARNING_METHODS, method)) throw new Error('unsupported learning action')
  const allowed: readonly string[] = LEARNING_METHODS[method as keyof typeof LEARNING_METHODS]
  if (Object.keys(action.args).some(key => !allowed.includes(key))) throw new Error('unknown learning argument')
  const native = nativeWork(work)
  await services.permissionService.assertCan({ actorUserId: native.authorizationUserId!, companyId: native.companyId, action: method === 'draft_knowledge_units' || method === 'draft_activity' || method === 'update_step' || method === 'add_steps' || method === 'finish_planning' || method === 'complete_mission' ? 'learning:submit' : 'learning:read', resource: { type: 'conversation', id: native.channelId } })
  const context = await api.loadLearningTurnContext(native, native.authorizationUserId)
  if (!context) throw new Error('conversation is not bound to a learning project')
  switch (method) {
    case 'draft_knowledge_units': case 'draft_activity': {
      await services.permissionService.assertCan({ actorUserId: native.authorizationUserId!, companyId: native.companyId,
        action: 'learning:submit', resource: { type: 'project', id: context.project.id } })
      const scope = { companyId: work.tenantId, projectId: context.project.id, actorId: work.agentId, actorKind: 'agent' as const }
      if (method === 'draft_knowledge_units') {
        const units = action.args['knowledgeUnits']
        if (!Array.isArray(units) || !units.length || units.length > 100) throw new Error('knowledgeUnits must contain 1 to 100 items')
        const knowledgeUnits = units.map(value => {
          if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('invalid knowledge unit')
          const item = value as Record<string, unknown>
          if (Object.keys(item).some(key => !['title', 'successCriteria', 'targetLevel', 'prerequisiteKnowledgeUnitIds'].includes(key))) throw new Error('unknown knowledge unit field')
          const targetLevel = draftLevel(item['targetLevel']), prerequisiteKnowledgeUnitIds = draftIds(item['prerequisiteKnowledgeUnitIds'])
          return { title: draftText(item['title']), successCriteria: draftText(item['successCriteria']),
            ...(targetLevel === undefined ? {} : { targetLevel }), ...(prerequisiteKnowledgeUnitIds === undefined ? {} : { prerequisiteKnowledgeUnitIds }) }
        })
        return api.createKnowledgeUnits({ ...scope, knowledgeUnits })
      }
      const { kind, evaluationMode, rubric, dueAt } = action.args
      if (kind !== 'LESSON' && kind !== 'PRACTICE' && kind !== 'ASSESSMENT' && kind !== 'PROJECT' && kind !== 'REVIEW') throw new Error('invalid activity kind')
      if (evaluationMode !== undefined && evaluationMode !== 'AGENT_FORMATIVE' && evaluationMode !== 'TEACHER_REQUIRED') throw new Error('invalid evaluationMode')
      if (rubric !== undefined && (!Array.isArray(rubric) || rubric.length > 100)) throw new Error('rubric is limited to 100 items')
      if (dueAt !== undefined && (typeof dueAt !== 'string' || dueAt.length > 100 || !Number.isFinite(Date.parse(dueAt)))) throw new Error('invalid dueAt')
      const targetLevel = draftLevel(action.args['targetLevel']), knowledgeUnitIds = draftIds(action.args['knowledgeUnitIds'])
      return api.draftActivity({ ...scope, title: draftText(action.args['title']), instructions: draftText(action.args['instructions']), kind,
        ...(evaluationMode === undefined ? {} : { evaluationMode }), ...(targetLevel === undefined ? {} : { targetLevel }),
        ...(rubric === undefined ? {} : { rubric }), ...(knowledgeUnitIds === undefined ? {} : { knowledgeUnitIds }), ...(dueAt === undefined ? {} : { dueAt }) })
    }
    case 'current': case 'get_learner_state': return context
    case 'list_knowledge_units': return context.knowledgeUnits
    case 'list_due': return context.due
    case 'get_mission': {
      const id = action.args['missionId']
      if (id === undefined) return context.activeMission ?? null
      if (typeof id !== 'string' || !id.trim()) throw new Error('missionId must be non-empty')
      if (!context.learnerId) throw new Error('current learning room has no learner scope')
      return api.getMission(id, work.tenantId, context.project.id, context.learnerId, work.sessionId)
    }
    case 'update_step': case 'add_steps': case 'finish_planning': case 'complete_mission': {
      const id = action.args['missionId']
      if (typeof id !== 'string' || !id.trim()) throw new Error('missionId must be non-empty')
      if (!context.learnerId) throw new Error('current learning room has no learner scope')
      await api.getMission(id, work.tenantId, context.project.id, context.learnerId, work.sessionId)
      if (method === 'update_step') {
        const stepId = action.args['stepId'], status = action.args['status']
        if (typeof stepId !== 'string' || !stepId.trim()) throw new Error('stepId must be non-empty')
        if (status !== 'OPEN' && status !== 'IN_PROGRESS' && status !== 'COMPLETED' && status !== 'CANCELLED') throw new Error('invalid step status')
        const input: Parameters<typeof api.updateMissionStep>[1] = { missionId: id, stepId, status }
        for (const key of ['outcome', 'sourceEvidenceId', 'attemptId'] as const) {
          const value = action.args[key]
          if (value === undefined) continue
          if (typeof value !== 'string' || !value.trim() || value.length > 10_000) throw new Error(`${key} must contain 1 to 10000 characters`)
          input[key] = value
        }
        return api.updateMissionStep(native, input)
      }
      if (method === 'add_steps') {
        const input = action.args['steps']
        if (!Array.isArray(input) || input.length < 1 || input.length > 64) throw new Error('steps must contain between 1 and 64 items')
        const steps = input.map<Parameters<typeof api.addMissionSteps>[2][number]>(value => {
          if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('step must be an object')
          const step = value as Record<string, unknown>
          if (Object.keys(step).some(key => !['kind', 'description', 'successCriteria', 'knowledgeUnitId'].includes(key))) throw new Error('unknown step argument')
          const kind = step['kind']
          if (kind !== 'LEARN' && kind !== 'PRACTICE' && kind !== 'CHECK' && kind !== 'REFLECT') throw new Error('invalid step kind')
          const description = step['description'], successCriteria = step['successCriteria'], knowledgeUnitId = step['knowledgeUnitId']
          for (const value of [description, successCriteria]) {
            if (typeof value !== 'string' || !value.trim() || value.length > 10_000) throw new Error('step description and successCriteria must contain 1 to 10000 characters')
          }
          if (knowledgeUnitId !== undefined && (typeof knowledgeUnitId !== 'string' || !knowledgeUnitId.trim())) throw new Error('knowledgeUnitId must be non-empty')
          return { kind, description: description as string, successCriteria: successCriteria as string, ...(typeof knowledgeUnitId === 'string' ? { knowledgeUnitId } : {}) }
        })
        return api.addMissionSteps(native, id, steps)
      }
      return method === 'finish_planning' ? api.finishMissionPlanning(native, id) : api.completeMission(native, id)
    }
    case 'get_activity': {
      const id = action.args['activityId']
      if (typeof id !== 'string' || !id.trim()) throw new Error('activityId must be non-empty')
      return api.getActivity(id, work.tenantId, context.project.id)
    }
    default: throw new Error('unsupported learning action')
  }
}
