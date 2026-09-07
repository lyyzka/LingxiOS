import { CALENDAR_METHODS } from './calendar.js'
import { DOCUMENT_METHODS } from './documents.js'
import { ROUTINE_METHODS } from './routines.js'
import { MEMORY_METHODS } from './memory.js'
import { CANVAS_METHODS } from './canvas.js'
import { LEARNING_METHODS } from './learning.js'
import { RESEARCH_METHODS } from './research.js'
import { DIRECTORY_METHODS } from './directory.js'
import { HANDOFF_METHODS } from './handoffs.js'
import { CHAT_METHODS } from './chat.js'
import { EMAIL_APPROVAL_METHODS, EMAIL_METHODS } from './email.js'
import { KNOWLEDGE_METHODS } from './actions.js'
import { PRESENTATION_METHODS } from './presentations.js'
import { POLL_METHODS } from './polls.js'
import { TEACHER_METHODS } from './teacher.js'
import { TEACHER_APPROVAL_TARGETS } from './approvals.js'
import { toolCatalog, TASK_TOOLS } from '../../tools/catalog.js'

export const LINGXILOOP_TOOLS = [
  ...TASK_TOOLS,
  ...toolCatalog('calendar', { ...CALENDAR_METHODS, update: ['eventId','expected','patch'],
    create: ['title','description','kind','startAt','endAt','allDay','recurrence','status','reminderMinutesBefore','reminderChannel','isPrivate','assigneeId','targetConversationId','agentPrompt'],
    delete: ['eventId','expected'] }, ['list','get','dispatches'], ['create','delete'], ['update','create','delete']),
  ...toolCatalog('documents', { ...DOCUMENT_METHODS, rename: ['documentId','title','expectedTitle'],
    create: ['title','body'], edit: ['documentId','expectedRevision','operations'], delete: ['documentId','expectedRevision'] },
    ['list','recent','read'], ['delete'], ['rename','create','edit','delete']),
  ...toolCatalog('routines', ROUTINE_METHODS, ['list'], ['create','activate'], ['pause','create','activate']),
  ...toolCatalog('memory', MEMORY_METHODS, ['list','recall'], [], ['note','verify','pin','delete']),
  ...toolCatalog('canvas', { ...CANVAS_METHODS, start_workspace: ['title','goal','members'], stop_workspace: [], assign: ['members'],
    steer_assignment: ['agentId','text'], stop_assignment: ['agentId'], handoff: ['toAgentId','task','context','frameIds'],
    submit_report: ['finding','evidenceRefs','confidence','unresolved','nextStep','verifiesReportId','disconfirmingChecks','verdict','consumedReportIds','conflictResolution'] },
    ['current','available_agents'], [], ['start_workspace','stop_workspace','assign','steer_assignment','stop_assignment','handoff','submit_report']),
  ...toolCatalog('learning', LEARNING_METHODS, ['current','get_learner_state','list_knowledge_units','list_due','get_mission','get_activity','list_attempts','get_attempt']),
  ...toolCatalog('research', RESEARCH_METHODS, ['search','read']),
  ...toolCatalog('directory', DIRECTORY_METHODS, ['self','participants','statuses']),
  ...toolCatalog('handoffs', HANDOFF_METHODS, ['list']),
  ...toolCatalog('chat', CHAT_METHODS, ['metadata','history','inbox','search','list_mutes']),
  ...toolCatalog('email', { ...EMAIL_METHODS, ...EMAIL_APPROVAL_METHODS }, Object.keys(EMAIL_METHODS), Object.keys(EMAIL_APPROVAL_METHODS)),
  ...toolCatalog('knowledge', KNOWLEDGE_METHODS, ['list_sources','check_source'], ['set_source_enabled','delete_source']),
  ...toolCatalog('presentations', PRESENTATION_METHODS, ['get'], ['approve_outline']),
  ...toolCatalog('polls', POLL_METHODS, ['show']),
  ...toolCatalog('teacher', { ...TEACHER_METHODS,
    ...Object.fromEntries(Object.entries(TEACHER_APPROVAL_TARGETS).map(([method, target]) => [method, method === 'review_evaluation' ? [target.key,'decision','reason'] : [target.key]])),
    transition_course: ['command'], set_teacher_membership: ['userId','enabled'] },
    ['current','overview','list_learners','get_learner','get_attempt','list_objectives','list_activities','list_reviews','list_rooms','get_digest_schedule'],
    [...Object.keys(TEACHER_APPROVAL_TARGETS),'transition_course','set_teacher_membership']),
]
export const LINGXILOOP_CAPABILITY_METHODS: Readonly<Record<string, readonly string[]>> = Object.fromEntries(
  [...new Set(LINGXILOOP_TOOLS.map(tool => tool.action.split('.')[0]!))].map(namespace =>
    [namespace, LINGXILOOP_TOOLS.filter(tool => tool.action.startsWith(namespace + '.')).map(tool => tool.action.split('.')[1]!)])
)
