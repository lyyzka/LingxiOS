import type { CapabilityGrant } from '../protocol/types.js'

export interface ToolDefinition {
  name: string
  action: string
  description: string
  parameters: { type: 'object'; properties: Record<string, Record<string, unknown>>; required?: string[]; additionalProperties: false }
  effect: 'read' | 'transaction' | 'idempotent' | 'uncertain'
  approval: boolean
  readback?: string
}
const arrays = new Set(['deliverables','constraints','actions','acceptance','frameIds','unresolved','disconfirmingChecks',
  'consumedReportIds','evidenceClientMsgNos','documentIds','canvasFrameIds','knowledgeUnitIds','contextMessageIds',
  'attachmentClientMsgNos','sourceIds','pageIds','sectionIds','optionIds','objectiveIds','cc','options'])
const objects = new Set(['args','patch','frame','schedule'])
const objectArrays = new Set(['operations','members','evidenceRefs','conflictResolution','knowledgeUnits','rubric','steps','items','objectives'])
const booleans = new Set(['allDay','isPrivate','pinned','explicit','muted','unreadOnly','enabled','attentionOnly'])
const integers = new Set(['sinceMinutes','limit','expectedVersion','demonstratedLevel','targetLevel','beforeSequence','targetSlideCount','expiresInMinutes','windowDays','weekday','reminderMinutesBefore'])
const required: Record<string, string[]> = {
  'task.ask': ['question'], 'task.check_receipt': ['idempotencyKey','action','expected'], 'task.check_resource': ['action','args','expected'],
  'calendar.get': ['eventId'], 'calendar.dispatches': ['eventId'], 'calendar.update': ['eventId','expected','patch'], 'calendar.delete': ['eventId','expected'],
  'documents.read': ['documentId'], 'documents.rename': ['documentId','title','expectedTitle'], 'documents.create': ['title','body'],
  'documents.edit': ['documentId','expectedRevision','operations'], 'documents.delete': ['documentId','expectedRevision'],
  'routines.create': ['kind','title','instructions','schedule'], 'routines.pause': ['routineId'], 'routines.activate': ['routineId'],
  'memory.note': ['body'], 'memory.verify': ['id','expectedVersion'], 'memory.pin': ['id','expectedVersion','pinned'], 'memory.delete': ['id','expectedVersion'],
  'canvas.create_frame': ['frame'], 'canvas.update_frame': ['frameId','patch'], 'canvas.append_content': ['frameId','content'], 'canvas.delete_frame': ['frameId'],
  'canvas.start_workspace': ['title','goal','members'], 'canvas.assign': ['members'], 'canvas.handoff': ['toAgentId','task'],
  'canvas.submit_report': ['finding','evidenceRefs','confidence'], 'research.search': ['query'], 'research.read': ['url'],
  'email.send': ['to','subject','body'], 'email.reply': ['conversationId','messageId','body'],
  'polls.create': ['question','options'], 'polls.vote': ['messageId','optionIds'], 'polls.close': ['messageId'], 'polls.show': ['messageId'],
}
function parameter(namespace: string, field: string): Record<string, unknown> {
  if (field === 'expected') return { type: ['object','array','string','number','boolean','null'], description: 'The exact observed resource or complete expected receipt value.' }
  if (field === 'expectedRevision') return { type: namespace === 'documents' ? 'string' : 'integer' }
  if (field === 'to' && namespace === 'email' || arrays.has(field)) return { type: 'array', items: { type: 'string' } }
  if (objectArrays.has(field)) return { type: 'array', items: { type: 'object' } }
  if (objects.has(field)) return { type: 'object', description: 'Native business fields; preserve required revision and scope fields from the current resource.' }
  if (field === 'rubricResults') return { type: ['object','array'] }
  if (booleans.has(field)) return { type: 'boolean' }
  if (integers.has(field)) return { type: ['integer','null'] }
  if (field === 'confidence') return { type: 'number', minimum: 0, maximum: 1 }
  return { type: ['string','null'] }
}
export function toolCatalog(namespace: string, methods: Readonly<Record<string, readonly string[]>>,
  reads: readonly string[], approvals: readonly string[] = [], transactional: readonly string[] = []): ToolDefinition[] {
  return Object.entries(methods).map(([method, fields]) => ({
    name: `${namespace}__${method}`, action: `${namespace}.${method}`,
    description: `${namespace}.${method}: ${reads.includes(method) ? 'Read current authorized data' : 'Execute an authorized business operation'}. `
      + (approvals.includes(method) ? 'Requires a human approval of the native preview. ' : '')
      + 'Use keyword arguments from the business capability instructions. Native schemas and permissions are enforced.',
    parameters: { type: 'object', properties: Object.fromEntries(fields.map(field => [field, parameter(namespace, field)])),
      ...(required[`${namespace}.${method}`] ? { required: required[`${namespace}.${method}`]! } : {}), additionalProperties: false },
    effect: reads.includes(method) ? 'read' : transactional.includes(method) ? 'transaction' : 'uncertain',
    approval: approvals.includes(method),
  }))
}
export const TASK_TOOLS = toolCatalog('task', {
  contract: ['deliverables','constraints','actions','acceptance'], ask: ['question'],
  check_receipt: ['idempotencyKey','action','expected'], check_resource: ['action','args','expected'], inspect: [],
}, ['check_receipt','check_resource','inspect'])
export function grantedTools(catalog: readonly ToolDefinition[], grants: readonly CapabilityGrant[]): ToolDefinition[] {
  return catalog.filter(tool => grants.some(grant => {
    const [namespace, method] = tool.action.split('.')
    return grant.name === namespace && (!grant.methods || grant.methods.includes(method!))
  }))
}
