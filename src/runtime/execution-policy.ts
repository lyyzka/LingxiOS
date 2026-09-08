import type { CapabilityGrant, CodeExecutionMode, TurnContext, WorkItem } from '../protocol/types.js'
import type { ToolDefinition } from '../tools/catalog.js'
import { grantedTools } from '../tools/catalog.js'
import type { RuntimePolicy } from './policy.js'
import { fingerprint } from '../context/compiler.js'

export type HarnessMode = 'chat' | 'read' | 'execute'
export function executionMode(work: Pick<WorkItem, 'meta'>): HarnessMode {
  const mode = work.meta?.['mode'] ?? 'execute'
  if (!['chat', 'read', 'execute'].includes(String(mode))) throw new Error('invalid execution mode')
  return mode as HarnessMode
}

/** Modes only narrow authorization. Internal task inspection remains available to the runtime. */
export function permitsTool(work: Pick<WorkItem, 'meta'>, tool: ToolDefinition): boolean {
  const mode = executionMode(work)
  if (work.meta?.['executionClass'] === 'conversation' && tool.execution?.class === 'operation') return false
  return mode === 'execute' || mode === 'read' && tool.effect === 'read'
}

export interface ExecutionSnapshot {
  mode: HarnessMode
  codeExecution: CodeExecutionMode
  grants: CapabilityGrant[]
  tools: ToolDefinition[]
  hash: string
}

/** Evaluate policy once per hop; never let a worker policy widen the control-plane catalog. */
export function executionSnapshot(context: TurnContext, policy: RuntimePolicy): ExecutionSnapshot {
  const mode = executionMode(context.work)
  const proposed = policy.kernelCapabilities(context)
  const tools = grantedTools(context.tools ?? [], proposed).filter(tool => permitsTool(context.work, tool))
  // Legacy hosts without a catalog retain advisory grants; production hosts always send a catalog.
  const grants = context.tools === undefined ? mode === 'execute' ? structuredClone(proposed) : [] : [...new Set(tools.map(tool => tool.action.split('.')[0]!))]
    .map(name => ({ name, methods: tools.filter(tool => tool.action.startsWith(name + '.')).map(tool => tool.action.split('.')[1]!) }))
  const configured = context.work.meta?.['codeExecution']
  const proposedCode = policy.codeExecutionMode?.(context) ?? 'enabled'
  const codeExecution = context.work.meta?.['executionClass'] !== 'conversation' && mode === 'execute' && (configured === undefined || configured === 'enabled') && proposedCode === 'enabled' ? 'enabled' : 'disabled'
  const value = { mode, codeExecution, grants, tools } as const
  return { ...value, hash: fingerprint(value) }
}
