import type { SqlPool } from '../../control-plane/pg-store.js'
import type { RunEvent, WorkItem } from '../../protocol/types.js'
import type { LingxiLoopServices } from './service-contracts.js'

const validIndex = (value: unknown): value is number => Number.isSafeInteger(value) && Number(value) >= 0

export async function deliverLingxiLoopEvent(database: SqlPool, services: LingxiLoopServices,
  work: Omit<WorkItem, 'leaseToken'>, event: RunEvent, channelType: number): Promise<void> {
  const canvasId = work.meta?.['canvasId']
  if (work.kind === 'canvas_worker' && typeof canvasId === 'string') {
    if (event.kind === 'run.started') {
      const updated = await database.query(`UPDATE canvas_agent_assignments
        SET status='working',started_at=COALESCE(started_at,NOW()),updated_at=NOW()
        WHERE id=$1 AND status NOT IN('completed','failed','cancelled') RETURNING id`, [work.meta?.['assignmentId']])
      if (updated.rows.length && services.setCanvasStatus) await services.setCanvasStatus({
        companyId: work.tenantId, canvasId, actorId: work.agentId, actorKind: 'agent', status: 'working',
      })
    }
    return
  }
  if (work.kind === 'memory_synthesis') return
  const messageId = `preview-${event.runId}`
  const publish = async (chunks: Array<Record<string, unknown>>, sequence = event.seq * 2) => {
    if (services.publishAssistantStream) await services.publishAssistantStream({ type: 'assistant.stream',
      companyId: work.tenantId, conversationId: work.sessionId, messageId, authorId: work.agentId, sequence, chunks })
  }
  if (event.kind === 'run.started' || event.kind === 'model.started') {
    await publish([{ type: 'step-start', path: [], messageId }])
  } else if (event.kind === 'model.delta') {
    const { delta, partIndex, partType } = event.data
    if (typeof delta !== 'string' || !delta || !validIndex(partIndex) || partType !== 'reasoning' && partType !== 'text') throw new Error('invalid model.delta event')
    await publish([...(event.data['partStart'] === true ? [{ type: 'part-start', path: [partIndex], part: { type: partType } }] : []),
      { type: 'text-delta', path: [partIndex], textDelta: delta }])
  } else if (event.kind === 'model.completed') {
    const index = event.data['finishPartIndex']
    if (index !== undefined && !validIndex(index)) throw new Error('invalid model.completed event')
    if (validIndex(index)) await publish([{ type: 'part-finish', path: [index] }])
  } else if (event.kind === 'tool.started') {
    const { toolCallId, partIndex, name } = event.data
    const text = '{}'
    if (typeof toolCallId !== 'string' || !toolCallId.startsWith('host:') || !validIndex(partIndex)
      || typeof name !== 'string' || !name) throw new Error('invalid tool.started event')
    await publish([{ type: 'part-start', path: [partIndex], part: { type: 'tool-call', toolCallId, toolName: name } },
      { type: 'text-delta', path: [partIndex], textDelta: text }, { type: 'tool-call-args-text-finish', path: [partIndex] }])
  } else if (event.kind === 'tool.completed') {
    const { toolCallId, partIndex, result, isError } = event.data
    const text = JSON.stringify(result)
    if (typeof toolCallId !== 'string' || !toolCallId.startsWith('host:') || !validIndex(partIndex)
      || typeof isError !== 'boolean' || result === undefined || !text || text.length > 8_000) throw new Error('invalid tool.completed event')
    await publish([{ type: 'result', path: [partIndex], result, isError }, { type: 'part-finish', path: [partIndex] }])
  } else if (event.kind === 'run.failed' || event.kind === 'run.cancelled') {
    await publish([{ type: 'error', path: [], error: String(event.data['error'] ?? event.kind), code: event.kind }])
  } else if (event.kind === 'run.completed' && event.data['deferred'] === true) {
    await publish([{ type: 'message-finish', path: [], finishReason: 'stop', usage: { inputTokens: 0, outputTokens: 0 } }])
  } else if (event.kind === 'approval.pending') {
    const approvalId = String(event.data['approvalId'] ?? '')
    if (!approvalId) throw new Error('approval.pending must identify its approval')
    const { rows } = await database.query(`SELECT id,agent_id,action,args,summary,status,requested_at,resolved_at,resolved_by,requested_by,scope,preview
      FROM approvals WHERE id=$1 AND company_id=$2 AND source='AGENT_OS'`, [approvalId, work.tenantId])
    const approval = rows[0]
    if (approval) await services.wukongClient().sendMessage(work.sessionId, channelType, work.agentId, {
      version: 1, kind: 'approval', clientMsgNo: `approval-${approvalId}`, body: String(approval['summary']),
      refs: { approvalId, runId: event.runId, agentId: work.agentId }, data: {
        id: approval['id'], agentId: approval['agent_id'], kind: String((approval['scope'] as Record<string, unknown> | undefined)?.['risk'] ?? 'sensitive_or_destructive_action'),
        summary: approval['summary'], status: approval['status'], payload: { action: approval['action'], args: approval['args'] },
        requestedAt: approval['requested_at'], resolvedAt: approval['resolved_at'], resolvedBy: approval['resolved_by'],
        requestedBy: approval['requested_by'], scope: approval['scope'], preview: approval['preview'], suppressAgentWake: true,
      },
    })
    await publish([{ type: 'message-finish', path: [], finishReason: 'tool-calls', usage: { inputTokens: 0, outputTokens: 0 } }], event.seq * 2 + 1)
  } else if (event.visibility === 'user' && event.kind !== 'run.completed') {
    await services.wukongClient().sendMessage(work.sessionId, channelType, work.agentId, {
      version: 1, kind: 'tool_activity', clientMsgNo: `activity-${event.runId}-${event.seq}`, body: event.kind,
      refs: { runId: event.runId, agentId: work.agentId }, data: { stage: event.stage, suppressAgentWake: true },
    })
  }
}

export async function finishLingxiLoopStream(database: SqlPool, services: LingxiLoopServices, work: Omit<WorkItem, 'leaseToken'>) {
  if (!services.publishAssistantStream) return
  const { rows } = await database.query(`SELECT
    (SELECT COALESCE(MAX(seq),0) FROM lingxios.agent_run_events WHERE run_id=$1) AS seq,
    COALESCE(SUM(input_tokens),0) AS input_tokens,COALESCE(SUM(output_tokens),0) AS output_tokens
    FROM lingxios.agent_model_budget_calls WHERE work_id=$1`, [work.id])
  const row = rows[0] ?? {}
  await services.publishAssistantStream({ type: 'assistant.stream', companyId: work.tenantId, conversationId: work.sessionId,
    messageId: `preview-${work.id}`, authorId: work.agentId, sequence: Number(row['seq'] ?? 0) * 2 + 1,
    chunks: [{ type: 'message-finish', path: [], finishReason: 'stop', usage: {
      inputTokens: Number(row['input_tokens'] ?? 0), outputTokens: Number(row['output_tokens'] ?? 0),
    } }] })
}
