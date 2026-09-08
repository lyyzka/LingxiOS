import { isDeepStrictEqual } from 'node:util'
import type { SqlQueryable } from '../control-plane/pg-store.js'
import type { WorkItem } from '../protocol/types.js'
import { enqueueChild, requestSnapshot } from '../app/jobs.js'
import { authorizeConversationWork, digest, identifier } from './access.js'
import type { GraphInput, GraphNode } from './types.js'

/** Deliberately only a bounded DAG of existing agent work, with no expression evaluator. */
export function graphNodes(input: GraphInput): GraphNode[] {
  identifier(input.id)
  if (!Array.isArray(input.nodes) || !input.nodes.length || input.nodes.length > 64) throw new Error('graph requires 1-64 nodes')
  const nodes = new Map<string, GraphNode>()
  for (const node of input.nodes) {
    if (!node || typeof node !== 'object' || Object.keys(node).some(key => !['id','agentId','text','dependsOn'].includes(key))) throw new Error('invalid graph node fields')
    identifier(node.id); identifier(node.agentId)
    if (nodes.has(node.id) || typeof node.text !== 'string' || !node.text.trim() || node.text.length > 100_000
      || node.dependsOn !== undefined && (!Array.isArray(node.dependsOn) || node.dependsOn.length > 64
        || new Set(node.dependsOn).size !== node.dependsOn.length)) throw new Error('invalid or duplicate graph node')
    for (const dependency of node.dependsOn ?? []) identifier(dependency)
    nodes.set(node.id, { id: node.id, agentId: node.agentId, text: node.text, dependsOn: [...node.dependsOn ?? []].sort() })
  }
  const ordered: GraphNode[] = [], visiting = new Set<string>(), visited = new Set<string>()
  function visit(id: string) {
    const node = nodes.get(id)
    if (!node) throw new Error('graph dependency is missing')
    if (visiting.has(id)) throw new Error('cyclic graph dependency')
    if (visited.has(id)) return
    visiting.add(id)
    for (const dependency of node.dependsOn!) visit(dependency)
    visiting.delete(id); visited.add(id); ordered.push(node)
  }
  for (const id of [...nodes.keys()].sort()) visit(id)
  return ordered
}
const graphId = (parent: Omit<WorkItem, 'leaseToken'>, version: number, id: string) => 'graph:' + digest([parent.id, version, id])

/** Caller supplies a transaction holding the parent work lock. */
export async function enqueueGraph(database: SqlQueryable, parent: Omit<WorkItem, 'leaseToken'>, version: number | null, input: GraphInput) {
  const nodes = graphNodes(input), request = await requestSnapshot(database, parent.id, version)
  await authorizeConversationWork(database, parent, 'execute', true)
  const id = graphId(parent, request.revisions.length + 1, input.id)
  const definition = { id: input.id, nodes }
  const prior = await database.query('SELECT definition FROM lingxios.agent_graphs WHERE id=$1', [id])
  if (prior.rows[0]) {
    if (!isDeepStrictEqual(prior.rows[0]['definition'], definition)) throw new Error('graph identity reused with different nodes')
    return { id: input.id, nodes: nodes.map(node => ({ id: node.id, workId: 'graph-node:' + digest([id, node.id]) })), deduplicated: true }
  }
  await database.query(`INSERT INTO lingxios.agent_graphs(id,parent_work_id,request_version,definition) VALUES($1,$2,$3,$4::jsonb)`,
    [id, parent.id, version, JSON.stringify(definition)])
  const result: Array<{ id: string; workId: string }> = []
  for (const node of nodes) {
    const workId = 'graph-node:' + digest([id, node.id])
    await enqueueChild(database, parent, version, { id: workId, agentId: node.agentId, text: node.text,
      dependsOn: node.dependsOn!.map(dependency => 'graph-node:' + digest([id, dependency])) }, id)
    await database.query('INSERT INTO lingxios.agent_graph_nodes(graph_id,node_id,work_id) VALUES($1,$2,$3)', [id, node.id, workId])
    result.push({ id: node.id, workId })
  }
  return { id: input.id, nodes: result, deduplicated: false }
}

export async function readGraph(database: SqlQueryable, parent: Omit<WorkItem, 'leaseToken'>, version: number, id: string) {
  identifier(id)
  await authorizeConversationWork(database, parent, 'read')
  const { rows } = await database.query(`SELECT node.node_id,work.id,work.status,work.goal_outcome,work.error,result.id AS result_id,
      result.message->>'body' AS result_text FROM lingxios.agent_graphs graph
    JOIN lingxios.agent_graph_nodes node ON node.graph_id=graph.id JOIN lingxios.agent_work_items work ON work.id=node.work_id
    LEFT JOIN lingxios.agent_results result ON result.id=work.result_id
    WHERE graph.id=$1 AND graph.parent_work_id=$2 AND graph.request_version=$3 ORDER BY node.node_id LIMIT 64`, [graphId(parent, version, id), parent.id, version])
  return rows.length ? { id, nodes: rows.map(row => ({ id: String(row['node_id']), workId: String(row['id']), status: String(row['status']),
    resultId: row['result_id'] as string | null, text: row['result_text'] as string | null, goalOutcome: row['goal_outcome'], error: row['error'] })) } : null
}

/** Stores the precise fan-in set; the existing delegated taskRef is its stable anchor. */
export async function waitForChildren(database: SqlQueryable, parent: Omit<WorkItem, 'leaseToken'>, version: number | null, children: string[]) {
  if (!Array.isArray(children) || !children.length || children.length > 64 || new Set(children).size !== children.length) throw new Error('wait requires 1-64 distinct children')
  for (const id of children) identifier(id)
  await requestSnapshot(database, parent.id, version)
  await authorizeConversationWork(database, parent, 'execute', true)
  const ids = [...children].sort(), taskRef = ids[0]!
  const { rows } = await database.query(`SELECT id FROM lingxios.agent_work_items WHERE id=ANY($1::text[])
    AND tenant_id=$2 AND principal_id IS NOT DISTINCT FROM $3 AND meta->>'parentWorkId'=$4 AND meta->'parentRequestVersion'=$5::jsonb`,
  [ids, parent.tenantId, parent.principalId ?? null, parent.id, JSON.stringify(version)])
  if (rows.length !== ids.length) throw new Error('wait children are outside the parent request')
  await database.query(`INSERT INTO lingxios.agent_work_waits(parent_work_id,request_version,task_ref,children)
    VALUES($1,$2,$3,$4::jsonb) ON CONFLICT DO NOTHING`, [parent.id, version, taskRef, JSON.stringify(ids)])
  const prior = await database.query(`SELECT children FROM lingxios.agent_work_waits WHERE parent_work_id=$1 AND request_version=$2 AND task_ref=$3`, [parent.id, version, taskRef])
  if (!isDeepStrictEqual(prior.rows[0]?.['children'], ids)) throw new Error('wait anchor reused with a different dependency set')
  return { type: 'defer' as const, reason: 'child', data: { taskRef } }
}

export async function verifyGraphResults(database: SqlQueryable, parent: Omit<WorkItem, 'leaseToken'>, version: number) {
  const { rows } = await database.query(`SELECT graph.id,COUNT(*)::integer AS nodes,
    COUNT(*) FILTER(WHERE work.status='succeeded' AND work.cancel_requested_at IS NULL
      AND result.request_version=jsonb_array_length(work.steer_inputs)+1
      AND result.message->'envelope'->'goalOutcome'->>'status'='satisfied')::integer AS satisfied
    FROM lingxios.agent_graphs graph JOIN lingxios.agent_graph_nodes node ON node.graph_id=graph.id
    JOIN lingxios.agent_work_items work ON work.id=node.work_id LEFT JOIN lingxios.agent_results result ON result.id=work.result_id
    WHERE graph.parent_work_id=$1 AND graph.request_version=$2 GROUP BY graph.id ORDER BY graph.id LIMIT 64`, [parent.id, version])
  return rows.map(row => ({ checker: String(row['id']), status: row['nodes'] === row['satisfied'] ? 'passed' as const : 'inconclusive' as const,
    evidence: { nodes: Number(row['nodes']), satisfied: Number(row['satisfied']), scope: 'current_committed_graph_results' } }))
}
