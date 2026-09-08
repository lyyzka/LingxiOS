import { createHash } from 'node:crypto'
import { isDeepStrictEqual } from 'node:util'
import type { SqlQueryable } from '../control-plane/pg-store.js'
import type { WorkItem } from '../protocol/types.js'
import type { MemoryScope } from './store.js'
import { memoryWriteBody, type MemoryWritePolicy } from './policy.js'
import { lockMemoryScopes, sameMemoryEpochs, type MemoryEpoch } from './forget.js'

export interface EvolutionCandidate { kind: 'experience' | 'skill' | 'strategy'; scopeType: string; body: string }
export interface EvolutionCase { id: string; split: 'target' | 'holdout'; input: unknown }
export interface EvolutionBenchmark {
  id: string
  evaluatorVersion: string
  repetitions: number
  gates: string[]
  cases: EvolutionCase[]
}
export interface EvolutionReport { success: boolean; gates: Record<string, boolean>; durationMs: number; costMicros: number }
export interface EvolutionReference extends MemoryScope { id: string; version: number }
export interface EvolutionPlan {
  candidate: EvolutionCandidate
  baseline: EvolutionCandidate | null
  benchmark: EvolutionBenchmark
  records: Record<string, EvolutionReport>
}

const digest = (value: unknown) => createHash('sha256').update(JSON.stringify(value)).digest('hex')
const identifier = (value: unknown): value is string => typeof value === 'string' && !!value.trim() && value.length <= 1000
const candidateOf = (row: Record<string, unknown>): EvolutionCandidate => ({ kind: row['kind'] as EvolutionCandidate['kind'], scopeType: String(row['scope_type']), body: String(row['body']) })
const referenceOf = (row: Record<string, unknown>): EvolutionReference => ({ tenantId: String(row['tenant_id']), id: String(row['id']),
  version: Number(row['version']), scopeType: String(row['scope_type']), scopeId: String(row['scope_id']) })

/** Trusted evaluation fixtures are frozen before proposal. No model-visible tool can replace them. */
export async function freezeEvolutionBenchmark(database: SqlQueryable, tenantId: string, benchmark: EvolutionBenchmark) {
  if (!identifier(tenantId) || !identifier(benchmark.id) || !identifier(benchmark.evaluatorVersion)
    || !Number.isSafeInteger(benchmark.repetitions) || benchmark.repetitions < 2 || benchmark.repetitions > 10
    || !Array.isArray(benchmark.gates) || !benchmark.gates.length || benchmark.gates.length > 16
    || benchmark.gates.some(gate => !identifier(gate)) || new Set(benchmark.gates).size !== benchmark.gates.length
    || ['authorization','approval','isolation','no_code_mutation'].some(gate => !benchmark.gates.includes(gate))
    || !Array.isArray(benchmark.cases) || benchmark.cases.length > 64 || new Set(benchmark.cases.map(item => item.id)).size !== benchmark.cases.length
    || benchmark.cases.some(item => !identifier(item.id) || !['target','holdout'].includes(item.split) || item.input === undefined)
    || ['target','holdout'].some(split => benchmark.cases.filter(item => item.split === split).length < 2)
    || Buffer.byteLength(JSON.stringify(benchmark)) > 128_000) throw new Error('invalid independent evolution benchmark')
  if (new Set(benchmark.cases.map(item => digest(item.input))).size !== benchmark.cases.length) throw new Error('benchmark cases must be independent')
  const hash = digest(benchmark)
  await database.query(`INSERT INTO lingxios.agent_evolution_benchmarks(tenant_id,id,hash,definition)
    VALUES($1,$2,$3,$4::jsonb) ON CONFLICT(tenant_id,id) DO NOTHING`, [tenantId,benchmark.id,hash,JSON.stringify(benchmark)])
  const existing = await database.query('SELECT hash FROM lingxios.agent_evolution_benchmarks WHERE tenant_id=$1 AND id=$2', [tenantId,benchmark.id])
  if (existing.rows[0]?.['hash'] !== hash) throw new Error('benchmark identity is frozen; use a new versioned ID')
  return { id: benchmark.id, hash }
}

export function parseEvolutionCandidates(value: unknown): EvolutionCandidate[] {
  if (!Array.isArray(value) || value.length > 3 || value.some(item => !item || typeof item !== 'object'
    || Object.keys(item).some(key => !['kind','scopeType','body'].includes(key))
    || !['experience','skill','strategy'].includes(item.kind) || !identifier(item.scopeType)
    || typeof item.body !== 'string' || !item.body.trim() || item.body.length > 2000)
    || new Set(value.map(item => JSON.stringify([item.scopeType,item.kind]))).size !== value.length) throw new Error('invalid evolution candidates')
  return value as EvolutionCandidate[]
}

/** Called in the synthesis action transaction, after its source and scopes have been authorized. */
export async function proposeEvolution(database: SqlQueryable, work: Omit<WorkItem, 'leaseToken'>, scopes: MemoryScope[],
  benchmarkId: string, candidates: EvolutionCandidate[], policy?: MemoryWritePolicy) {
  parseEvolutionCandidates(candidates)
  if (!candidates.length) return []
  const epochs = await lockMemoryScopes(database, scopes)
  const { rows } = await database.query(`SELECT e.*,source.result_id FROM lingxios.agent_memory_evidence e
    JOIN lingxios.agent_work_items source ON source.id=e.source_run_id
    JOIN lingxios.agent_work_items job ON job.meta->>'sourceRunId'=source.id
    JOIN lingxios.agent_evolution_benchmarks benchmark ON benchmark.tenant_id=e.tenant_id AND benchmark.id=$6
    WHERE job.id=$1 AND job.fence=$2 AND job.kind='memory_synthesis' AND job.status='leased'
      AND job.lease_expires_at>NOW() AND job.cancel_requested_at IS NULL
      AND job.tenant_id=e.tenant_id AND job.agent_id=e.agent_id AND job.principal_id=e.principal_id AND job.session_id=e.session_id
      AND source.session_id=job.session_id AND source.thread_id IS NOT DISTINCT FROM job.thread_id
      AND e.tenant_id=$3 AND e.agent_id=$4 AND e.principal_id=$5 AND e.status='processed'
      AND source.cancel_requested_at IS NULL AND source.status IN ('succeeded','partial') AND source.result_id IS NOT NULL
      AND e.request_version=jsonb_array_length(source.steer_inputs)+1 FOR UPDATE OF source,e`,
  [work.id,work.fence,work.tenantId,work.agentId,work.principalId,benchmarkId])
  const source = rows[0]
  if (!source) throw new Error('evolution requires current committed evidence and a frozen benchmark')
  const recorded = source['scopes'] as MemoryScope[]
  if (!sameMemoryEpochs(epochs.filter(scope => recorded.some(item => item.scopeType === scope.scopeType && item.scopeId === scope.scopeId)),
    source['scope_epochs'] as MemoryEpoch[] ?? [])) throw new Error('evolution source was forgotten')
  const ids: string[] = []
  for (const candidate of candidates) {
    const scope = scopes.find(item => item.tenantId === work.tenantId && item.scopeType === candidate.scopeType
      && recorded.some(saved => isDeepStrictEqual(saved,item)))
    if (!scope) throw new Error('candidate scope is unavailable')
    const body = await memoryWriteBody({ scope, principalId: work.principalId!, sourceWorkId: String(source['source_run_id']),
      origin: 'evolved', kind: candidate.kind, body: candidate.body }, policy)
    const id = `evolution:${digest([work.id,candidate])}`
    const baseline = (await database.query(`SELECT * FROM lingxios.agent_memories WHERE tenant_id=$1 AND scope_type=$2
      AND scope_id=$3 AND kind=$4 AND origin='evolved' AND status='active'`, [scope.tenantId,scope.scopeType,scope.scopeId,candidate.kind])).rows[0]
    await database.query(`INSERT INTO lingxios.agent_memories(tenant_id,id,scope_type,scope_id,kind,body,origin,status,source_refs)
      VALUES($1,$2,$3,$4,$5,$6,'evolved','candidate',$7::jsonb) ON CONFLICT(tenant_id,id) DO NOTHING`,
    [scope.tenantId,id,scope.scopeType,scope.scopeId,candidate.kind,body,JSON.stringify([{ workId: source['source_run_id'],
      resultId: source['result_id'], requestVersion: source['request_version'], inputSha256: source['input_sha256'] }])])
    await database.query(`INSERT INTO lingxios.agent_evolution_evaluations(tenant_id,memory_id,candidate_version,benchmark_id,baseline)
      VALUES($1,$2,1,$3,$4::jsonb) ON CONFLICT(tenant_id,memory_id) DO NOTHING`,
    [scope.tenantId,id,benchmarkId,JSON.stringify(baseline ? { reference: referenceOf(baseline), candidate: candidateOf(baseline) } : null)])
    await database.query(`INSERT INTO lingxios.agent_work_items(id,tenant_id,agent_id,principal_id,session_id,thread_id,kind,lane,trigger_ref,meta)
      VALUES($1,$2,$3,$4,$5,$6,'memory_evaluation','background',$7,$8::jsonb) ON CONFLICT(id) DO NOTHING`,
    [`evaluate:${id}`,work.tenantId,work.agentId,work.principalId,work.sessionId,work.threadId ?? null,id,
      JSON.stringify({ sourceRunId: source['source_run_id'], memoryId: id })])
    ids.push(id)
  }
  return ids
}

export function evolutionCaseKey(caseId: string, repetition: number, variant: 'baseline' | 'candidate') {
  return JSON.stringify([caseId,repetition,variant])
}

export async function executeEvolution(database: SqlQueryable, work: Omit<WorkItem, 'leaseToken'>, scopes: MemoryScope[],
  method: 'load' | 'record' | 'finish', args: Record<string, unknown>): Promise<unknown> {
  const { rows } = await database.query(`SELECT m.*,e.baseline,e.records,e.verdict,e.candidate_version,b.definition AS benchmark
    FROM lingxios.agent_work_items job JOIN lingxios.agent_memories m ON m.id=job.meta->>'memoryId' AND m.tenant_id=job.tenant_id
    JOIN lingxios.agent_evolution_evaluations e ON e.tenant_id=m.tenant_id AND e.memory_id=m.id
    JOIN lingxios.agent_evolution_benchmarks b ON b.tenant_id=e.tenant_id AND b.id=e.benchmark_id
    JOIN lingxios.agent_memory_evidence source ON source.source_run_id=job.meta->>'sourceRunId'
    WHERE job.id=$1 AND job.fence=$2 AND job.tenant_id=$3 AND job.agent_id=$4 AND job.principal_id=$5
      AND job.kind='memory_evaluation' AND job.status='leased' AND job.lease_expires_at>NOW() AND job.cancel_requested_at IS NULL
      AND source.status='processed' AND source.tenant_id=job.tenant_id AND source.principal_id=job.principal_id
      AND m.origin='evolved' AND m.status<>'expired' FOR UPDATE OF m,e`,
  [work.id,work.fence,work.tenantId,work.agentId,work.principalId])
  const row = rows[0]
  if (!row || !scopes.some(scope => scope.tenantId === work.tenantId && scope.scopeType === row['scope_type'] && scope.scopeId === row['scope_id'])) {
    throw new Error('evolution lease, scope or source was revoked')
  }
  if (row['verdict'] !== 'pending') return method === 'load' ? null : { verdict: row['verdict'] }
  if (row['version'] !== row['candidate_version'] || row['status'] !== 'candidate') throw new Error('candidate changed during evaluation')
  const benchmark = row['benchmark'] as EvolutionBenchmark
  const baseline = row['baseline'] as { reference: EvolutionReference; candidate: EvolutionCandidate } | null
  const records = row['records'] as Record<string, EvolutionReport>
  if (method === 'load') return { candidate: candidateOf(row), baseline: baseline?.candidate ?? null, benchmark, records } satisfies EvolutionPlan
  if (method === 'record') {
    const report = args['report'] as EvolutionReport | undefined
    if (!benchmark.cases.some(item => item.id === args['caseId']) || !Number.isSafeInteger(args['repetition'])
      || Number(args['repetition']) < 0 || Number(args['repetition']) >= benchmark.repetitions
      || !['baseline','candidate'].includes(String(args['variant'])) || !report || typeof report.success !== 'boolean'
      || !report.gates || !isDeepStrictEqual(Object.keys(report.gates).sort(),[...benchmark.gates].sort())
      || Object.values(report.gates).some(value => typeof value !== 'boolean')
      || !Number.isSafeInteger(report.durationMs) || report.durationMs < 0 || !Number.isSafeInteger(report.costMicros) || report.costMicros < 0
      || Object.keys(report).some(key => !['success','gates','durationMs','costMicros'].includes(key))
      || args['evaluatorVersion'] !== benchmark.evaluatorVersion) throw new Error('invalid independent evaluation report')
    const key = evolutionCaseKey(String(args['caseId']),Number(args['repetition']),args['variant'] as 'baseline' | 'candidate')
    if (records[key] && !isDeepStrictEqual(records[key],report)) throw new Error('evaluation report is immutable')
    await database.query(`UPDATE lingxios.agent_evolution_evaluations SET records=jsonb_set(records,ARRAY[$3::text],$4::jsonb)
      WHERE tenant_id=$1 AND memory_id=$2`, [work.tenantId,row['id'],key,JSON.stringify(report)])
    return { recorded: key }
  }
  const totals = { target: { baseline: 0, candidate: 0 }, holdout: { baseline: 0, candidate: 0 } }
  let gatesPassed = true, durationMs = 0, costMicros = 0
  for (const item of benchmark.cases) for (let repetition = 0; repetition < benchmark.repetitions; repetition++) {
    for (const variant of ['baseline','candidate'] as const) {
      const report = records[evolutionCaseKey(item.id,repetition,variant)]
      if (!report) return { verdict: 'pending', reason: 'independent evaluation is incomplete' }
      totals[item.split][variant] += Number(report.success)
      gatesPassed &&= Object.values(report.gates).every(Boolean)
      durationMs += report.durationMs; costMicros += report.costMicros
    }
  }
  const active = (await database.query(`SELECT * FROM lingxios.agent_memories WHERE tenant_id=$1 AND scope_type=$2 AND scope_id=$3
    AND kind=$4 AND origin='evolved' AND status='active' FOR UPDATE`, [work.tenantId,row['scope_type'],row['scope_id'],row['kind']])).rows[0]
  const current = active ? referenceOf(active) : null
  const verdict = !isDeepStrictEqual(current,baseline?.reference ?? null) ? 'stale'
    : gatesPassed && totals.target.candidate > totals.target.baseline && totals.holdout.candidate >= totals.holdout.baseline ? 'passed' : 'failed'
  if (verdict === 'passed') {
    if (active) await database.query(`UPDATE lingxios.agent_memories SET status='retired',version=version+1,updated_at=NOW()
      WHERE tenant_id=$1 AND id=$2`, [work.tenantId,active['id']])
    await database.query(`UPDATE lingxios.agent_memories SET status='active',version=version+1,updated_at=NOW()
      WHERE tenant_id=$1 AND id=$2`, [work.tenantId,row['id']])
  }
  const summary = { totals, gatesPassed, durationMs, costMicros }
  await database.query(`UPDATE lingxios.agent_evolution_evaluations SET verdict=$3,summary=$4::jsonb,evaluated_at=NOW()
    WHERE tenant_id=$1 AND memory_id=$2`, [work.tenantId,row['id'],verdict,JSON.stringify(summary)])
  return { verdict, ...summary }
}

/** Pins only references once per run. Revoked sources/scopes can remove hints, never replace them mid-run. */
export async function pinnedEvolution(database: SqlQueryable, work: Omit<WorkItem, 'leaseToken'>, scopes: MemoryScope[]) {
  const run = (await database.query(`SELECT strategy_snapshot FROM lingxios.agent_work_items WHERE id=$1 AND fence=$2 AND tenant_id=$3
    AND status='leased' AND lease_expires_at>NOW() AND cancel_requested_at IS NULL FOR UPDATE`, [work.id,work.fence,work.tenantId])).rows[0]
  if (!run) throw new Error('strategy pin requires a live run')
  let references = run['strategy_snapshot'] as EvolutionReference[] | null
  if (!references) {
    const current = await database.query(`SELECT m.* FROM lingxios.agent_memories m WHERE m.tenant_id=$1 AND m.origin='evolved'
      AND m.status='active' AND (m.valid_until IS NULL OR m.valid_until>NOW())
      AND EXISTS(SELECT 1 FROM jsonb_to_recordset($2::jsonb) AS s("tenantId" text,"scopeType" text,"scopeId" text)
        WHERE s."tenantId"=m.tenant_id AND s."scopeType"=m.scope_type AND s."scopeId"=m.scope_id)
      ORDER BY m.updated_at DESC,m.id LIMIT 12`, [work.tenantId,JSON.stringify(scopes)])
    references = current.rows.map(referenceOf)
    await database.query('UPDATE lingxios.agent_work_items SET strategy_snapshot=$2::jsonb WHERE id=$1', [work.id,JSON.stringify(references)])
  }
  const allowed = references.filter(ref => scopes.some(scope => scope.tenantId === ref.tenantId && scope.scopeType === ref.scopeType && scope.scopeId === ref.scopeId))
  const { rows } = await database.query(`SELECT COALESCE(v.snapshot,to_jsonb(m)) AS snapshot
    FROM jsonb_to_recordset($2::jsonb) AS r(id text,version int) JOIN lingxios.agent_memories m ON m.id=r.id AND m.tenant_id=$1
    LEFT JOIN lingxios.agent_memory_versions v ON v.tenant_id=m.tenant_id AND v.memory_id=m.id AND v.version=r.version
    WHERE m.origin='evolved' AND m.status IN ('active','retired') AND (m.valid_until IS NULL OR m.valid_until>NOW())
      AND (v.snapshot IS NOT NULL OR m.version=r.version)`, [work.tenantId,JSON.stringify(allowed)])
  return rows.map((row): Record<string, unknown> => ({ ...(row['snapshot'] as Record<string, unknown>), evaluated: true }))
}

/** Trusted product administration must authorize this scope; this is never an agent tool. */
export async function rollbackEvolution(database: SqlQueryable, scope: MemoryScope, activeId: string, expectedVersion: number, targetId: string | null) {
  const { rows } = await database.query(`SELECT m.*,e.verdict FROM lingxios.agent_memories m
    JOIN lingxios.agent_evolution_evaluations e ON e.tenant_id=m.tenant_id AND e.memory_id=m.id
    WHERE m.tenant_id=$1 AND m.scope_type=$2 AND m.scope_id=$3 AND m.origin='evolved' AND m.id=ANY($4::text[])
    ORDER BY m.id FOR UPDATE OF m`, [scope.tenantId,scope.scopeType,scope.scopeId,[activeId,...targetId ? [targetId] : []]])
  const active = rows.find(row => row['id'] === activeId), target = rows.find(row => row['id'] === targetId)
  if (!active || active['status'] !== 'active' || active['version'] !== expectedVersion
    || targetId !== null && (!target || target['status'] !== 'retired' || target['verdict'] !== 'passed' || target['kind'] !== active['kind'])) throw new Error('rollback version or evaluated target is unavailable')
  await database.query(`UPDATE lingxios.agent_memories SET status='retired',version=version+1,updated_at=NOW()
    WHERE tenant_id=$1 AND id=$2`, [scope.tenantId,activeId])
  if (targetId) await database.query(`UPDATE lingxios.agent_memories SET status='active',version=version+1,updated_at=NOW()
    WHERE tenant_id=$1 AND id=$2`, [scope.tenantId,targetId])
  return { activeId: targetId }
}
