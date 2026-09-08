import { createHash } from 'node:crypto'
import { isDeepStrictEqual } from 'node:util'
import type { KernelArtifact } from '../protocol/types.js'
import type { SqlPool } from './pg-store.js'
import type { StoreLeaseProof } from './stores.js'
import { progressFacts } from '../runtime/corrections.js'

export interface ExecutionStep {
  id: string
  requestVersion: number
  kind: string
  input: Record<string, unknown>
  output?: string
  artifacts: KernelArtifact[]
}
export interface StepStore {
  save(proof: StoreLeaseProof, step: ExecutionStep): Promise<void>
  get(workId: string, id: string, requestVersion: number): Promise<ExecutionStep | null>
  list(workId: string, requestVersion?: number): Promise<ExecutionStep[]>
}
export class PgStepStore implements StepStore {
  constructor(private readonly database: SqlPool) {}
  async save(proof: StoreLeaseProof, step: ExecutionStep) {
    const inputHash = createHash('sha256').update(JSON.stringify(step.input)).digest('hex')
    let observed: unknown
    try { observed = step.output ? JSON.parse(step.output).receipts : undefined } catch { /* Text output contains no resource facts. */ }
    const facts = JSON.stringify(progressFacts({ artifacts: step.artifacts, observations: observed }))
    const progressHash = facts !== '{}' && step.output !== undefined ? createHash('sha256').update(facts).digest('hex') : null
    const { rows } = await this.database.query(`WITH saved AS (INSERT INTO lingxios.agent_steps
      (work_id,step_id,request_version,kind,input_hash,input,output,artifacts,completed_at,progress_hash)
      SELECT $1,$4,$5,$6,$7,$8::jsonb,$9::jsonb,$10::jsonb,CASE WHEN $9::jsonb IS NULL THEN NULL ELSE NOW() END,$11
      FROM lingxios.agent_work_items work WHERE work.id=$1 AND work.fence=$2 AND work.lease_token_hash=$3
        AND work.status='leased' AND work.lease_expires_at>NOW() AND work.cancel_requested_at IS NULL
        AND jsonb_array_length(work.steer_inputs)+1=$5
      ON CONFLICT(work_id,step_id) DO UPDATE SET output=EXCLUDED.output,artifacts=EXCLUDED.artifacts,completed_at=EXCLUDED.completed_at,progress_hash=EXCLUDED.progress_hash
      WHERE agent_steps.request_version=EXCLUDED.request_version AND agent_steps.kind=EXCLUDED.kind
        AND agent_steps.input_hash=EXCLUDED.input_hash
        AND (agent_steps.output IS NULL OR (agent_steps.output=EXCLUDED.output AND agent_steps.artifacts=EXCLUDED.artifacts))
      RETURNING step_id), progressed AS (UPDATE lingxios.agent_work_items SET last_progress_at=NOW()
        WHERE id=$1 AND fence=$2 AND $11::text IS NOT NULL AND EXISTS(SELECT 1 FROM saved)
          AND NOT EXISTS(SELECT 1 FROM lingxios.agent_steps WHERE work_id=$1 AND request_version=$5 AND progress_hash=$11) RETURNING id)
      SELECT step_id FROM saved`, [proof.workId, proof.fence, proof.leaseTokenHash, step.id, step.requestVersion,
      step.kind, inputHash, JSON.stringify(step.input), step.output === undefined ? null : JSON.stringify(step.output), JSON.stringify(step.artifacts),progressHash])
    if (!rows.length) throw new Error('step identity changed or attempt no longer owns this request')
  }
  async get(workId: string, id: string, requestVersion: number) {
    const { rows } = await this.database.query('SELECT * FROM lingxios.agent_steps WHERE work_id=$1 AND step_id=$2 AND request_version=$3', [workId, id, requestVersion])
    return rows[0] ? fromRow(rows[0]) : null
  }
  async list(workId: string, requestVersion?: number) {
    const { rows } = await this.database.query(`SELECT * FROM lingxios.agent_steps WHERE work_id=$1
      AND ($2::integer IS NULL OR request_version=$2) ORDER BY step_seq LIMIT 2049`, [workId, requestVersion ?? null])
    if (rows.length > 2048) throw new Error('step history exceeds the bounded recovery limit')
    return rows.map(fromRow)
  }
}
export function fromRow(row: Record<string, unknown>): ExecutionStep {
  return { id: String(row['step_id']), kind: String(row['kind']), requestVersion: Number(row['request_version']),
    input: row['input'] as Record<string, unknown>, ...(row['output'] === null ? {} : { output: String(row['output']) }),
    artifacts: row['artifacts'] as KernelArtifact[] }
}

export class MemoryStepStore implements StepStore {
  private readonly entries = new Map<string, ExecutionStep>()
  async save(proof: StoreLeaseProof, step: ExecutionStep) {
    const key = JSON.stringify([proof.workId, step.id]), prior = this.entries.get(key)
    if (prior && (!isDeepStrictEqual(prior.input, step.input) || prior.kind !== step.kind || prior.requestVersion !== step.requestVersion
      || prior.output !== undefined && !isDeepStrictEqual(prior, step))) throw new Error('step identity changed')
    this.entries.set(key, structuredClone(step))
  }
  async get(workId: string, id: string, version: number) {
    const step = this.entries.get(JSON.stringify([workId, id]))
    return step?.requestVersion === version ? structuredClone(step) : null
  }
  async list(workId: string, version?: number) {
    return [...this.entries].filter(([key, step]) => JSON.parse(key)[0] === workId && (version === undefined || step.requestVersion === version))
      .map(([, step]) => structuredClone(step))
  }
}
