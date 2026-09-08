/**
 * Postgres store implementations over an injected pg-compatible pool (any
 * object with `query` and `connect`, e.g. `pg.Pool`). Keeping the driver out
 * of this package's dependency tree lets deployments pin their own.
 *
 * Semantics mirror `memory-store.ts`, which is the executable specification;
 * the schema lives in `db/schema.sql`.
 */
import { createHash, randomBytes, randomUUID } from 'node:crypto'
import { isDeepStrictEqual } from 'node:util'
import { resumeDependents } from './dependencies.js'
import { fromRow as stepFromRow, type ExecutionStep } from './steps.js'
import { sessionKeyOf, workStatusOf } from '../protocol/types.js'
import { cancelDescendants, cancelRun, enqueueWork } from '../app/jobs.js'
import type {
  HostActionResult, SessionRecord, WorkCompletion, WorkItem,
} from '../protocol/types.js'
import type {
  ActionIntent, ActionLedgerStore, ActionResolution, EnqueueResult, EnqueueWorkInput, EventStore, HeartbeatRow,
  LeasedWork, SaveSessionResult, SessionStore, StoredRunEvent, WorkStore, WorkStoreOptions,
  ModelBudgetLimits, ModelBudgetReservation, ModelBudgetStore,
} from './stores.js'

/** The subset of `pg.Pool` this module needs. */
export interface SqlQueryable {
  query(text: string, params?: unknown[]): Promise<{ rows: Record<string, unknown>[]; rowCount: number | null }>
}

export interface SqlPool extends SqlQueryable {
  connect(): Promise<SqlClient>
}

export interface SqlClient extends SqlQueryable {
  /** An error discards the connection, including any unfinished transaction. */
  release(error?: Error): void
}

function hashToken(token: string): string {
  return createHash('sha256').update(token).digest('hex')
}

export async function withTransaction<T>(pool: SqlPool, body: (client: SqlClient) => Promise<T>): Promise<T> {
  const client = await pool.connect()
  try {
    await client.query('BEGIN')
    const result = await body(client)
    await client.query('COMMIT')
    return result
  } catch (error) {
    await client.query('ROLLBACK').catch(() => undefined)
    throw error
  } finally {
    client.release()
  }
}

const WORK_SESSION_KEY_SQL =
  `'[' || to_json(work.tenant_id)::text || ',' || to_json(work.agent_id)::text || ',' || to_json(work.session_id)::text || ',' || COALESCE(to_json(work.thread_id)::text, 'null') || ']'`

export function workItemFromRow(row: Record<string, unknown>, leaseToken: string, homeEpoch: number): WorkItem {
  return {
    id: String(row['id']),
    fence: Number(row['fence']),
    homeEpoch,
    tenantId: String(row['tenant_id']),
    agentId: String(row['agent_id']),
    sessionId: String(row['session_id']),
    ...(row['thread_id'] !== null ? { threadId: String(row['thread_id']) } : {}),
    kind: String(row['kind']),
    lane: row['lane'] as WorkItem['lane'],
    triggerRef: String(row['trigger_ref']),
    ...(row['principal_id'] ? { principalId: String(row['principal_id']) } : {}),
    ...(row['conversation'] ? { conversation: row['conversation'] as NonNullable<WorkItem['conversation']> } : {}),
    createdAt: new Date(row['created_at'] as string | Date).toISOString(),
    availableAt: new Date(row['available_at'] as string | Date).toISOString(),
    attempts: Number(row['attempts'] ?? 0),
    preemptions: Number(row['preemptions'] ?? 0),
    leaseToken,
    ...(row['meta'] ? { meta: row['meta'] as Record<string, unknown> } : {}),
  }
}

export class PgWorkStore implements WorkStore {
  async children(parent: Omit<WorkItem, 'leaseToken'>) {
    const { rows } = await this.pool.query(`SELECT work.id,work.status,result.message->>'body' AS result_text,work.goal_outcome FROM lingxios.agent_work_items work
      LEFT JOIN lingxios.agent_results result ON result.id=work.result_id
      WHERE work.tenant_id=$2 AND work.principal_id IS NOT DISTINCT FROM $3 AND (
        (work.meta->>'parentWorkId'=$1 AND COALESCE((work.meta->>'parentRequestVersion')::integer,1)=
          (SELECT jsonb_array_length(steer_inputs)+1 FROM lingxios.agent_work_items WHERE id=$1))
        OR work.id IN(SELECT dependency_id FROM lingxios.agent_work_dependencies WHERE work_id=$1))
      ORDER BY work.created_at,work.id LIMIT 64`, [parent.id,parent.tenantId,parent.principalId ?? null])
    return rows.map(row => ({ id: String(row['id']), status: String(row['status']), resultText: row['result_text'] as string | null,
      goalOutcome: row['goal_outcome'] as WorkCompletion['goalOutcome'] | null }))
  }
  async getAttempt(id: string, fence: number, leaseTokenHash: string) {
    const { rows } = await this.pool.query(`SELECT work.* FROM lingxios.agent_work_items work
      JOIN lingxios.agent_attempts attempt ON attempt.work_id=work.id
      WHERE work.id=$1 AND attempt.fence=$2 AND attempt.lease_token_hash=$3`, [id, fence, leaseTokenHash])
    if (!rows[0]) return null
    const { leaseToken: _token, ...work } = workItemFromRow(rows[0], '', 1)
    return { ...work, fence }
  }
  async ownsBudgetRoot(work: Omit<WorkItem, 'leaseToken'>, rootWorkId: string): Promise<boolean> {
    if (rootWorkId === work.id) return true
    if (work.meta?.['rootWorkId'] !== rootWorkId || typeof work.meta?.['parentWorkId'] !== 'string') return false
    const { rows } = await this.pool.query(`WITH RECURSIVE lineage AS (
      SELECT id,meta,ARRAY[id] AS path FROM lingxios.agent_work_items
      WHERE id=$1 AND tenant_id=$3 AND principal_id IS NOT DISTINCT FROM $4
      UNION ALL SELECT parent.id,parent.meta,child.path||parent.id
      FROM lingxios.agent_work_items parent JOIN lineage child ON parent.id=child.meta->>'parentWorkId'
      WHERE parent.tenant_id=$3 AND parent.principal_id IS NOT DISTINCT FROM $4
        AND NOT parent.id=ANY(child.path) AND cardinality(child.path)<64
    ) SELECT id FROM lineage WHERE id=$2`, [work.id,rootWorkId,work.tenantId,work.principalId ?? null])
    return rows.length === 1
  }
  async hasChild(parent: Omit<WorkItem, 'leaseToken'>, childId: string, requestVersion: number): Promise<boolean> {
    const { rows } = await this.pool.query(`SELECT id FROM lingxios.agent_work_items WHERE id=$1
      AND tenant_id=$2 AND principal_id IS NOT DISTINCT FROM $3
      AND meta->>'parentWorkId'=$4 AND meta->'parentRequestVersion'=$5::jsonb`,
    [childId, parent.tenantId, parent.principalId ?? null, parent.id, JSON.stringify(requestVersion)])
    return rows.length === 1
  }
  private readonly leaseTtlSeconds: number
  private readonly workerTimeoutSeconds: number

  constructor(private readonly pool: SqlPool, options: WorkStoreOptions = {}) {
    this.leaseTtlSeconds = Math.ceil((options.leaseTtlMs ?? 45_000) / 1000)
    this.workerTimeoutSeconds = Math.ceil((options.workerTimeoutMs ?? 90_000) / 1000)
  }

  async enqueue(input: EnqueueWorkInput): Promise<EnqueueResult> { return enqueueWork(this.pool, input) }

  async claim(workerId: string, requestId?: string, workKinds?: readonly string[], lanes?: readonly WorkItem['lane'][], executionClass?: import('../protocol/types.js').ExecutionClass): Promise<WorkItem | null> {
    const kinds = workKinds ? [...new Set(workKinds)].sort() : null
    const filter = lanes || executionClass ? { kinds, ...(lanes ? { lanes: [...new Set(lanes)].sort() } : {}), ...(executionClass ? { executionClass } : {}) } : kinds
    return withTransaction(this.pool, async (client) => {
      if (requestId) {
        // ponytail: seven-day dedupe window bounds storage; use partitioned retention if claim volume demands it.
        await client.query(`DELETE FROM lingxios.agent_claim_requests
          WHERE completed=TRUE AND created_at<NOW()-INTERVAL '7 days'`)
        const inserted = await client.query(
          `INSERT INTO lingxios.agent_claim_requests (request_id, worker_id, work_kinds)
           VALUES ($1,$2,$3::jsonb) ON CONFLICT (request_id) DO NOTHING RETURNING request_id`, [requestId, workerId, JSON.stringify(filter)])
        if (!inserted.rows[0]) {
          const prior = await client.query(
            `SELECT worker_id, work_kinds, completed, response FROM lingxios.agent_claim_requests
              WHERE request_id=$1 FOR UPDATE`, [requestId])
          if (prior.rows[0]?.['worker_id'] !== workerId) throw new Error('claim request identity reused by another worker')
          if (!isDeepStrictEqual(prior.rows[0]?.['work_kinds'], filter)) throw new Error('claim request task types changed or lanes changed')
          if (prior.rows[0]?.['completed']) return prior.rows[0]['response'] as WorkItem | null
        }
      }
      const finish = async (work: WorkItem | null) => {
        if (requestId) await client.query(
          `UPDATE lingxios.agent_claim_requests SET completed=TRUE, response=$2::jsonb
            WHERE request_id=$1`, [requestId, JSON.stringify(work)])
        return work
      }
      await client.query(
        `INSERT INTO lingxios.agent_os_workers (worker_id, last_seen_at, updated_at) VALUES ($1, NOW(), NOW())
         ON CONFLICT (worker_id) DO UPDATE SET last_seen_at = NOW(), updated_at = NOW()`,
        [workerId],
      )
      await client.query(`DELETE FROM lingxios.agent_os_session_leases WHERE expires_at <= NOW()`)
      const { rows } = await client.query(
        `SELECT work.*
           FROM lingxios.agent_work_items work
           LEFT JOIN lingxios.agent_os_session_routes route
             ON route.session_key = ${WORK_SESSION_KEY_SQL}
          WHERE (work.status = 'queued' OR (work.status = 'leased' AND work.lease_expires_at <= NOW()))
            AND ($2::text[] IS NULL OR work.kind=ANY($2::text[]))
            AND ($3::text[] IS NULL OR work.lane=ANY($3::text[]))
            AND ($4::text IS NULL OR COALESCE(work.meta->>'executionClass',
              CASE WHEN work.lane IN ('interactive','approval') THEN 'conversation' ELSE 'operation' END)=$4)
            AND (work.kind NOT IN ('memory_synthesis','memory_index','memory_evaluation') OR work.attempts < 3)
            AND work.cancel_requested_at IS NULL
            AND work.available_at <= NOW()
            AND NOT EXISTS (SELECT 1 FROM lingxios.agent_work_dependencies dependency
              JOIN lingxios.agent_work_items prerequisite ON prerequisite.id=dependency.dependency_id
              LEFT JOIN lingxios.agent_results result ON result.id=prerequisite.result_id
              WHERE dependency.work_id=work.id AND (prerequisite.status<>'succeeded' OR prerequisite.cancel_requested_at IS NOT NULL
                OR result.id IS NULL OR result.request_version<>jsonb_array_length(prerequisite.steer_inputs)+1
                OR result.message->'envelope'->'goalOutcome'->>'status' IS DISTINCT FROM 'satisfied'))
            AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements_text(COALESCE(work.meta->'dependsOn','[]'::jsonb)) dependency(id)
              LEFT JOIN lingxios.agent_work_items prerequisite ON prerequisite.id=dependency.id AND prerequisite.tenant_id=work.tenant_id
                AND prerequisite.principal_id IS NOT DISTINCT FROM work.principal_id
              WHERE prerequisite.id IS NULL OR prerequisite.status IN ('queued','leased','waiting'))
            AND NOT EXISTS (
              SELECT 1 FROM lingxios.agent_os_session_leases lease
               WHERE lease.session_key = ${WORK_SESSION_KEY_SQL}
                 AND lease.expires_at > NOW())
          ORDER BY (CASE work.lane
                     WHEN 'interactive' THEN 4 WHEN 'approval' THEN 3
                     WHEN 'collaboration' THEN 2 ELSE 1 END
                     + FLOOR(GREATEST(0,EXTRACT(EPOCH FROM (NOW()-work.available_at)))/60)) DESC,
                   work.priority DESC, (route.worker_id=$1) DESC NULLS LAST, work.created_at ASC
          FOR UPDATE OF work SKIP LOCKED
          LIMIT 1`,
        [workerId, kinds, lanes ?? null, executionClass ?? null],
      )
      const row = rows[0]
      if (!row) return finish(null)
      const sessionKey = sessionKeyOf({ tenantId: String(row['tenant_id']), agentId: String(row['agent_id']), sessionId: String(row['session_id']), ...(row['thread_id'] === null ? {} : { threadId: String(row['thread_id']) }) })
      // Win session exclusivity before changing its route or filesystem epoch.
      const proposedFence = Number(row['fence']) + 1
      const { rows: sessionLease } = await client.query(
        `INSERT INTO lingxios.agent_os_session_leases (session_key, work_id, fence, expires_at)
         VALUES ($1, $2, $3, NOW() + make_interval(secs => $4::int))
         ON CONFLICT (session_key) DO NOTHING RETURNING session_key`,
        [sessionKey, row['id'], proposedFence, this.leaseTtlSeconds],
      )
      if (!sessionLease[0]) return finish(null)
      const { rows: routes } = await client.query(
        `INSERT INTO lingxios.agent_os_session_routes (session_key, worker_id, home_epoch, updated_at)
         VALUES ($1, $2, 1, NOW())
         ON CONFLICT (session_key) DO UPDATE
           SET worker_id = EXCLUDED.worker_id,
               home_epoch = CASE
                 WHEN lingxios.agent_os_session_routes.worker_id = EXCLUDED.worker_id THEN lingxios.agent_os_session_routes.home_epoch
                 ELSE lingxios.agent_os_session_routes.home_epoch + 1 END,
               updated_at = NOW()
         RETURNING home_epoch`,
        [sessionKey, workerId],
      )
      const token = randomBytes(32).toString('base64url')
      const { rows: claimed } = await client.query(
        `UPDATE lingxios.agent_work_items
            SET status = 'leased', fence = fence + 1, lease_token_hash = $2, leased_by = $3,
                lease_expires_at = NOW() + make_interval(secs => $4::int),
                attempts = attempts + 1, started_at=NOW(),heartbeat_at=NOW(),last_progress_at=COALESCE(last_progress_at,NOW()),updated_at = NOW()
          WHERE id = $1
          RETURNING *`,
        [row['id'], hashToken(token), workerId, this.leaseTtlSeconds],
      )
      return finish(workItemFromRow(claimed[0]!, token, Number(routes[0]!['home_epoch'])))
    })
  }

  async heartbeat(id: string, fence: number, leaseTokenHash: string): Promise<HeartbeatRow | null> {
    const { rows } = await this.pool.query(
      `WITH renewed AS (
         UPDATE lingxios.agent_work_items
            SET lease_expires_at = NOW() + make_interval(secs => $4::int),heartbeat_at=NOW()
          WHERE id = $1 AND fence = $2 AND lease_token_hash = $3 AND status = 'leased'
            AND lease_expires_at > NOW()
          RETURNING cancel_requested_at, preempt_requested_at, steer_inputs, leased_by
       ), session_renewed AS (
         UPDATE lingxios.agent_os_session_leases
            SET expires_at = NOW() + make_interval(secs => $4::int), updated_at = NOW()
          WHERE work_id = $1 AND fence = $2 AND EXISTS (SELECT 1 FROM renewed)
       ), worker_seen AS (
         INSERT INTO lingxios.agent_os_workers (worker_id, last_seen_at, updated_at)
         SELECT leased_by, NOW(), NOW() FROM renewed WHERE leased_by IS NOT NULL
         ON CONFLICT (worker_id) DO UPDATE SET last_seen_at = NOW(), updated_at = NOW()
       )
       SELECT cancel_requested_at, preempt_requested_at, steer_inputs FROM renewed`,
      [id, fence, leaseTokenHash, this.leaseTtlSeconds],
    )
    const row = rows[0]
    if (!row) return null
    return {
      cancelRequested: row['cancel_requested_at'] !== null,
      preemptRequested: row['preempt_requested_at'] !== null,
      steer: (row['steer_inputs'] ?? []) as HeartbeatRow['steer'],
    }
  }

  async getLeased(id: string, fence: number, leaseTokenHash: string): Promise<LeasedWork | null> {
    const { rows } = await this.pool.query(
      `SELECT work.*, COALESCE(route.home_epoch, 1) AS home_epoch
         FROM lingxios.agent_work_items work
         LEFT JOIN lingxios.agent_os_session_routes route
           ON route.session_key = ${WORK_SESSION_KEY_SQL}
        WHERE work.id = $1 AND work.fence = $2 AND work.lease_token_hash = $3
          AND work.status = 'leased' AND work.lease_expires_at > NOW()`,
      [id, fence, leaseTokenHash],
    )
    const row = rows[0]
    if (!row) return null
    const { leaseToken: _omit, ...work } = workItemFromRow(row, '', Number(row['home_epoch']))
    return { work, status: 'leased', cancelRequested: row['cancel_requested_at'] !== null }
  }

  async yieldWork(id: string, fence: number, leaseTokenHash: string): Promise<boolean> {
    return withTransaction(this.pool, async (client) => {
      const { rows } = await client.query(
        `UPDATE lingxios.agent_work_items
            SET status = 'queued', fence = fence + 1, lease_token_hash = NULL, leased_by = NULL,
                lease_expires_at = NULL, preempt_requested_at = NULL, preemptions = preemptions + 1,
                available_at = NOW() + INTERVAL '1 second', updated_at = NOW()
          WHERE id = $1 AND fence = $2 AND lease_token_hash = $3 AND status = 'leased'
            AND preempt_requested_at IS NOT NULL
            AND lease_expires_at > NOW()
          RETURNING id`,
        [id, fence, leaseTokenHash],
      )
      if (!rows[0]) return false
      await client.query(`DELETE FROM lingxios.agent_os_session_leases WHERE work_id = $1 AND fence = $2`, [id, fence])
      return true
    })
  }

  async complete(id: string, fence: number, leaseTokenHash: string, completion: WorkCompletion): Promise<boolean> {
    const status = workStatusOf(completion)
    if (status === 'waiting') throw new Error('waiting requires wait')
    return this.settle(id, fence, leaseTokenHash, status, completion)
  }

  async wait(id: string, fence: number, leaseTokenHash: string, goalOutcome: import('../protocol/outcome.js').WaitingOutcome): Promise<boolean> {
    return this.settle(id, fence, leaseTokenHash, 'waiting', { goalOutcome })
  }

  private async settle(id: string, fence: number, leaseTokenHash: string, status: string, completion: Omit<WorkCompletion, 'status'>): Promise<boolean> {
    const settled = await withTransaction(this.pool, async (client) => {
      const { rows } = await client.query(
        `UPDATE lingxios.agent_work_items
            SET status = $4, error = $5, goal_outcome = $6::jsonb, lease_token_hash = NULL,
                lease_expires_at = NULL, finished_at = CASE WHEN $4='waiting' THEN NULL ELSE NOW() END, updated_at = NOW()
          WHERE id = $1 AND fence = $2 AND lease_token_hash = $3 AND status = 'leased'
            AND lease_expires_at > NOW()
            AND ($4 IN ('failed','cancelled') OR cancel_requested_at IS NULL)
            AND ($4 IN ('failed','cancelled') OR jsonb_typeof(meta->'text') IS DISTINCT FROM 'string' OR $6::jsonb IS NOT NULL)
            AND ($4 IN ('failed','cancelled') OR $7::integer IS NULL OR $7::integer = jsonb_array_length(steer_inputs) + 1)
          RETURNING id`,
        [id, fence, leaseTokenHash, status, completion.error ?? null,
          completion.goalOutcome ? JSON.stringify(completion.goalOutcome) : null, completion.goalOutcome?.requestVersion ?? null],
      )
      if (!rows[0]) return false
      await client.query(`DELETE FROM lingxios.agent_os_session_leases WHERE work_id = $1 AND fence = $2`, [id, fence])
      if (status !== 'waiting') await cancelDescendants(client, id)
      return true
    })
    // Post-commit check also closes the child-finished-before-parent-parked race.
    if (settled) await resumeDependents(this.pool, id)
    return settled
  }

  async requestCancel(id: string): Promise<boolean> {
    return withTransaction(this.pool, async client => {
      const { rows } = await client.query('SELECT * FROM lingxios.agent_work_items WHERE id=$1 FOR UPDATE', [id])
      const row = rows[0]
      if (!row) return false
      return cancelRun(client, { runId: id, tenantId: String(row['tenant_id']), agentId: String(row['agent_id']),
        sessionId: String(row['session_id']), principalId: row['principal_id'] as string | null,
        ...(row['thread_id'] ? { threadId: String(row['thread_id']) } : {}) })
    })
  }

  async requestPreempt(id: string): Promise<boolean> {
    const { rowCount } = await this.pool.query(
      `UPDATE lingxios.agent_work_items SET preempt_requested_at = NOW(), updated_at = NOW()
        WHERE id = $1 AND status = 'leased'`,
      [id],
    )
    return (rowCount ?? 0) > 0
  }

  async addSteer(id: string, text: string): Promise<boolean> {
    return withTransaction(this.pool, async client => {
    const { rowCount } = await client.query(
      `UPDATE lingxios.agent_work_items
          SET steer_inputs = steer_inputs || jsonb_build_array(jsonb_build_object(
                'id', gen_random_uuid()::text, 'text', $2::text, 'createdAt', NOW())),
              status=CASE WHEN status='waiting' THEN 'queued' ELSE status END,available_at=NOW(),goal_outcome=NULL,updated_at = NOW()
        WHERE id = $1 AND status IN ('queued','leased','waiting') AND cancel_requested_at IS NULL`,
      [id, text],
    )
    if (rowCount) await cancelDescendants(client, id)
    return (rowCount ?? 0) > 0
    })
  }
}

export class PgSessionStore implements SessionStore {
  constructor(private readonly pool: SqlPool) {}

  async get(key: string, workId?: string): Promise<SessionRecord | null> {
    const { rows } = await this.pool.query(
      `SELECT session.*,COALESCE(request.request_snapshot,session.request_snapshot) AS effective_request_snapshot
       FROM lingxios.agent_os_sessions session
       LEFT JOIN lingxios.agent_request_snapshots request ON request.work_id=$2
       WHERE session.session_key=$1`, [key, workId ?? null],
    )
    return rows[0] ? sessionFromRow(rows[0]) : null
  }

  /** Session, request version and execution journal are from one MVCC snapshot. */
  async context(work: Omit<WorkItem, 'leaseToken'>): Promise<{ session: SessionRecord | null; steps: ExecutionStep[]; requestVersion: number }> {
    const { rows } = await this.pool.query(`SELECT jsonb_array_length(work.steer_inputs)+1 AS request_version,
      CASE WHEN session.session_key IS NULL THEN NULL ELSE to_jsonb(session)||jsonb_build_object(
        'effective_request_snapshot',COALESCE(request.request_snapshot,session.request_snapshot)) END AS session,
      COALESCE((SELECT jsonb_agg(step ORDER BY step_seq) FROM
        (SELECT * FROM lingxios.agent_steps WHERE work_id=work.id ORDER BY step_seq LIMIT 2049) step),'[]'::jsonb) AS steps
      FROM lingxios.agent_work_items work LEFT JOIN lingxios.agent_os_sessions session ON session.session_key=$2
      LEFT JOIN lingxios.agent_request_snapshots request ON request.work_id=work.id
      WHERE work.id=$1 AND work.fence=$3 AND work.status='leased' AND work.lease_expires_at>NOW()`, [work.id, sessionKeyOf(work), work.fence])
    const row = rows[0]
    if (!row) throw new Error('context attempt is no longer current')
    const steps = row['steps'] as Record<string, unknown>[]
    if (steps.length > 2048) throw new Error('step history exceeds the bounded recovery limit')
    return { session: row['session'] ? sessionFromRow(row['session'] as Record<string, unknown>) : null,
      steps: steps.map(stepFromRow), requestVersion: Number(row['request_version']) }
  }

  async save(session: SessionRecord, proof?: import('./stores.js').StoreLeaseProof): Promise<SaveSessionResult> {
    return withTransaction(this.pool, async client => {
      if (proof) {
        if (session.revision > 0) await client.query(
          'SELECT session_key FROM lingxios.agent_os_sessions WHERE session_key=$1 FOR UPDATE', [session.key])
        const lease = await client.query(`SELECT id FROM lingxios.agent_work_items
          WHERE id=$1 AND fence=$2 AND lease_token_hash=$3 AND status='leased' AND lease_expires_at>NOW() FOR UPDATE`,
        [proof.workId, proof.fence, proof.leaseTokenHash])
        if (!lease.rows[0]) return { ok: false, conflict: true }
      }
      let rows: Record<string, unknown>[]
      if (session.revision > 0) {
        ({ rows } = await client.query(
        `UPDATE lingxios.agent_os_sessions
            SET summary=$3, history=$4::jsonb, applied_work_ids=$5::jsonb,
                compaction_epoch=$6, prompt_context=$7::jsonb, request_snapshot=$8::jsonb,
                revision=revision+1, updated_at=NOW()
          WHERE session_key=$1 AND revision=$2
          RETURNING revision`,
        [session.key, session.revision, session.summary ?? null, JSON.stringify(session.history),
          JSON.stringify(session.appliedWorkIds), session.compactionEpoch,
          session.promptContext ? JSON.stringify(session.promptContext) : null,
          session.request ? JSON.stringify(session.request) : null]))
      } else ({ rows } = await client.query(
      `INSERT INTO lingxios.agent_os_sessions
         (session_key, tenant_id, agent_id, session_id, thread_id, summary, history,
          applied_work_ids, revision, compaction_epoch, prompt_context, request_snapshot, updated_at)
       SELECT $1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,1,$10,$11::jsonb,$12::jsonb,NOW()
        WHERE $9 = 0
       ON CONFLICT (session_key) DO NOTHING
       RETURNING revision`,
      [session.key, session.tenantId, session.agentId, session.sessionId, session.threadId ?? null,
        session.summary ?? null, JSON.stringify(session.history), JSON.stringify(session.appliedWorkIds),
        session.revision, session.compactionEpoch,
        session.promptContext ? JSON.stringify(session.promptContext) : null,
        session.request ? JSON.stringify(session.request) : null]))
      if (!rows[0]) return { ok: false, conflict: true }
      // The required agent_session_request_snapshot trigger writes the request in this transaction.
      return { ok: true, revision: Number(rows[0]['revision']) }
    })
  }
}

function sessionFromRow(row: Record<string, unknown>): SessionRecord {
  return {
    key: String(row['session_key']),
    tenantId: String(row['tenant_id']),
    agentId: String(row['agent_id']),
    sessionId: String(row['session_id']),
    ...(row['thread_id'] !== null ? { threadId: String(row['thread_id']) } : {}),
    ...(row['summary'] !== null ? { summary: String(row['summary']) } : {}),
    ...(row['effective_request_snapshot'] ? { request: row['effective_request_snapshot'] as NonNullable<SessionRecord['request']> } : {}),
    history: row['history'] as SessionRecord['history'],
    appliedWorkIds: (row['applied_work_ids'] ?? []) as string[],
    revision: Number(row['revision']),
    compactionEpoch: Number(row['compaction_epoch'] ?? 0),
    ...(row['prompt_context'] ? { promptContext: row['prompt_context'] as NonNullable<SessionRecord['promptContext']> } : {}),
  }
}

export class PgEventStore implements EventStore {
  constructor(private readonly pool: SqlPool, private readonly queueDelivery = false) {}

  async append(event: StoredRunEvent, proof?: import('./stores.js').StoreLeaseProof, work?: Omit<WorkItem, 'leaseToken'>): Promise<boolean> {
    const { rows } = await this.pool.query(
      `INSERT INTO lingxios.agent_run_events (run_id, seq, tenant_id, agent_id, kind, stage, visibility, data, recorded_at, expires_at, delivery_work)
       SELECT $1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,CASE WHEN $13::text IS NULL THEN NULL ELSE $13::timestamptz END,$14::jsonb
        WHERE $10::text IS NULL OR EXISTS (SELECT 1 FROM lingxios.agent_work_items
          WHERE id=$10 AND fence=$11 AND lease_token_hash=$12 AND status='leased' AND lease_expires_at>NOW())
       ON CONFLICT (run_id, seq) DO NOTHING RETURNING run_id`,
      [event.runId, event.seq, event.tenantId, event.agentId, event.kind, event.stage,
        event.visibility, JSON.stringify(event.data), event.recordedAt,
        proof?.workId ?? null, proof?.fence ?? null, proof?.leaseTokenHash ?? null,
        typeof event.data['traceExpiresAt'] === 'string' ? event.data['traceExpiresAt'] : null,
        this.queueDelivery && work && (event.visibility === 'user' || /^(run\.|model\.(started|delta|completed)$|tool\.|approval\.)/.test(event.kind)) ? JSON.stringify(work) : null],
    )
    return rows.length > 0
  }

  async listRange(runId: string, fromSeqExclusive: number, toSeqInclusive: number, kinds?: readonly string[]): Promise<StoredRunEvent[]> {
    const { rows } = await this.pool.query(
      `SELECT * FROM lingxios.agent_run_events
        WHERE run_id = $1 AND seq > $2 AND seq <= $3 AND (expires_at IS NULL OR expires_at>NOW())
          AND ($4::text[] IS NULL OR kind = ANY($4::text[]))
        ORDER BY seq`,
      [runId, fromSeqExclusive, toSeqInclusive, kinds ? [...kinds] : null],
    )
    return rows.map((row) => ({
      runId: String(row['run_id']),
      seq: Number(row['seq']),
      tenantId: String(row['tenant_id']),
      agentId: String(row['agent_id']),
      kind: String(row['kind']),
      stage: row['stage'] as StoredRunEvent['stage'],
      visibility: row['visibility'] as StoredRunEvent['visibility'],
      data: row['data'] as Record<string, unknown>,
      recordedAt: new Date(row['recorded_at'] as string | Date).toISOString(),
    }))
  }
}

export class PgActionLedger implements ActionLedgerStore {
  async artifacts(workId: string) {
    const { rows } = await this.pool.query(`SELECT artifact FROM lingxios.agent_action_intents intent
      JOIN lingxios.agent_action_ledger receipt USING(idempotency_key)
      CROSS JOIN LATERAL jsonb_array_elements(COALESCE(receipt.result->'artifacts','[]'::jsonb)) artifact
      WHERE intent.intent->>'workId'=$1 AND receipt.result->>'ok'='true'
      ORDER BY receipt.recorded_at,intent.idempotency_key LIMIT 513`, [workId])
    if (rows.length > 512) throw new Error('native artifact inventory exceeds 512 records')
    return rows.map(row => row['artifact'] as import('../protocol/types.js').KernelArtifact)
  }
  constructor(private readonly pool: SqlPool) {}

  async hasWait(workId: string, requestVersion: number, wait: { approvalId: string } | { question: string }): Promise<boolean> {
    const { rows } = await this.pool.query(`SELECT i.idempotency_key FROM lingxios.agent_action_intents i
      LEFT JOIN lingxios.agent_action_ledger r USING(idempotency_key)
      LEFT JOIN LATERAL (SELECT resolution->'result' AS result FROM lingxios.agent_action_resolutions
        WHERE idempotency_key=i.idempotency_key ORDER BY resolution_seq DESC LIMIT 1) rr ON TRUE
      WHERE i.intent->>'workId'=$1 AND i.intent->'requestVersion'=$2::jsonb
        AND COALESCE(rr.result,r.result) IS NOT NULL
        AND COALESCE(COALESCE(rr.result,r.result)->>'executionState','')<>'unknown'
        AND CASE WHEN $3::text IS NOT NULL THEN COALESCE(rr.result,r.result)->'approval'->>'status'='PENDING' AND COALESCE(rr.result,r.result)->'approval'->>'id'=$3
          ELSE i.intent->'action'->>'action'='task.ask' AND COALESCE(rr.result,r.result)->>'ok'='true'
            AND COALESCE(rr.result,r.result)->'directive'->>'type'='defer' AND COALESCE(rr.result,r.result)->'directive'->>'reason'='user'
            AND COALESCE(rr.result,r.result)->'directive'->'data'->>'question'=$4::text END LIMIT 1`,
    [workId, JSON.stringify(requestVersion), 'approvalId' in wait ? wait.approvalId : null, 'question' in wait ? wait.question : null])
    return rows.length === 1
  }

  async hasSuccessfulAction(workId: string, requestVersion: number, actions: readonly string[]): Promise<boolean> {
    if (!actions.length) return false
    const { rows } = await this.pool.query(`SELECT i.idempotency_key FROM lingxios.agent_action_intents i
      LEFT JOIN lingxios.agent_action_ledger r USING(idempotency_key)
      LEFT JOIN LATERAL (SELECT resolution->'result' AS result FROM lingxios.agent_action_resolutions
        WHERE idempotency_key=i.idempotency_key ORDER BY resolution_seq DESC LIMIT 1) rr ON TRUE
      WHERE i.intent->>'workId'=$1 AND i.intent->'requestVersion'=$2::jsonb
        AND i.intent->'action'->>'action'=ANY($3::text[])
        AND COALESCE(rr.result,r.result)->>'ok'='true'
        AND COALESCE(COALESCE(rr.result,r.result)->>'executionState','')<>'unknown'
        AND COALESCE(rr.result,r.result)->'approval' IS NULL LIMIT 1`,
    [workId, JSON.stringify(requestVersion), [...actions]])
    return rows.length === 1
  }

  async unsettled(workId: string): Promise<Array<{ actionKey: string; action: string; state: 'unknown' | 'awaiting_approval' }>> {
    const { rows } = await this.pool.query(`WITH actions AS (
      SELECT i.idempotency_key,i.intent,COALESCE(rr.result,r.result) AS result
      FROM lingxios.agent_action_intents i LEFT JOIN lingxios.agent_action_ledger r USING(idempotency_key)
      LEFT JOIN LATERAL (SELECT resolution->'result' AS result FROM lingxios.agent_action_resolutions
        WHERE idempotency_key=i.idempotency_key ORDER BY resolution_seq DESC LIMIT 1) rr ON TRUE)
      SELECT idempotency_key,intent->'action'->>'action' AS action,
        CASE WHEN result->'approval'->>'status'='PENDING' THEN 'awaiting_approval' ELSE 'unknown' END AS state
      FROM actions WHERE intent->>'workId'=$1 AND intent->'action'->>'action' NOT LIKE 'task.%'
        AND (result IS NULL OR result->>'executionState'='unknown' OR result->'approval'->>'status'='PENDING')
      ORDER BY idempotency_key LIMIT 65`, [workId])
    return rows.map(row => ({ actionKey: String(row['idempotency_key']), action: String(row['action']), state: row['state'] as 'unknown' | 'awaiting_approval' }))
  }

  async reserve(idempotencyKey: string, fingerprint: string, intent: ActionIntent): Promise<'started' | 'existing'> {
    if (!intent || intent.action?.idempotencyKey !== idempotencyKey) throw new Error('action intent is required and must match its key')
    const { rows } = await this.pool.query(
      `INSERT INTO lingxios.agent_action_intents (idempotency_key, fingerprint, intent)
       VALUES ($1, $2, $3::jsonb)
       ON CONFLICT (idempotency_key) DO NOTHING RETURNING idempotency_key`, [idempotencyKey, fingerprint, JSON.stringify(intent)],
    )
    if (rows.length > 0) return 'started'
    const existing = await this.pool.query(`SELECT fingerprint FROM lingxios.agent_action_intents WHERE idempotency_key = $1`, [idempotencyKey])
    if (existing.rows[0]?.['fingerprint'] !== fingerprint) {
      throw new Error('action identity mismatch; reconcile before replay')
    }
    return 'existing'
  }

  async findIntent(idempotencyKey: string): Promise<ActionIntent | null> {
    const { rows } = await this.pool.query('SELECT intent FROM lingxios.agent_action_intents WHERE idempotency_key=$1', [idempotencyKey])
    return rows[0]?.['intent'] as ActionIntent | null ?? null
  }

  async find(idempotencyKey: string): Promise<HostActionResult | null> {
    const { rows } = await this.pool.query(
      `SELECT COALESCE((SELECT resolution->'result' FROM lingxios.agent_action_resolutions
          WHERE idempotency_key=$1 ORDER BY resolution_seq DESC LIMIT 1),
        (SELECT result FROM lingxios.agent_action_ledger WHERE idempotency_key=$1)) AS result`, [idempotencyKey],
    )
    return rows[0] ? rows[0]['result'] as HostActionResult : null
  }

  async listCell(workId: string, cellId: string, requestVersion: number | null) {
    const { rows } = await this.pool.query(
      `SELECT i.intent, COALESCE(rr.result,r.result) AS result FROM lingxios.agent_action_intents i
       LEFT JOIN lingxios.agent_action_ledger r USING(idempotency_key)
       LEFT JOIN LATERAL (SELECT resolution->'result' AS result FROM lingxios.agent_action_resolutions
         WHERE idempotency_key=i.idempotency_key ORDER BY resolution_seq DESC LIMIT 1) rr ON TRUE
       WHERE i.intent->>'workId'=$1 AND i.intent->'action'->>'cellId'=$2
         AND i.intent->'requestVersion' IS NOT DISTINCT FROM $3::jsonb
       ORDER BY (i.intent->'action'->>'callIndex')::integer`,
      [workId, cellId, requestVersion === null ? null : JSON.stringify(requestVersion)],
    )
    return rows.map(row => ({
      intent: row['intent'] as ActionIntent,
      result: row['result'] as HostActionResult | null ?? null,
    }))
  }

  async record(idempotencyKey: string, result: HostActionResult): Promise<HostActionResult> {
    const { rows } = await this.pool.query(
      `INSERT INTO lingxios.agent_action_ledger (idempotency_key, result) VALUES ($1, $2::jsonb)
       ON CONFLICT (idempotency_key) DO UPDATE SET result=EXCLUDED.result,recorded_at=NOW()
       WHERE lingxios.agent_action_ledger.result->'approval'->>'status'='PENDING' RETURNING result`,
      [idempotencyKey, JSON.stringify(result)],
    )
    if (rows[0]) return result
    const existing = await this.find(idempotencyKey)
    return existing ?? result
  }

  async recordResolution(resolution: ActionResolution): Promise<'recorded' | 'existing'> {
    const { rows } = await this.pool.query(
      `INSERT INTO lingxios.agent_action_resolutions (resolution_id,idempotency_key,resolution)
       VALUES ($1,$2,$3::jsonb) ON CONFLICT (resolution_id) DO NOTHING RETURNING resolution_id`,
      [resolution.id, resolution.actionKey, JSON.stringify(resolution)],
    )
    if (rows[0]) return 'recorded'
    const prior = await this.pool.query(
      `SELECT resolution FROM lingxios.agent_action_resolutions WHERE resolution_id=$1`, [resolution.id])
    if (!isDeepStrictEqual(prior.rows[0]?.['resolution'], resolution)) {
      throw new Error('resolution identity reused with different evidence')
    }
    return 'existing'
  }
}

export class PgModelBudgetStore implements ModelBudgetStore {
  constructor(private readonly pool: SqlPool) {}

  async reserve(rootWorkId: string, callId: string, limits: ModelBudgetLimits, proof?: import('./stores.js').StoreLeaseProof): Promise<ModelBudgetReservation> {
    return withTransaction(this.pool, async client => {
      if (proof) {
        const owner = await client.query(`SELECT id FROM lingxios.agent_work_items
          WHERE id=$1 AND fence=$2 AND lease_token_hash=$3 AND status='leased'
            AND lease_expires_at>NOW() AND cancel_requested_at IS NULL FOR UPDATE`,
          [proof.workId, proof.fence, proof.leaseTokenHash])
        if (!owner.rows.length) throw new Error('model reservation requires a live uncancelled attempt')
      }
      await client.query(`INSERT INTO lingxios.agent_model_budgets
        (root_work_id,max_model_calls,max_tokens,max_cost_micros,deadline_at,max_execution_ms)
        VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(root_work_id) DO NOTHING`,
      [rootWorkId, limits.maxModelCalls, limits.maxTokens, limits.maxCostMicros, limits.deadlineAt, limits.maxExecutionMs ?? 1_800_000])
      await client.query('SELECT root_work_id FROM lingxios.agent_model_budgets WHERE root_work_id=$1 FOR UPDATE', [rootWorkId])
      if (limits.maxExecutionMs !== undefined) await client.query(`UPDATE lingxios.agent_model_budgets budget
        SET deadline_at=NOW()+GREATEST(0,budget.max_execution_ms-COALESCE((
          SELECT SUM(GREATEST(0,EXTRACT(EPOCH FROM (COALESCE(attempt.ended_at,LEAST(NOW(),attempt.lease_expires_at))-attempt.started_at))*1000))
          FROM lingxios.agent_attempts attempt JOIN lingxios.agent_work_items work ON work.id=attempt.work_id
          WHERE COALESCE(work.meta->>'rootWorkId',work.id)=budget.root_work_id),0))*INTERVAL '1 millisecond'
        WHERE root_work_id=$1`, [rootWorkId])
      const prior = await client.query(`SELECT 1 FROM lingxios.agent_model_budget_calls
        WHERE root_work_id=$1 AND call_id=$2`, [rootWorkId, callId])
      if (!prior.rows.length) {
        const inserted = await client.query(`INSERT INTO lingxios.agent_model_budget_calls(root_work_id,call_id,reserved_tokens,reserved_cost_micros,work_id,fence,lease_token_hash,pricing)
          SELECT root_work_id,$2,$3,$4,$5,$6,$7,$8::jsonb FROM lingxios.agent_model_budgets WHERE root_work_id=$1
            AND NOW()<deadline_at AND model_calls<max_model_calls AND tokens+$3<=max_tokens AND cost_micros+$4<=max_cost_micros
          ON CONFLICT DO NOTHING RETURNING call_id`, [rootWorkId, callId, limits.reservedTokens ?? 0, limits.reservedCostMicros ?? 0, proof?.workId ?? rootWorkId, proof?.fence ?? null, proof?.leaseTokenHash ?? null, limits.pricing ? JSON.stringify(limits.pricing) : null])
        if (inserted.rows.length) await client.query(`UPDATE lingxios.agent_model_budgets
          SET model_calls=model_calls+1,tokens=tokens+$2,cost_micros=cost_micros+$3,updated_at=NOW() WHERE root_work_id=$1`, [rootWorkId, limits.reservedTokens ?? 0, limits.reservedCostMicros ?? 0])
      }
      const { rows } = await client.query(`SELECT b.*,EXISTS(SELECT 1 FROM lingxios.agent_model_budget_calls c
        WHERE c.root_work_id=b.root_work_id AND c.call_id=$2) AS allowed
        FROM lingxios.agent_model_budgets b WHERE root_work_id=$1 FOR UPDATE`, [rootWorkId, callId])
      const row = rows[0]!
      return { allowed: Boolean(row['allowed']), remainingCalls: Math.max(0, Number(row['max_model_calls']) - Number(row['model_calls'])),
        remainingTokens: Math.max(0, Number(row['max_tokens']) - Number(row['tokens'])),
        remainingCostMicros: Math.max(0, Number(row['max_cost_micros']) - Number(row['cost_micros'])),
        deadlineAt: new Date(row['deadline_at'] as string | Date).toISOString() }
    })
  }

  async record(rootWorkId: string, callId: string, inputTokens: number, outputTokens: number, costMicros: number, proof?: import('./stores.js').StoreLeaseProof, observation?: import('../model/execution.js').ModelCallObservation): Promise<void> {
    await withTransaction(this.pool, async client => {
      const issued = await client.query(`SELECT input_tokens,output_tokens,cost_micros,pricing FROM lingxios.agent_model_budget_calls
        WHERE root_work_id=$1 AND call_id=$2 AND ($3::text IS NULL OR (work_id=$3 AND fence=$4 AND lease_token_hash=$5)) FOR UPDATE`,
      [rootWorkId, callId, proof?.workId ?? null, proof?.fence ?? null, proof?.leaseTokenHash ?? null])
      if (!issued.rows.length) throw new Error('model call reservation does not belong to this attempt')
      const pricing = issued.rows[0]!['pricing'] as import('../model/execution.js').ModelPricing | null
      if (pricing) {
        costMicros = Math.ceil((inputTokens * pricing.inputMicrosPerMillion + outputTokens * pricing.outputMicrosPerMillion) / 1_000_000)
        if (observation) observation = { ...observation, cost: { amountMicros: costMicros, pricing,
          usage: observation.usage?.available ? 'measured' : 'estimated' } }
      }
      if (issued.rows[0]!['input_tokens'] !== null) {
        if (Number(issued.rows[0]!['input_tokens']) !== inputTokens || Number(issued.rows[0]!['output_tokens']) !== outputTokens
          || Number(issued.rows[0]!['cost_micros']) !== costMicros) throw new Error('model usage settlement cannot be rewritten')
        return
      }
      const { rows } = await client.query(`UPDATE lingxios.agent_model_budget_calls SET input_tokens=$3,output_tokens=$4,cost_micros=$5,observation=$6::jsonb
        WHERE root_work_id=$1 AND call_id=$2 AND input_tokens IS NULL RETURNING call_id,reserved_tokens,reserved_cost_micros`,
      [rootWorkId, callId, inputTokens, outputTokens, costMicros, observation ? JSON.stringify(observation) : null])
      if (rows.length) await client.query(`UPDATE lingxios.agent_model_budgets SET tokens=tokens+$2+$3,
        cost_micros=cost_micros+$4,updated_at=NOW() WHERE root_work_id=$1`, [rootWorkId, inputTokens - Number(rows[0]!['reserved_tokens']), outputTokens, costMicros - Number(rows[0]!['reserved_cost_micros'])])
    })
  }
}
