/**
 * Postgres store implementations over an injected pg-compatible pool (any
 * object with `query` and `connect`, e.g. `pg.Pool`). Keeping the driver out
 * of this package's dependency tree lets deployments pin their own.
 *
 * Semantics mirror `memory-store.ts`, which is the executable specification;
 * the schema lives in `db/schema.sql`.
 */
import { createHash, randomBytes, randomUUID } from 'node:crypto'
import { sessionKeyOf } from '../protocol/types.js'
import type {
  HostActionResult, SessionRecord, WorkCompletion, WorkItem,
} from '../protocol/types.js'
import type {
  ActionIntent, ActionLedgerStore, EnqueueResult, EnqueueWorkInput, EventStore, HeartbeatRow,
  LeasedWork, SaveSessionResult, SessionStore, StoredRunEvent, WorkStore, WorkStoreOptions,
} from './stores.js'

/** The subset of `pg.Pool` this module needs. */
export interface SqlQueryable {
  query(text: string, params?: unknown[]): Promise<{ rows: Record<string, unknown>[]; rowCount: number | null }>
}

export interface SqlPool extends SqlQueryable {
  connect(): Promise<SqlClient>
}

export interface SqlClient extends SqlQueryable {
  release(): void
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

function workItemFromRow(row: Record<string, unknown>, leaseToken: string, homeEpoch: number): WorkItem {
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
    createdAt: new Date(row['created_at'] as string | Date).toISOString(),
    availableAt: new Date(row['available_at'] as string | Date).toISOString(),
    attempts: Number(row['attempts'] ?? 0),
    preemptions: Number(row['preemptions'] ?? 0),
    leaseToken,
    ...(row['meta'] ? { meta: row['meta'] as Record<string, unknown> } : {}),
  }
}

export class PgWorkStore implements WorkStore {
  async hasPendingChild(parent: Omit<WorkItem, 'leaseToken'>, childId: string, requestVersion: number): Promise<boolean> {
    const { rows } = await this.pool.query(`SELECT id FROM lingxios.agent_work_items WHERE id=$1
      AND tenant_id=$2 AND session_id=$3 AND principal_id IS NOT DISTINCT FROM $4
      AND status IN ('queued','leased') AND cancel_requested_at IS NULL
      AND meta->>'parentWorkId'=$5 AND meta->'parentRequestVersion'=$6::jsonb`,
    [childId, parent.tenantId, parent.sessionId, parent.principalId ?? null, parent.id, JSON.stringify(requestVersion)])
    return rows.length === 1
  }
  private readonly leaseTtlSeconds: number
  private readonly workerTimeoutSeconds: number

  constructor(private readonly pool: SqlPool, options: WorkStoreOptions = {}) {
    this.leaseTtlSeconds = Math.ceil((options.leaseTtlMs ?? 45_000) / 1000)
    this.workerTimeoutSeconds = Math.ceil((options.workerTimeoutMs ?? 90_000) / 1000)
  }

  async enqueue(input: EnqueueWorkInput): Promise<EnqueueResult> {
    const id = input.id ?? randomUUID()
    const { rows } = await this.pool.query(
      `INSERT INTO lingxios.agent_work_items
         (id, tenant_id, agent_id, session_id, thread_id, kind, lane, trigger_ref, principal_id, priority, available_at, meta)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,COALESCE($11::timestamptz, NOW()),$12::jsonb)
       ON CONFLICT (id) DO NOTHING
       RETURNING id`,
      [id, input.tenantId, input.agentId, input.sessionId, input.threadId ?? null, input.kind, input.lane,
        input.triggerRef, input.principalId ?? null, input.priority ?? 0, input.availableAt ?? null,
        input.meta ? JSON.stringify(input.meta) : null],
    )
    if (rows.length === 0) {
      const existing = await this.pool.query(
        `SELECT id FROM lingxios.agent_work_items WHERE id=$1 AND tenant_id=$2 AND agent_id=$3
           AND session_id=$4 AND thread_id IS NOT DISTINCT FROM $5 AND kind=$6 AND lane=$7
           AND trigger_ref=$8 AND principal_id IS NOT DISTINCT FROM $9 AND priority=$10
           AND meta IS NOT DISTINCT FROM $11::jsonb`,
        [id, input.tenantId, input.agentId, input.sessionId, input.threadId ?? null, input.kind, input.lane,
          input.triggerRef, input.principalId ?? null, input.priority ?? 0,
          input.meta ? JSON.stringify(input.meta) : null],
      )
      if (existing.rows.length !== 1) throw new Error('work identity reused with a different request or principal')
    }
    return { id, deduplicated: rows.length === 0 }
  }

  async claim(workerId: string): Promise<WorkItem | null> {
    return withTransaction(this.pool, async (client) => {
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
           LEFT JOIN lingxios.agent_os_workers route_worker ON route_worker.worker_id = route.worker_id
          WHERE (work.status = 'queued' OR (work.status = 'leased' AND work.lease_expires_at <= NOW()))
            AND (work.kind NOT IN ('memory_synthesis','memory_index') OR work.attempts < 3)
            AND work.cancel_requested_at IS NULL
            AND work.available_at <= NOW()
            AND (route.session_key IS NULL OR route.worker_id = $1
                 OR route_worker.last_seen_at IS NULL
                 OR route_worker.last_seen_at <= NOW() - make_interval(secs => $2::int))
            AND NOT EXISTS (
              SELECT 1 FROM lingxios.agent_os_session_leases lease
               WHERE lease.session_key = ${WORK_SESSION_KEY_SQL}
                 AND lease.expires_at > NOW())
          ORDER BY CASE work.lane
                     WHEN 'interactive' THEN 4 WHEN 'approval' THEN 3
                     WHEN 'collaboration' THEN 2 ELSE 1 END DESC,
                   work.priority DESC, work.created_at ASC
          FOR UPDATE OF work SKIP LOCKED
          LIMIT 1`,
        [workerId, this.workerTimeoutSeconds],
      )
      const row = rows[0]
      if (!row) return null
      const sessionKey = sessionKeyOf({ tenantId: String(row['tenant_id']), agentId: String(row['agent_id']), sessionId: String(row['session_id']), ...(row['thread_id'] === null ? {} : { threadId: String(row['thread_id']) }) })
      const { rows: routes } = await client.query(
        `INSERT INTO lingxios.agent_os_session_routes (session_key, worker_id, home_epoch, updated_at)
         VALUES ($1, $2, 1, NOW())
         ON CONFLICT (session_key) DO UPDATE
           SET worker_id = EXCLUDED.worker_id,
               home_epoch = CASE
                 WHEN lingxios.agent_os_session_routes.worker_id = EXCLUDED.worker_id THEN lingxios.agent_os_session_routes.home_epoch
                 ELSE lingxios.agent_os_session_routes.home_epoch + 1 END,
               updated_at = NOW()
         WHERE lingxios.agent_os_session_routes.worker_id = EXCLUDED.worker_id
            OR NOT EXISTS (
              SELECT 1 FROM lingxios.agent_os_workers owner
               WHERE owner.worker_id = lingxios.agent_os_session_routes.worker_id
                 AND owner.last_seen_at > NOW() - make_interval(secs => $3::int))
         RETURNING home_epoch`,
        [sessionKey, workerId, this.workerTimeoutSeconds],
      )
      if (!routes[0]) return null
      const proposedFence = Number(row['fence']) + 1
      const { rows: sessionLease } = await client.query(
        `INSERT INTO lingxios.agent_os_session_leases (session_key, work_id, fence, expires_at)
         VALUES ($1, $2, $3, NOW() + make_interval(secs => $4::int))
         ON CONFLICT (session_key) DO NOTHING RETURNING session_key`,
        [sessionKey, row['id'], proposedFence, this.leaseTtlSeconds],
      )
      if (!sessionLease[0]) return null
      const token = randomBytes(32).toString('base64url')
      const { rows: claimed } = await client.query(
        `UPDATE lingxios.agent_work_items
            SET status = 'leased', fence = fence + 1, lease_token_hash = $2, leased_by = $3,
                lease_expires_at = NOW() + make_interval(secs => $4::int),
                attempts = attempts + 1, updated_at = NOW()
          WHERE id = $1
          RETURNING *`,
        [row['id'], hashToken(token), workerId, this.leaseTtlSeconds],
      )
      return workItemFromRow(claimed[0]!, token, Number(routes[0]['home_epoch']))
    })
  }

  async heartbeat(id: string, fence: number, leaseTokenHash: string): Promise<HeartbeatRow | null> {
    const { rows } = await this.pool.query(
      `WITH renewed AS (
         UPDATE lingxios.agent_work_items
            SET lease_expires_at = NOW() + make_interval(secs => $4::int), updated_at = NOW()
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
    return withTransaction(this.pool, async (client) => {
      const { rows } = await client.query(
        `UPDATE lingxios.agent_work_items
            SET status = $4, result_text = $5, error = $6, goal_outcome = $7::jsonb, lease_token_hash = NULL,
                lease_expires_at = NULL, finished_at = NOW(), updated_at = NOW()
          WHERE id = $1 AND fence = $2 AND lease_token_hash = $3 AND status = 'leased'
            AND lease_expires_at > NOW()
            AND ($4 <> 'completed' OR cancel_requested_at IS NULL)
            AND ($4 <> 'completed' OR jsonb_typeof(meta->'text') IS DISTINCT FROM 'string' OR $7::jsonb IS NOT NULL)
            AND ($4 <> 'completed' OR $8::integer IS NULL OR $8::integer = jsonb_array_length(steer_inputs) + 1)
          RETURNING id`,
        [id, fence, leaseTokenHash, completion.status, completion.resultText ?? null, completion.error ?? null,
          completion.goalOutcome ? JSON.stringify(completion.goalOutcome) : null, completion.goalOutcome?.requestVersion ?? null],
      )
      if (!rows[0]) return false
      await client.query(`DELETE FROM lingxios.agent_os_session_leases WHERE work_id = $1 AND fence = $2`, [id, fence])
      return true
    })
  }

  async requestCancel(id: string): Promise<boolean> {
    const { rowCount } = await this.pool.query(
      `UPDATE lingxios.agent_work_items
          SET cancel_requested_at = NOW(),
              status = CASE WHEN status = 'queued' THEN 'cancelled' ELSE status END,
              updated_at = NOW()
        WHERE id = $1 AND status IN ('queued','leased')`,
      [id],
    )
    return (rowCount ?? 0) > 0
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
    const { rowCount } = await this.pool.query(
      `UPDATE lingxios.agent_work_items
          SET steer_inputs = steer_inputs || jsonb_build_array(jsonb_build_object(
                'id', gen_random_uuid()::text, 'text', $2::text, 'createdAt', NOW())),
              updated_at = NOW()
        WHERE id = $1 AND status = 'leased'`,
      [id, text],
    )
    return (rowCount ?? 0) > 0
  }
}

export class PgSessionStore implements SessionStore {
  constructor(private readonly pool: SqlPool) {}

  async get(key: string): Promise<SessionRecord | null> {
    const { rows } = await this.pool.query(
      `SELECT * FROM lingxios.agent_os_sessions WHERE session_key = $1`, [key],
    )
    const row = rows[0]
    if (!row) return null
    return {
      key: String(row['session_key']),
      tenantId: String(row['tenant_id']),
      agentId: String(row['agent_id']),
      sessionId: String(row['session_id']),
      ...(row['thread_id'] !== null ? { threadId: String(row['thread_id']) } : {}),
      ...(row['summary'] ? { summary: String(row['summary']) } : {}),
      ...(row['request_snapshot'] ? { request: row['request_snapshot'] as NonNullable<SessionRecord['request']> } : {}),
      history: row['history'] as SessionRecord['history'],
      appliedWorkIds: (row['applied_work_ids'] ?? []) as string[],
      revision: Number(row['revision']),
      compactionEpoch: Number(row['compaction_epoch'] ?? 0),
      ...(row['prompt_context'] ? { promptContext: row['prompt_context'] as NonNullable<SessionRecord['promptContext']> } : {}),
    }
  }

  async save(session: SessionRecord): Promise<SaveSessionResult> {
    if (session.revision > 0) {
      const { rows } = await this.pool.query(
        `UPDATE lingxios.agent_os_sessions
            SET summary=$3, history=$4::jsonb, applied_work_ids=$5::jsonb,
                compaction_epoch=$6, prompt_context=$7::jsonb, request_snapshot=$8::jsonb,
                revision=revision+1, updated_at=NOW()
          WHERE session_key=$1 AND revision=$2 RETURNING revision`,
        [session.key, session.revision, session.summary ?? null, JSON.stringify(session.history),
          JSON.stringify(session.appliedWorkIds), session.compactionEpoch,
          session.promptContext ? JSON.stringify(session.promptContext) : null,
          session.request ? JSON.stringify(session.request) : null],
      )
      return rows[0] ? { ok: true, revision: Number(rows[0]['revision']) } : { ok: false, conflict: true }
    }
    const { rows } = await this.pool.query(
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
        session.request ? JSON.stringify(session.request) : null],
    )
    if (!rows[0]) return { ok: false, conflict: true }
    return { ok: true, revision: Number(rows[0]['revision']) }
  }
}

export class PgEventStore implements EventStore {
  constructor(private readonly pool: SqlPool) {}

  async append(event: StoredRunEvent): Promise<boolean> {
    const { rows } = await this.pool.query(
      `INSERT INTO lingxios.agent_run_events (run_id, seq, tenant_id, agent_id, kind, stage, visibility, data, recorded_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9)
       ON CONFLICT (run_id, seq) DO NOTHING RETURNING run_id`,
      [event.runId, event.seq, event.tenantId, event.agentId, event.kind, event.stage,
        event.visibility, JSON.stringify(event.data), event.recordedAt],
    )
    return rows.length > 0
  }

  async listRange(runId: string, fromSeqExclusive: number, toSeqInclusive: number, kinds?: readonly string[]): Promise<StoredRunEvent[]> {
    const { rows } = await this.pool.query(
      `SELECT * FROM lingxios.agent_run_events
        WHERE run_id = $1 AND seq > $2 AND seq <= $3
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
  constructor(private readonly pool: SqlPool) {}

  async hasWait(workId: string, requestVersion: number, wait: { approvalId: string } | { question: string }): Promise<boolean> {
    const { rows } = await this.pool.query(`SELECT i.idempotency_key FROM lingxios.agent_action_intents i
      JOIN lingxios.agent_action_ledger r USING(idempotency_key)
      WHERE i.intent->>'workId'=$1 AND i.intent->'requestVersion'=$2::jsonb
        AND COALESCE(r.result->>'executionState','')<>'unknown'
        AND CASE WHEN $3::text IS NOT NULL THEN r.result->'approval'->>'status'='PENDING' AND r.result->'approval'->>'id'=$3
          ELSE i.intent->'action'->>'action'='task.ask' AND r.result->>'ok'='true'
            AND r.result->'directive'->>'type'='defer' AND r.result->'directive'->>'reason'='user'
            AND r.result->'directive'->'data'->>'question'=$4::text END LIMIT 1`,
    [workId, JSON.stringify(requestVersion), 'approvalId' in wait ? wait.approvalId : null, 'question' in wait ? wait.question : null])
    return rows.length === 1
  }

  async unsettled(workId: string): Promise<Array<{ actionKey: string; action: string; state: 'unknown' | 'awaiting_approval' }>> {
    const { rows } = await this.pool.query(`SELECT i.idempotency_key,i.intent->'action'->>'action' AS action,
      CASE WHEN r.result->'approval'->>'status'='PENDING' THEN 'awaiting_approval' ELSE 'unknown' END AS state
      FROM lingxios.agent_action_intents i LEFT JOIN lingxios.agent_action_ledger r USING(idempotency_key)
      WHERE i.intent->>'workId'=$1 AND i.intent->'action'->>'action' NOT LIKE 'task.%'
        AND (r.idempotency_key IS NULL OR r.result->>'executionState'='unknown' OR r.result->'approval'->>'status'='PENDING')
      ORDER BY i.idempotency_key LIMIT 65`, [workId])
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
      `SELECT result FROM lingxios.agent_action_ledger WHERE idempotency_key = $1`, [idempotencyKey],
    )
    return rows[0] ? rows[0]['result'] as HostActionResult : null
  }

  async record(idempotencyKey: string, result: HostActionResult): Promise<HostActionResult> {
    const { rows } = await this.pool.query(
      `INSERT INTO lingxios.agent_action_ledger (idempotency_key, result) VALUES ($1, $2::jsonb)
       ON CONFLICT (idempotency_key) DO NOTHING RETURNING result`,
      [idempotencyKey, JSON.stringify(result)],
    )
    if (rows[0]) return result
    const existing = await this.find(idempotencyKey)
    return existing ?? result
  }
}
