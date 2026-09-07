import { PgActionLedger, type SqlPool } from '../src/control-plane/pg-store.js'
import { actionFingerprint } from '../src/control-plane/service.js'
import type { HostAction, WorkItem } from '../src/protocol/types.js'

/** Native adapter tests use the same durable intent boundary as runtime calls. */
export async function seedAction(database: SqlPool, work: Omit<WorkItem, 'leaseToken'>, action: HostAction) {
  await database.query(`INSERT INTO lingxios.agent_work_items
    (id,tenant_id,agent_id,session_id,thread_id,principal_id,kind,lane,trigger_ref,fence,status,lease_expires_at)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'leased',NOW()+INTERVAL '1 hour') ON CONFLICT DO NOTHING`,
  [work.id,work.tenantId,work.agentId,work.sessionId,work.threadId ?? null,work.principalId ?? null,work.kind,work.lane,work.triggerRef,work.fence])
  await new PgActionLedger(database).reserve(action.idempotencyKey, actionFingerprint(work, action), {
    workId: work.id, tenantId: work.tenantId, principalId: work.principalId ?? null, agentId: work.agentId,
    sessionId: work.sessionId, threadId: work.threadId ?? null, requestVersion: 1, action,
  })
}
