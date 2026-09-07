import type { SqlQueryable } from '../control-plane/pg-store.js'
import { releaseVersions } from '../versions.js'

export async function checkStorage(database: SqlQueryable): Promise<void> {
  const { rows } = await database.query('SELECT version FROM lingxios.schema_version WHERE singleton=TRUE')
    .catch((cause: unknown) => { throw new Error('LingxiOS schema is missing or unavailable; explicitly install the packaged schema before startup', { cause }) })
  if (rows[0]?.['version'] !== releaseVersions.schema) throw new Error(`LingxiOS requires the initial schema version ${releaseVersions.schema}`)
  const probes = {
    agent_results: 'work_id,candidate_hash,request_version,fence,message,committed_at',
    agent_attempts: 'work_id,fence,lease_token_hash,worker_id,started_at,heartbeat_at,lease_expires_at,ended_at,reason',
    agent_steps: 'step_seq,work_id,step_id,request_version,kind,input_hash,input,output,artifacts,created_at,completed_at',
    agent_verifications: 'work_id,request_version,candidate_hash,checker,status,evidence,observed_at',
    lecture_decks: 'id,tenant_id,principal_id,revision,status,record,created_at,updated_at',
    lecture_checkpoints: 'deck_id,revision,stage,stage_key,input_hash,output_hash,attempts,result,completed_at',
    agent_document_outbox: 'id,event,delivered_at,available_at,claim_token,attempts',
    agent_canvas_outbox: 'id,event,delivered_at,available_at,claim_token,attempts',
    agent_routines: 'id,tenant_id,agent_id,session_id,principal_id,project_id,course_id,thread_id,kind,title,instructions,schedule,timezone,status,version,next_run_at,pause_reason,created_at,updated_at',
    agent_routine_runs: 'routine_id,routine_version,scheduled_at,work_id',
    agent_work_items: 'id,fence,tenant_id,agent_id,session_id,thread_id,kind,lane,trigger_ref,principal_id,priority,status,created_at,available_at,attempts,preemptions,lease_token_hash,leased_by,lease_expires_at,cancel_requested_at,preempt_requested_at,steer_inputs,result_text,goal_outcome,error,meta,finished_at,updated_at',
    agent_model_budgets: 'root_work_id,max_model_calls,max_tokens,max_cost_micros,deadline_at,model_calls,tokens,cost_micros,updated_at',
    agent_model_budget_calls: 'root_work_id,call_id,reserved_tokens,reserved_cost_micros,input_tokens,output_tokens,cost_micros',
    agent_os_sessions: 'session_key,tenant_id,agent_id,session_id,thread_id,summary,history,applied_work_ids,revision,compaction_epoch,prompt_context,request_snapshot,updated_at',
    agent_request_snapshots: 'work_id,session_key,request_snapshot,updated_at',
    agent_inbox_events: 'event_id,work_input,recorded_at',
    agent_os_session_leases: 'session_key,work_id,fence,expires_at,updated_at',
    agent_os_session_routes: 'session_key,worker_id,home_epoch,updated_at',
    agent_os_workers: 'worker_id,last_seen_at,updated_at',
    agent_claim_requests: 'request_id,worker_id,work_kinds,completed,response,created_at',
    agent_run_events: 'run_id,seq,tenant_id,agent_id,kind,stage,visibility,data,recorded_at,expires_at,delivery_work,delivered_at,available_at,claim_token,attempts',
    agent_action_ledger: 'idempotency_key,result,recorded_at',
    agent_action_intents: 'idempotency_key,fingerprint,intent,recorded_at',
    agent_action_resolutions: 'resolution_id,resolution_seq,idempotency_key,resolution,recorded_at',
    agent_delivery_outbox: 'run_id,work,delivered_at,available_at,claim_token,attempts',
    agent_calendar_outbox: 'id,work_id,event,delivered_at,available_at,claim_token,attempts',
    agent_messages: 'run_id,tenant_id,agent_id,session_id,message,home_epoch,committed_at',
    agent_memories: 'tenant_id,id,scope_type,scope_id,body,kind,origin,pinned,version,status,source_refs,valid_until,updated_at',
    agent_memory_versions: 'tenant_id,memory_id,version,snapshot,replaced_at',
    agent_memory_embeddings: 'tenant_id,memory_id,version,model_key,model,embedding',
    agent_memory_evidence: 'source_run_id,tenant_id,agent_id,principal_id,session_id,request_version,source_ref,input_sha256,input_text,assistant_text,input_truncated,assistant_truncated,status,created_at',
  }
  for (const [table, columns] of Object.entries(probes)) {
    await database.query(`SELECT ${columns} FROM lingxios.${table} WHERE FALSE`)
  }
  const constraints = await database.query(`SELECT conname,contype FROM pg_constraint
    WHERE connamespace='lingxios'::regnamespace AND convalidated`)
  const names = new Set(constraints.rows.map(row => row['conname']))
  for (const table of Object.keys(probes)) {
    if (!names.has(`${table}_pkey`)) throw new Error(`LingxiOS primary key is missing: ${table}`)
  }
  for (const name of ['agent_work_items_status_check', 'agent_work_items_lane_check', 'lecture_decks_status_check',
    'lecture_decks_revision_check', 'lecture_checkpoints_deck_id_fkey', 'agent_model_budget_calls_root_work_id_fkey']) {
    if (!names.has(name)) throw new Error(`LingxiOS required constraint is missing: ${name}`)
  }
  const snapshot = await database.query(`SELECT 1 FROM pg_trigger WHERE tgrelid='lingxios.agent_os_sessions'::regclass
    AND tgname='agent_session_request_snapshot' AND tgenabled IN ('O','A') AND NOT tgisinternal`)
  if (!snapshot.rows.length) throw new Error('LingxiOS request snapshot trigger is missing or disabled')
  const triggers = await database.query(`SELECT 1 FROM pg_trigger
    WHERE tgrelid='lingxios.agent_work_items'::regclass AND tgname='agent_work_memory_evidence'
      AND tgenabled IN ('O','A') AND NOT tgisinternal`)
  if (!triggers.rows.length) throw new Error('LingxiOS memory evidence invalidation trigger is missing or disabled')
  const history = await database.query(`SELECT 1 FROM pg_trigger
    WHERE tgrelid='lingxios.agent_memories'::regclass AND tgname='agent_memory_version_history'
      AND tgenabled IN ('O','A') AND NOT tgisinternal`)
  if (!history.rows.length) throw new Error('LingxiOS memory version history trigger is missing or disabled')
}
