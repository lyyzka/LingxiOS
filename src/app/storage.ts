import type { SqlQueryable } from '../control-plane/pg-store.js'
import { releaseVersions } from '../versions.js'

export async function checkStorage(database: SqlQueryable): Promise<void> {
  const { rows } = await database.query('SELECT version FROM lingxios.schema_version WHERE singleton=TRUE')
    .catch((cause: unknown) => { throw new Error('LingxiOS schema is missing or unavailable; explicitly install the packaged schema before startup', { cause }) })
  if (rows[0]?.['version'] !== releaseVersions.schema) throw new Error(`LingxiOS requires the initial schema version ${releaseVersions.schema}`)
  const probes = {
    agent_conversations: 'tenant_id,id,version,policy',
    agent_conversation_threads: 'tenant_id,conversation_id,id',
    agent_im_messages: 'tenant_id,conversation_id,message_id,version,thread_id,fingerprint,input,audience,outcome',
    agent_reply_slots: 'tenant_id,conversation_id,message_id,message_version,agent_id,work_id',
    agent_conversation_controls: 'tenant_id,conversation_id,command_id,work_id,actor_id,fingerprint,result',
    agent_graphs: 'id,parent_work_id,request_version,definition',
    agent_graph_nodes: 'graph_id,node_id,work_id',
    agent_work_dependencies: 'work_id,dependency_id',
    agent_work_waits: 'parent_work_id,request_version,task_ref,children',
    agent_shared_states: 'tenant_id,conversation_id,thread_key,id,audience,version,fields',
    agent_shared_operations: 'seq,tenant_id,conversation_id,thread_key,state_id,operation_id,fingerprint,origin,changes,result',
    agent_results: 'id,work_id,request_version,fence,home_epoch,message,committed_at',
    agent_attempts: 'work_id,fence,lease_token_hash,worker_id,started_at,heartbeat_at,lease_expires_at,ended_at,reason',
    agent_steps: 'step_seq,work_id,step_id,request_version,kind,input_hash,progress_hash,input,output,artifacts,created_at,completed_at',
    agent_verifications: 'work_id,request_version,candidate_hash,checker,status,evidence,observed_at',
    agent_work_items: 'id,fence,tenant_id,agent_id,session_id,thread_id,kind,lane,trigger_ref,principal_id,priority,status,created_at,available_at,attempts,preemptions,lease_token_hash,leased_by,lease_expires_at,cancel_requested_at,preempt_requested_at,steer_inputs,strategy_snapshot,result_id,goal_outcome,error,meta,conversation,finished_at,updated_at',
    agent_model_budgets: 'root_work_id,max_model_calls,max_tokens,max_cost_micros,deadline_at,model_calls,tokens,cost_micros,updated_at',
    agent_model_budget_calls: 'root_work_id,call_id,reserved_tokens,reserved_cost_micros,input_tokens,output_tokens,cost_micros,pricing',
    agent_os_sessions: 'session_key,tenant_id,agent_id,session_id,thread_id,summary,history,applied_work_ids,revision,compaction_epoch,prompt_context,request_snapshot,updated_at',
    agent_request_snapshots: 'work_id,session_key,request_snapshot,updated_at',
    agent_inbox_events: 'event_id,work_input,recorded_at',
    agent_os_session_leases: 'session_key,work_id,fence,expires_at,updated_at',
    agent_os_session_routes: 'session_key,worker_id,home_epoch,updated_at',
    agent_os_workers: 'worker_id,last_seen_at,updated_at',
    agent_claim_requests: 'request_id,worker_id,work_kinds,completed,response,created_at',
    agent_run_events: 'run_id,seq,tenant_id,agent_id,kind,stage,visibility,data,recorded_at,expires_at,delivery_work,delivered_at,available_at,claim_token,attempts',
    agent_action_ledger: 'idempotency_key,result,recorded_at',
    agent_approvals: 'id,action_key,preview,tool_contract_hash,decision,decided_by,decided_at,created_at',
    agent_action_intents: 'idempotency_key,fingerprint,intent,recorded_at',
    agent_action_resolutions: 'resolution_id,resolution_seq,idempotency_key,resolution,recorded_at',
    agent_delivery_outbox: 'result_id,delivered_at,available_at,claim_token,attempts,receipt',
    agent_memory_scopes: 'tenant_id,scope_type,scope_id,epoch,forgotten_at',
    agent_memories: 'tenant_id,id,scope_type,scope_id,path,title,description,layer,body,kind,origin,pinned,version,status,source_refs,valid_until,updated_at,search_text,search_vector',
    agent_memory_versions: 'tenant_id,memory_id,version,snapshot,replaced_at',
    agent_memory_embeddings: 'tenant_id,memory_id,version,model_key,model,embedding',
    agent_evolution_benchmarks: 'tenant_id,id,hash,definition,created_at',
    agent_evolution_evaluations: 'tenant_id,memory_id,candidate_version,benchmark_id,baseline,records,verdict,summary,evaluated_at',
    agent_memory_evidence: 'source_run_id,tenant_id,agent_id,principal_id,session_id,request_version,source_ref,input_sha256,input_text,assistant_text,input_truncated,assistant_truncated,status,created_at,scope_epochs',
    agent_memory_evidence_scopes: 'tenant_id,agent_id,principal_id,scope_type,scope_id,epoch,source_run_id,status,job_id,created_at',
    agent_memory_commands: 'tenant_id,scope_type,scope_id,epoch,action_id,fingerprint,result',
    agent_memory_reviews: 'action_id,tenant_id,scope_type,scope_id,work_id,fence,request_version,epoch,preview_hash,review',
    agent_memory_conflicts: 'id,tenant_id,scope_type,scope_id,source_run_ids,memory_ids,reason,created_at',
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
  for (const name of ['agent_work_items_status_check', 'agent_work_items_lane_check', 'agent_model_budget_calls_root_work_id_fkey']) {
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
