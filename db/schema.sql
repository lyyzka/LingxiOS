-- LingxiOS Agent OS — control-plane schema (protocol v5)
--
-- Apply with: psql -f db/schema.sql
-- All tables are owned by the control plane; workers never touch the database.

BEGIN;
CREATE SCHEMA lingxios;

CREATE TABLE lingxios.schema_version (
  singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
  version INT NOT NULL
);


CREATE TABLE lingxios.agent_work_items (
  id                   TEXT PRIMARY KEY,
  fence                BIGINT NOT NULL DEFAULT 0,
  tenant_id            TEXT NOT NULL,
  agent_id             TEXT NOT NULL,
  session_id           TEXT NOT NULL,
  thread_id            TEXT,
  kind                 TEXT NOT NULL,
  lane                 TEXT NOT NULL CHECK (lane IN ('interactive','approval','collaboration','background')),
  trigger_ref          TEXT NOT NULL,
  principal_id         TEXT,
  priority             INT NOT NULL DEFAULT 0,
  status               TEXT NOT NULL DEFAULT 'queued'
                       CHECK (status IN ('queued','leased','waiting','succeeded','partial','blocked','failed','cancelled')),
  started_at           TIMESTAMPTZ,
  heartbeat_at         TIMESTAMPTZ,
  last_progress_at     TIMESTAMPTZ,
  created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  available_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  attempts             INT NOT NULL DEFAULT 0,
  preemptions          INT NOT NULL DEFAULT 0,
  lease_token_hash     TEXT,
  leased_by            TEXT,
  lease_expires_at     TIMESTAMPTZ,
  cancel_requested_at  TIMESTAMPTZ,
  preempt_requested_at TIMESTAMPTZ,
  steer_inputs         JSONB NOT NULL DEFAULT '[]'::jsonb,
  result_id            TEXT,
  strategy_snapshot    JSONB,
  goal_outcome         JSONB,
  error                TEXT,
  meta                 JSONB,
  finished_at          TIMESTAMPTZ,
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX agent_work_items_claim_idx
  ON lingxios.agent_work_items (status, available_at)
  WHERE status IN ('queued','leased');
CREATE INDEX agent_work_items_created_idx ON lingxios.agent_work_items(created_at DESC,id DESC);

CREATE TABLE lingxios.agent_attempts (
  work_id TEXT NOT NULL REFERENCES lingxios.agent_work_items(id) ON DELETE CASCADE,
  fence BIGINT NOT NULL,
  lease_token_hash TEXT,
  worker_id TEXT,
  started_at TIMESTAMPTZ NOT NULL,
  heartbeat_at TIMESTAMPTZ NOT NULL,
  lease_expires_at TIMESTAMPTZ,
  ended_at TIMESTAMPTZ,
  reason TEXT,
  PRIMARY KEY(work_id,fence)
);

CREATE FUNCTION lingxios.track_attempt() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='UPDATE' AND OLD.status='leased' AND (NEW.status<>'leased' OR NEW.fence<>OLD.fence) THEN
    UPDATE lingxios.agent_attempts SET ended_at=LEAST(NOW(),COALESCE(OLD.lease_expires_at,NOW())),
      reason=CASE WHEN NEW.status='leased' THEN 'lease_expired' ELSE NEW.status END
      WHERE work_id=OLD.id AND fence=OLD.fence AND ended_at IS NULL;
  END IF;
  IF NEW.status='leased' THEN
    INSERT INTO lingxios.agent_attempts(work_id,fence,lease_token_hash,worker_id,started_at,heartbeat_at,lease_expires_at)
      VALUES(NEW.id,NEW.fence,NEW.lease_token_hash,NEW.leased_by,NOW(),COALESCE(NEW.heartbeat_at,NOW()),NEW.lease_expires_at)
      ON CONFLICT(work_id,fence) DO UPDATE SET heartbeat_at=EXCLUDED.heartbeat_at,lease_expires_at=EXCLUDED.lease_expires_at;
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER agent_work_attempt AFTER INSERT OR UPDATE ON lingxios.agent_work_items
  FOR EACH ROW EXECUTE FUNCTION lingxios.track_attempt();

CREATE TABLE lingxios.agent_steps (
  step_seq BIGINT GENERATED ALWAYS AS IDENTITY,
  work_id TEXT NOT NULL REFERENCES lingxios.agent_work_items(id) ON DELETE CASCADE,
  step_id TEXT NOT NULL,
  request_version INT NOT NULL CHECK(request_version>0),
  kind TEXT NOT NULL,
  input_hash TEXT NOT NULL,
  progress_hash TEXT,
  input JSONB NOT NULL,
  output JSONB,
  artifacts JSONB NOT NULL DEFAULT '[]',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  PRIMARY KEY(work_id,step_id)
);

CREATE TABLE lingxios.agent_verifications (
  work_id TEXT NOT NULL REFERENCES lingxios.agent_work_items(id) ON DELETE CASCADE,
  request_version INT NOT NULL,
  candidate_hash TEXT NOT NULL,
  checker TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('passed','failed','inconclusive')),
  evidence JSONB NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(work_id,request_version,candidate_hash,checker)
);

CREATE TABLE lingxios.agent_model_budgets (
  root_work_id       TEXT PRIMARY KEY REFERENCES lingxios.agent_work_items(id),
  max_model_calls    INT NOT NULL CHECK (max_model_calls > 0),
  max_tokens         BIGINT NOT NULL CHECK (max_tokens > 0),
  max_cost_micros    BIGINT NOT NULL CHECK (max_cost_micros > 0),
  deadline_at        TIMESTAMPTZ NOT NULL,
  max_execution_ms   BIGINT NOT NULL DEFAULT 1800000 CHECK(max_execution_ms>0),
  model_calls        INT NOT NULL DEFAULT 0,
  tokens             BIGINT NOT NULL DEFAULT 0,
  cost_micros        BIGINT NOT NULL DEFAULT 0,
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE lingxios.agent_model_budget_calls (
  pricing JSONB,
  root_work_id TEXT NOT NULL REFERENCES lingxios.agent_model_budgets(root_work_id) ON DELETE CASCADE,
  call_id      TEXT NOT NULL,
  work_id TEXT REFERENCES lingxios.agent_work_items(id),
  fence BIGINT,
  lease_token_hash TEXT,
  observation JSONB,
  failed_at TIMESTAMPTZ,
  last_error TEXT,
  delivered_at TIMESTAMPTZ,
  available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  claim_token TEXT,
  attempts INT NOT NULL DEFAULT 0,
  reserved_tokens BIGINT NOT NULL DEFAULT 0 CHECK (reserved_tokens >= 0),
  reserved_cost_micros BIGINT NOT NULL DEFAULT 0 CHECK (reserved_cost_micros >= 0),
  input_tokens BIGINT,
  output_tokens BIGINT,
  cost_micros  BIGINT,
  PRIMARY KEY (root_work_id, call_id)
);

CREATE INDEX agent_model_budget_calls_work_idx ON lingxios.agent_model_budget_calls(work_id);

-- A retried claim returns the original answer, including null, instead of
-- leasing another work item after the first response was lost.
CREATE TABLE lingxios.agent_claim_requests (
  request_id TEXT PRIMARY KEY,
  worker_id TEXT NOT NULL,
  work_kinds JSONB NOT NULL,
  completed BOOLEAN NOT NULL DEFAULT FALSE,
  response JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX agent_claim_requests_created_idx
  ON lingxios.agent_claim_requests(created_at) WHERE completed=TRUE;

-- One live lease per session key.
CREATE TABLE lingxios.agent_os_session_leases (
  session_key TEXT PRIMARY KEY,
  work_id     TEXT NOT NULL,
  fence       BIGINT NOT NULL,
  expires_at  TIMESTAMPTZ NOT NULL,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Session -> worker routing with home epochs.
CREATE TABLE lingxios.agent_os_session_routes (
  session_key TEXT PRIMARY KEY,
  worker_id   TEXT NOT NULL,
  home_epoch  BIGINT NOT NULL DEFAULT 1,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Worker liveness.
CREATE TABLE lingxios.agent_os_workers (
  worker_id    TEXT PRIMARY KEY,
  last_seen_at TIMESTAMPTZ NOT NULL,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Durable conversational sessions (optimistic concurrency via revision).
CREATE TABLE lingxios.agent_os_sessions (
  session_key      TEXT PRIMARY KEY,
  tenant_id        TEXT NOT NULL,
  agent_id         TEXT NOT NULL,
  session_id       TEXT NOT NULL,
  thread_id        TEXT,
  summary          TEXT,
  history          JSONB NOT NULL DEFAULT '[]'::jsonb,
  applied_work_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
  revision         BIGINT NOT NULL DEFAULT 0,
  compaction_epoch INT NOT NULL DEFAULT 0,
  prompt_context   JSONB,
  request_snapshot JSONB,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Run event ledger: dedupe on (run_id, seq); the attempt range is enforced by
-- the control plane before insert.
CREATE TABLE lingxios.agent_run_events (
  run_id      TEXT NOT NULL,
  seq         BIGINT NOT NULL,
  tenant_id   TEXT NOT NULL,
  agent_id    TEXT NOT NULL,
  kind        TEXT NOT NULL,
  stage       TEXT NOT NULL,
  visibility  TEXT NOT NULL CHECK (visibility IN ('user','internal')),
  data        JSONB NOT NULL DEFAULT '{}'::jsonb,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at  TIMESTAMPTZ,
  delivery_work JSONB,
  failed_at TIMESTAMPTZ,
  last_error TEXT,
  delivered_at TIMESTAMPTZ,
  available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  claim_token TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (run_id, seq)
);
CREATE INDEX agent_run_events_delivery_idx ON lingxios.agent_run_events(available_at,run_id,seq) WHERE delivery_work IS NOT NULL AND delivered_at IS NULL;
CREATE INDEX agent_run_events_expiry_idx ON lingxios.agent_run_events(expires_at) WHERE expires_at IS NOT NULL;

-- An intent without a receipt is uncertain, never automatically re-executed.
CREATE TABLE lingxios.agent_action_intents (
  idempotency_key TEXT PRIMARY KEY,
  fingerprint TEXT NOT NULL,
  intent JSONB NOT NULL CHECK (jsonb_typeof(intent) = 'object'),
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX agent_action_intents_work_idx ON lingxios.agent_action_intents((intent->>'workId'));

-- Receipts complement pre-execution intents; missing receipts require reconciliation.
CREATE TABLE lingxios.agent_action_ledger (
  idempotency_key TEXT PRIMARY KEY REFERENCES lingxios.agent_action_intents(idempotency_key),
  result          JSONB NOT NULL,
  recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE lingxios.agent_approvals (
  id TEXT PRIMARY KEY,
  action_key TEXT NOT NULL UNIQUE REFERENCES lingxios.agent_action_intents(idempotency_key),
  preview JSONB NOT NULL,
  decision BOOLEAN,
  decided_by TEXT,
  decided_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK ((decision IS NULL) = (decided_at IS NULL)),
  CHECK ((decision IS NULL) = (decided_by IS NULL))
);

-- Immutable submitted responses. Work and delivery rows refer to this one record.
CREATE TABLE lingxios.agent_results (
  id TEXT PRIMARY KEY,
  work_id TEXT NOT NULL REFERENCES lingxios.agent_work_items(id),
  request_version INT NOT NULL CHECK (request_version>0),
  fence BIGINT NOT NULL,
  home_epoch BIGINT NOT NULL CHECK (home_epoch>0),
  message JSONB NOT NULL,
  committed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(work_id,id)
);
ALTER TABLE lingxios.agent_work_items ADD CONSTRAINT agent_work_result_fkey
  FOREIGN KEY(id,result_id) REFERENCES lingxios.agent_results(work_id,id);

CREATE TABLE lingxios.agent_delivery_outbox (
  result_id TEXT PRIMARY KEY REFERENCES lingxios.agent_results(id),
  failed_at TIMESTAMPTZ,
  last_error TEXT,
  delivered_at TIMESTAMPTZ,
  available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  claim_token TEXT,
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 30)
);

-- Each unfinished request owns its snapshot; the session row only keeps the
-- active pointer for backward-compatible readers.
CREATE TABLE lingxios.agent_request_snapshots (
  work_id          TEXT PRIMARY KEY REFERENCES lingxios.agent_work_items(id) ON DELETE CASCADE,
  session_key      TEXT NOT NULL REFERENCES lingxios.agent_os_sessions(session_key) ON DELETE CASCADE,
  request_snapshot JSONB NOT NULL CHECK (jsonb_typeof(request_snapshot)='object'),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX agent_request_snapshots_session_idx
  ON lingxios.agent_request_snapshots(session_key);

CREATE TABLE lingxios.agent_inbox_events (
  event_id    TEXT PRIMARY KEY,
  work_input  JSONB NOT NULL CHECK (jsonb_typeof(work_input)='object'),
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE FUNCTION lingxios.sync_agent_request_snapshot() RETURNS trigger AS $$
BEGIN
  IF NEW.request_snapshot IS NOT NULL AND NEW.request_snapshot->>'workId' IS NOT NULL THEN
    INSERT INTO lingxios.agent_request_snapshots(work_id,session_key,request_snapshot)
    VALUES(NEW.request_snapshot->>'workId',NEW.session_key,NEW.request_snapshot)
    ON CONFLICT(work_id) DO UPDATE SET request_snapshot=EXCLUDED.request_snapshot,updated_at=NOW()
      WHERE lingxios.agent_request_snapshots.session_key=EXCLUDED.session_key;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER agent_session_request_snapshot AFTER INSERT OR UPDATE OF request_snapshot ON lingxios.agent_os_sessions
FOR EACH ROW EXECUTE FUNCTION lingxios.sync_agent_request_snapshot();

INSERT INTO lingxios.agent_request_snapshots(work_id,session_key,request_snapshot)
SELECT request_snapshot->>'workId',session_key,request_snapshot FROM lingxios.agent_os_sessions
WHERE request_snapshot IS NOT NULL AND request_snapshot->>'workId' IS NOT NULL
ON CONFLICT(work_id) DO NOTHING;

-- Reconciliation is append-only: it settles an uncertain action without
-- overwriting either the pre-execution intent or its original receipt.
CREATE TABLE lingxios.agent_action_resolutions (
  resolution_id TEXT PRIMARY KEY,
  resolution_seq BIGSERIAL,
  idempotency_key TEXT NOT NULL REFERENCES lingxios.agent_action_intents(idempotency_key),
  resolution JSONB NOT NULL CHECK (jsonb_typeof(resolution) = 'object'),
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX agent_action_resolutions_seq_idx
  ON lingxios.agent_action_resolutions(resolution_seq);
CREATE INDEX agent_action_resolutions_action_idx
  ON lingxios.agent_action_resolutions(idempotency_key, resolution_seq DESC);

CREATE TABLE lingxios.agent_memories (
  tenant_id TEXT NOT NULL,
  id TEXT NOT NULL,
  scope_type TEXT NOT NULL CHECK (length(scope_type) BETWEEN 1 AND 1000),
  scope_id TEXT NOT NULL,
  body TEXT NOT NULL CHECK (length(body) BETWEEN 1 AND 2000),
  kind TEXT NOT NULL,
  origin TEXT NOT NULL CHECK (origin IN ('explicit','synthesized','evolved')),
  pinned BOOLEAN NOT NULL DEFAULT FALSE,
  version INTEGER NOT NULL DEFAULT 1 CHECK (version > 0),
  status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('candidate','active','retired','expired')),
  source_refs JSONB NOT NULL CHECK (jsonb_typeof(source_refs)='array' AND jsonb_array_length(source_refs) BETWEEN 1 AND 64),
  valid_until TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id,id)
);
CREATE INDEX agent_memories_scope ON lingxios.agent_memories(tenant_id,scope_type,scope_id,updated_at DESC);
CREATE UNIQUE INDEX agent_evolution_active ON lingxios.agent_memories(tenant_id,scope_type,scope_id,kind)
  WHERE origin='evolved' AND status='active';

CREATE TABLE lingxios.agent_evolution_benchmarks (
  tenant_id TEXT NOT NULL,
  id TEXT NOT NULL,
  hash TEXT NOT NULL,
  definition JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(tenant_id,id)
);

CREATE TABLE lingxios.agent_evolution_evaluations (
  tenant_id TEXT NOT NULL,
  memory_id TEXT NOT NULL,
  candidate_version INT NOT NULL,
  benchmark_id TEXT NOT NULL,
  baseline JSONB NOT NULL,
  records JSONB NOT NULL DEFAULT '{}',
  verdict TEXT NOT NULL DEFAULT 'pending' CHECK (verdict IN ('pending','passed','failed','stale')),
  summary JSONB,
  evaluated_at TIMESTAMPTZ,
  PRIMARY KEY(tenant_id,memory_id),
  FOREIGN KEY(tenant_id,memory_id) REFERENCES lingxios.agent_memories(tenant_id,id) ON DELETE CASCADE,
  FOREIGN KEY(tenant_id,benchmark_id) REFERENCES lingxios.agent_evolution_benchmarks(tenant_id,id)
);

CREATE TABLE lingxios.agent_memory_embeddings (
  tenant_id TEXT NOT NULL,
  memory_id TEXT NOT NULL,
  version INTEGER NOT NULL CHECK (version > 0),
  model_key TEXT NOT NULL,
  model TEXT NOT NULL,
  embedding DOUBLE PRECISION[] NOT NULL CHECK (cardinality(embedding) BETWEEN 1 AND 4096),
  PRIMARY KEY (tenant_id,memory_id,model_key),
  FOREIGN KEY (tenant_id,memory_id) REFERENCES lingxios.agent_memories(tenant_id,id) ON DELETE CASCADE
);

CREATE TABLE lingxios.agent_memory_versions (
  tenant_id TEXT NOT NULL,
  memory_id TEXT NOT NULL,
  version INTEGER NOT NULL CHECK (version > 0),
  snapshot JSONB NOT NULL CHECK (jsonb_typeof(snapshot)='object'),
  replaced_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id,memory_id,version),
  FOREIGN KEY (tenant_id,memory_id) REFERENCES lingxios.agent_memories(tenant_id,id) ON DELETE CASCADE
);

-- Archive the replaced version in the mutation transaction. Explicit forgetting
-- deletes its history through the foreign key instead of retaining forgotten text.
CREATE FUNCTION lingxios.archive_memory_version() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.version <= OLD.version THEN
    RAISE EXCEPTION 'memory versions must increase';
  END IF;
  INSERT INTO lingxios.agent_memory_versions(tenant_id,memory_id,version,snapshot)
    VALUES(OLD.tenant_id,OLD.id,OLD.version,to_jsonb(OLD));
  RETURN NEW;
END;
$$;
CREATE TRIGGER agent_memory_version_history
  BEFORE UPDATE OF version ON lingxios.agent_memories
  FOR EACH ROW WHEN (OLD.version IS DISTINCT FROM NEW.version)
  EXECUTE FUNCTION lingxios.archive_memory_version();

CREATE TABLE lingxios.agent_memory_evidence (
  scopes JSONB NOT NULL CHECK (jsonb_typeof(scopes)='array' AND jsonb_array_length(scopes) BETWEEN 1 AND 12),
  source_run_id TEXT PRIMARY KEY REFERENCES lingxios.agent_work_items(id),
  tenant_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  principal_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  request_version INTEGER NOT NULL CHECK (request_version > 0),
  source_ref TEXT NOT NULL,
  input_sha256 TEXT NOT NULL CHECK (input_sha256 ~ '^[a-f0-9]{64}$'),
  input_text TEXT NOT NULL CHECK (length(input_text) <= 16000),
  assistant_text TEXT NOT NULL CHECK (length(assistant_text) <= 16000),
  input_truncated BOOLEAN NOT NULL,
  assistant_truncated BOOLEAN NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','processed','rejected','superseded')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Invalidate sources atomically on every cancellation/continuation path, including
-- server-side ingress that does not call the worker's control-plane methods.
CREATE FUNCTION lingxios.supersede_memory_evidence() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  WITH superseded AS (
    UPDATE lingxios.agent_memory_evidence SET status='superseded'
    WHERE source_run_id=NEW.id AND status IN ('pending','processed')
      AND (NEW.cancel_requested_at IS NOT NULL OR NEW.status='cancelled'
        OR request_version<>jsonb_array_length(NEW.steer_inputs)+1)
    RETURNING source_run_id,tenant_id)
  UPDATE lingxios.agent_memories memory SET status='expired',version=version+1,updated_at=NOW()
    FROM superseded source WHERE memory.tenant_id=source.tenant_id
      AND ((memory.origin='synthesized' AND NOT memory.pinned AND memory.status='active')
        OR (memory.origin='evolved' AND memory.status<>'expired'))
      AND memory.source_refs @> jsonb_build_array(jsonb_build_object('workId',source.source_run_id));
  RETURN NEW;
END;
$$;
CREATE TRIGGER agent_work_memory_evidence
  AFTER UPDATE OF cancel_requested_at,status,steer_inputs ON lingxios.agent_work_items
  FOR EACH ROW
  WHEN (OLD.cancel_requested_at IS DISTINCT FROM NEW.cancel_requested_at
    OR OLD.status IS DISTINCT FROM NEW.status OR OLD.steer_inputs IS DISTINCT FROM NEW.steer_inputs)
  EXECUTE FUNCTION lingxios.supersede_memory_evidence();

INSERT INTO lingxios.schema_version(singleton, version) VALUES(TRUE, 7);
COMMIT;
