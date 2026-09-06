-- LingxiOS Agent OS — control-plane schema (protocol v2)
--
-- Apply with: psql -f db/schema.sql
-- All tables are owned by the control plane; workers never touch the database.

CREATE SCHEMA IF NOT EXISTS lingxios;

CREATE TABLE IF NOT EXISTS lingxios.schema_version (
  singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
  version INT NOT NULL
);
INSERT INTO lingxios.schema_version(singleton, version) VALUES(TRUE, 4)
ON CONFLICT (singleton) DO UPDATE SET version=GREATEST(lingxios.schema_version.version, EXCLUDED.version);

CREATE TABLE IF NOT EXISTS lingxios.agent_work_items (
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
                       CHECK (status IN ('queued','leased','completed','failed','cancelled')),
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
  result_text          TEXT,
  goal_outcome         JSONB,
  error                TEXT,
  meta                 JSONB,
  finished_at          TIMESTAMPTZ,
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS agent_work_items_claim_idx
  ON lingxios.agent_work_items (status, available_at)
  WHERE status IN ('queued','leased');

CREATE TABLE IF NOT EXISTS lingxios.agent_model_budgets (
  root_work_id       TEXT PRIMARY KEY REFERENCES lingxios.agent_work_items(id),
  max_model_calls    INT NOT NULL CHECK (max_model_calls > 0),
  max_tokens         BIGINT NOT NULL CHECK (max_tokens > 0),
  max_cost_micros    BIGINT NOT NULL CHECK (max_cost_micros > 0),
  deadline_at        TIMESTAMPTZ NOT NULL,
  model_calls        INT NOT NULL DEFAULT 0,
  tokens             BIGINT NOT NULL DEFAULT 0,
  cost_micros        BIGINT NOT NULL DEFAULT 0,
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS lingxios.agent_model_budget_calls (
  root_work_id TEXT NOT NULL REFERENCES lingxios.agent_model_budgets(root_work_id) ON DELETE CASCADE,
  call_id      TEXT NOT NULL,
  input_tokens BIGINT,
  output_tokens BIGINT,
  cost_micros  BIGINT,
  PRIMARY KEY (root_work_id, call_id)
);

-- Native professional HTML lecture decks. The record is versioned as one
-- immutable JSON document; checkpoints make long generation resumable.
CREATE TABLE IF NOT EXISTS lingxios.lecture_decks (
  id           TEXT PRIMARY KEY,
  tenant_id    TEXT NOT NULL,
  principal_id TEXT NOT NULL,
  revision     INT NOT NULL CHECK (revision > 0),
  status       TEXT NOT NULL CHECK (status IN ('planning','generating','validating','publishing','ready','failed','cancelled')),
  record       JSONB NOT NULL CHECK (jsonb_typeof(record) = 'object'),
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (tenant_id, id)
);
CREATE INDEX IF NOT EXISTS lecture_decks_owner_idx ON lingxios.lecture_decks(tenant_id,principal_id,updated_at DESC);

CREATE TABLE IF NOT EXISTS lingxios.lecture_checkpoints (
  deck_id      TEXT NOT NULL REFERENCES lingxios.lecture_decks(id) ON DELETE CASCADE,
  revision     INT NOT NULL CHECK (revision > 0),
  stage        TEXT NOT NULL CHECK (stage IN ('plan-course','plan-chapter','author-slide','validate-slide','repair-slide','validate-deck','publish-deck')),
  stage_key    TEXT NOT NULL,
  input_hash   TEXT NOT NULL,
  output_hash  TEXT NOT NULL,
  attempts     INT NOT NULL DEFAULT 1 CHECK (attempts > 0),
  result       JSONB NOT NULL,
  completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (deck_id,revision,stage,stage_key)
);

-- A retried claim returns the original answer, including null, instead of
-- leasing another work item after the first response was lost.
CREATE TABLE IF NOT EXISTS lingxios.agent_claim_requests (
  request_id TEXT PRIMARY KEY,
  worker_id TEXT NOT NULL,
  completed BOOLEAN NOT NULL DEFAULT FALSE,
  response JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS agent_claim_requests_created_idx
  ON lingxios.agent_claim_requests(created_at) WHERE completed=TRUE;

-- Package-owned schedules; no dependency on the retired product agent runtime.
CREATE TABLE IF NOT EXISTS lingxios.agent_routines (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  principal_id TEXT NOT NULL,
  project_id TEXT,
  course_id TEXT,
  thread_id TEXT,
  kind TEXT NOT NULL,
  title TEXT,
  instructions TEXT,
  schedule JSONB NOT NULL,
  timezone TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('active','paused')),
  version INT NOT NULL DEFAULT 1 CHECK (version>0),
  next_run_at TIMESTAMPTZ,
  pause_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CHECK (kind<>'teacher_digest' OR (project_id IS NOT NULL AND course_id IS NOT NULL AND thread_id IS NULL)),
  CHECK (kind='teacher_digest' OR (title IS NOT NULL AND instructions IS NOT NULL AND course_id IS NULL)),
  CHECK ((status='active' AND next_run_at IS NOT NULL) OR (status='paused' AND next_run_at IS NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS agent_teacher_digest_scope_idx ON lingxios.agent_routines(tenant_id,agent_id,session_id,kind) WHERE kind='teacher_digest';
CREATE INDEX IF NOT EXISTS agent_routines_due_idx ON lingxios.agent_routines(next_run_at) WHERE status='active';
CREATE TABLE IF NOT EXISTS lingxios.agent_routine_runs (
  routine_id TEXT NOT NULL REFERENCES lingxios.agent_routines(id) ON DELETE CASCADE,
  routine_version INT NOT NULL,
  scheduled_at TIMESTAMPTZ NOT NULL,
  work_id TEXT NOT NULL UNIQUE REFERENCES lingxios.agent_work_items(id) ON DELETE CASCADE,
  PRIMARY KEY(routine_id,routine_version,scheduled_at)
);

-- One live lease per session key.
CREATE TABLE IF NOT EXISTS lingxios.agent_os_session_leases (
  session_key TEXT PRIMARY KEY,
  work_id     TEXT NOT NULL,
  fence       BIGINT NOT NULL,
  expires_at  TIMESTAMPTZ NOT NULL,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Session -> worker routing with home epochs.
CREATE TABLE IF NOT EXISTS lingxios.agent_os_session_routes (
  session_key TEXT PRIMARY KEY,
  worker_id   TEXT NOT NULL,
  home_epoch  BIGINT NOT NULL DEFAULT 1,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Worker liveness.
CREATE TABLE IF NOT EXISTS lingxios.agent_os_workers (
  worker_id    TEXT PRIMARY KEY,
  last_seen_at TIMESTAMPTZ NOT NULL,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Durable conversational sessions (optimistic concurrency via revision).
CREATE TABLE IF NOT EXISTS lingxios.agent_os_sessions (
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
CREATE TABLE IF NOT EXISTS lingxios.agent_run_events (
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
  PRIMARY KEY (run_id, seq)
);
ALTER TABLE lingxios.agent_run_events ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS agent_run_events_expiry_idx ON lingxios.agent_run_events(expires_at) WHERE expires_at IS NOT NULL;

-- An intent without a receipt is uncertain, never automatically re-executed.
CREATE TABLE IF NOT EXISTS lingxios.agent_action_intents (
  idempotency_key TEXT PRIMARY KEY,
  fingerprint TEXT NOT NULL,
  intent JSONB NOT NULL CHECK (jsonb_typeof(intent) = 'object'),
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Receipts complement pre-execution intents; missing receipts require reconciliation.
CREATE TABLE IF NOT EXISTS lingxios.agent_action_ledger (
  idempotency_key TEXT PRIMARY KEY REFERENCES lingxios.agent_action_intents(idempotency_key),
  result          JSONB NOT NULL,
  recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS lingxios.agent_messages (
  run_id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  message JSONB NOT NULL,
  home_epoch BIGINT NOT NULL DEFAULT 1 CHECK (home_epoch > 0),
  committed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS lingxios.agent_delivery_outbox (
  run_id TEXT PRIMARY KEY REFERENCES lingxios.agent_messages(run_id),
  work JSONB NOT NULL,
  delivered_at TIMESTAMPTZ,
  available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  claim_token TEXT,
  attempts INTEGER NOT NULL DEFAULT 0
);

-- Each unfinished request owns its snapshot; the session row only keeps the
-- active pointer for backward-compatible readers.
CREATE TABLE IF NOT EXISTS lingxios.agent_request_snapshots (
  work_id          TEXT PRIMARY KEY REFERENCES lingxios.agent_work_items(id) ON DELETE CASCADE,
  session_key      TEXT NOT NULL REFERENCES lingxios.agent_os_sessions(session_key) ON DELETE CASCADE,
  request_snapshot JSONB NOT NULL CHECK (jsonb_typeof(request_snapshot)='object'),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS agent_request_snapshots_session_idx
  ON lingxios.agent_request_snapshots(session_key);

CREATE TABLE IF NOT EXISTS lingxios.agent_inbox_events (
  event_id    TEXT PRIMARY KEY,
  work_input  JSONB NOT NULL CHECK (jsonb_typeof(work_input)='object'),
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION lingxios.sync_agent_request_snapshot() RETURNS trigger AS $$
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
DROP TRIGGER IF EXISTS agent_session_request_snapshot ON lingxios.agent_os_sessions;
CREATE TRIGGER agent_session_request_snapshot AFTER INSERT OR UPDATE OF request_snapshot ON lingxios.agent_os_sessions
FOR EACH ROW EXECUTE FUNCTION lingxios.sync_agent_request_snapshot();

INSERT INTO lingxios.agent_request_snapshots(work_id,session_key,request_snapshot)
SELECT request_snapshot->>'workId',session_key,request_snapshot FROM lingxios.agent_os_sessions
WHERE request_snapshot IS NOT NULL AND request_snapshot->>'workId' IS NOT NULL
ON CONFLICT(work_id) DO NOTHING;

-- Reconciliation is append-only: it settles an uncertain action without
-- overwriting either the pre-execution intent or its original receipt.
CREATE TABLE IF NOT EXISTS lingxios.agent_action_resolutions (
  resolution_id TEXT PRIMARY KEY,
  resolution_seq BIGSERIAL,
  idempotency_key TEXT NOT NULL REFERENCES lingxios.agent_action_intents(idempotency_key),
  resolution JSONB NOT NULL CHECK (jsonb_typeof(resolution) = 'object'),
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
ALTER TABLE lingxios.agent_action_resolutions ADD COLUMN IF NOT EXISTS resolution_seq BIGSERIAL;
CREATE UNIQUE INDEX IF NOT EXISTS agent_action_resolutions_seq_idx
  ON lingxios.agent_action_resolutions(resolution_seq);
CREATE INDEX IF NOT EXISTS agent_action_resolutions_action_idx
  ON lingxios.agent_action_resolutions(idempotency_key, resolution_seq DESC);

CREATE TABLE IF NOT EXISTS lingxios.agent_calendar_outbox (
  id TEXT PRIMARY KEY REFERENCES lingxios.agent_action_intents(idempotency_key),
  work_id TEXT NOT NULL REFERENCES lingxios.agent_work_items(id),
  event JSONB NOT NULL CHECK (jsonb_typeof(event)='object' AND event->>'type'='calendar.changed'),
  delivered_at TIMESTAMPTZ,
  available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  claim_token TEXT,
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 30)
);
CREATE INDEX IF NOT EXISTS agent_calendar_outbox_pending
  ON lingxios.agent_calendar_outbox(available_at,id) WHERE delivered_at IS NULL;

CREATE TABLE IF NOT EXISTS lingxios.agent_canvas_outbox (
  id TEXT PRIMARY KEY,
  event JSONB NOT NULL CHECK (jsonb_typeof(event)='object' AND event->>'type'='canvas.changed'),
  delivered_at TIMESTAMPTZ,
  available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  claim_token TEXT,
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 30)
);
CREATE INDEX IF NOT EXISTS agent_canvas_outbox_pending
  ON lingxios.agent_canvas_outbox(available_at,id) WHERE delivered_at IS NULL;

CREATE TABLE IF NOT EXISTS lingxios.agent_document_outbox (
  id TEXT PRIMARY KEY,
  event JSONB NOT NULL CHECK (jsonb_typeof(event)='object' AND event->>'type' IN ('doc.changed','doc.update')),
  delivered_at TIMESTAMPTZ,
  available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  claim_token TEXT,
  attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 30)
);
CREATE INDEX IF NOT EXISTS agent_document_outbox_pending
  ON lingxios.agent_document_outbox(available_at,id) WHERE delivered_at IS NULL;

CREATE TABLE IF NOT EXISTS lingxios.agent_memories (
  tenant_id TEXT NOT NULL,
  id TEXT NOT NULL,
  scope_type TEXT NOT NULL CHECK (scope_type IN ('learner','course','agent_role')),
  scope_id TEXT NOT NULL,
  body TEXT NOT NULL CHECK (length(body) BETWEEN 1 AND 2000),
  kind TEXT NOT NULL,
  origin TEXT NOT NULL CHECK (origin IN ('explicit','synthesized')),
  pinned BOOLEAN NOT NULL DEFAULT FALSE,
  version INTEGER NOT NULL DEFAULT 1 CHECK (version > 0),
  status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','expired')),
  source_refs JSONB NOT NULL CHECK (jsonb_typeof(source_refs)='array' AND jsonb_array_length(source_refs) BETWEEN 1 AND 64),
  valid_until TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id,id)
);
CREATE INDEX IF NOT EXISTS agent_memories_scope ON lingxios.agent_memories(tenant_id,scope_type,scope_id,updated_at DESC);

CREATE TABLE IF NOT EXISTS lingxios.agent_memory_embeddings (
  tenant_id TEXT NOT NULL,
  memory_id TEXT NOT NULL,
  version INTEGER NOT NULL CHECK (version > 0),
  model_key TEXT NOT NULL,
  model TEXT NOT NULL,
  embedding DOUBLE PRECISION[] NOT NULL CHECK (cardinality(embedding) BETWEEN 1 AND 4096),
  PRIMARY KEY (tenant_id,memory_id,model_key),
  FOREIGN KEY (tenant_id,memory_id) REFERENCES lingxios.agent_memories(tenant_id,id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS lingxios.agent_memory_versions (
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
CREATE OR REPLACE FUNCTION lingxios.archive_memory_version() RETURNS trigger
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
CREATE OR REPLACE TRIGGER agent_memory_version_history
  BEFORE UPDATE OF version ON lingxios.agent_memories
  FOR EACH ROW WHEN (OLD.version IS DISTINCT FROM NEW.version)
  EXECUTE FUNCTION lingxios.archive_memory_version();

CREATE TABLE IF NOT EXISTS lingxios.agent_memory_evidence (
  source_run_id TEXT PRIMARY KEY REFERENCES lingxios.agent_messages(run_id),
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
CREATE OR REPLACE FUNCTION lingxios.supersede_memory_evidence() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  WITH superseded AS (
    UPDATE lingxios.agent_memory_evidence SET status='superseded'
    WHERE source_run_id=NEW.id AND status IN ('pending','processed')
      AND (NEW.cancel_requested_at IS NOT NULL OR NEW.status='cancelled'
        OR request_version<>jsonb_array_length(NEW.steer_inputs)+1)
    RETURNING source_run_id,tenant_id)
  UPDATE lingxios.agent_memories memory SET status='expired',version=version+1,updated_at=NOW()
    FROM superseded source WHERE memory.tenant_id=source.tenant_id AND memory.origin='synthesized'
      AND NOT memory.pinned AND memory.status='active'
      AND memory.source_refs @> jsonb_build_array(jsonb_build_object('workId',source.source_run_id));
  RETURN NEW;
END;
$$;
CREATE OR REPLACE TRIGGER agent_work_memory_evidence
  AFTER UPDATE OF cancel_requested_at,status,steer_inputs ON lingxios.agent_work_items
  FOR EACH ROW
  WHEN (OLD.cancel_requested_at IS DISTINCT FROM NEW.cancel_requested_at
    OR OLD.status IS DISTINCT FROM NEW.status OR OLD.steer_inputs IS DISTINCT FROM NEW.steer_inputs)
  EXECUTE FUNCTION lingxios.supersede_memory_evidence();
