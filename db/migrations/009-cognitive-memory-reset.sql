-- Destructive memory-only reset from schema 8. Stop workers and back up before explicitly applying.
-- Product tasks, results, conversations, receipts and frozen evolution benchmarks remain intact.
BEGIN;
LOCK TABLE lingxios.agent_work_items IN SHARE ROW EXCLUSIVE MODE;
DO $$
BEGIN
  IF (SELECT version FROM lingxios.schema_version WHERE singleton) <> 8 THEN
    RAISE EXCEPTION 'cognitive memory reset requires schema version 8';
  END IF;
  IF EXISTS(SELECT 1 FROM lingxios.agent_work_items WHERE status='leased' AND lease_expires_at>NOW()) THEN
    RAISE EXCEPTION 'stop and drain workers before resetting memory';
  END IF;
END;
$$;
DROP TRIGGER agent_work_memory_evidence ON lingxios.agent_work_items;
UPDATE lingxios.agent_work_items SET status='cancelled',cancel_requested_at=clock_timestamp(),finished_at=clock_timestamp(),
  error='Retired by cognitive memory rebuild',meta=meta-'memorySnapshot'
  WHERE kind IN ('memory_synthesis','memory_index','memory_evaluation') AND status IN ('queued','leased','failed');
DROP TABLE lingxios.agent_memory_embeddings,lingxios.agent_memory_versions,lingxios.agent_evolution_evaluations;
DROP TABLE lingxios.agent_memories,lingxios.agent_memory_evidence,lingxios.agent_memory_scopes;
DROP FUNCTION lingxios.archive_memory_version();
DROP FUNCTION lingxios.supersede_memory_evidence();

CREATE TABLE lingxios.agent_memory_scopes (
  tenant_id TEXT NOT NULL,
  scope_type TEXT NOT NULL,
  scope_id TEXT NOT NULL,
  epoch BIGINT NOT NULL DEFAULT 0 CHECK (epoch >= 0),
  forgotten_at TIMESTAMPTZ,
  PRIMARY KEY (tenant_id,scope_type,scope_id)
);

CREATE TABLE lingxios.agent_memories (
  tenant_id TEXT NOT NULL,
  id TEXT NOT NULL,
  scope_type TEXT NOT NULL CHECK (length(scope_type) BETWEEN 1 AND 1000),
  scope_id TEXT NOT NULL,
  path TEXT NOT NULL CHECK (length(path) BETWEEN 4 AND 512),
  title TEXT NOT NULL CHECK (length(title) BETWEEN 1 AND 200),
  description TEXT NOT NULL CHECK (length(description) BETWEEN 1 AND 500),
  layer TEXT NOT NULL DEFAULT 'reference' CHECK (layer IN ('core','reference')),
  body TEXT NOT NULL CHECK (octet_length(body) BETWEEN 1 AND 16384),
  search_text TEXT NOT NULL,
  search_vector TSVECTOR GENERATED ALWAYS AS (to_tsvector('simple',search_text)) STORED,
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
CREATE UNIQUE INDEX agent_memories_path ON lingxios.agent_memories(tenant_id,scope_type,scope_id,path);
CREATE INDEX agent_memories_search ON lingxios.agent_memories USING GIN(search_vector);
CREATE UNIQUE INDEX agent_evolution_active ON lingxios.agent_memories(tenant_id,scope_type,scope_id,kind)
  WHERE origin='evolved' AND status='active';

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
  scope_epochs JSONB NOT NULL DEFAULT '[]' CHECK (jsonb_typeof(scope_epochs)='array'),
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
  search_text TEXT NOT NULL DEFAULT '',
  search_vector TSVECTOR GENERATED ALWAYS AS (to_tsvector('simple',search_text)) STORED,
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','processed','rejected','superseded')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX agent_memory_evidence_search ON lingxios.agent_memory_evidence USING GIN(search_vector);
CREATE INDEX agent_memory_evidence_identity ON lingxios.agent_memory_evidence(tenant_id,agent_id,principal_id,created_at,source_run_id);

-- Each destination owns its processing state; consuming one shared source does not consume another scope.
CREATE TABLE lingxios.agent_memory_evidence_scopes (
  tenant_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  principal_id TEXT NOT NULL,
  scope_type TEXT NOT NULL,
  scope_id TEXT NOT NULL,
  epoch BIGINT NOT NULL,
  source_run_id TEXT NOT NULL REFERENCES lingxios.agent_memory_evidence(source_run_id) ON DELETE CASCADE,
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','processed','rejected','superseded')),
  job_id TEXT REFERENCES lingxios.agent_work_items(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id,scope_type,scope_id,source_run_id),
  FOREIGN KEY (tenant_id,scope_type,scope_id) REFERENCES lingxios.agent_memory_scopes(tenant_id,scope_type,scope_id)
);
CREATE INDEX agent_memory_evidence_pending ON lingxios.agent_memory_evidence_scopes(tenant_id,agent_id,principal_id,scope_type,scope_id,epoch,created_at)
  WHERE status='pending';

CREATE TABLE lingxios.agent_memory_commands (
  tenant_id TEXT NOT NULL,
  scope_type TEXT NOT NULL,
  scope_id TEXT NOT NULL,
  epoch BIGINT NOT NULL,
  action_id TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  result JSONB NOT NULL,
  PRIMARY KEY (tenant_id,scope_type,scope_id,epoch,action_id),
  FOREIGN KEY (tenant_id,scope_type,scope_id) REFERENCES lingxios.agent_memory_scopes(tenant_id,scope_type,scope_id)
);
CREATE TABLE lingxios.agent_memory_reviews (
  action_id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  scope_type TEXT NOT NULL,
  scope_id TEXT NOT NULL,
  work_id TEXT NOT NULL REFERENCES lingxios.agent_work_items(id),
  fence BIGINT NOT NULL,
  request_version INTEGER NOT NULL,
  epoch BIGINT NOT NULL,
  preview_hash TEXT NOT NULL,
  review JSONB NOT NULL,
  FOREIGN KEY (tenant_id,scope_type,scope_id) REFERENCES lingxios.agent_memory_scopes(tenant_id,scope_type,scope_id)
);
CREATE TABLE lingxios.agent_memory_conflicts (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  scope_type TEXT NOT NULL,
  scope_id TEXT NOT NULL,
  source_run_ids JSONB NOT NULL,
  memory_ids JSONB NOT NULL,
  reason TEXT NOT NULL CHECK (octet_length(reason) BETWEEN 1 AND 4096),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  FOREIGN KEY (tenant_id,scope_type,scope_id) REFERENCES lingxios.agent_memory_scopes(tenant_id,scope_type,scope_id)
);

-- Invalidate sources atomically on every cancellation/continuation path, including
-- server-side ingress that does not call the worker's control-plane methods.
CREATE FUNCTION lingxios.supersede_memory_evidence() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  UPDATE lingxios.agent_memory_evidence_scopes SET status='superseded'
    WHERE source_run_id=NEW.id AND (NEW.cancel_requested_at IS NOT NULL OR NEW.status='cancelled'
      OR EXISTS(SELECT 1 FROM lingxios.agent_memory_evidence e WHERE e.source_run_id=NEW.id
        AND e.request_version<>jsonb_array_length(NEW.steer_inputs)+1));
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

UPDATE lingxios.schema_version SET version=9 WHERE singleton;
COMMIT;
