-- LingxiOS Agent OS — control-plane schema (protocol v2)
--
-- Apply with: psql -f db/schema.sql
-- All tables are owned by the control plane; workers never touch the database.

CREATE TABLE IF NOT EXISTS agent_work_items (
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
  error                TEXT,
  meta                 JSONB,
  finished_at          TIMESTAMPTZ,
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS agent_work_items_claim_idx
  ON agent_work_items (status, available_at)
  WHERE status IN ('queued','leased');

-- One live lease per session key.
CREATE TABLE IF NOT EXISTS agent_os_session_leases (
  session_key TEXT PRIMARY KEY,
  work_id     TEXT NOT NULL,
  fence       BIGINT NOT NULL,
  expires_at  TIMESTAMPTZ NOT NULL,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Session -> worker routing with home epochs.
CREATE TABLE IF NOT EXISTS agent_os_session_routes (
  session_key TEXT PRIMARY KEY,
  worker_id   TEXT NOT NULL,
  home_epoch  BIGINT NOT NULL DEFAULT 1,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Worker liveness.
CREATE TABLE IF NOT EXISTS agent_os_workers (
  worker_id    TEXT PRIMARY KEY,
  last_seen_at TIMESTAMPTZ NOT NULL,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Durable conversational sessions (optimistic concurrency via revision).
CREATE TABLE IF NOT EXISTS agent_os_sessions (
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
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Run event ledger: dedupe on (run_id, seq); the attempt range is enforced by
-- the control plane before insert.
CREATE TABLE IF NOT EXISTS agent_run_events (
  run_id      TEXT NOT NULL,
  seq         BIGINT NOT NULL,
  tenant_id   TEXT NOT NULL,
  agent_id    TEXT NOT NULL,
  kind        TEXT NOT NULL,
  stage       TEXT NOT NULL,
  visibility  TEXT NOT NULL CHECK (visibility IN ('user','internal')),
  data        JSONB NOT NULL DEFAULT '{}'::jsonb,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (run_id, seq)
);

-- Host-action idempotency ledger: at-least-once delivery, at-most-once effect.
CREATE TABLE IF NOT EXISTS agent_action_ledger (
  idempotency_key TEXT PRIMARY KEY,
  result          JSONB NOT NULL,
  recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
