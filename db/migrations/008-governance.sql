-- Stop old workers and drain or explicitly settle pending approvals/unknown effects first.
-- Old approval hashes deliberately remain NULL; they must never become approved for new semantics.
BEGIN;
LOCK TABLE lingxios.schema_version IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
  IF (SELECT version FROM lingxios.schema_version WHERE singleton) IS DISTINCT FROM 7 THEN
    RAISE EXCEPTION 'migration 008 requires schema version 7';
  END IF;
END $$;
CREATE TABLE lingxios.agent_memory_scopes (
  tenant_id TEXT NOT NULL,
  scope_type TEXT NOT NULL,
  scope_id TEXT NOT NULL,
  epoch BIGINT NOT NULL DEFAULT 0 CHECK (epoch >= 0),
  forgotten_at TIMESTAMPTZ,
  PRIMARY KEY (tenant_id,scope_type,scope_id)
);
ALTER TABLE lingxios.agent_memory_evidence ADD COLUMN scope_epochs JSONB NOT NULL DEFAULT '[]' CHECK (jsonb_typeof(scope_epochs)='array');
ALTER TABLE lingxios.agent_approvals ADD COLUMN tool_contract_hash TEXT CHECK (tool_contract_hash ~ '^[a-f0-9]{64}$');
UPDATE lingxios.schema_version SET version=8 WHERE singleton;
COMMIT;
