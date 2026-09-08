-- Additive migration for schema 10. Deploy before the updated control plane; old workers must be drained.
BEGIN;
DO $$ BEGIN
  IF (SELECT version FROM lingxios.schema_version WHERE singleton) <> 10 THEN
    RAISE EXCEPTION 'migration 012 requires schema 10';
  END IF;
END $$;
CREATE TABLE IF NOT EXISTS lingxios.agent_memory_capture (
  result_id TEXT PRIMARY KEY REFERENCES lingxios.agent_results(id) ON DELETE CASCADE,
  available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts>=0),
  claim_token TEXT, claim_until TIMESTAMPTZ, completed_at TIMESTAMPTZ
);
CREATE OR REPLACE TRIGGER agent_work_wakeup AFTER INSERT OR UPDATE OF status,available_at,steer_inputs,cancel_requested_at,preempt_requested_at
  ON lingxios.agent_work_items FOR EACH ROW EXECUTE FUNCTION lingxios.notify_runtime('lingxios_work');
COMMIT;
