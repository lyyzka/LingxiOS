-- Optional additive performance migration for schema 10; old runtimes remain compatible.
-- Rollback: drop the four agent_*_wakeup triggers and lingxios.notify_runtime().
BEGIN;
DO $$ BEGIN
  IF (SELECT version FROM lingxios.schema_version WHERE singleton) <> 10 THEN
    RAISE EXCEPTION 'migration 011 requires schema 10';
  END IF;
END $$;

CREATE OR REPLACE FUNCTION lingxios.sync_agent_request_snapshot() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.request_snapshot IS NOT NULL AND NEW.request_snapshot->>'workId' IS NOT NULL THEN
    INSERT INTO lingxios.agent_request_snapshots(work_id,session_key,request_snapshot)
    VALUES(NEW.request_snapshot->>'workId',NEW.session_key,NEW.request_snapshot)
    ON CONFLICT(work_id) DO UPDATE SET request_snapshot=EXCLUDED.request_snapshot,updated_at=NOW()
      WHERE lingxios.agent_request_snapshots.session_key=EXCLUDED.session_key
        AND lingxios.agent_request_snapshots.request_snapshot IS DISTINCT FROM EXCLUDED.request_snapshot;
  END IF;
  RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION lingxios.notify_runtime() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  -- PostgreSQL coalesces identical notifications inside a transaction and publishes after COMMIT.
  PERFORM pg_notify(TG_ARGV[0], '');
  RETURN NULL;
END $$;
CREATE OR REPLACE TRIGGER agent_work_wakeup AFTER INSERT OR UPDATE OF status,available_at,steer_inputs
  ON lingxios.agent_work_items FOR EACH ROW EXECUTE FUNCTION lingxios.notify_runtime('lingxios_work');
CREATE OR REPLACE TRIGGER agent_events_wakeup AFTER INSERT
  ON lingxios.agent_run_events FOR EACH ROW EXECUTE FUNCTION lingxios.notify_runtime('lingxios_outbox');
CREATE OR REPLACE TRIGGER agent_delivery_wakeup AFTER INSERT
  ON lingxios.agent_delivery_outbox FOR EACH ROW EXECUTE FUNCTION lingxios.notify_runtime('lingxios_outbox');
CREATE OR REPLACE TRIGGER agent_usage_wakeup AFTER UPDATE OF observation
  ON lingxios.agent_model_budget_calls FOR EACH ROW EXECUTE FUNCTION lingxios.notify_runtime('lingxios_outbox');
COMMIT;
