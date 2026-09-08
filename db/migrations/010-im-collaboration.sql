-- Additive 3.1 -> 3.2 migration. Stop ingress and drain old Workers before applying.
BEGIN;
LOCK TABLE lingxios.schema_version IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
  IF (SELECT version FROM lingxios.schema_version WHERE singleton) <> 9 THEN
    RAISE EXCEPTION 'migration 010 requires schema 9';
  END IF;
  IF EXISTS (SELECT 1 FROM lingxios.agent_work_items WHERE status='leased' AND lease_expires_at>NOW()) THEN
    RAISE EXCEPTION 'drain worker leases before migration 010';
  END IF;
END $$;

ALTER TABLE lingxios.agent_work_items ADD COLUMN conversation JSONB;
ALTER TABLE lingxios.agent_delivery_outbox ADD COLUMN receipt JSONB;

CREATE TABLE lingxios.agent_conversations (
  tenant_id TEXT NOT NULL, id TEXT NOT NULL, version BIGINT NOT NULL CHECK(version>0),
  policy JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), PRIMARY KEY(tenant_id,id)
);
CREATE TABLE lingxios.agent_conversation_threads (
  tenant_id TEXT NOT NULL, conversation_id TEXT NOT NULL, id TEXT NOT NULL,
  PRIMARY KEY(tenant_id,conversation_id,id),
  FOREIGN KEY(tenant_id,conversation_id) REFERENCES lingxios.agent_conversations(tenant_id,id)
);
CREATE TABLE lingxios.agent_im_messages (
  tenant_id TEXT NOT NULL, conversation_id TEXT NOT NULL, message_id TEXT NOT NULL, version BIGINT NOT NULL CHECK(version>0),
  thread_id TEXT, fingerprint TEXT NOT NULL, input JSONB NOT NULL, audience JSONB NOT NULL,
  outcome JSONB NOT NULL, recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(tenant_id,conversation_id,message_id,version),
  FOREIGN KEY(tenant_id,conversation_id) REFERENCES lingxios.agent_conversations(tenant_id,id)
);
CREATE TABLE lingxios.agent_reply_slots (
  tenant_id TEXT NOT NULL, conversation_id TEXT NOT NULL, message_id TEXT NOT NULL, message_version BIGINT NOT NULL,
  agent_id TEXT NOT NULL, work_id TEXT NOT NULL UNIQUE REFERENCES lingxios.agent_work_items(id),
  PRIMARY KEY(tenant_id,conversation_id,message_id,message_version,agent_id),
  FOREIGN KEY(tenant_id,conversation_id,message_id,message_version)
    REFERENCES lingxios.agent_im_messages(tenant_id,conversation_id,message_id,version)
);
CREATE TABLE lingxios.agent_conversation_controls (
  tenant_id TEXT NOT NULL, conversation_id TEXT NOT NULL, command_id TEXT NOT NULL,
  work_id TEXT NOT NULL REFERENCES lingxios.agent_work_items(id), actor_id TEXT NOT NULL,
  fingerprint TEXT NOT NULL, result BOOLEAN NOT NULL, recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(tenant_id,conversation_id,command_id),
  FOREIGN KEY(tenant_id,conversation_id) REFERENCES lingxios.agent_conversations(tenant_id,id)
);
CREATE TABLE lingxios.agent_graphs (
  id TEXT PRIMARY KEY, parent_work_id TEXT NOT NULL REFERENCES lingxios.agent_work_items(id),
  request_version INTEGER NOT NULL CHECK(request_version>0), definition JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE lingxios.agent_graph_nodes (
  graph_id TEXT NOT NULL REFERENCES lingxios.agent_graphs(id), node_id TEXT NOT NULL,
  work_id TEXT NOT NULL UNIQUE REFERENCES lingxios.agent_work_items(id), PRIMARY KEY(graph_id,node_id)
);
CREATE TABLE lingxios.agent_work_dependencies (
  work_id TEXT NOT NULL REFERENCES lingxios.agent_work_items(id),
  dependency_id TEXT NOT NULL REFERENCES lingxios.agent_work_items(id),
  PRIMARY KEY(work_id,dependency_id), CHECK(work_id<>dependency_id)
);
CREATE TABLE lingxios.agent_work_waits (
  parent_work_id TEXT NOT NULL REFERENCES lingxios.agent_work_items(id), request_version INTEGER NOT NULL,
  task_ref TEXT NOT NULL, children JSONB NOT NULL, PRIMARY KEY(parent_work_id,request_version,task_ref)
);
CREATE TABLE lingxios.agent_shared_states (
  tenant_id TEXT NOT NULL, conversation_id TEXT NOT NULL, thread_key TEXT NOT NULL, id TEXT NOT NULL,
  audience JSONB NOT NULL, version BIGINT NOT NULL DEFAULT 0, fields JSONB NOT NULL DEFAULT '{}',
  PRIMARY KEY(tenant_id,conversation_id,thread_key,id),
  FOREIGN KEY(tenant_id,conversation_id) REFERENCES lingxios.agent_conversations(tenant_id,id)
);
CREATE TABLE lingxios.agent_shared_operations (
  seq BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
  tenant_id TEXT NOT NULL, conversation_id TEXT NOT NULL, thread_key TEXT NOT NULL, state_id TEXT NOT NULL,
  operation_id TEXT NOT NULL, fingerprint TEXT NOT NULL, origin JSONB NOT NULL,
  changes JSONB NOT NULL, result JSONB NOT NULL, recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY(tenant_id,conversation_id,thread_key,state_id,operation_id),
  FOREIGN KEY(tenant_id,conversation_id,thread_key,state_id)
    REFERENCES lingxios.agent_shared_states(tenant_id,conversation_id,thread_key,id)
);
CREATE INDEX agent_graphs_parent ON lingxios.agent_graphs(parent_work_id,request_version);
CREATE INDEX agent_dependencies_target ON lingxios.agent_work_dependencies(dependency_id);
CREATE INDEX agent_im_messages_context ON lingxios.agent_im_messages(tenant_id,conversation_id,thread_id,recorded_at);
UPDATE lingxios.schema_version SET version=10 WHERE singleton;
COMMIT;
