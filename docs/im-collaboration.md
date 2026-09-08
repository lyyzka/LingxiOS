# IM collaboration (3.2)

The authenticated product adapter owns IM authentication, policy versions, presence, networking and external message deduplication. LingxiOS owns `Message → Run → Graph → Action/Canvas → Result → Outbox → Message`. These APIs are server APIs: derive tenant, author and permissions from the authenticated IM service, never from model output or an unverified client payload.

```ts
const control = await createLingxiOS({
  database: pool,
  delivery: {
    async onEvent(work, event, context) { /* optional authorized streaming */ },
    async deliverMessage(work, message, context) {
      const im = context?.im
      if (!im) throw new Error('This adapter requires IM work')
      const sent = await transport.send({
        tenantId: im.tenantId, conversationId: im.conversationId, threadId: im.threadId,
        participantIds: im.audience.participantIds, text: message.body,
        replyTo: im.source.messageId, idempotencyKey: im.messageKey,
      })
      return { messageId: sent.id }
    },
  },
})
await control.conversations.sync({
  tenantId: 'team', conversationId: 'room', version: 1, kind: 'group',
  owner: { kind: 'participant', id: 'alice' }, defaultAgentId: 'assistant',
  participants: [
    { id: 'alice', kind: 'human', capabilities: ['read', 'execute'] },
    { id: 'assistant', kind: 'agent', capabilities: ['read', 'execute', 'speak'] },
    { id: 'researcher', kind: 'agent', capabilities: ['read', 'execute', 'speak'] },
  ],
})
const accepted = await control.conversations.ingest({
  tenantId: 'team', conversationId: 'room', policyVersion: 1,
  messageId: 'upstream-42', version: 1, author: { id: 'alice', kind: 'human' },
  text: 'Research the proposal.', mentions: ['assistant', 'researcher'],
})
```

Each explicit eligible Agent receives its own stable reply slot and Run identity. Without mentions, the default Agent receives the slot; without an eligible target the receipt records `no_speaker`. Textual mentions do not grant permissions. Duplicate message identities return the original Run set; changed content requires a higher version. Edits cancel older runs and invalidate pending older replies. Policies are monotonic and idempotent; membership and read/execute/speak capabilities are rechecked during execution, restoration, approval/input continuation and delivery. Ownership alone cannot cancel someone else's run: `cancelConversationRun` requires explicit `control` for that operation and records its command ID.

Register a thread using `conversations.registerThread({ tenantId, conversationId, threadId, policyVersion })` before ingesting it. Conversation identity is separate from execution session identity. Use returned Run identities for continuation and reads. Session keys, leases and Kernel homes include tenant, Agent and a derived session partition for conversation, thread, principal, audience and policy version. Delegates receive independent sessions. IM memory is partitioned by those identities; legacy private memory is not silently imported into a group.

Audiences use `{ visibility: 'conversation' }` or `{ visibility: 'participants', participantIds }`. Both freeze the recipient set. A removed reader invalidates old work rather than widening or silently changing its recipients. Internal delegates retain the parent's disclosure boundary and have `work.conversation.internal=true`; their results feed the graph without an IM outbox or public event stream. Direct run/artifact administration remains scoped to the original principal; public committed messages can be read by an authenticated current audience member. Product-provided context evidence, memory and dynamic text must declare an audience that contains every output recipient. Native tools still authorize their own domain resources.

Agent and system messages never auto-trigger Agents. `causedBy: { resultId }` records an outbox echo, and `replyTo: { messageId, version }` links messages without allowing a private cause to be disclosed to a wider audience. Internal cooperation uses explicit delegation. `replyKey` identifies a logical Agent reply, while `messageKey` identifies a committed result version (including approval/input waits and final answers). The adapter must pass `messageKey` unchanged on retries and return the same upstream `messageId`, including after a send succeeds but its acknowledgement is lost. IM service idempotency and recipient enforcement are adapter obligations.

## Durable graphs and Canvas state

Agents use native `graph.start/read` and `shared_state.create/read/update` tools. A custom capability resolver must explicitly grant their methods; delegates are intersected with each live ancestor's permissions. Trusted transactional tools can use the same operations through `ActionContext.enqueueGraph`, `waitForChildren`, `enqueueChild`, `readChild` and shared-state methods. Host graph APIs take the original Run identity and require its persisted request snapshot.

```ts
const graph = await control.graphs.enqueue(run, {
  id: 'proposal', nodes: [
    { id: 'cost', agentId: 'researcher', text: 'Assess cost.' },
    { id: 'risk', agentId: 'researcher', text: 'Assess risk.' },
    { id: 'join', agentId: 'assistant', text: 'Combine findings.', dependsOn: ['cost', 'risk'] },
  ],
})
const directive = await control.graphs.waitForChildren(run, graph.nodes.map(node => node.workId))
// A processor returns this defer directive; graph.start does both operations in its action transaction.
const state = { tenantId: 'team', conversationId: 'room', principalId: 'alice', stateId: 'proposal' }
await control.sharedState.create(state)
const receipt = await control.sharedState.apply(state, {
  operationId: 'edit-1', changes: [{ field: 'title', expectedVersion: 0, value: 'Proposal' }],
})
```

Graph definitions contain only node IDs, target Agents, text and dependencies. Up to 64 children per parent and 64 ancestry levels are allowed. Creation and edges commit together. Duplicate/missing/self/cyclic dependencies and cross-request references are rejected. A prerequisite must have a current committed satisfied result; a failed branch blocks its downstream nodes while independent branches continue. Exact wait sets are persistent and handle children finishing before the parent parks. Restart restores successful results and receipts; cancellation/revision invalidates descendants, and old fences cannot mutate work. Graph creation or shared-state success alone does not establish the root goal: completion still requires graph results, resource verification, settled effects, approvals and delivery obligations.

State changes compare each top-level field's version. Different fields merge; a conflict returns `ok:false`, conflicting fields and the current snapshot without overwriting anything. Nested objects and arrays replace whole values. Deletion retains a versioned tombstone, so recreation must use that version. Retrying an operation returns its original receipt, while changed arguments or actors with the same ID are rejected. Use a new operation after resolving a conflict. Updates and provenance (actor, source message, Run, graph, action key and field versions) commit together. Limits: 64 changes/64 KB per update, 256 fields/256 KB per state, 32 JSON nesting levels; history pages contain at most 64 operations. Canvas rendering and domain validation belong to the product.

`readConversationTrace({ tenantId, conversationId, threadId?, principalId, messageId, version })` returns bounded references across runs, graph nodes, actions, state operations, results and outbox receipts (256 per category, with `truncated`). It does not expose private intermediate payloads. Context history uses the latest 100 visible messages plus the trigger. This release has no rich-text CRDT, offline sync, general workflow DSL, or IM network service.

Validation uses the simulated IM adapter, PGlite regressions, real PostgreSQL connection races and a killed/restarted Worker. A successful deterministic release does not claim production IM integration or live-model quality. Installation and rollback are described in [packaged runtime](packaged-runtime.md#installation-and-versions).
