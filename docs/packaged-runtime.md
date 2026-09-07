# Packaged runtime

## Ownership

`createLingxiOS()` owns the control plane: durable ingress, reads, cancellation, revisions, input continuation, approvals, reconciliation, diagnostics, delivery retries, memory administration, and Worker connections. It requires a PostgreSQL-compatible pool and does not claim work.

`createWorker()` from `lingxios/worker` owns execution. It receives a local or HTTP control-plane connection, a model driver or provider configuration, Kernel configuration, runtime policy, product processors, and an optional evolution evaluator. Multiple Workers coordinate through leases and fencing.

The consuming product owns authentication, authorization policy, native services, its business schema, transactions, delivery transport, and model-cost ledger. It registers tools through public contracts; LingxiOS has no product-specific export or table access.

## Installation and versions

`packageResources()` returns the package schema and Python runner. Apply the schema in an explicit product migration while holding that product's migration lock. Application startup must call its own migration-readiness check before `createLingxiOS()` and must not execute package DDL.

Every public entry uses runtime version `2.0.0`, schema `7`, control-plane protocol `5`, Kernel protocol `2`, and assistant message `2`. Consumers should pin the exact npm version and verify the installed schema marker.

The package exports only:

- `lingxios`: control plane, schema resources, tools, state, diagnostics, memory and evolution contracts
- `lingxios/worker`: Worker factory, model and Kernel ports, product processor contracts
- `lingxios/ui`: browser-safe committed-message and replay reducers
- `lingxios/eval`: verification and evaluation helpers

## Execution contract

An authenticated request stores its original text, principal, tenant, Agent, conversation, optional thread, attachments, and later revisions. A Worker lease restores the session and request snapshot before any model or tool call. Session ownership and work fencing prevent stale Workers from committing.

Direct tools and Python `host.*` calls use the same action executor. Input parsing and authorization happen before intent persistence. Transactional native tools commit the business write and receipt together. Idempotent external calls carry the durable action ID. An unknown effect is not retried until its tool-specific reconciler resolves it.

`decideApproval()` binds a decision to the original identity, action arguments, request version, resource snapshot, and approval version. Continuation rechecks current authorization. `continueInput()` accepts only a committed response from the original principal and current waiting version.

Model calls reserve a durable root budget before provider access and settle tokens plus a frozen price/cost snapshot afterward. Missing provider usage is recorded as estimated, never zero. The model, tool, database, delivery, and shutdown paths receive bounded deadlines and cancellation signals.

## Outcomes and delivery

The work status is `queued`, `leased`, `waiting`, `succeeded`, `partial`, `blocked`, `failed`, or `cancelled`. Goal status and delivery status are independent. `readRunState()` uses one database snapshot to return the current run, authoritative committed message, and delivery state.

Simple text can finish without a synthetic self-assessment object. Resource-producing work is checked against current action receipts, native readback, and committed artifact bytes. Unknown effects, missing requested artifacts, and failed checks prevent a satisfied outcome. Content review is bounded and keeps verified work between correction attempts.

Result, event, and model-ledger outboxes use expiring claims, bounded retries, exponential backoff, stored errors, terminal failure markers, and trusted retry APIs. A stalled native transport cannot block other channels or Worker scheduling.

## Artifacts and UI

Artifacts are limited to 16 MiB. The control plane snapshots them by content hash before committing the message. `readArtifact()` checks the authenticated run identity, manifest entry, path containment, size, and SHA-256 digest before returning bytes.

The browser consumes committed messages and ordered events through `lingxios/ui`. Reconnects page through `readEvents()` and then apply `readRunState()`; reducers reject stale fences and request versions. Waiting, partial completion, verification gaps, delivery failure, citations, and artifact provenance stay explicit.

## Memory evolution

Memory synthesis may propose tenant-scoped experience, skill, or strategy candidates only when a product configured a frozen benchmark. A candidate is activated after repeated target improvement, no holdout regression, and all deterministic authorization, approval, isolation, and no-code-mutation gates pass. Source revocation expires dependent candidates. Each run pins its active strategy versions; later activation affects new runs only. Trusted administrators can inspect or roll back evaluated versions.

## Operations

`listRuns()`, `readDiagnostics()`, `readOperations()`, metrics, and delivery retry APIs expose bounded metadata without prompts, credentials, tool payloads, or lease secrets. Products must authorize tenant, conversation, or platform-administrator scope before calling them.

Use `doctor()` for schema, Python, storage, and isolation readiness. `npm test` includes package-boundary installation. PostgreSQL recovery, capacity, and Linux image checks are separate release gates. Live-model evaluation is opt-in because it requires provider credentials; its report must retain completion rate, failures, latency, token usage, and cost.
