# Packaged runtime

## Ownership

`createLingxiOS()` owns the control plane: durable ingress, reads, cancellation, revisions, input continuation, approvals, reconciliation, diagnostics, delivery retries, memory administration, and Worker connections. It requires a PostgreSQL-compatible pool and does not claim work.

`createWorker()` from `lingxios/worker` owns execution. It receives a local or HTTP control-plane connection, a model driver or provider configuration, Kernel configuration, runtime policy, product processors, and an optional evolution evaluator. Multiple Workers coordinate through leases and fencing.

The consuming product owns authentication, authorization policy, native services, its business schema, transactions, delivery transport, and model-cost ledger. It registers tools through public contracts; LingxiOS has no product-specific export or table access.

## Installation and versions

`packageResources()` returns the fresh-install schema, the `migration008` upgrade from schema 7, and the Python runner. Apply exactly the appropriate SQL in an explicit product migration while holding that product's migration lock. Application startup must call its own migration-readiness check before `createLingxiOS()` and must not execute package DDL.

Every public entry uses runtime version `3.0.0`, schema `8`, control-plane protocol `6`, Kernel protocol `2`, and assistant message `2`. Protocol 6 rejects older workers that cannot enforce execution modes. Consumers should pin the exact npm version and verify the installed schema marker. The major release removes `smallModel` and `AGENT_OS_SMALL_MODEL*`; every generation, review, synthesis and compaction call uses the configured primary `model`. Optional embeddings remain a separate vector protocol.

Before upgrading, stop ingress and workers, drain running tasks and explicitly resolve unknown effects and pending approvals. Never backfill an old approval with a new tool hash: the migration leaves the new column NULL. Approval resumption requires a new preview/action identity after the old intent has been settled. A configured `HarnessProfile` pins new runs to its behavior hash; changed profile, authored workflow, tool semantics or worker model configuration blocks recovery under a different deployment. Roll back new-run routing to a retained matching deployment; do not downgrade schema 8 underneath active workers or replay unknown effects. Restoring a schema-7 backup also requires restoring the matching application and accounting for externally committed effects.

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

`npm run check:release` emits `release-results/qualifications/<source-hash>.json` only after its gates pass. It binds the exact source files, test-set hash, commit, package/protocol/schema versions and environment to the result. Source changes during a check prevent qualification. The report explicitly records live-model and consuming-product integration as not run; deterministic qualification alone does not authorize a production rollout.
