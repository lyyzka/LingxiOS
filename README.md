# LingxiOS

LingxiOS is a product-neutral agent execution runtime for Node.js and PostgreSQL. It provides durable requests, fenced workers, native tool transactions, recovery, approval and input waits, model budgets, verified outcomes, committed artifacts, delivery outboxes, and versioned memory.

The package has four public entries: `@lyyzka/lingxios`, `@lyyzka/lingxios/worker`, `@lyyzka/lingxios/ui`, and `@lyyzka/lingxios/eval`. Product rules and services stay in the consuming application.

## Install

```sh
npm install @lyyzka/lingxios@3.2.0
```

Configure the GitHub Packages registry for the `@lyyzka` scope before installing:

```ini
@lyyzka:registry=https://npm.pkg.github.com
```

Requires Node.js 22.13+, PostgreSQL, and Python 3. Install `packageResources().schema` through the product's explicit migration process before an application starts. LingxiOS performs read-only schema checks at startup and never applies DDL itself.

## Control plane and Worker

```ts
import { createLingxiOS } from '@lyyzka/lingxios'
import { createWorker } from '@lyyzka/lingxios/worker'
import { pool } from './database.js'
import { context, delivery, tools } from './native-agent-bindings.js'

const control = await createLingxiOS({
  database: pool,
  tools,
  contextProvider: context,
  delivery,
  homesRoot: '/persistent/agent-homes',
})

const worker = createWorker({
  controlPlane: control,
  model: { apiKey: process.env.AGENT_MODEL_API_KEY! },
  kernel: { homesRoot: '/persistent/agent-homes' },
})

await control.enqueue({
  id: 'request-1',
  tenantId: 'tenant',
  agentId: 'assistant',
  sessionId: 'conversation',
  principalId: 'authenticated-user',
  text: 'Create the requested document and verify it.',
})
await worker.runNext()
console.log(await control.readRunState({
  runId: 'request-1', tenantId: 'tenant', agentId: 'assistant',
  sessionId: 'conversation', principalId: 'authenticated-user',
}))

await worker.stop()
await control.stop()
```

Creating a control plane never claims work. A Worker receives the model, Kernel, runtime policy, and optional native processors. Web/API processes expose authenticated ingress and control operations; Worker processes alone execute tasks.

## Native tools and recovery

Each `ToolDefinition` supplies one input parser and model-visible schema plus authorization, effect type, execution, approval preview, reconciliation, and verification. Validation and authorization occur before an action intent is recorded. PostgreSQL writes receive the same transaction as their action receipt. Reads may be retried; confirmed receipts are restored; unknown external effects wait for reconciliation.

Work lifecycle, goal outcome, and delivery state are separate. `readRunState()` returns a consistent snapshot of all three. A `satisfied` goal requires current authoritative checks when the request created resources; model self-assessment alone cannot mark verification as passed.

Evolution candidates remain inactive until a frozen benchmark improves target cases, passes authorization, approval, isolation and code-mutation gates, and does not regress its holdout set. Active strategy references are pinned per run and can be rolled back through the trusted control API.

Production Python execution requires OS isolation. The packaged Worker defaults to Linux Bubblewrap in production and fails readiness when the isolation self-check fails. Artifact downloads verify the committed path, size, and SHA-256 digest.

See [runtime and deployment details](docs/packaged-runtime.md), [Harness semantics](docs/harness-v3.md), and [production recovery](deploy/README.md).

Optional `control.memory` provides scoped Markdown documents, always-loaded core memory, Chinese/English PostgreSQL search, committed history, versioned edits, background reflection, diagnostics and rollback. Version 3.2 requires schema 10 and protocol 8. Schema-9 installations apply the additive `packageResources().migration010`; existing memory and business records remain intact. IM conversation policy, multi-Agent reply slots, durable DAGs and field-versioned shared state are available through the [IM collaboration API](docs/im-collaboration.md). See the [memory configuration and cutover procedure](docs/packaged-runtime.md#cognitive-memory).
