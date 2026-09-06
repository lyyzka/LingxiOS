# LingxiOS

AgentOS and Harness in one npm package: durable PostgreSQL work queues, fenced workers, versioned requests, Python execution, action receipts, goal outcomes and committed messages/files.

The first release targets the core runtime. The optional LingxiLoop integration contains the implemented bindings; it does not claim complete product capability coverage. There is one initial schema and no legacy migration or dual-write path.

## Run a request

Requires Node.js 22.13+, Python 3 and a PostgreSQL pool. Apply `packageResources().schema` to a new application database before starting. The application checks the installed schema and does not fall back to in-memory storage.

```ts
import { createLingxiOS } from 'lingxios'
import { pool } from './database.js'

const app = await createLingxiOS({
  database: pool,
  model: { apiKey: process.env.AGENT_OS_MODEL_API_KEY! },
  kernel: { homesRoot: '/persistent/agent-homes' },
})

try {
  // Authenticate and authorize this identity in the calling server.
  const identity = {
    runId: 'request-1', tenantId: 'tenant', agentId: 'assistant', sessionId: 'conversation',
  }
  await app.enqueue({
    ...identity, id: identity.runId, principalId: 'authenticated-user',
    text: 'Calculate six times seven using Python.',
  })
  await app.runNext()
  console.log(await app.readMessage(identity))
  console.log(await app.readOutcome(identity))
} finally {
  await app.stop()
}
```

The default model is `deepseek-ai/DeepSeek-V4-Flash` at SiliconFlow, with `reasoning_effort=high`. The calling application owns the database pool. Use `start()` for continuous local workers, or `/worker` and the authenticated HTTP control plane for separate worker processes.

## Public entries

| Entry | Purpose |
| --- | --- |
| `lingxios` | Application, diagnostics and packaged schema/runner locations |
| `lingxios/worker` | Worker process and startup configuration |
| `lingxios/ui` | Browser-safe committed message and event consumers |
| `lingxios/eval` | Recorded resource checks and model review |
| `lingxios/lingxiloop` | Optional native product bindings |

Goal status is separate from worker lifecycle. `satisfied` records the model's requirement assessment; it is not independent proof of correctness. Unknown actions, missing receipts and known resource failures prevent a satisfied result. Input/approval waits retain the same request and version. Committed files are hashed snapshots; recovering an interrupted worker does not blindly repeat unknown effects.

Python execution needs deployment isolation suitable for your trust model; Python-level guards are not an OS security boundary. Production rejects the process kernel by default: inject an OS-isolated `kernelFactory`, or explicitly opt into trusted model code. Remote workers upload hash-checked artifacts to the control plane, so their workspace does not need to share a filesystem; artifacts are limited to 16 MiB each.

See [runtime API and deployment details](docs/packaged-runtime.md). `npm test` includes standalone tarball installation. `npm run check:release` also requires isolated PostgreSQL fixtures and the native LingxiLoop reference checkout; it does not deploy a product or contact real email recipients.
