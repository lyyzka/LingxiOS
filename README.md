# LingxiOS

AgentOS and Harness in one npm package: durable PostgreSQL work queues, fenced workers, versioned requests, Python execution, action receipts, goal outcomes and committed messages/files.

The first release includes the packaged runtime and all currently implemented LingxiLoop bindings. LectureDeck is the sole presentation workflow; it persists outlines, requires approval before slide generation, and recovers fenced workers through the control plane. There is one initial schema and no legacy migration or dual-write path.

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
| `lingxios/lecture-deck` | Package-owned lecture requests, operations, and artifacts |

Goal status is separate from worker lifecycle. `satisfied` records the model's requirement assessment; it is not independent proof of correctness. Unknown actions, missing receipts and known resource failures prevent a satisfied result. Input/approval waits retain the same request and version. Committed files are hashed snapshots; recovering an interrupted worker does not blindly repeat unknown effects.

The shared prompt separates instruction/data boundaries, tool use, action recovery, delivery, and final JSON assessment. Its organization draws on the [system prompt reference collection](https://github.com/asgeirtj/system_prompts_leaks) and [Anthropic's prompting guidance](https://platform.claude.com/docs/en/build-with-claude/prompt-engineering/claude-prompting-best-practices); the collection is an unofficial reference, not a verified provider contract. Compaction preserves revisions and uncertain action outcomes without promoting source text into instructions. Prompt contract upgrades refresh frozen instructions while retaining session history.

`eval/runtime-cases.json` includes quoted instruction injection and code examples without execution, alongside format and artifact checks. With model credentials configured, run `npm run eval:live -- --output NEW_DIRECTORY --repeat 3` to measure behavior; deterministic tests alone do not establish model quality or injection resistance.

Python execution needs deployment isolation suitable for your trust model; Python-level guards are not an OS security boundary. Production defaults to Linux Bubblewrap and fails startup if its self-check fails. Remote workers receive lease-fenced contexts over the control-plane HTTP API and cannot access PostgreSQL or committed artifacts directly. Artifacts are hash-checked and limited to 16 MiB each.

See [runtime API and deployment details](docs/packaged-runtime.md) and [production deployment and recovery](deploy/README.md). `npm test` includes standalone tarball installation. `npm run check:release` builds once and runs package, PostgreSQL, native binding, Worker recovery, capacity, and image gates; live model and signed human acceptance evidence are required for a release result.
