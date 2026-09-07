# Prompt runtime

PE is compiled runtime configuration, with a separate data channel. The compiler produces provider instructions, provenance-bearing data items and a content-free manifest. The runtime owns authorization, request durability, budgets and acceptance; prompt text cannot replace those checks.

## Design references

- [Claude Code prompt-management analysis, pinned at 7b7b915](https://github.com/liuup/claude-code-analysis/blob/7b7b915d7da804088a8152ed24c68e3da2d1110e/analysis/04g-prompt-management.md): named sections, stable prefixes, independent task prompts and observable assembly. This is a third-party analysis, not an authenticated production specification.
- [Gemini CLI PromptProvider, pinned at 85aca16](https://github.com/google-gemini/gemini-cli/blob/85aca163f6c73ac6ce380b5447359146b8adcae4/packages/core/src/prompts/promptProvider.ts): gather live configuration separately from section rendering. LingxiOS uses its existing trusted RuntimePolicy rather than adding environment flags, template files or another provider class.

No upstream prompt text is copied. Platform instructions remain LingxiOS policy. Product rules may contribute trusted instructions; there is deliberately no runtime option to replace platform authorization or isolation rules.

## Assembly contract

```text
Live TurnContext + trusted RuntimePolicy
    → buildPromptContext
    → compileContext
    → PromptContext { systemInstructions, blocks, manifest, fingerprint }
    → durable request + history + live observations
    → model execution boundary → provider

Trusted auxiliary purpose → compileAuxiliaryPrompt
    → isolated instructions + manifest → structured/compaction call
```

| Owner | Responsibility |
| --- | --- |
| `prompts/sections.ts` | Immutable named platform sections: identity, authority, trust, workflow, tools, evidence, delivery, completion. |
| `prompts/provider.ts` | Gather live product rules, one capability-resolver result, current persona, tool schemas and source versions. Never reuse persisted system instructions. |
| `context/compiler.ts` | Validate sections, preserve order and data provenance, render instructions, compute hashes and UTF-8 byte counts. No filesystem, environment, clock or mutable session cache. |
| `runtime/runtime.ts` | Restore original requests, apply steering, preserve tool pairs, trim optional memory, compact history, enforce mandatory-input budgets and acceptance. |
| `model/execution.ts` | Match the manifest to actual instructions before budget reservation, exclude diagnostic metadata from model token estimates, persist prompt identity with call usage and retries. |

The fixed platform sections always lead. Product rules from `RuntimePolicy.productRules()` follow as a stable section. Current grants occupy the dynamic instruction suffix. Persona, requests, attachments, observations and memory remain data. Historical system-role items are demoted before reaching the provider. Typed trust labels are provenance, not authentication: only application-controlled configuration may enter the trusted-rule callback.

The compiler rejects duplicate/reserved source IDs, invalid section fields, truncated trusted instructions, data marked as an instruction prefix, and stable instructions after a dynamic section. It never silently reorders sections or truncates instructions to fit. Auxiliary task rules remain next to their domain validators (content review, memory, evaluation); all use the same isolated compiler and purpose identity.

## Prefixes and invalidation

`ContextBlock.cache` is `prefix` or `dynamic`; trusted caller blocks default to dynamic. The manifest records the exact leading instruction bytes, their SHA-256 and their section count. Boundaries are metadata and are never sent as magic text to a model. Providers continue receiving the existing string instruction contract.

There is no process-wide cache of tenant content. Each hop rebuilds from live configuration, including after resume or compaction. A rule-content change changes its hash even if the caller forgets to bump a version. Revocation changes the dynamic suffix immediately. Persona changes leave instruction bytes stable. Tool schemas and source versions participate in the full prompt-context fingerprint; model/provider configuration and complete input hashes remain in call events. Compaction epochs are lifecycle metadata, not synthetic cache breakers.

Prefix metadata describes reusable bytes, not a guaranteed provider cache hit. It does not establish tenant isolation at a provider or control provider cache retention. UTF-8 byte counts are conservative diagnostics, not measured token usage. This implementation has no custom cache service, tokenizer dependency or cache-clearing API.

## Auxiliary calls and audit

Execution, content review, memory proposal/verification, evaluation review and compaction have distinct purposes. Auxiliary HTTP calls have no tools. The execution wrapper normalizes compaction to the dedicated isolated prompt before both accounting and dispatch; custom drivers receive the same policy as the built-in driver. For compatibility, legacy `compact({instructions, ...})` inputs remain accepted but cannot substitute execution/persona instructions for the compaction policy.

`PromptContext.manifest` contains section source/version/trust/placement/truncation, content hashes, byte counts, the instruction hash and the stable-prefix boundary. `model.started` includes the current context fingerprint and manifest. Durable `ModelCallObservation` and `model.request.*` events carry the manifest and instruction hash for calls using the compiler; legacy/custom structured calls still get their actual instruction hash. Manifests are local diagnostics and do not become provider request fields or consume model-token reservations.

Prompt bodies are absent from manifests. Existing model-payload opt-in, redaction, sampling and retention govern full execution input capture. This is not a new unconditional prompt dump. Input hashes and manifests explain prompt changes; replay still requires the corresponding inputs and provider configuration. Model quality and actual cache savings require separate live evaluation.

## Compatibility and validation

`PromptContext.version` stays 3 and its new manifest is optional on persisted records. Old snapshots are rebuilt from current trusted configuration while preserving request/history. The default prompt contract is `prompt-v3.1`. `RuntimePolicy.productRules`, `compileContext`, `auxiliaryInstructions` and model string-instruction interfaces remain supported. No database migration, approval-policy change, dependency addition or production rollout is part of this change.

The public package exports `buildPromptContext`, `compileContext`, `compileAuxiliaryPrompt` and their result/manifest types so a configured application can inspect the same compilation used by execution.

`test/prompt-runtime.test.ts` covers deterministic UTF-8 boundaries, invalid sections, live revocation, stale snapshots, tenant isolation, tool/schema invalidation, single capability resolution, auxiliary accounting and provider serialization. Existing harness, runtime, model, compaction, memory and evaluation tests cover request preservation, correction bounds and acceptance. Run `npm test` for the package suite. Docker/native product and live-model release gates remain required before production rollout.
