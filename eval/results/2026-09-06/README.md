# Candidate live runtime evaluation

Final core-release evidence is in `core-release`: `deepseek-ai/DeepSeek-V4-Flash`, `reasoning_effort=high`, real PostgreSQL, four cases. All four passed deterministic checks and uncalibrated semantic reviews with no execution/review errors or timeouts. The cases cover full derivation, one hint, chat without Python, and an independently downloaded JSON file. The earlier `deepseek-high2` run also passed all eight samples across two repeats. These are agent-authored examples, not a representative or human-calibrated benchmark. Each report identifies the implementation it exercised.

The following entries preserve earlier models and failed development runs. They do not describe the current default model or override the latest report.

`hint-review8k` keeps the runtime's 4096-token output budget but raises only the review allowance to 8192. Its answer committed, but the review reached its 60-second timeout. Increasing the token allowance alone did not establish a reliable review or a quality pass. The report records both budgets explicitly.

`hint-v3` and `full-v3` exercise the updated default prompt, which explicitly follows requested quantity and detail without imposing a global hints-only policy. All three hint responses committed, but all three semantic reviews ended with `finishReasons: ["length"]`; the single full-derivation response committed and received `meets_rubric`. The hint run remains failed, and this small uncontrolled comparison does not establish a quality improvement rate. Both runs used PGlite. The implementation hashes distinguish the prompt versions.

Additional runs are preserved in `postgresql` (four real PostgreSQL samples), `hint-review-shape` and `hint-review-detail` (one targeted PGlite sample each). The PostgreSQL run committed all four responses and passed its deterministic checks, but had an unavailable review and an uncertain review. The first targeted hint review returned invalid review structure; the second produced a valid `does_not_meet` verdict because the answer supplied two hints where the user explicitly requested one. These are observed failures, not evidence of a resolved quality issue. The runner now records selected cases and available failure diagnostics; the earlier reports cannot be retroactively given missing diagnostics.

These are real calls to the configured `Qwen/Qwen3.5-4B` provider through the public LingxiOS app, its production driver and Python runner, with isolated PGlite storage. The four agent-authored cases have not been reviewed by a human. Product integration, safety coverage, grader calibration and old/minimal-harness comparisons were not evaluated.

| Observation | Initial run (two repeats) | Diagnostic run (one repeat) |
| --- | ---: | ---: |
| Samples | 8 | 4 |
| Locally committed answers | 7 | 4 |
| Exact chat answer / no execution / no artifacts | 2/2 each | 1/1 each |
| Downloaded JSON file contents match | 2/2 | 1/1 |
| Uncalibrated review: meets rubric | 3 | 3 |
| Uncalibrated review: uncertain | 0 | 1 |
| No semantic assessment | 5 | 0 |

The initial full-derivation attempt failed before committing an answer. Four other initial samples had unavailable semantic reviews; that runner did not retain their detailed failure stages, so their causes are unresolved. The diagnostic runner adds tool calls, failure details and implementation hashes. Its file-delivery review remained uncertain even though the independent download/content check passed. This does not justify changing uncertainty into success.

Both runs are preserved, including failures. The initial summary was corrected from its unchanged per-case reports to separate deterministic checks from review errors. Model usage and finish reasons are recorded per observed call; absent usage for failed calls is not evidence of zero cost. `externalDelivery: not_observed` means these runs did not exercise native product delivery. Local commitment also does not establish overall goal satisfaction: these earlier runs reported unchecked/inconclusive acceptance.

See each directory's `summary.json`, the original dataset, per-case reports and `.artifact` bytes. Reproduce with `npm run eval:live -- --output NEW_DIRECTORY --repeat 2` after configuring the model. These results are development evidence, not a release gate pass or a representative quality estimate.
