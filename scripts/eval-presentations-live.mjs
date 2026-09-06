import { createHash } from 'node:crypto'
import { mkdir, readFile, writeFile } from 'node:fs/promises'
import { resolve, join } from 'node:path'
import { OpenAIChatDriver, DEFAULT_MODEL } from '../dist/src/model/openai.js'
import { reviewAnswer } from '../dist/src/eval/review.js'

const args = process.argv.slice(2)
if (args.length !== 2 || args[0] !== '--output') throw new Error('usage: node scripts/eval-presentations-live.mjs --output NEW_DIRECTORY')
const model = {
  id: process.env.AGENT_OS_MODEL ?? process.env.OPENAI_MODEL ?? DEFAULT_MODEL.id,
  apiKey: process.env.AGENT_OS_MODEL_API_KEY ?? process.env.OPENAI_API_KEY,
  baseUrl: process.env.AGENT_OS_MODEL_BASE_URL ?? process.env.OPENAI_BASE_URL ?? DEFAULT_MODEL.baseUrl,
  reasoningEffort: process.env.AGENT_OS_REASONING_EFFORT ?? DEFAULT_MODEL.reasoningEffort,
  maxOutputTokens: 4096,
  contextWindowTokens: 32768,
}
if (!model.id?.trim() || !model.apiKey?.trim()) throw new Error('configure model id and API key before running live evaluation')

const datasetBytes = await readFile(new URL('../eval/presentation-cases.json', import.meta.url))
const dataset = JSON.parse(datasetBytes)
if (dataset.version !== 1 || !Array.isArray(dataset.cases) || !dataset.cases.length || dataset.cases.some(sample =>
  !/^[a-z0-9-]{1,80}$/.test(sample.id) || !sample.originalInput?.trim() || !sample.expectedMethod?.trim()
  || !Array.isArray(sample.requiredCode) || !sample.rubric?.trim())) throw new Error('invalid presentation evaluation dataset')

const output = resolve(args[1])
await mkdir(output)
await writeFile(join(output, 'dataset.json'), datasetBytes, { flag: 'wx' })
const driver = new OpenAIChatDriver(model.id, model)
const instructions = `You are an assistant with a PPT generation capability. Use the Python tool for requested presentation work.
Available methods:
- host.presentations.create(requirements=..., title=..., sourceIds=[...]?, targetSlideCount=3..40?, language=...?)
- host.presentations.revise_outline(presentationId=..., expectedRevision=..., feedback=...?, targetSlideCount=3..40?)
- host.presentations.approve_outline(presentationId=..., expectedRevision=...)
Creation and revision are asynchronous. Report only observed status and never claim a finished deck before verification. Approve an outline only when the human explicitly requests approval.`
const reports = []
for (const sample of dataset.cases) {
  const started = Date.now()
  const report = { caseId: sample.id, checks: [], failures: [] }
  try {
    const items = [{ role: 'user', content: sample.originalInput }]
    const outputs = []
    let code = ''
    let usage = { available: true, inputTokens: 0, outputTokens: 0 }
    for (let turn = 0; turn < 3 && !code.includes(`host.presentations.${sample.expectedMethod}(`); turn++) {
      const result = await driver.run({ instructions, items, signal: AbortSignal.timeout(120_000) })
      report.model = result.model ?? model.id
      report.diagnostics = result.diagnostics
      outputs.push(...result.output)
      if (!result.usage.available) usage.available = false
      usage.inputTokens += result.usage.inputTokens
      usage.outputTokens += result.usage.outputTokens
      items.push(...result.output)
      const call = result.output.find(item => item.type === 'function_call')
      if (!call) break
      try { code = JSON.parse(call.arguments).code ?? '' } catch { report.failures.push('Tool arguments were not valid JSON'); break }
      items.push({ type: 'function_call_output', callId: call.callId,
        output: code.includes('dir(host.presentations)') ? "['approve_outline', 'create', 'revise_outline']" : '{"status":"planning","presentationId":"evaluation-deck"}' })
    }
    report.usage = usage
    report.output = outputs
    report.checks.push({ id: 'bounded_tool_loop', status: outputs.filter(item => item.type === 'function_call').length <= 3 ? 'pass' : 'fail' })
    report.code = code
    const normalizedCode = code.toLowerCase()
    report.checks.push({ id: 'expected_method', status: normalizedCode.includes(`host.presentations.${sample.expectedMethod.toLowerCase()}(`) ? 'pass' : 'fail' })
    report.checks.push({ id: 'required_arguments', status: sample.requiredCode.every(value => normalizedCode.includes(value.toLowerCase())) ? 'pass' : 'fail' })
    report.checks.push({ id: 'forbidden_actions', status: (sample.forbiddenCode ?? []).every(value => !normalizedCode.includes(value.toLowerCase())) ? 'pass' : 'fail' })
    report.review = await reviewAnswer(model, { originalInput: sample.originalInput, revisions: [], answer: code || result.text, rubric: sample.rubric }, AbortSignal.timeout(120_000))
  } catch (error) {
    report.failures.push(String(error instanceof Error ? error.message : error).replaceAll(model.apiKey, '[redacted]').slice(0, 2000))
  }
  report.durationMs = Date.now() - started
  reports.push(report)
  await writeFile(join(output, `${sample.id}.json`), JSON.stringify(report, null, 2).replaceAll(model.apiKey, '[redacted]'), { flag: 'wx' })
  console.log(JSON.stringify({ caseId: sample.id, checks: report.checks, review: report.review?.verdict ?? 'not_observed', failures: report.failures }))
}
const summary = {
  version: 1,
  mode: 'live_model_presentation_planning',
  model: model.id,
  providerOrigin: new URL(model.baseUrl).origin,
  datasetSha256: createHash('sha256').update(datasetBytes).digest('hex'),
  datasetReview: dataset.reviewStatus,
  sampleCount: reports.length,
  deterministicFailures: reports.filter(report => report.checks.some(check => check.status !== 'pass')).length,
  executionOrReviewErrors: reports.filter(report => report.failures.length).length,
  semanticPasses: reports.filter(report => report.review?.verdict === 'meets_rubric').length,
  reports: reports.map(report => `${report.caseId}.json`),
}
await writeFile(join(output, 'summary.json'), JSON.stringify(summary, null, 2), { flag: 'wx' })
process.exitCode = summary.deterministicFailures || summary.executionOrReviewErrors || summary.semanticPasses !== summary.sampleCount ? 1 : 0
