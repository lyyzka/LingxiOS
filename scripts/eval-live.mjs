import { mkdtemp, mkdir, readFile, readdir, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { resolve, join, dirname } from 'node:path'
import { createHash, randomUUID } from 'node:crypto'
import { isDeepStrictEqual } from 'node:util'
import { Pool } from 'pg'
import { PGlite } from '@electric-sql/pglite'
import { createLingxiOS, packageResources, releaseVersions, DEFAULT_MODEL } from 'lingxios'
import { createWorker } from 'lingxios/worker'
import { executeRequest, reviewAnswer } from 'lingxios/eval'
import { intEnv } from '../dist/src/config.js'

const args = process.argv.slice(2)
if (![4, 6].includes(args.length) || args[0] !== '--output' || args[2] !== '--repeat' || !/^(?:[1-9]|10)$/.test(args[3])
  || (args.length === 6 && args[4] !== '--case')) {
  throw new Error('usage: node scripts/eval-live.mjs --output NEW_DIRECTORY --repeat 1..10 [--case CASE_ID]')
}
const model = { id: process.env.AGENT_OS_MODEL ?? process.env.OPENAI_MODEL ?? DEFAULT_MODEL.id,
  apiKey: process.env.AGENT_OS_MODEL_API_KEY ?? process.env.OPENAI_API_KEY,
  baseUrl: process.env.AGENT_OS_MODEL_BASE_URL ?? process.env.OPENAI_BASE_URL ?? DEFAULT_MODEL.baseUrl,
  reasoningEffort: process.env.AGENT_OS_REASONING_EFFORT ?? DEFAULT_MODEL.reasoningEffort,
  maxOutputTokens: 4096, contextWindowTokens: 32768 }
if (!model.id?.trim() || !model.apiKey?.trim()) throw new Error('configure model id and API key before running live evaluation')
const reviewModel = { ...model, maxOutputTokens: intEnv('LINGXIOS_EVAL_REVIEW_MAX_OUTPUT_TOKENS', 4096, { min: 128, max: 16384 }) }
const reviewTimeoutMs = intEnv('LINGXIOS_EVAL_REVIEW_TIMEOUT_MS', 60_000, { min: 10_000, max: 300_000 })
const output = resolve(args[1])
const datasetBytes = await readFile(new URL('../eval/runtime-cases.json', import.meta.url))
const dataset = JSON.parse(datasetBytes.toString('utf8'))
if (dataset.version !== 1 || !Array.isArray(dataset.cases) || !dataset.cases.length || dataset.cases.length > 100
  || dataset.cases.some(sample => !sample || typeof sample.id !== 'string' || !/^[a-z0-9-]{1,80}$/.test(sample.id)
    || typeof sample.originalInput !== 'string' || !sample.originalInput.trim()
    || typeof sample.rubric !== 'string' || !sample.rubric.trim())
  || new Set(dataset.cases.map(sample => sample.id)).size !== dataset.cases.length) throw new Error('invalid evaluation dataset')
const cases = args.length === 6 ? dataset.cases.filter(sample => sample.id === args[5]) : dataset.cases
if (!cases.length) throw new Error('unknown evaluation case')
await mkdir(output) // A fresh directory prevents overwriting an earlier run.
await writeFile(join(output, 'dataset.json'), datasetBytes, { flag: 'wx' })
const hash = value => createHash('sha256').update(value).digest('hex')
const runnerSha256 = hash(await readFile(new URL(import.meta.url)))
const runtimeDirectory = new URL('../dist/src/', import.meta.url)
const implementation = createHash('sha256')
for (const file of (await readdir(runtimeDirectory, { recursive: true })).filter(file => file.endsWith('.js')).sort()) {
  implementation.update(file.replaceAll('\\', '/')).update('\0').update(await readFile(new URL(file.replaceAll('\\', '/'), runtimeDirectory)))
}
for (const [name, path] of Object.entries(packageResources())) implementation.update(name).update('\0').update(await readFile(path))
const implementationSha256 = implementation.digest('hex')
const dependencyLockSha256 = hash(await readFile(new URL('../package-lock.json', import.meta.url)))
const redactError = error => String(error instanceof Error ? error.message : 'Unknown failure').replaceAll(model.apiKey, '[redacted]').slice(0, 2000)
const reports = []
const connectionString = process.env.LINGXIOS_TEST_DATABASE_URL
let databaseVersion
let databaseInitialized = false
const temporaryRoot = resolve(tmpdir())
for (let repeat = 1; repeat <= Number(args[3]); repeat++) {
  for (const sample of cases) {
    const directory = await mkdtemp(join(temporaryRoot, 'lingxios-live-'))
    const pool = connectionString ? new Pool({ connectionString, max: 4, connectionTimeoutMillis: 5000 }) : undefined
    const db = pool ? { query: pool.query.bind(pool), exec: pool.query.bind(pool), close: () => pool.end() }
      : new PGlite(join(directory, 'db'))
    const database = pool ?? { query: async (sql, params) => {
      const result = await db.query(sql, params)
      return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length }
    }, connect: async () => ({ query: database.query, release() {} }) }
    let app
    let worker
    let timeout
    let cancellation
    let timedOut = false
    const identity = { runId: randomUUID(), tenantId: 'evaluation-' + randomUUID(), agentId: 'assistant', sessionId: randomUUID(), principalId: 'evaluator' }
    const report = { caseId: sample.id, repeat, originalInputSha256: hash(sample.originalInput), checks: [], failures: [] }
    const started = Date.now()
    try {
      if (pool && !databaseInitialized) {
        const tables = await pool.query("SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname <> 'information_schema' AND c.relkind IN ('r','p','v','m','f') LIMIT 1")
        if (tables.rows.length) throw new Error('LINGXIOS_TEST_DATABASE_URL must name an empty disposable PostgreSQL database')
        databaseVersion = (await pool.query('SELECT version() AS version')).rows[0].version
      }
      if (!pool || !databaseInitialized) await db.exec(await readFile(packageResources().schema, 'utf8'))
      databaseInitialized = true
      app = await createLingxiOS({ database, homesRoot: join(directory, 'homes') })
      worker = createWorker({ controlPlane: app, model, smallModel: model, kernel: { homesRoot: join(directory, 'homes'), allowNetwork: false } })
      timeout = setTimeout(() => {
        timedOut = true
        cancellation = app.cancel(identity).catch(() => { report.failures.push('Timeout cancellation failed') })
      }, 120_000)
      const result = await executeRequest(app, worker, { id: identity.runId, ...identity, text: sample.originalInput })
      report.body = result.message?.body ?? null
      report.outcome = result.outcome
      report.assessment = result.message?.envelope.assessment ?? null
      report.delivery = result.delivery
      report.externalDelivery = result.externalDelivery
      report.usage = await app.readUsage(identity)
      report.executionDurationMs = Date.now() - started
      report.checks.push({ id: 'committed_response', status: result.message ? 'pass' : 'not_observed' })
      if (sample.expectedOutcome) report.checks.push({ id: 'goal_status', status: result.outcome
        ? result.outcome.status === sample.expectedOutcome ? 'pass' : 'fail' : 'not_observed' })
      if (sample.expectedBody !== undefined) report.checks.push({ id: 'answer_value', status: result.message?.body.trim() === sample.expectedBody ? 'pass' : result.message ? 'fail' : 'not_observed' })
      const events = (await db.query('SELECT kind,data FROM lingxios.agent_run_events WHERE run_id=$1 ORDER BY seq', [identity.runId])).rows
      report.modelCalls = events.filter(event => event.kind === 'model.completed').map(event => ({ model: event.data.model, usage: event.data.usage, diagnostics: event.data.diagnostics }))
      report.runtimeFailures = events.filter(event => event.kind.endsWith('.failed') || event.kind === 'response.withheld')
        .map(event => ({ kind: event.kind, error: redactError(new Error(event.data.error ?? event.data.violation ?? 'Failure recorded')) }))
      const saved = (await db.query('SELECT history FROM lingxios.agent_os_sessions WHERE tenant_id=$1 AND agent_id=$2 AND session_id=$3', [identity.tenantId, identity.agentId, identity.sessionId])).rows[0]
      report.toolCalls = (saved?.history ?? []).filter(item => item.type === 'function_call')
      if (sample.forbidExecution) report.checks.push({ id: 'no_execution', status: events.some(event => event.kind.startsWith('ipython.')) ? 'fail' : result.message ? 'pass' : 'not_observed' })
      if (sample.forbidArtifacts) report.checks.push({ id: 'no_artifacts', status: result.message ? result.message.envelope.artifacts.length ? 'fail' : 'pass' : 'not_observed' })
      if (sample.artifact) {
        const artifact = await app.readArtifact(identity, sample.artifact.path)
        let status = 'not_observed'
        let observedJson
        if (artifact) {
          report.artifact = artifact.artifact
          report.artifactFile = `${sample.id}-${repeat}.artifact`
          await writeFile(join(output, report.artifactFile), artifact.bytes, { flag: 'wx' })
          try {
            const parsed = JSON.parse(artifact.bytes.toString('utf8'))
            status = isDeepStrictEqual(parsed, sample.artifact.expectedJson) ? 'pass' : 'fail'
            if (artifact.bytes.length <= 16_384) observedJson = parsed
          }
          catch { status = 'fail' }
        }
        report.checks.push({ id: 'artifact_json', status })
        report.resourceObservations = [{ resource: `artifact:${sample.artifact.path}`, requestVersion: result.message?.envelope.requestVersion ?? 1,
          value: artifact ? { status: 'observed', source: 'app.readArtifact', artifact: artifact.artifact,
            ...(observedJson === undefined ? { jsonContent: 'not_observed' } : { json: observedJson }), externalDelivery: result.externalDelivery } : { status: 'not_observed' } }]
      }
      clearTimeout(timeout)
      if (report.body) {
        try { report.review = await reviewAnswer(reviewModel, { originalInput: sample.originalInput, revisions: [], answer: report.body, rubric: sample.rubric,
          ...(report.resourceObservations ? { observations: report.resourceObservations } : {}) }, AbortSignal.timeout(reviewTimeoutMs)) }
        catch (error) {
          report.failures.push(`Semantic review unavailable: ${redactError(error)}`)
          if (Array.isArray(error?.diagnostics?.finishReasons)) {
            report.reviewFailureDiagnostics = {
              finishReasons: error.diagnostics.finishReasons.slice(0, 16).map(reason => redactError(new Error(String(reason)))),
              ...(Number.isSafeInteger(error.diagnostics.status) ? { status: error.diagnostics.status } : {}),
              ...(Number.isSafeInteger(error.diagnostics.attempts) ? { attempts: error.diagnostics.attempts } : {}),
            }
          }
        }
      }
    } catch (error) {
      report.failures.push(`Runtime execution or observation failed: ${redactError(error)}`)
    } finally {
      clearTimeout(timeout)
      await cancellation
      report.timedOut = timedOut
      report.durationMs = Date.now() - started
      await worker?.stop()
      await app?.stop()
      await db.close()
      if (dirname(resolve(directory)) !== temporaryRoot) throw new Error('evaluation cleanup escaped its temporary root')
      try { await rm(directory, { recursive: true, force: true, maxRetries: 10, retryDelay: 200 }) }
      catch (error) { report.failures.push(`Temporary cleanup failed: ${redactError(error)}`) }
    }
    reports.push(report)
    await writeFile(join(output, `${sample.id}-${repeat}.json`), JSON.stringify(report, null, 2).replaceAll(model.apiKey, '[redacted]'), { flag: 'wx' })
    console.log(JSON.stringify({ caseId: sample.id, repeat, checks: report.checks, review: report.review?.verdict ?? 'not_observed', failures: report.failures }))
  }
}
await writeFile(join(output, 'summary.json'), JSON.stringify({ version: 1, mode: 'live_model_runtime',
  runtimeVersions: releaseVersions, runnerSha256, implementationSha256, dependencyLockSha256,
  databaseEngine: connectionString ? 'postgresql' : 'pglite', databaseVersion,
  model: model.id, reasoningEffort: model.reasoningEffort, providerOrigin: new URL(model.baseUrl).origin,
  budgets: { runtimeOutputTokens: model.maxOutputTokens, reviewOutputTokens: reviewModel.maxOutputTokens, reviewTimeoutMs, contextWindowTokens: model.contextWindowTokens },
  datasetSha256: hash(datasetBytes), datasetReview: dataset.reviewStatus,
  selectedCases: cases.map(sample => sample.id),
  productIntegration: 'not_evaluated', safetyCoverage: 'not_assessed', semanticCalibration: 'not_calibrated',
  sampleCount: reports.length, deterministicFailures: reports.filter(report => report.checks.some(check => check.status !== 'pass')).length,
  executionOrReviewErrors: reports.filter(report => report.failures.length).length, timedOut: reports.filter(report => report.timedOut).length,
  reports: reports.map(report => `${report.caseId}-${report.repeat}.json`) }, null, 2), { flag: 'wx' })
process.exitCode = reports.some(report => report.failures.length || report.timedOut || report.checks.some(check => check.status !== 'pass') || report.review?.verdict !== 'meets_rubric') ? 1 : 0
