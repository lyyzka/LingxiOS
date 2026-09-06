import { createHash } from 'node:crypto'
import { appendFile, mkdir, writeFile } from 'node:fs/promises'
import { join, resolve } from 'node:path'
import { DEFAULT_MODEL, OpenAIChatDriver } from '../dist/src/model/openai.js'
import { LectureDeckService, MemoryLectureRepository, ModelLectureAuthor, ModelLectureReviewer } from '../dist/src/lecture-deck/index.js'

const args = process.argv.slice(2)
if (args.length !== 2 || args[0] !== '--output') throw new Error('usage: npm run eval:presentation-artifact -- --output NEW_DIRECTORY')
const output = resolve(args[1])
await mkdir(output)

const modelId = process.env.AGENT_OS_MODEL ?? process.env.OPENAI_MODEL ?? DEFAULT_MODEL.id
const apiKey = process.env.AGENT_OS_MODEL_API_KEY ?? process.env.OPENAI_API_KEY
const baseUrl = process.env.AGENT_OS_MODEL_BASE_URL ?? process.env.OPENAI_BASE_URL ?? DEFAULT_MODEL.baseUrl
if (!apiKey?.trim()) throw new Error('configure AGENT_OS_MODEL_API_KEY or OPENAI_API_KEY')

const calls = []
const driver = new OpenAIChatDriver(modelId, { apiKey, baseUrl, reasoningEffort: 'high', maxOutputTokens: 16_384, requestTimeoutMs: 90_000 })
const model = { ...driver, structured: async request => {
  const started = Date.now(), result = await driver.structured(request)
  calls.push({ model: result.model, durationMs: Date.now() - started, usage: result.usage })
  return result
} }
const claims = [
  '本评测使用合成资料，不代表真实公司经营数据。', '季度目标是验证关键工作流并降低用户完成任务的步骤数。',
  '研究样本显示首次设置流程是主要流失点。', '团队将激活定义为用户完成首次有效任务。',
  '合成基线中的激活率为 42%，实验组为 49%。', '引导步骤从 7 步减少到 4 步。',
  '合成周活跃用户从 8.2 万增至 9.6 万，次周留存从 31% 提升至 35%。',
  '移动端贡献合成活跃用户的 58%，桌面端用户更常使用批量处理。',
  '搜索延迟合成中位数从 820ms 降至 510ms，关键流程成功率从 96.8% 升至 98.1%。',
  '客服反馈中最常见的问题是权限提示不够清楚。', '本季度发布简化设置、批量处理和权限说明三项改进。',
  '跨端状态同步仍存在可见延迟；同步一致性和历史数据迁移是下一季度主要风险。',
  '下一季度优先修复同步一致性，再扩大批量处理覆盖。', '风险缓解方案包括灰度发布和迁移前校验。',
]
const evidenceItems = claims.map((excerpt, index) => ({ marker: `S${index + 1}`, sourceId: 'synthetic-q3', sourceVersion: '1', chunkId: `chunk-${index + 1}`, title: 'Q3 产品复盘合成资料', excerpt }))
const artifactPath = join(output, 'lecture.html')
const started = Date.now()
const progress = async event => {
  const observed = { at: new Date().toISOString(), elapsedMs: Date.now() - started, ...event }
  await appendFile(join(output, 'progress.ndjson'), `${JSON.stringify(observed)}\n`)
  console.log(JSON.stringify(observed))
}
let record
try {
  const service = new LectureDeckService({
    repository: new MemoryLectureRepository(), author: new ModelLectureAuthor(model), reviewer: new ModelLectureReviewer(model),
    evidence: { search: async () => evidenceItems }, publisher: { publish: async (_record, artifact) => writeFile(artifactPath, artifact.bytes, { flag: 'wx' }) },
    modelStageTimeoutMs: 90_000,
  })
  record = await service.create({ tenantId: 'eval', principalId: 'visual-reviewer' }, {
    title: '2026 Q3 产品复盘', requirements: '为中文管理层制作一份 12 页教学型季度产品复盘。只能使用给定合成资料；所有指标明确标注为合成数据。强调结论、因果链、风险与下一步。',
    audience: '中文管理层', language: 'zh-CN', targetSlideCount: 12, durationMinutes: 25, sourceIds: ['synthetic-q3'],
  }, AbortSignal.timeout(30 * 60_000), progress)
  if (record.status !== 'ready' || !record.manifest) throw new Error(record.error ?? `generation ended with ${record.status}`)
  await writeFile(join(output, 'manifest.json'), JSON.stringify(record.manifest, null, 2), { flag: 'wx' })
} catch (error) {
  record = { status: 'failed', error: error instanceof Error ? error.message : String(error) }
}

const summary = { version: 1, mode: 'lingxios_native_lecture_live_model', model: modelId, providerOrigin: new URL(baseUrl).origin,
  status: record.status, artifact: record.status === 'ready' ? 'lecture.html' : null, artifactSha256: record.artifact?.sha256 ?? null,
  pageCount: record.manifest?.slides.length ?? 0, validation: record.report ?? null, modelCalls: calls.length,
  usage: calls.reduce((sum, call) => ({ inputTokens: sum.inputTokens + call.usage.inputTokens, outputTokens: sum.outputTokens + call.usage.outputTokens }), { inputTokens: 0, outputTokens: 0 }),
  error: record.error ?? null }
await writeFile(join(output, 'summary.json'), JSON.stringify(summary, null, 2), { flag: 'wx' })
console.log(JSON.stringify({ output, ...summary }))
if (record.status !== 'ready') process.exitCode = 1
