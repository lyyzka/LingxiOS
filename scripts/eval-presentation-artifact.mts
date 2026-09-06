import { createHash, randomUUID } from 'node:crypto'
import { mkdir, readFile, writeFile } from 'node:fs/promises'
import { join, resolve } from 'node:path'
import { pathToFileURL } from 'node:url'

const args = process.argv.slice(2)
if (args.length !== 2 || args[0] !== '--output') throw new Error('usage: tsx scripts/eval-presentation-artifact.mts --output NEW_DIRECTORY')
const output = resolve(args[1])
await mkdir(output)

const sourceRoot = resolve(process.env.LINGXILOOP_SOURCE ?? '../LingxiLoop/server/src')
const generation = await import(pathToFileURL(join(sourceRoot, 'modules/presentations/generation.ts')).href)
const renderer = await import(pathToFileURL(join(sourceRoot, 'modules/presentations/renderer.ts')).href)
const validator = await import(pathToFileURL(join(sourceRoot, 'modules/presentations/static-validator.ts')).href)
const modelId = process.env.AGENT_OS_MODEL ?? process.env.OPENAI_MODEL
const apiKey = process.env.AGENT_OS_MODEL_API_KEY ?? process.env.OPENAI_API_KEY
const baseUrl = (process.env.AGENT_OS_MODEL_BASE_URL ?? process.env.OPENAI_BASE_URL ?? 'https://api.openai.com/v1').replace(/\/+$/, '')
const timeoutMs = Number(process.env.LINGXIOS_PRESENTATION_EVAL_TIMEOUT_MS ?? 180_000)
if (!modelId?.trim() || !apiKey?.trim()) throw new Error('configure model id and API key before running artifact evaluation')
if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1_000 || timeoutMs > 600_000) throw new Error('invalid presentation evaluation timeout')

const claims = [
  '本评测使用合成资料，不代表真实公司经营数据。', '季度目标是验证关键工作流并降低用户完成任务的步骤数。',
  '核心用户包括首次使用者和每周重复使用者。', '研究样本显示首次设置流程是主要流失点。',
  '团队将激活定义为用户完成首次有效任务。', '合成基线中的激活率为 42%。',
  '合成实验将引导步骤从 7 步减少到 4 步。', '合成实验组激活率为 49%。',
  '每周活跃用户在合成数据中从 8.2 万增至 9.6 万。', '次周留存在合成数据中从 31% 提升至 35%。',
  '移动端贡献了合成活跃用户的 58%。', '桌面端用户更常使用批量处理功能。',
  '搜索延迟的合成中位数从 820 毫秒降至 510 毫秒。', '关键流程的合成成功率从 96.8% 升至 98.1%。',
  '客服反馈中最常见的问题是权限提示不够清楚。', '新版权限说明在合成可用性测试中减少了误操作。',
  '本季度发布了简化设置、批量处理和权限说明三项改进。', '批量处理功能的合成周使用率为 18%。',
  '团队发现跨端状态同步仍存在可见延迟。', '同步延迟和历史数据迁移是下一季度主要风险。',
  '下一季度优先修复同步一致性，再扩大批量处理覆盖。', '风险缓解方案包括灰度发布和迁移前校验。',
  '所有合成指标只用于验证演示生成能力。', '管理层审批后才能把此结构应用于真实业务资料。',
]
const material = {
  schemaVersion: 'presentation_material_v1' as const,
  sourceId: 'synthetic-q3-product-review', title: 'Q3 产品复盘合成资料', truncated: false, assets: [],
  blocks: claims.map((text, ordinal) => ({ chunkId: `chunk-${ordinal + 1}`, ordinal, text,
    pageNumber: ordinal + 1, sectionTitle: ordinal < 5 ? '目标与用户' : ordinal < 10 ? '增长' : ordinal < 15 ? '体验与质量' : ordinal < 20 ? '发布与问题' : '计划与风险' })),
}
const request = { title: '2026 Q3 产品复盘', requirements: '面向中文管理层的 24 页季度产品复盘。只能使用合成资料，所有指标必须明确标注为合成数据。', language: 'zh-CN', targetPageCount: 24 }
const modelCalls: Array<{ purpose: string; pageId?: string; inputTokens?: number; outputTokens?: number }> = []
const model = { async complete(input: { purpose: string; pageId?: string; system: string; user: string }) {
  const response = await fetch(`${baseUrl}/chat/completions`, { method: 'POST', headers: { 'content-type': 'application/json', authorization: `Bearer ${apiKey}` },
    body: JSON.stringify({ model: modelId, max_tokens: 16384, reasoning_effort: 'high', enable_thinking: true,
      response_format: { type: 'json_object' }, messages: [{ role: 'system', content: input.system }, { role: 'user', content: input.user }] }),
    signal: AbortSignal.timeout(timeoutMs) })
  if (!response.ok) throw new Error(`model provider returned ${response.status}: ${(await response.text()).slice(0, 500)}`)
  const payload = await response.json() as { usage?: { prompt_tokens?: number; completion_tokens?: number }; choices?: Array<{ message?: { content?: string } }> }
  modelCalls.push({ purpose: input.purpose, ...(input.pageId ? { pageId: input.pageId } : {}), inputTokens: payload.usage?.prompt_tokens, outputTokens: payload.usage?.completion_tokens })
  const text = payload.choices?.[0]?.message?.content?.trim() ?? ''
  const fenced = /^```(?:json)?\s*([\s\S]*?)\s*```$/i.exec(text)
  return JSON.parse(fenced?.[1] ?? text)
} }

const evidence = generation.buildEvidenceLedger([material], { title: request.title, requirements: request.requirements })
const sources = [{ sourceId: material.sourceId, title: material.title, visibilityScope: 'PROJECT' as const, status: 'ready' as const }]
const identity = { companyId: 'eval-company', conversationId: 'eval-conversation', presentationId: `eval-${randomUUID()}`, jobId: `job-${randomUUID()}` }
let plan
try {
  plan = await generation.generateDeckPlan({ model, ...identity, ...request, sources, evidence, assets: [] })
} catch (error) {
  const report = { version: 1, mode: 'live_model_presentation_artifact', model: modelId, providerOrigin: new URL(baseUrl).origin,
    presentationId: identity.presentationId, artifact: null, pageCount: 0, status: 'generation_failed',
    failure: String(error instanceof Error ? error.message : error).replaceAll(apiKey, '[redacted]').slice(0, 2000), modelCalls }
  await writeFile(join(output, 'summary.json'), JSON.stringify(report, null, 2), { flag: 'wx' })
  console.error(JSON.stringify({ status: report.status, failure: report.failure }))
  process.exit(1)
}
await writeFile(join(output, 'outline.json'), JSON.stringify(plan, null, 2), { flag: 'wx' })
const pages = plan.sections.flatMap((section: { pages: unknown[] }) => section.pages)
const specs = []
for (let index = 0; index < pages.length; index++) {
  const page = pages[index]
  const section = plan.sections.find((candidate: { pages: Array<{ id: string }> }) => candidate.pages.some(item => item.id === page.id))
  specs.push(await generation.generateSlide({ model, ...identity, page, section, previousPage: pages[index - 1] ?? null,
    nextPage: pages[index + 1] ?? null, previousPageSummary: specs.at(-1)?.conclusion ?? null, evidence, assets: [] }))
}
const issues = await generation.runDeckCritic({ model, ...identity, plan, specs, evidence, assets: [] })
if (issues.length) throw new Error(`presentation critic rejected artifact: ${JSON.stringify(issues).slice(0, 2000)}`)
const compiled = renderer.compileLectureDeck({ title: request.title, specs, evidence, assets: [] })
const validation = validator.validatePresentationHtml(compiled.html)
if (!validation.passed) throw new Error(`static validation failed: ${JSON.stringify(validation.issues).slice(0, 2000)}`)
const artifact = join(output, '2026-Q3-product-review.html')
await writeFile(artifact, compiled.html, { flag: 'wx' })
const report = { version: 1, mode: 'live_model_presentation_artifact', model: modelId, providerOrigin: new URL(baseUrl).origin,
  presentationId: identity.presentationId, artifact: '2026-Q3-product-review.html', artifactSha256: createHash('sha256').update(compiled.html).digest('hex'),
  pageCount: specs.length, outline: 'outline.json', status: 'awaiting_human_artifact_approval', validation, modelCalls }
await writeFile(join(output, 'summary.json'), JSON.stringify(report, null, 2), { flag: 'wx' })
console.log(JSON.stringify({ artifact, pageCount: specs.length, status: report.status }))
