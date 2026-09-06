import assert from 'node:assert/strict'
import test from 'node:test'
import { LectureDeckService, MemoryLectureRepository, ModelLectureAuthor, buildStandaloneLecture, validateDeck, validateLectureMarkup, type CoursePlan, type LectureAuthor, type SlideSpec } from '../src/lecture-deck/index.js'

const slide = (input: Parameters<LectureAuthor['slide']>[0]): SlideSpec => ({
  id: input.pageId, order: input.order, chapterId: input.chapter.id,
  role: input.order === 0 ? 'cover' : input.order === input.course.targetSlideCount - 1 ? 'summary' : 'content',
  purpose: input.order === input.course.targetSlideCount - 1 ? 'review' : 'explain',
  title: input.previous ? `${input.previous.title} revised` : `Slide ${input.order + 1}`,
  conclusion: input.instruction ?? `Conclusion ${input.order + 1}`, visualKind: 'diagram',
  bodyHtml: `<svg viewBox="0 0 1280 720" role="img" aria-label="diagram"><rect data-anchor-id="main" x="100" y="100" width="300" height="180" fill="#53d6c7"/></svg>`,
  anchors: [{ id: 'main', x: 100, y: 100, width: 300, height: 180 }],
  bindings: [{ claimId: 'claim', snapshotId: input.evidence[0]!.id, evidenceMarkers: ['S1'], kind: 'source', statement: 'Supported statement' }],
  steps: [{ id: 'step', title: 'Explain', explanation: 'Detailed explanation', anchorIds: ['main'], claimIds: ['claim'] }],
})

test('native lecture pipeline publishes offline HTML and revises only selected pages', async () => {
  const repository = new MemoryLectureRepository(), artifacts: Uint8Array[] = []
  const author: LectureAuthor = {
    async plan(request) {
      const ids = Array.from({ length: request.targetSlideCount }, (_, index) => `pg_page_${index + 1}`)
      return { title: request.title ?? 'Course', audience: request.audience ?? 'Learners', prerequisites: [], objectives: [{ id: 'o1', description: 'Understand the topic' }],
        chapters: [{ id: 'chapter-1', order: 0, title: 'Foundations', objectiveIds: ['o1'], slideIds: ids }], targetSlideCount: request.targetSlideCount,
        durationMinutes: request.durationMinutes, terminology: {} } satisfies CoursePlan
    },
    async slide(input) { return slide(input) },
  }
  const service = new LectureDeckService({ repository, author,
    evidence: { search: async () => [{ marker: 'S1', sourceId: 'source', sourceVersion: 'v1', chunkId: 'chunk', title: 'Source', excerpt: 'Evidence text.' }] },
    reviewer: { review: async () => ({ passed: true, issues: [] }) },
    publisher: { publish: async (_record, artifact) => { artifacts.push(artifact.bytes) } }, now: () => '2026-01-01T00:00:00.000Z' })
  const scope = { tenantId: 'tenant', principalId: 'teacher' }
  const created = await service.create(scope, { requirements: 'Teach the topic', targetSlideCount: 3, sourceIds: ['source'] })
  assert.equal(created.status, 'ready')
  assert.equal(created.manifest?.slides.length, 3)
  assert.equal(artifacts.length, 1)
  assert.match(new TextDecoder().decode(artifacts[0]), /application\/json/)
  assert.doesNotMatch(new TextDecoder().decode(artifacts[0]), /<script[^>]+src=/)
  const html = new TextDecoder().decode(artifacts[0])
  for (const feature of [/id="pages"/, /async function goTo/, /\.animate\(/, /classList\.add\('is-focus'\)/]) assert.match(html, feature)

  const originalBodies = created.manifest!.slides.map(item => item.bodyHtml)
  const revised = await service.revise(scope, created.id, { instruction: 'Turn this into a process', scope: 'page', pageIds: ['pg_page_2'], expectedRevision: 1 })
  assert.equal(revised.status, 'ready')
  assert.equal(revised.revision, 2)
  assert.equal(revised.manifest!.slides[0]!.bodyHtml, originalBodies[0])
  assert.match(revised.manifest!.slides[1]!.title, /revised/)
  assert.equal(revised.manifest!.slides[2]!.bodyHtml, originalBodies[2])
  await assert.rejects(service.revise(scope, created.id, { instruction: 'stale', scope: 'deck', expectedRevision: 1 }), /revision changed/)
})

test('lecture contract validates every generated page in 30, 60 and 100 page decks', () => {
  for (const count of [30, 60, 100]) {
    const chapter = { id: 'chapter', order: 0, title: 'Long course', objectiveIds: ['o'], slideIds: Array.from({ length: count }, (_, index) => `pg_${index + 1}`) }
    const evidence = [{ version: 1 as const, id: 'snapshot', items: [{ marker: 'S1', sourceId: 's', sourceVersion: 'v', chunkId: 'c', title: 'Source', excerpt: 'Text' }] }]
    const course = { title: 'Long', audience: 'Learners', prerequisites: [], objectives: [{ id: 'o', description: 'Learn' }], chapters: [chapter], targetSlideCount: count, durationMinutes: 150, terminology: {} }
    const slides = chapter.slideIds.map((id, order) => slide({ request: { requirements: 'Long', targetSlideCount: count, durationMinutes: 150 }, course, chapter, order, pageId: id, evidence }))
    const manifest = { schemaVersion: 'lingxi-lecture/v1' as const, deckId: 'deck', revision: 1, title: 'Long', language: 'zh-CN',
      theme: { id: 'theme', background: '#000000', surface: '#111111', text: '#ffffff', muted: '#aaaaaa', accent: '#00ffff' },
      course, slides, evidence, createdAt: '2026-01-01T00:00:00.000Z' }
    assert.equal(validateDeck(manifest).passed, true)
    assert.ok(buildStandaloneLecture(manifest).size > 0)
    manifest.slides[count - 1] = { ...manifest.slides[count - 1]!, bodyHtml: '<script>alert(1)</script>' }
    assert.ok(validateDeck(manifest).issues.some(issue => issue.pageId === `pg_${count}` && issue.code === 'security.markup'))
  }
})

test('rejects empty teaching pages and encoded or CSS-based active content', () => {
  assert.ok(validateLectureMarkup('<a href="java&#x73;cript:alert(1)">x</a>').length)
  assert.ok(validateLectureMarkup('<svg style="background:url(https://attacker.test/x)"></svg>').length)
  assert.ok(validateLectureMarkup('<svg><image src="https://attacker.test/x"/></svg>').length)
  const bad = slide({ request: { requirements: 'x', targetSlideCount: 3, durationMinutes: 10 },
    course: { title: 'x', audience: 'x', prerequisites: [], objectives: [{ id: 'o', description: 'x' }], chapters: [{ id: 'c', order: 0, title: 'c', objectiveIds: ['o'], slideIds: ['pg_x'] }], targetSlideCount: 3, durationMinutes: 10, terminology: {} },
    chapter: { id: 'c', order: 0, title: 'c', objectiveIds: ['o'], slideIds: ['pg_x'] }, order: 0, pageId: 'pg_x', evidence: [{ version: 1, id: 'e', items: [{ marker: 'S1', sourceId: 's', sourceVersion: 'v', chunkId: 'c', title: 's', excerpt: 'e' }] }] })
  bad.role = 'content'; bad.bodyHtml = '<p>Only text</p>'; bad.anchors = []; bad.steps = []; bad.bindings = []
  const report = validateDeck({ schemaVersion: 'lingxi-lecture/v1', deckId: 'd', revision: 1, title: 'x', language: 'zh-CN', theme: { id: 'x', background: '#000000', surface: '#111111', text: '#ffffff', muted: '#aaaaaa', accent: '#00ffff' },
    course: { title: 'x', audience: 'x', prerequisites: [], objectives: [{ id: 'o', description: 'x' }], chapters: [{ id: 'c', order: 0, title: 'c', objectiveIds: ['o'], slideIds: ['pg_x'] }], targetSlideCount: 3, durationMinutes: 10, terminology: {} }, slides: [bad], evidence: [], createdAt: '2026-01-01T00:00:00.000Z' })
  for (const code of ['visual.missing', 'visual.primary', 'citation.missing', 'step.missing']) assert.ok(report.issues.some(issue => issue.code === code), code)
})

test('real model adapter repairs malformed structured output before accepting it', async () => {
  let calls = 0
  const valid = { title: 'Course', audience: 'Learners', prerequisites: [], objectives: [{ id: 'o', description: 'Learn' }], chapters: [{ id: 'c', order: 0, title: 'Chapter', objectiveIds: ['o'], slideIds: ['pg_1', 'pg_2', 'pg_3'] }], targetSlideCount: 3, durationMinutes: 10, terminology: {} }
  const model = { structured: async () => ({ value: ++calls === 1 ? { title: 'broken' } : valid, model: 'test', usage: { available: true, inputTokens: 1, outputTokens: 1 } }) } as unknown as ConstructorParameters<typeof ModelLectureAuthor>[0]
  const result = await new ModelLectureAuthor(model).plan({ requirements: 'Teach', targetSlideCount: 3, durationMinutes: 10 }, { version: 1, id: 'e', items: [] })
  assert.equal(result.chapters[0]?.slideIds.length, 3)
  assert.equal(calls, 2)
})

test('cancellation while independent review is pending prevents publication', async () => {
  const repository = new MemoryLectureRepository(); let release!: () => void, reviewed!: () => void, published = 0
  const reviewing = new Promise<void>(resolve => { release = resolve })
  const entered = new Promise<void>(resolve => { reviewed = resolve })
  const author: LectureAuthor = { async plan(request) { const ids = ['pg_1', 'pg_2', 'pg_3']; return { title: 'x', audience: 'x', prerequisites: [], objectives: [{ id: 'o', description: 'x' }], chapters: [{ id: 'c', order: 0, title: 'c', objectiveIds: ['o'], slideIds: ids }], targetSlideCount: request.targetSlideCount, durationMinutes: request.durationMinutes, terminology: {} } }, async slide(input) { return slide(input) } }
  const service = new LectureDeckService({ repository, author, evidence: { search: async () => [{ marker: 'S1', sourceId: 's', sourceVersion: 'v', chunkId: 'c', title: 's', excerpt: 'e' }] },
    reviewer: { review: async () => { reviewed(); await reviewing; return { passed: true, issues: [] } } }, publisher: { publish: async () => { published++ } } })
  const scope = { tenantId: 't', principalId: 'p' }, record = await service.begin(scope, { requirements: 'x', targetSlideCount: 3 }, 'deck_cancel')
  const running = service.run(scope, record.id)
  await entered
  assert.equal((await service.cancel(scope, record.id)).status, 'cancelled')
  release()
  assert.equal((await running).status, 'cancelled')
  assert.equal(published, 0)
})

test('retry resumes completed slide checkpoints instead of regenerating them', async () => {
  const repository = new MemoryLectureRepository(), calls = new Map<string, number>()
  const author: LectureAuthor = {
    async plan(request) { const ids = ['pg_1', 'pg_2', 'pg_3']; return { title: 'x', audience: 'x', prerequisites: [], objectives: [{ id: 'o', description: 'x' }], chapters: [{ id: 'c', order: 0, title: 'c', objectiveIds: ['o'], slideIds: ids }], targetSlideCount: request.targetSlideCount, durationMinutes: request.durationMinutes, terminology: {} } },
    async slide(input) { const count = (calls.get(input.pageId) ?? 0) + 1; calls.set(input.pageId, count); if (input.pageId === 'pg_2' && count === 1) throw new Error('simulated worker loss'); return slide(input) },
  }
  const service = new LectureDeckService({ repository, author, evidence: { search: async () => [{ marker: 'S1', sourceId: 's', sourceVersion: 'v', chunkId: 'c', title: 's', excerpt: 'e' }] }, reviewer: { review: async () => ({ passed: true, issues: [] }) }, publisher: { publish: async () => {} } })
  const scope = { tenantId: 't', principalId: 'p' }, failed = await service.create(scope, { requirements: 'x', targetSlideCount: 3 })
  assert.equal(failed.status, 'failed')
  const ready = await service.retry(scope, failed.id)
  assert.equal(ready.status, 'ready')
  assert.equal(calls.get('pg_1'), 1)
  assert.equal(calls.get('pg_2'), 2)
  assert.equal(calls.get('pg_3'), 1)
})

test('reports page progress and bounds a stalled model stage', async () => {
  const progress: string[] = []
  const service = new LectureDeckService({ repository: new MemoryLectureRepository(), modelStageTimeoutMs: 1_000,
    author: { async plan(_request, _evidence, signal) {
      await new Promise((_, reject) => signal?.addEventListener('abort', () => reject(signal.reason), { once: true }))
      throw new Error('unreachable')
    }, async slide(input) { return slide(input) } },
    evidence: { search: async () => [] }, reviewer: { review: async () => ({ passed: true, issues: [] }) }, publisher: { publish: async () => {} } })
  const result = await service.create({ tenantId: 't', principalId: 'p' }, { requirements: 'x', targetSlideCount: 3 }, undefined,
    event => { progress.push(`${event.stage}.${event.status}`) })
  assert.equal(result.status, 'failed')
  assert.match(result.error ?? '', /plan-course timed out after 1000ms/)
  assert.deepEqual(progress, ['plan-course.started', 'plan-course.failed'])
})
