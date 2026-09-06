import assert from 'node:assert/strict'
import test from 'node:test'
import { LectureDeckService, MemoryLectureRepository, buildStandaloneLecture, validateDeck, type CoursePlan, type LectureAuthor, type SlideSpec } from '../src/lecture-deck/index.js'

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

  const originalBodies = created.manifest!.slides.map(item => item.bodyHtml)
  const revised = await service.revise(scope, created.id, { instruction: 'Turn this into a process', scope: 'page', pageIds: ['pg_page_2'], expectedRevision: 1 })
  assert.equal(revised.status, 'ready')
  assert.equal(revised.revision, 2)
  assert.equal(revised.manifest!.slides[0]!.bodyHtml, originalBodies[0])
  assert.match(revised.manifest!.slides[1]!.title, /revised/)
  assert.equal(revised.manifest!.slides[2]!.bodyHtml, originalBodies[2])
  await assert.rejects(service.revise(scope, created.id, { instruction: 'stale', scope: 'deck', expectedRevision: 1 }), /revision changed/)
})

test('lecture contract supports page 100 and blocks active or online content', () => {
  const chapter = { id: 'chapter', order: 0, title: 'Long course', objectiveIds: ['o'], slideIds: Array.from({ length: 100 }, (_, index) => `pg_${index + 1}`) }
  const evidence = [{ version: 1 as const, id: 'snapshot', items: [{ marker: 'S1', sourceId: 's', sourceVersion: 'v', chunkId: 'c', title: 'Source', excerpt: 'Text' }] }]
  const slides = chapter.slideIds.map((id, order) => slide({ request: { requirements: 'Long', targetSlideCount: 100, durationMinutes: 150 },
    course: { title: 'Long', audience: 'Learners', prerequisites: [], objectives: [{ id: 'o', description: 'Learn' }], chapters: [chapter], targetSlideCount: 100, durationMinutes: 150, terminology: {} }, chapter, order, pageId: id, evidence }))
  const manifest = { schemaVersion: 'lingxi-lecture/v1' as const, deckId: 'deck', revision: 1, title: 'Long', language: 'zh-CN',
    theme: { id: 'theme', background: '#000000', surface: '#111111', text: '#ffffff', muted: '#aaaaaa', accent: '#00ffff' },
    course: { title: 'Long', audience: 'Learners', prerequisites: [], objectives: [{ id: 'o', description: 'Learn' }], chapters: [chapter], targetSlideCount: 100, durationMinutes: 150, terminology: {} }, slides, evidence, createdAt: '2026-01-01T00:00:00.000Z' }
  assert.equal(validateDeck(manifest).passed, true)
  assert.ok(buildStandaloneLecture(manifest).size > 0)
  manifest.slides[99] = { ...manifest.slides[99]!, bodyHtml: '<script>alert(1)</script>' }
  assert.ok(validateDeck(manifest).issues.some(issue => issue.code === 'security.markup'))
})
