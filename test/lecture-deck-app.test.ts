import assert from 'node:assert/strict'
import test from 'node:test'
import { mkdtemp, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { PGlite } from '@electric-sql/pglite'
import { createLingxiOS, packageResources } from '../src/index.js'
import { FileLecturePublisher, LectureDeckService, PostgresLectureRepository, type LectureAuthor } from '../src/lecture-deck/index.js'

test('public app entry queues, runs, resumes idempotently and reads committed lecture HTML', async () => {
  const database = new PGlite(), directory = await mkdtemp(join(tmpdir(), 'lingxios-lecture-'))
  await database.exec(await readFile(packageResources().schema, 'utf8'))
  const pool = { query: async (sql: string, values?: readonly unknown[]) => { const result = await database.query(sql, values as unknown[]); return { rows: result.rows as Record<string, unknown>[], rowCount: result.affectedRows ?? result.rows.length } },
    connect: async () => ({ query: pool.query, release() {} }) }
  const author: LectureAuthor = {
    async plan(request) { const ids = ['pg_1', 'pg_2', 'pg_3']; return { title: 'Native lecture', audience: 'Learners', prerequisites: [], objectives: [{ id: 'o', description: 'Learn' }], chapters: [{ id: 'c', order: 0, title: 'Chapter', objectiveIds: ['o'], slideIds: ids }], targetSlideCount: request.targetSlideCount, durationMinutes: request.durationMinutes, terminology: {} } },
    async slide(input) { return { id: input.pageId, order: input.order, chapterId: input.chapter.id, role: input.order ? 'content' : 'cover', purpose: 'explain', title: `Page ${input.order + 1}`, conclusion: 'Grounded conclusion', visualKind: 'diagram',
      bodyHtml: '<svg role="img" aria-label="concept" viewBox="0 0 1280 720"><rect data-anchor-id="main" x="100" y="100" width="500" height="300" fill="#53d6c7"/></svg>', anchors: [{ id: 'main', x: 100, y: 100, width: 500, height: 300 }],
      steps: [{ id: 'explain', title: 'Explain', explanation: 'Explanation', anchorIds: ['main'], claimIds: ['claim'] }], bindings: [{ claimId: 'claim', snapshotId: input.evidence[0]!.id, evidenceMarkers: ['S1'], kind: 'source', statement: 'Claim' }] } },
  }
  const publisher = new FileLecturePublisher(join(directory, 'published'))
  const lectureDeck = new LectureDeckService({ repository: new PostgresLectureRepository(pool), publisher, author,
    evidence: { search: async () => [{ marker: 'S1', sourceId: 'source', sourceVersion: 'v1', chunkId: 'chunk', title: 'Source', excerpt: 'Evidence' }] }, reviewer: { review: async () => ({ passed: true, issues: [] }) } })
  const app = await createLingxiOS({ database: pool, lectureDeck, model: { id: 'unused', apiKey: 'unused' }, kernel: { homesRoot: join(directory, 'homes') } })
  try {
    const input = { id: 'request-1', tenantId: 'tenant', agentId: 'agent', sessionId: 'session', principalId: 'teacher', request: { requirements: 'Teach', targetSlideCount: 3 } }
    const queued = await app.lectures!.enqueueLecture(input)
    assert.deepEqual((await lectureDeck.get({ tenantId: 'tenant', principalId: 'teacher' }, queued.deckId)).request,
      { requirements: 'Teach', targetSlideCount: 3, durationMinutes: 10 })
    assert.equal((await app.lectures!.enqueueLecture(input)).deduplicated, true)
    assert.equal(await app.runNext(), true)
    await app.lectures!.enqueueLectureOperation({ ...input, deckId: queued.deckId, operation: 'approve_outline', idempotencyKey: 'approve-1', request: { expectedRevision: 1 } })
    assert.equal(await app.runNext(), true)
    assert.equal((await app.lectures!.readLecture({ tenantId: 'tenant', principalId: 'teacher', deckId: queued.deckId })).status, 'ready')
    const html = await app.lectures!.readLectureHtml({ tenantId: 'tenant', principalId: 'teacher', deckId: queued.deckId })
    assert.match(new TextDecoder().decode(html!), /Content-Security-Policy/)
  } finally {
    await app.stop(); await database.close(); await rm(directory, { recursive: true, force: true })
  }
})
