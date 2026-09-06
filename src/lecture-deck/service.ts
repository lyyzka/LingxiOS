import { randomUUID } from 'node:crypto'
import type { ModelDriver } from '../model/driver.js'
import type { WorkProcessor } from '../runtime/runtime.js'
import { snapshotEvidence, type EvidenceItem, type EvidenceSnapshot } from '../context/evidence.js'
import {
  DEFAULT_LECTURE_THEME, LECTURE_SCHEMA_VERSION, contentHash, validateCreateRequest, validateRevisionRequest,
  type ChapterPlan, type CoursePlan, type DeckManifest, type LectureDeckCreateRequest, type LectureDeckRevisionRequest,
  type SlideSpec, type ValidationReport,
} from './contracts.js'
import { buildStandaloneLecture, type LectureArtifact } from './standalone.js'
import { validateDeck } from './validation.js'

export type LectureStage = 'plan-course' | 'plan-chapter' | 'author-slide' | 'validate-slide' | 'repair-slide' | 'validate-deck' | 'publish-deck'
export type LectureStatus = 'planning' | 'generating' | 'validating' | 'ready' | 'failed' | 'cancelled'
export interface LectureCheckpoint { stage: LectureStage; key: string; inputHash: string; output: unknown; outputHash: string; attempts: number; completedAt: string }
export interface LectureRecord {
  id: string; tenantId: string; principalId: string; revision: number; status: LectureStatus; request: LectureDeckCreateRequest
  manifest?: DeckManifest; report?: ValidationReport; artifact?: Omit<LectureArtifact, 'bytes'>; error?: string
}
export interface LectureRepository {
  create(record: LectureRecord): Promise<void>
  get(tenantId: string, id: string): Promise<LectureRecord | null>
  save(record: LectureRecord, expectedRevision: number): Promise<void>
  checkpoint(deckId: string, revision: number, value: LectureCheckpoint): Promise<void>
  readCheckpoint(deckId: string, revision: number, stage: LectureStage, key: string, inputHash: string): Promise<LectureCheckpoint | null>
}
export interface LectureEvidenceProvider {
  search(input: { tenantId: string; principalId: string; sourceIds: string[]; queries: string[]; limit: number; signal?: AbortSignal }): Promise<EvidenceItem[]>
}
export interface LecturePublisher { publish(record: LectureRecord, artifact: LectureArtifact): Promise<void> }
export interface LectureReviewer { review(deck: DeckManifest, signal?: AbortSignal): Promise<ValidationReport> }
export interface LectureAuthor {
  plan(request: LectureDeckCreateRequest, evidence: EvidenceSnapshot, signal?: AbortSignal): Promise<CoursePlan>
  slide(input: { request: LectureDeckCreateRequest; course: CoursePlan; chapter: ChapterPlan; order: number; pageId: string; evidence: EvidenceSnapshot[]; previous?: SlideSpec; instruction?: string }, signal?: AbortSignal): Promise<SlideSpec>
}

export interface LectureDeckDependencies {
  repository: LectureRepository; evidence: LectureEvidenceProvider; author: LectureAuthor; reviewer: LectureReviewer; publisher: LecturePublisher
  now?: () => string; maxRepairAttempts?: number; maxArtifactBytes?: number
}

const abortIfNeeded = (signal?: AbortSignal) => { if (signal?.aborted) throw signal.reason instanceof Error ? signal.reason : new Error('lecture generation cancelled') }
const unique = <T>(items: readonly T[]) => [...new Set(items)]

export class LectureDeckService {
  private readonly now: () => string
  private readonly active = new Map<string, AbortController>()
  constructor(private readonly deps: LectureDeckDependencies) { this.now = deps.now ?? (() => new Date().toISOString()) }

  async begin(scope: { tenantId: string; principalId: string }, value: unknown): Promise<LectureRecord> {
    const request = validateCreateRequest(value)
    const record: LectureRecord = { id: randomUUID(), ...scope, revision: 1, status: 'planning', request }
    await this.deps.repository.create(record)
    return record
  }

  async create(scope: { tenantId: string; principalId: string }, value: unknown, signal?: AbortSignal): Promise<LectureRecord> {
    const record = await this.begin(scope, value)
    return this.run(scope, record.id, signal)
  }

  async run(scope: { tenantId: string; principalId: string }, id: string, signal?: AbortSignal): Promise<LectureRecord> {
    const record = await this.owned(scope, id)
    if (record.status !== 'planning') throw new Error('lecture is not awaiting generation')
    return this.withCancellation(record.id, signal, activeSignal => this.generate(record, activeSignal))
  }

  async retry(scope: { tenantId: string; principalId: string }, id: string, signal?: AbortSignal): Promise<LectureRecord> {
    const record = await this.owned(scope, id)
    if (!['failed', 'cancelled'].includes(record.status)) throw new Error('only failed or cancelled lectures can be retried')
    const { error: _error, ...retryable } = record
    const next: LectureRecord = { ...retryable, status: 'planning' }
    await this.deps.repository.save(next, record.revision)
    return this.withCancellation(next.id, signal, activeSignal => this.generate(next, activeSignal))
  }

  async revise(scope: { tenantId: string; principalId: string }, id: string, value: unknown, signal?: AbortSignal): Promise<LectureRecord> {
    const request = validateRevisionRequest(value)
    const current = await this.owned(scope, id)
    if (!current.manifest || current.status !== 'ready') throw new Error('only ready lectures can be revised')
    if (current.revision !== request.expectedRevision) throw new Error('lecture revision changed')
    const affected = this.affectedSlides(current.manifest, request)
    const revision = current.revision + 1
    const { artifact: _artifact, report: _report, ...revisable } = current
    const next: LectureRecord = { ...revisable, revision, status: 'generating' }
    await this.deps.repository.save(next, current.revision)
    try {
      return await this.withCancellation(next.id, signal, async activeSignal => {
        const slides = [...current.manifest!.slides]
        for (const original of affected) {
          abortIfNeeded(activeSignal)
          const chapter = current.manifest!.course.chapters.find(item => item.id === original.chapterId)!
          slides[original.order] = await this.authorSlide(next, current.manifest!.course, chapter, original.order, original.id, current.manifest!.evidence, original, request.instruction, activeSignal)
        }
        const { contentHash: _hash, ...base } = current.manifest!
        return await this.finish(next, { ...base, revision, slides, createdAt: this.now() }, activeSignal)
      })
    } catch (error) { return this.fail(next, error) }
  }

  async get(scope: { tenantId: string; principalId: string }, id: string) { return this.owned(scope, id) }

  async cancel(scope: { tenantId: string; principalId: string }, id: string): Promise<LectureRecord> {
    const current = await this.owned(scope, id)
    if (['ready', 'failed', 'cancelled'].includes(current.status)) return current
    this.active.get(id)?.abort(new Error('lecture generation cancelled'))
    const cancelled = { ...current, revision: current.revision + 1, status: 'cancelled' as const }
    await this.deps.repository.save(cancelled, current.revision)
    return cancelled
  }

  private async generate(record: LectureRecord, signal?: AbortSignal): Promise<LectureRecord> {
    try {
      abortIfNeeded(signal)
      const sourceIds = record.request.sourceIds ?? []
      const broadItems = await this.deps.evidence.search({ tenantId: record.tenantId, principalId: record.principalId, sourceIds, queries: [record.request.requirements], limit: 200, ...(signal ? { signal } : {}) })
      const broad = snapshotEvidence(`${record.id}:course`, broadItems)
      const course = await this.cached(record, 'plan-course', 'course', { request: record.request, evidence: broad }, () => this.deps.author.plan(record.request, broad, signal))
      this.validatePlan(course, record.request.targetSlideCount)
      record = { ...record, status: 'generating' }
      await this.deps.repository.save(record, record.revision)
      const evidence: EvidenceSnapshot[] = [broad]
      for (const chapter of course.chapters) {
        abortIfNeeded(signal)
        const items = await this.deps.evidence.search({ tenantId: record.tenantId, principalId: record.principalId, sourceIds, queries: [chapter.title, ...chapter.objectiveIds.map(id => course.objectives.find(item => item.id === id)?.description ?? id)], limit: 200, ...(signal ? { signal } : {}) })
        evidence.push(snapshotEvidence(`${record.id}:${chapter.id}`, items))
      }
      const chapterForOrder = new Map(course.chapters.flatMap(chapter => chapter.slideIds.map(id => [id, chapter] as const)))
      const orderedIds = course.chapters.flatMap(chapter => chapter.slideIds).slice(0, course.targetSlideCount)
      const slides: SlideSpec[] = []
      for (let order = 0; order < orderedIds.length; order++) {
        const pageId = orderedIds[order]!
        slides.push(await this.authorSlide(record, course, chapterForOrder.get(pageId)!, order, pageId, evidence, undefined, undefined, signal))
      }
      const manifest: DeckManifest = { schemaVersion: LECTURE_SCHEMA_VERSION, deckId: record.id, revision: record.revision,
        title: record.request.title ?? course.title, language: record.request.language ?? 'zh-CN', theme: record.request.theme ?? DEFAULT_LECTURE_THEME,
        course, slides, evidence, createdAt: this.now() }
      return await this.finish(record, manifest, signal)
    } catch (error) { return this.fail(record, error) }
  }

  private async authorSlide(record: LectureRecord, course: CoursePlan, chapter: ChapterPlan, order: number, pageId: string, evidence: EvidenceSnapshot[], previous?: SlideSpec, instruction?: string, signal?: AbortSignal) {
    if (!chapter) throw new Error(`no chapter planned for slide ${order}`)
    const key = previous?.id ?? pageId
    const input = { request: record.request, course, chapter, order, pageId: key, evidence, ...(previous ? { previous } : {}), ...(instruction ? { instruction } : {}) }
    let slide = await this.cached(record, previous ? 'repair-slide' : 'author-slide', key, input, () => this.deps.author.slide(input, signal))
    slide = { ...slide, id: key, order, chapterId: chapter.id }
    for (let attempt = 0; attempt < (this.deps.maxRepairAttempts ?? 2); attempt++) {
      const probe = this.partialManifest(record, course, evidence, slide)
      if (validateDeck(probe).issues.every(issue => issue.pageId !== slide.id && !issue.code.startsWith('security.'))) break
      slide = await this.deps.author.slide({ ...input, previous: slide, instruction: 'Repair deterministic validation errors only.' }, signal)
      slide = { ...slide, id: key, order, chapterId: chapter.id }
    }
    return slide
  }

  private partialManifest(record: LectureRecord, course: CoursePlan, evidence: EvidenceSnapshot[], slide: SlideSpec): DeckManifest {
    return { schemaVersion: LECTURE_SCHEMA_VERSION, deckId: record.id, revision: record.revision, title: course.title, language: record.request.language ?? 'zh-CN',
      theme: record.request.theme ?? DEFAULT_LECTURE_THEME, course: { ...course, chapters: course.chapters.map(chapter => ({ ...chapter, slideIds: chapter.id === slide.chapterId ? [slide.id] : [] })) }, slides: [{ ...slide, order: 0 }], evidence, createdAt: this.now() }
  }

  private async finish(record: LectureRecord, manifest: DeckManifest, signal?: AbortSignal) {
    abortIfNeeded(signal)
    let report = validateDeck(manifest)
    if (report.passed) {
      const semantic = await this.deps.reviewer.review(manifest, signal)
      report = { passed: semantic.passed, issues: [...report.issues, ...semantic.issues] }
    }
    if (!report.passed) throw new Error(`lecture quality gate failed: ${report.issues.map(issue => issue.code).join(', ')}`)
    const unhashed = structuredClone(manifest)
    delete unhashed.contentHash
    manifest = { ...manifest, contentHash: contentHash(unhashed) }
    const artifact = buildStandaloneLecture(manifest, this.deps.maxArtifactBytes)
    const ready: LectureRecord = { ...record, status: 'ready', manifest, report, artifact: { filename: artifact.filename, mime: artifact.mime, size: artifact.size, sha256: artifact.sha256 } }
    await this.deps.publisher.publish(ready, artifact)
    await this.deps.repository.save(ready, record.revision)
    return ready
  }

  private async fail(record: LectureRecord, error: unknown): Promise<LectureRecord> {
    const latest = await this.deps.repository.get(record.tenantId, record.id)
    if (latest?.status === 'cancelled' || latest && latest.revision !== record.revision) return latest
    const failed = { ...record, status: 'failed' as const, error: error instanceof Error ? error.message : String(error) }
    await this.deps.repository.save(failed, record.revision).catch(() => undefined)
    return failed
  }

  private async withCancellation<T>(id: string, external: AbortSignal | undefined, run: (signal: AbortSignal) => Promise<T>): Promise<T> {
    const controller = new AbortController()
    const abort = () => controller.abort(external?.reason)
    if (external?.aborted) abort()
    else external?.addEventListener('abort', abort, { once: true })
    this.active.set(id, controller)
    try { return await run(controller.signal) }
    finally { external?.removeEventListener('abort', abort); if (this.active.get(id) === controller) this.active.delete(id) }
  }

  private async cached<T>(record: LectureRecord, stage: LectureStage, key: string, input: unknown, run: () => Promise<T>): Promise<T> {
    const inputHash = contentHash(input)
    const cached = await this.deps.repository.readCheckpoint(record.id, record.revision, stage, key, inputHash)
    if (cached) return structuredClone(cached.output) as T
    const output = await run()
    await this.deps.repository.checkpoint(record.id, record.revision, { stage, key, inputHash, output: structuredClone(output), outputHash: contentHash(output), attempts: 1, completedAt: this.now() })
    return output
  }

  private validatePlan(plan: CoursePlan, count: number) {
    if (!plan?.title?.trim() || plan.targetSlideCount !== count || plan.chapters.length < 1 || plan.chapters.flatMap(chapter => chapter.slideIds).length !== count) throw new Error('course plan does not satisfy the requested slide count')
    const ids = plan.chapters.flatMap(chapter => chapter.slideIds)
    if (new Set(ids).size !== ids.length || ids.some(id => !/^pg_[a-zA-Z0-9_-]{1,80}$/.test(id))) throw new Error('course plan contains invalid page identities')
  }

  private affectedSlides(deck: DeckManifest, request: LectureDeckRevisionRequest) {
    const slides = request.scope === 'deck' ? deck.slides : request.scope === 'page' ? deck.slides.filter(slide => request.pageIds!.includes(slide.id)) : deck.slides.filter(slide => request.sectionIds!.includes(slide.chapterId))
    const requested = request.scope === 'page' ? request.pageIds! : request.scope === 'section' ? request.sectionIds! : []
    if (request.scope !== 'deck' && unique(slides.map(slide => request.scope === 'page' ? slide.id : slide.chapterId)).length !== unique(requested).length) throw new Error('revision references an unknown page or section')
    return slides
  }

  private async owned(scope: { tenantId: string; principalId: string }, id: string) {
    const record = await this.deps.repository.get(scope.tenantId, id)
    if (!record || record.principalId !== scope.principalId) throw new Error('lecture not found')
    return record
  }
}

export class ModelLectureAuthor implements LectureAuthor {
  constructor(private readonly model: ModelDriver) {}
  async plan(request: LectureDeckCreateRequest, evidence: EvidenceSnapshot, signal?: AbortSignal): Promise<CoursePlan> {
    const result = await this.model.structured({ instructions: `Create a rigorous course plan as JSON. Use exactly ${request.targetSlideCount} stable page IDs matching pg_[A-Za-z0-9_-]+. Every chapter must list its slideIds. Do not invent facts beyond supplied untrusted evidence.`, input: { request, evidence }, signal })
    return result.value as CoursePlan
  }
  async slide(input: Parameters<LectureAuthor['slide']>[0], signal?: AbortSignal): Promise<SlideSpec> {
    const result = await this.model.structured({ instructions: 'Author one professional 1280x720 teaching slide as strict JSON. bodyHtml may contain semantic HTML and inline SVG only; no scripts, event handlers, external resources, foreignObject, or style tags. Give every explanation step real in-canvas anchors and claim-level evidence bindings. Mark invented values as teaching-example.', input, signal })
    return result.value as SlideSpec
  }
}

export class ModelLectureReviewer implements LectureReviewer {
  constructor(private readonly model: ModelDriver) {}
  async review(deck: DeckManifest, signal?: AbortSignal): Promise<ValidationReport> {
    const result = await this.model.structured({ instructions: 'Independently review this lecture for citation support, teaching continuity, meaningful visuals, skipped prerequisites, and professional readability. Return JSON {passed:boolean,issues:[{code,message,pageId?,objectId?}]}. Do not claim deterministic browser geometry was checked.', input: deck, signal })
    const report = result.value as ValidationReport
    if (!report || typeof report.passed !== 'boolean' || !Array.isArray(report.issues)) throw new Error('lecture reviewer returned an invalid report')
    return report
  }
}

/** Native long-work adapter; register it as the `lecture_deck` WorkProcessor. */
export function lectureDeckProcessor(service: LectureDeckService): WorkProcessor {
  return { async process(work, context) {
    if (!work.principalId) throw new Error('lecture work requires an authenticated principal')
    const operation = work.meta?.['operation'] ?? 'create'
    await context.emit({ kind: 'lecture.stage', stage: 'started', visibility: 'user', data: { operation } })
    const scope = { tenantId: work.tenantId, principalId: work.principalId }
    const result = operation === 'create' ? await service.create(scope, work.meta?.['request'], context.signal)
      : operation === 'revise' ? await service.revise(scope, String(work.meta?.['deckId'] ?? ''), work.meta?.['request'], context.signal)
      : operation === 'retry' ? await service.retry(scope, String(work.meta?.['deckId'] ?? ''), context.signal)
      : operation === 'cancel' ? await service.cancel(scope, String(work.meta?.['deckId'] ?? ''))
      : (() => { throw new Error('unsupported lecture operation') })()
    if (result.status === 'failed') throw new Error(result.error ?? 'lecture generation failed')
    await context.emit({ kind: 'lecture.stage', stage: 'completed', visibility: 'user', data: { deckId: result.id, revision: result.revision, status: result.status, artifact: result.artifact ?? null } })
  } }
}
