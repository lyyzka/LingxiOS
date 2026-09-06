import { createHash, randomUUID } from 'node:crypto'
import type { ModelDriver } from '../model/driver.js'
import type { WorkProcessor } from '../runtime/runtime.js'
import { snapshotEvidence, type EvidenceItem, type EvidenceSnapshot } from '../context/evidence.js'
import {
  COURSE_PLAN_OUTPUT, DEFAULT_LECTURE_THEME, LECTURE_SCHEMA_VERSION, SLIDE_OUTPUT, contentHash, parseCoursePlan, parseSlideSpec, validateCreateRequest, validateRevisionRequest,
  type ChapterPlan, type CoursePlan, type DeckManifest, type LectureDeckCreateRequest, type LectureDeckRevisionRequest,
  type SlideSpec, type ValidationReport,
} from './contracts.js'
import { buildStandaloneLecture, type LectureArtifact } from './standalone.js'
import { validateDeck } from './validation.js'

export type LectureStage = 'plan-course' | 'plan-chapter' | 'author-slide' | 'validate-slide' | 'repair-slide' | 'validate-deck' | 'publish-deck'
export type LectureStatus = 'planning' | 'awaiting_outline_approval' | 'generating' | 'validating' | 'publishing' | 'ready' | 'failed' | 'cancelled'
export interface LectureCheckpoint { stage: LectureStage; key: string; inputHash: string; output: unknown; outputHash: string; attempts: number; completedAt: string }
export interface LectureRecord {
  id: string; tenantId: string; principalId: string; revision: number; status: LectureStatus; request: LectureDeckCreateRequest
  manifest?: DeckManifest; report?: ValidationReport; artifact?: Omit<LectureArtifact, 'bytes'>; error?: string
  outline?: CoursePlan; evidence?: EvidenceSnapshot[]; outlineApproved?: boolean
  revisionRequest?: LectureDeckRevisionRequest
  stateVersion?: number
  rootWorkId?: string
}
export interface LectureRepository {
  create(record: LectureRecord): Promise<void>
  get(tenantId: string, id: string): Promise<LectureRecord | null>
  save(record: LectureRecord, expectedRevision: number): Promise<void>
  claimPublication(record: LectureRecord): Promise<boolean>
  checkpoint(deckId: string, revision: number, value: LectureCheckpoint): Promise<void>
  readCheckpoint(deckId: string, revision: number, stage: LectureStage, key: string, inputHash: string): Promise<LectureCheckpoint | null>
}
export interface LectureEvidenceProvider {
  search(input: { tenantId: string; principalId: string; sourceIds: string[]; queries: string[]; limit: number; signal?: AbortSignal }): Promise<EvidenceItem[]>
}
export interface LecturePublisher { publish(record: LectureRecord, artifact: LectureArtifact): Promise<void>; read?(record: LectureRecord): Promise<Uint8Array | null> }
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
const contentHashBytes = (bytes: Uint8Array) => createHash('sha256').update(bytes).digest('hex')

export class LectureDeckService {
  private readonly now: () => string
  private readonly active = new Map<string, AbortController>()
  constructor(readonly dependencies: LectureDeckDependencies) { this.now = dependencies.now ?? (() => new Date().toISOString()) }
  private get deps() { return this.dependencies }

  async begin(scope: { tenantId: string; principalId: string }, value: unknown, id: string = randomUUID()): Promise<LectureRecord> {
    const request = validateCreateRequest(value)
    const existing = await this.deps.repository.get(scope.tenantId, id)
    if (existing) {
      if (existing.principalId !== scope.principalId || contentHash(existing.request) !== contentHash(request)) throw new Error('lecture identity reused with different input')
      return existing
    }
    const record: LectureRecord = { id, ...scope, revision: 1, status: 'planning', request }
    await this.deps.repository.create(record)
    return record
  }

  async create(scope: { tenantId: string; principalId: string }, value: unknown, signal?: AbortSignal): Promise<LectureRecord> {
    const record = await this.begin(scope, value)
    return this.run(scope, record.id, signal)
  }

  async run(scope: { tenantId: string; principalId: string }, id: string, signal?: AbortSignal): Promise<LectureRecord> {
    const record = await this.owned(scope, id)
    if (record.status === 'ready' || record.status === 'awaiting_outline_approval' || record.status === 'cancelled') return record
    if (record.status === 'publishing') return this.publish(record, signal)
    if (!['planning', 'generating', 'validating'].includes(record.status)) throw new Error('lecture is not awaiting generation or recovery')
    return this.withCancellation(record.id, signal, activeSignal => this.generate(record, activeSignal))
  }

  async retry(scope: { tenantId: string; principalId: string }, id: string, signal?: AbortSignal): Promise<LectureRecord> {
    const record = await this.owned(scope, id)
    if (!['failed', 'cancelled'].includes(record.status)) throw new Error('only failed or cancelled lectures can be retried')
    const { error: _error, ...retryable } = record
    const next: LectureRecord = { ...retryable, status: record.outlineApproved ? 'generating' : 'planning' }
    await this.deps.repository.save(next, record.revision)
    return this.withCancellation(next.id, signal, activeSignal => this.generate(next, activeSignal))
  }

  async revise(scope: { tenantId: string; principalId: string }, id: string, value: unknown, signal?: AbortSignal): Promise<LectureRecord> {
    const request = validateRevisionRequest(value)
    const current = await this.owned(scope, id)
    if (!current.manifest || current.status !== 'ready') throw new Error('only ready lectures can be revised')
    if (current.revision !== request.expectedRevision) throw new Error('lecture revision changed')
    this.affectedSlides(current.manifest, request)
    const revision = current.revision + 1
    const { artifact: _artifact, report: _report, ...revisable } = current
    const next: LectureRecord = { ...revisable, revision, status: 'awaiting_outline_approval', outlineApproved: false, revisionRequest: request }
    await this.deps.repository.save(next, current.revision)
    abortIfNeeded(signal)
    return next
  }

  async approveOutline(scope: { tenantId: string; principalId: string }, id: string, expectedRevision: number) {
    const record = await this.owned(scope, id)
    if (record.revision !== expectedRevision) throw new Error('lecture revision changed')
    if (record.outlineApproved) return record
    if (record.status !== 'awaiting_outline_approval' || !record.outline) throw new Error('lecture outline is not awaiting approval')
    const approved: LectureRecord = { ...record, outlineApproved: true, status: 'generating' }
    await this.deps.repository.save(approved, expectedRevision)
    return approved
  }

  async reviseOutline(scope: { tenantId: string; principalId: string }, id: string, input: { expectedRevision: number; feedback?: string; targetSlideCount?: number }) {
    const record = await this.owned(scope, id)
    if (record.revision !== input.expectedRevision || record.status !== 'awaiting_outline_approval') throw new Error('lecture outline changed or is not awaiting approval')
    if (!input.feedback?.trim() && input.targetSlideCount === undefined) throw new Error('outline feedback or targetSlideCount is required')
    const request = validateCreateRequest({ ...record.request,
      requirements: [record.request.requirements, input.feedback].filter(Boolean).join('\n'),
      ...(input.targetSlideCount === undefined ? {} : { targetSlideCount: input.targetSlideCount }) })
    const next: LectureRecord = { id: record.id, tenantId: record.tenantId, principalId: record.principalId,
      revision: record.revision + 1, status: 'planning', request,
      ...(record.stateVersion === undefined ? {} : { stateVersion: record.stateVersion }),
      ...(record.rootWorkId ? { rootWorkId: record.rootWorkId } : {}) }
    await this.deps.repository.save(next, record.revision)
    return next
  }

  async get(scope: { tenantId: string; principalId: string }, id: string) { return this.owned(scope, id) }

  async readHtml(scope: { tenantId: string; principalId: string }, id: string): Promise<Uint8Array | null> {
    const record = await this.owned(scope, id)
    if (record.status !== 'ready' || !record.artifact || !this.deps.publisher.read) return null
    const bytes = await this.deps.publisher.read(record)
    if (!bytes || contentHashBytes(bytes) !== record.artifact.sha256 || bytes.length !== record.artifact.size) throw new Error('published lecture artifact does not match its commitment')
    return bytes
  }

  async cancel(scope: { tenantId: string; principalId: string }, id: string): Promise<LectureRecord> {
    const current = await this.owned(scope, id)
    if (['publishing', 'ready', 'failed', 'cancelled'].includes(current.status)) return current
    this.active.get(id)?.abort(new Error('lecture generation cancelled'))
    const cancelled = { ...current, revision: current.revision + 1, status: 'cancelled' as const }
    await this.deps.repository.save(cancelled, current.revision)
    return cancelled
  }

  private async generate(record: LectureRecord, signal?: AbortSignal): Promise<LectureRecord> {
    try {
      abortIfNeeded(signal)
      if (!record.outline) {
      const sourceIds = record.request.sourceIds ?? []
      const broadItems = await this.deps.evidence.search({ tenantId: record.tenantId, principalId: record.principalId, sourceIds, queries: [record.request.requirements], limit: 200, ...(signal ? { signal } : {}) })
      const broad = this.trimEvidence(snapshotEvidence(`${record.id}:course`, broadItems), 40)
      const course = await this.cached(record, 'plan-course', 'course', { request: record.request, evidence: broad }, () => this.deps.author.plan(record.request, broad, signal))
      this.validatePlan(course, record.request.targetSlideCount)
      const evidence: EvidenceSnapshot[] = [broad]
      for (const chapter of course.chapters) {
        abortIfNeeded(signal)
        const items = await this.deps.evidence.search({ tenantId: record.tenantId, principalId: record.principalId, sourceIds, queries: [chapter.title, ...chapter.objectiveIds.map(id => course.objectives.find(item => item.id === id)?.description ?? id)], limit: 200, ...(signal ? { signal } : {}) })
        evidence.push(this.trimEvidence(snapshotEvidence(`${record.id}:${chapter.id}`, items), 60))
      }
      const waiting: LectureRecord = { ...record, outline: course, evidence, outlineApproved: false, status: 'awaiting_outline_approval' }
      await this.deps.repository.save(waiting, record.revision)
      return waiting
      }
      if (!record.outlineApproved) throw new Error('lecture outline must be approved before authoring slides')
      const course = record.outline, evidence = record.evidence ?? []
      record = { ...record, status: 'generating' }
      await this.deps.repository.save(record, record.revision)
      if (record.revisionRequest && record.manifest) {
        const slides = [...record.manifest.slides]
        for (const original of this.affectedSlides(record.manifest, record.revisionRequest)) {
          abortIfNeeded(signal)
          const chapter = course.chapters.find(item => item.id === original.chapterId)!
          slides[original.order] = await this.authorSlide(record, course, chapter, original.order, original.id, evidence, original, record.revisionRequest.instruction, signal)
        }
        const { contentHash: _hash, ...base } = record.manifest
        return await this.finish(record, { ...base, revision: record.revision, slides, createdAt: this.now() }, signal)
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
    const scopedEvidence = this.slideEvidence(evidence, chapter, order)
    const input = { request: record.request, course, chapter, order, pageId: key, evidence: scopedEvidence, ...(previous ? { previous } : {}), ...(instruction ? { instruction } : {}) }
    let slide = await this.cached(record, previous ? 'repair-slide' : 'author-slide', key, input, () => this.deps.author.slide(input, signal))
    slide = { ...slide, id: key, order, chapterId: chapter.id }
    for (let attempt = 0; attempt < (this.deps.maxRepairAttempts ?? 2); attempt++) {
      const probe = this.partialManifest(record, course, scopedEvidence, slide)
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
    record = { ...record, status: 'validating' }
    await this.deps.repository.save(record, record.revision)
    let report = validateDeck(manifest)
    if (report.passed) {
      const semantic = await this.deps.reviewer.review(manifest, signal)
      report = { passed: semantic.passed === true && semantic.issues.length === 0, issues: [...report.issues, ...semantic.issues] }
    }
    if (!report.passed) throw new Error(`lecture quality gate failed: ${report.issues.map(issue => issue.code).join(', ')}`)
    abortIfNeeded(signal)
    const unhashed = structuredClone(manifest)
    delete unhashed.contentHash
    manifest = { ...manifest, contentHash: contentHash(unhashed) }
    const artifact = buildStandaloneLecture(manifest, this.deps.maxArtifactBytes)
    const ready: LectureRecord = { ...record, status: 'ready', manifest, report, artifact: { filename: artifact.filename, mime: artifact.mime, size: artifact.size, sha256: artifact.sha256 } }
    abortIfNeeded(signal)
    const publishing: LectureRecord = { ...ready, status: 'publishing' }
    if (!await this.deps.repository.claimPublication(publishing)) return await this.owned(record, record.id)
    return this.publish(publishing, signal)
  }

  private async publish(record: LectureRecord, signal?: AbortSignal): Promise<LectureRecord> {
    abortIfNeeded(signal)
    if (!record.manifest || !record.artifact || !record.report?.passed || record.report.issues.length) throw new Error('lecture publication intent is incomplete')
    const artifact = buildStandaloneLecture(record.manifest, this.deps.maxArtifactBytes)
    if (artifact.sha256 !== record.artifact.sha256 || artifact.size !== record.artifact.size) throw new Error('lecture publication intent hash mismatch')
    const existing = await this.deps.publisher.read?.(record)
    if (existing && (contentHashBytes(existing) !== artifact.sha256 || existing.length !== artifact.size)) throw new Error('published lecture hash mismatch')
    if (!existing) await this.deps.publisher.publish(record, artifact)
    abortIfNeeded(signal)
    const ready: LectureRecord = { ...record, status: 'ready' }
    await this.deps.repository.save(ready, record.revision)
    return ready
  }

  private async fail(record: LectureRecord, error: unknown): Promise<LectureRecord> {
    const latest = await this.deps.repository.get(record.tenantId, record.id)
    if (latest?.status === 'cancelled' || latest && latest.revision !== record.revision) return latest
    // A publication intent is recoverable; never replace it with a pre-publication snapshot.
    if (latest?.status === 'publishing') throw error
    const failed = { ...(latest ?? record), status: 'failed' as const, error: error instanceof Error ? error.message : String(error) }
    await this.deps.repository.save(failed, record.revision)
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

  private trimEvidence(snapshot: EvidenceSnapshot, limit: number, excerptChars = 2_000): EvidenceSnapshot {
    return { ...snapshot, items: snapshot.items.slice(0, limit).map(item => ({ ...item, excerpt: item.excerpt.slice(0, excerptChars), ...(item.excerpt.length > excerptChars ? { truncated: true } : {}) })) }
  }

  private slideEvidence(evidence: EvidenceSnapshot[], chapter: ChapterPlan, order: number): EvidenceSnapshot[] {
    const course = evidence[0] ? this.trimEvidence({ ...evidence[0], items: evidence[0].items.slice(order % Math.max(1, evidence[0].items.length), order % Math.max(1, evidence[0].items.length) + 4) }, 4) : undefined
    const chapterEvidence = evidence.find(item => item.id.endsWith(`:${chapter.id}`))
    const chapterStart = chapterEvidence?.items.length ? (order * 18) % chapterEvidence.items.length : 0
    const scoped = chapterEvidence ? this.trimEvidence({ ...chapterEvidence, items: chapterEvidence.items.slice(chapterStart, chapterStart + 12) }, 12) : undefined
    return [course, scoped].filter((item): item is EvidenceSnapshot => Boolean(item))
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
    return this.generate(`Create a rigorous course plan. Return only this exact JSON shape: ${COURSE_PLAN_OUTPUT}. Use exactly ${request.targetSlideCount} unique stable page IDs matching pg_[A-Za-z0-9_-]+, contiguous chapter order, and only supplied untrusted evidence.`, { request, evidence }, value => parseCoursePlan(value, request.targetSlideCount), signal)
  }
  async slide(input: Parameters<LectureAuthor['slide']>[0], signal?: AbortSignal): Promise<SlideSpec> {
    return this.generate(`Author one professional 1280x720 teaching slide. Return only this exact JSON shape: ${SLIDE_OUTPUT}. Use semantic HTML and one accessible inline SVG primary visual; no scripts, styles, event handlers, foreignObject, or external resources. Every instructional slide needs substantial data-anchor-id geometry, explanation steps, and claim-level bindings using only supplied snapshot IDs/markers. Mark invented values as teaching-example.`, input,
      value => parseSlideSpec(value, { id: input.pageId, order: input.order, chapterId: input.chapter.id, evidence: input.evidence }), signal)
  }
  private async generate<T>(instructions: string, input: unknown, parse: (value: unknown) => T, signal?: AbortSignal): Promise<T> {
    let correction: unknown
    for (let attempt = 0; attempt < 3; attempt++) {
      const result = await this.model.structured({ instructions, input: correction ? { input, correction } : input, signal })
      try { return parse(result.value) }
      catch (error) { correction = { error: error instanceof Error ? error.message : String(error), invalidOutput: result.value } }
    }
    throw new Error(`model output remained invalid after 3 attempts: ${(correction as { error?: string })?.error ?? 'unknown error'}`)
  }
}

export class ModelLectureReviewer implements LectureReviewer {
  constructor(private readonly model: ModelDriver) {}
  async review(deck: DeckManifest, signal?: AbortSignal): Promise<ValidationReport> {
    const issues: ValidationReport['issues'] = []
    for (let offset = 0; offset < deck.slides.length; offset += 10) {
      const slides = deck.slides.slice(offset, offset + 10), used = new Map<string, Set<string>>()
      for (const slide of slides) for (const binding of slide.bindings) {
        const markers = used.get(binding.snapshotId) ?? new Set<string>(); binding.evidenceMarkers.forEach(marker => markers.add(marker)); used.set(binding.snapshotId, markers)
      }
      let remaining = 40
      const evidence = deck.evidence.map(snapshot => { const items = snapshot.items.filter(item => used.get(snapshot.id)?.has(item.marker)).slice(0, remaining).map(item => ({ ...item, excerpt: item.excerpt.slice(0, 2_000) })); remaining -= items.length; return { ...snapshot, items } }).filter(snapshot => snapshot.items.length)
      const result = await this.model.structured({ instructions: 'Independently review these lecture pages for citation support, teaching continuity, meaningful visuals, skipped prerequisites, and professional readability. Return strict JSON {"passed":boolean,"issues":[{"code":"string","message":"string","pageId":"optional","objectId":"optional"}]}. Do not claim browser geometry was checked.', input: { course: deck.course, slides, evidence }, signal })
      const report = result.value as ValidationReport
      if (!report || typeof report.passed !== 'boolean' || !Array.isArray(report.issues) || report.issues.some(issue => !issue || typeof issue.code !== 'string' || typeof issue.message !== 'string')) throw new Error('lecture reviewer returned an invalid report')
      if (!report.passed && report.issues.length === 0) issues.push({ code: 'review.rejected', message: 'Reviewer rejected these slides without a detailed issue' })
      issues.push(...report.issues)
    }
    return { passed: issues.length === 0, issues }
  }
}

/** Native long-work adapter; register it as the `lecture_deck` WorkProcessor. */
export function lectureDeckProcessor(service: LectureDeckService): WorkProcessor {
  return { async process(work, context) {
    if (!work.principalId) throw new Error('lecture work requires an authenticated principal')
    const operation = work.meta?.['operation'] ?? 'create'
    await context.emit({ kind: 'lecture.stage', stage: 'started', visibility: 'user', data: { operation } })
    const scope = { tenantId: work.tenantId, principalId: work.principalId }
    const result = operation === 'create' ? await service.run(scope, (await service.begin(scope, work.meta?.['request'], `deck_${contentHash([work.tenantId, work.id]).slice(0, 32)}`)).id, context.signal)
      : operation === 'run' ? await service.run(scope, String(work.meta?.['deckId'] ?? ''), context.signal)
      : operation === 'revise' ? await service.revise(scope, String(work.meta?.['deckId'] ?? ''), work.meta?.['request'], context.signal)
      : operation === 'retry' ? await service.retry(scope, String(work.meta?.['deckId'] ?? ''), context.signal)
      : operation === 'cancel' ? await service.cancel(scope, String(work.meta?.['deckId'] ?? ''))
      : (() => { throw new Error('unsupported lecture operation') })()
    if (result.status === 'failed') throw new Error(result.error ?? 'lecture generation failed')
    await context.emit({ kind: 'lecture.stage', stage: 'completed', visibility: 'user', data: { deckId: result.id, revision: result.revision, status: result.status, artifact: result.artifact ?? null } })
  } }
}
