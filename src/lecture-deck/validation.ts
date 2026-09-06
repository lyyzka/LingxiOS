import type { DeckManifest, SlideSpec, ValidationIssue, ValidationReport } from './contracts.js'
import { LECTURE_SCHEMA_VERSION, contentHash } from './contracts.js'

const forbiddenMarkup = /<(?:script|style|iframe|object|embed|link|meta|base|foreignObject|audio|video)\b|\bon\w+\s*=|\bsrcdoc\s*=|javascript:|data:(?!image\/(?:png|jpeg|gif|webp);base64,)|\b(?:href|src)\s*=\s*["']https?:/i
const validPageId = /^pg_[a-zA-Z0-9_-]{1,80}$/

export function validateDeck(input: DeckManifest): ValidationReport {
  const issues: ValidationIssue[] = []
  const add = (code: string, message: string, pageId?: string, objectId?: string) => issues.push({ code, message, ...(pageId ? { pageId } : {}), ...(objectId ? { objectId } : {}) })
  if (!input || input.schemaVersion !== LECTURE_SCHEMA_VERSION) add('schema.version', 'Unsupported lecture schema version')
  if (!Number.isSafeInteger(input?.revision) || input.revision < 1) add('schema.revision', 'Revision must be a positive integer')
  if (!Array.isArray(input?.slides) || input.slides.length < 1 || input.slides.length > 100) add('deck.pages', 'Deck must contain 1–100 slides')
  const snapshots = new Map((input?.evidence ?? []).map(snapshot => [snapshot.id, snapshot]))
  const pageIds = new Set<string>()
  const orders = new Set<number>()
  const chapterIds = new Set(input?.course?.chapters?.map(chapter => chapter.id) ?? [])
  for (const slide of input?.slides ?? []) {
    if (!validPageId.test(slide.id) || pageIds.has(slide.id)) add('slide.id', 'Page ID is invalid or duplicated', slide.id)
    pageIds.add(slide.id)
    if (!Number.isSafeInteger(slide.order) || slide.order < 0 || orders.has(slide.order)) add('slide.order', 'Slide order is invalid or duplicated', slide.id)
    orders.add(slide.order)
    if (!chapterIds.has(slide.chapterId)) add('slide.chapter', 'Slide references an unknown chapter', slide.id)
    if (!slide.title?.trim() || !slide.conclusion?.trim() || !slide.bodyHtml?.trim()) add('slide.content', 'Slide content is incomplete', slide.id)
    if (forbiddenMarkup.test(slide.bodyHtml)) add('security.markup', 'Unsafe or online markup is forbidden', slide.id)
    const anchors = new Map<string, SlideSpec['anchors'][number]>()
    for (const anchor of slide.anchors ?? []) {
      if (!anchor.id?.trim() || anchors.has(anchor.id) || [anchor.x, anchor.y, anchor.width, anchor.height].some(value => !Number.isFinite(value))
        || anchor.width <= 0 || anchor.height <= 0 || anchor.x < 0 || anchor.y < 0 || anchor.x + anchor.width > 1280 || anchor.y + anchor.height > 720) {
        add('anchor.geometry', 'Anchor is duplicated or outside the 1280×720 canvas', slide.id, anchor.id)
      }
      anchors.set(anchor.id, anchor)
      const escaped = anchor.id.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
      if (!new RegExp(`data-anchor-id\\s*=\\s*["']${escaped}["']`).test(slide.bodyHtml)) add('anchor.dom', 'Anchor has no matching rendered element', slide.id, anchor.id)
    }
    const claimIds = new Set<string>()
    for (const binding of slide.bindings ?? []) {
      if (!binding.claimId?.trim() || claimIds.has(binding.claimId)) add('citation.claim', 'Claim ID is missing or duplicated', slide.id, binding.claimId)
      claimIds.add(binding.claimId)
      if (binding.kind === 'teaching-example') {
        if (binding.evidenceMarkers.length) add('citation.example', 'Teaching examples cannot claim source evidence', slide.id, binding.claimId)
        continue
      }
      const snapshot = snapshots.get(binding.snapshotId)
      if (!snapshot) { add('citation.snapshot', 'Claim references an unknown evidence snapshot', slide.id, binding.claimId); continue }
      const markers = new Set(snapshot.items.map(item => item.marker))
      if (!binding.evidenceMarkers.length || binding.evidenceMarkers.some(marker => !markers.has(marker))) add('citation.marker', 'Claim references unknown evidence', slide.id, binding.claimId)
    }
    const steps = new Set<string>()
    for (const step of slide.steps ?? []) {
      if (!step.id?.trim() || steps.has(step.id)) add('step.id', 'Step ID is missing or duplicated', slide.id, step.id)
      steps.add(step.id)
      if (!step.anchorIds.length || step.anchorIds.some(id => !anchors.has(id))) add('step.anchor', 'Step references an unknown anchor', slide.id, step.id)
      if (step.claimIds.some(id => !claimIds.has(id))) add('step.claim', 'Step references an unknown claim', slide.id, step.id)
    }
  }
  const expectedOrders = Array.from({ length: input?.slides?.length ?? 0 }, (_, index) => index)
  if (JSON.stringify([...orders].sort((a, b) => a - b)) !== JSON.stringify(expectedOrders)) add('deck.order', 'Slide order must be contiguous from zero')
  for (const chapter of input?.course?.chapters ?? []) {
    if (chapter.slideIds.some(id => !pageIds.has(id))) add('chapter.slide', 'Chapter references an unknown slide', undefined, chapter.id)
  }
  if (input?.contentHash) {
    const { contentHash: _ignored, ...unhashed } = input
    if (input.contentHash !== contentHash(unhashed)) add('manifest.hash', 'Manifest content hash does not match')
  }
  return { passed: issues.length === 0, issues }
}

export function assertValidDeck(deck: DeckManifest): DeckManifest {
  const report = validateDeck(deck)
  if (!report.passed) throw new Error(`lecture validation failed: ${report.issues.map(issue => `${issue.code}${issue.pageId ? `(${issue.pageId})` : ''}`).join(', ')}`)
  return deck
}
