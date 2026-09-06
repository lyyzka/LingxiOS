import type { DeckManifest, SlideSpec, ValidationIssue, ValidationReport } from './contracts.js'
import { LECTURE_SCHEMA_VERSION, contentHash } from './contracts.js'

const validPageId = /^pg_[a-zA-Z0-9_-]{1,80}$/
const tags = new Set(['div', 'section', 'article', 'header', 'footer', 'p', 'span', 'strong', 'b', 'em', 'small', 'br', 'ul', 'ol', 'li', 'h1', 'h2', 'h3', 'h4', 'code', 'pre', 'table', 'thead', 'tbody', 'tr', 'th', 'td', 'svg', 'g', 'defs', 'marker', 'path', 'line', 'polyline', 'polygon', 'rect', 'circle', 'ellipse', 'text', 'tspan', 'image', 'use', 'title', 'desc'])
const voidTags = new Set(['br'])
const attributes = new Set(['id', 'class', 'role', 'viewbox', 'xmlns', 'width', 'height', 'x', 'y', 'x1', 'y1', 'x2', 'y2', 'cx', 'cy', 'r', 'rx', 'ry', 'd', 'points', 'fill', 'fill-opacity', 'stroke', 'stroke-width', 'stroke-linecap', 'stroke-linejoin', 'stroke-dasharray', 'opacity', 'transform', 'preserveaspectratio', 'marker-start', 'marker-mid', 'marker-end', 'refx', 'refy', 'markerwidth', 'markerheight', 'orient', 'font-size', 'font-weight', 'text-anchor', 'dominant-baseline', 'dx', 'dy', 'colspan', 'rowspan', 'href', 'src'])
const decodeEntities = (value: string) => value.replace(/&#(x[0-9a-f]+|\d+);?/gi, (_, code: string) => String.fromCodePoint(code[0]?.toLowerCase() === 'x' ? Number.parseInt(code.slice(1), 16) : Number(code)))
  .replace(/&(colon|tab|newline|amp|quot|apos|lt|gt);/gi, (_, name: string) => ({ colon: ':', tab: '\t', newline: '\n', amp: '&', quot: '"', apos: "'", lt: '<', gt: '>' })[name.toLowerCase()]!)

/** Conservative HTML/SVG allowlist. Generated slides do not need arbitrary web markup. */
export function validateLectureMarkup(markup: string): string[] {
  const errors: string[] = [], stack: string[] = [], tokens = /<!--[\s\S]*?-->|<\/?[^>]+>/g
  let cursor = 0, token: RegExpExecArray | null
  while ((token = tokens.exec(markup))) {
    if (markup.slice(cursor, token.index).includes('<')) errors.push('Malformed tag')
    cursor = token.index + token[0].length
    if (token[0].startsWith('<!--') || /^<!|^<\?/i.test(token[0])) { errors.push('Comments and declarations are forbidden'); continue }
    const closing = /^<\//.test(token[0]), match = /^<\/?\s*([a-z][\w-]*)/i.exec(token[0])
    if (!match) { errors.push('Malformed tag'); continue }
    const name = match[1]!.toLowerCase()
    if (!tags.has(name)) errors.push(`Forbidden tag: ${name}`)
    if (closing) { if (stack.pop() !== name) errors.push(`Mismatched closing tag: ${name}`); continue }
    const selfClosing = /\/\s*>$/.test(token[0]), tail = token[0].slice(match[0].length, token[0].length - (selfClosing ? 2 : 1))
    const attr = /\s+([:\w-]+)(?:\s*=\s*(?:"([^"]*)"|'([^']*)'))?/gy
    let offset = 0, item: RegExpExecArray | null
    const seen = new Set<string>()
    while (offset < tail.length) {
      attr.lastIndex = offset; item = attr.exec(tail)
      if (!item) { if (tail.slice(offset).trim()) errors.push('Malformed or unquoted attribute'); break }
      offset = attr.lastIndex
      const rawName = item[1]!, attrName = rawName.toLowerCase(), value = decodeEntities(item[2] ?? item[3] ?? '')
      if (seen.has(attrName)) errors.push(`Duplicate attribute: ${attrName}`)
      seen.add(attrName)
      if (item[2] === undefined && item[3] === undefined) errors.push(`Boolean attribute is forbidden: ${attrName}`)
      if (!(attributes.has(attrName) || attrName === 'data-anchor-id' || /^aria-[\w-]+$/.test(attrName))) errors.push(`Forbidden attribute: ${rawName}`)
      if (/^on/i.test(attrName) || attrName === 'style' || attrName === 'srcdoc') errors.push(`Active attribute is forbidden: ${rawName}`)
      if (attrName === 'href' && !/^#[\w.-]+$/.test(value.replace(/[\u0000-\u0020]+/g, ''))) errors.push('Only local fragment href values are allowed')
      if (attrName === 'src' && !/^data:image\/(?:png|jpeg|gif|webp);base64,[a-z0-9+/]+=*$/i.test(value)) errors.push('Only embedded raster image sources are allowed')
      if (/[<>]/.test(value)) errors.push(`Markup is forbidden in attribute: ${rawName}`)
    }
    if (!selfClosing && !voidTags.has(name)) stack.push(name)
  }
  if (markup.slice(cursor).includes('<')) errors.push('Malformed tag')
  if (stack.length) errors.push('Unclosed tag')
  return [...new Set(errors)]
}

export function validateDeck(input: DeckManifest): ValidationReport {
  const issues: ValidationIssue[] = []
  const add = (code: string, message: string, pageId?: string, objectId?: string) => issues.push({ code, message, ...(pageId ? { pageId } : {}), ...(objectId ? { objectId } : {}) })
  if (!input || input.schemaVersion !== LECTURE_SCHEMA_VERSION) add('schema.version', 'Unsupported lecture schema version')
  if (!Number.isSafeInteger(input?.revision) || input.revision < 1) add('schema.revision', 'Revision must be a positive integer')
  if (!Array.isArray(input?.slides) || input.slides.length < 1 || input.slides.length > 100) add('deck.pages', 'Deck must contain 1–100 slides')
  if (!input?.theme || ['background', 'surface', 'text', 'muted', 'accent'].some(key => !/^#[0-9a-f]{6}$/i.test(String(input.theme[key as keyof typeof input.theme] ?? '')))
    || input.theme.fontFamily && !/^[\w\s,"'-]+$/u.test(input.theme.fontFamily)) add('theme.tokens', 'Theme contains unsafe or invalid CSS tokens')
  const snapshots = new Map((input?.evidence ?? []).map(snapshot => [snapshot.id, snapshot]))
  const pageIds = new Set<string>(), orders = new Set<number>(), chapterIds = new Set(input?.course?.chapters?.map(chapter => chapter.id) ?? [])
  for (const slide of input?.slides ?? []) {
    if (!validPageId.test(slide.id) || pageIds.has(slide.id)) add('slide.id', 'Page ID is invalid or duplicated', slide.id)
    pageIds.add(slide.id)
    if (!Number.isSafeInteger(slide.order) || slide.order < 0 || orders.has(slide.order)) add('slide.order', 'Slide order is invalid or duplicated', slide.id)
    orders.add(slide.order)
    if (!chapterIds.has(slide.chapterId)) add('slide.chapter', 'Slide references an unknown chapter', slide.id)
    if (!slide.title?.trim() || !slide.conclusion?.trim() || !slide.bodyHtml?.trim()) add('slide.content', 'Slide content is incomplete', slide.id)
    for (const message of validateLectureMarkup(slide.bodyHtml ?? '')) add('security.markup', message, slide.id)
    const instructional = !['cover', 'section', 'ending'].includes(slide.role)
    if (instructional && !/<svg\b/i.test(slide.bodyHtml)) add('visual.missing', 'Instructional slides require a meaningful SVG visual', slide.id)
    if (/<svg\b/i.test(slide.bodyHtml) && !/<svg\b[^>]*(?:role=["']img["']|aria-label=["'][^"']+["'])/i.test(slide.bodyHtml) && !/<title\b/i.test(slide.bodyHtml)) add('visual.accessibility', 'SVG requires an accessible label or title', slide.id)
    const anchors = new Map<string, SlideSpec['anchors'][number]>()
    for (const anchor of slide.anchors ?? []) {
      if (!anchor.id?.trim() || anchors.has(anchor.id) || [anchor.x, anchor.y, anchor.width, anchor.height].some(value => !Number.isFinite(value))
        || anchor.width <= 0 || anchor.height <= 0 || anchor.x < 0 || anchor.y < 0 || anchor.x + anchor.width > 1280 || anchor.y + anchor.height > 720) add('anchor.geometry', 'Anchor is duplicated or outside the 1280×720 canvas', slide.id, anchor.id)
      anchors.set(anchor.id, anchor)
      const escaped = anchor.id.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
      if (!new RegExp(`data-anchor-id\\s*=\\s*["']${escaped}["']`).test(slide.bodyHtml)) add('anchor.dom', 'Anchor has no matching rendered element', slide.id, anchor.id)
    }
    if (instructional && (!anchors.size || Math.max(0, ...[...anchors.values()].map(anchor => anchor.width * anchor.height)) < 1280 * 720 * .05)) add('visual.primary', 'Instructional slide requires a substantial anchored primary visual', slide.id)
    const claimIds = new Set<string>()
    for (const binding of slide.bindings ?? []) {
      if (!binding.claimId?.trim() || claimIds.has(binding.claimId)) add('citation.claim', 'Claim ID is missing or duplicated', slide.id, binding.claimId)
      claimIds.add(binding.claimId)
      if (binding.kind === 'teaching-example') { if (binding.evidenceMarkers.length) add('citation.example', 'Teaching examples cannot claim source evidence', slide.id, binding.claimId); continue }
      const snapshot = snapshots.get(binding.snapshotId)
      if (!snapshot) { add('citation.snapshot', 'Claim references an unknown evidence snapshot', slide.id, binding.claimId); continue }
      const markers = new Set(snapshot.items.map(item => item.marker))
      if (!binding.evidenceMarkers.length || binding.evidenceMarkers.some(marker => !markers.has(marker))) add('citation.marker', 'Claim references unknown evidence', slide.id, binding.claimId)
    }
    if (instructional && !claimIds.size) add('citation.missing', 'Instructional slide requires claim-level evidence or an explicit teaching example', slide.id)
    const steps = new Set<string>(), usedClaims = new Set<string>()
    for (const step of slide.steps ?? []) {
      if (!step.id?.trim() || steps.has(step.id)) add('step.id', 'Step ID is missing or duplicated', slide.id, step.id)
      steps.add(step.id)
      if (!step.anchorIds.length || step.anchorIds.some(id => !anchors.has(id))) add('step.anchor', 'Step references an unknown anchor', slide.id, step.id)
      if (!step.claimIds.length || step.claimIds.some(id => !claimIds.has(id))) add('step.claim', 'Step requires known claim references', slide.id, step.id)
      step.claimIds.forEach(id => usedClaims.add(id))
    }
    if (instructional && !steps.size) add('step.missing', 'Instructional slide requires at least one explanation step', slide.id)
    if ([...claimIds].some(id => !usedClaims.has(id))) add('citation.unused', 'Every claim must be explained by a step', slide.id)
  }
  const expectedOrders = Array.from({ length: input?.slides?.length ?? 0 }, (_, index) => index)
  if (JSON.stringify([...orders].sort((a, b) => a - b)) !== JSON.stringify(expectedOrders)) add('deck.order', 'Slide order must be contiguous from zero')
  for (const chapter of input?.course?.chapters ?? []) if (chapter.slideIds.some(id => !pageIds.has(id))) add('chapter.slide', 'Chapter references an unknown slide', undefined, chapter.id)
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
