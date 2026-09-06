import { createHash } from 'node:crypto'
import { readFileSync } from 'node:fs'
import type { DeckManifest, EvidenceBinding, LectureAnchor } from './contracts.js'
import { assertValidDeck } from './validation.js'

export interface LectureArtifact { filename: 'lecture.html'; mime: 'text/html'; size: number; sha256: string; bytes: Uint8Array }

const escapeHtml = (value: string) => value.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;').replaceAll("'", '&#39;')
const safeJson = (value: unknown) => JSON.stringify(value).replaceAll('<', '\\u003c').replaceAll('\u2028', '\\u2028').replaceAll('\u2029', '\\u2029')
const runtime = readFileSync(new URL('./runtime.html', import.meta.url), 'utf8')
const bundleSlot = '<script id="zoomLectureBundle" type="application/json">null</script>'

const academicCss = `<style>
:root{--paper:#fbfaf7;--surface:#fff;--sunken:#f4f2ec;--ink-1:#23231f;--ink-2:#5f5e5a;--ink-3:#8a8880;--rule:rgba(35,35,31,.14);--rule-strong:rgba(35,35,31,.30);--accent:#534ab7;--font-serif:"Songti SC","Source Han Serif SC","Noto Serif SC","SimSun",Georgia,"Times New Roman",serif;--font-sans:-apple-system,BlinkMacSystemFont,"PingFang SC","Microsoft YaHei","Source Han Sans SC","Noto Sans SC","Segoe UI",Roboto,sans-serif;--font-mono:ui-monospace,SFMono-Regular,"SF Mono",Menlo,Consolas,monospace}
*{box-sizing:border-box}html,body{margin:0;width:1280px;height:720px;overflow:hidden;background:var(--paper);color:var(--ink-1);font-family:var(--font-sans)}.slide{position:relative;width:1280px;height:720px;overflow:hidden;background:var(--paper)}.eyebrow{position:absolute;z-index:2;left:64px;top:48px;font:400 14px/1.4 var(--font-mono);letter-spacing:.08em;color:var(--ink-3)}h1{position:absolute;z-index:2;left:64px;top:72px;width:1100px;margin:0;font:500 38px/1.24 var(--font-serif)}.opening h1{top:214px;width:900px;font-size:58px;line-height:1.18}.closing h1{top:230px;width:900px;font-size:50px}.visual{position:absolute}.visual>svg{display:block;width:100%;height:100%;max-width:100%;max-height:100%}svg text{fill:var(--ink-1)!important;font-family:var(--font-sans)!important;font-weight:400!important}.t,.ts,.tn{font-weight:400!important}.th{font-weight:500!important}.anchor{position:absolute;pointer-events:none}.footer{position:absolute;z-index:2;left:64px;right:64px;bottom:44px;padding-top:14px;border-top:.5px solid var(--rule);font:400 15px/1.5 var(--font-sans);color:var(--ink-3)}.page{float:right;font-family:var(--font-mono)}
</style>`

function normalizeSvgText(html: string) {
  return html.replace(/<text\b([^>]*)>/gi, (_tag, attrs: string) => /\bclass\s*=/.test(attrs)
    ? `<text${attrs.replace(/\bclass=(['"])(.*?)\1/i, (_match, quote: string, classes: string) => `class=${quote}${classes} t${quote}`)}>`
    : `<text class="t"${attrs}>`)
}

function standaloneBundle(deck: DeckManifest) {
  const slides: Record<string, string> = {}
  const lectureSlides = deck.slides.map((slide, index) => {
    const role = index === 0 ? 'opening' : index === deck.slides.length - 1 ? 'closing' : 'content'
    const file = `slides/s${String(index + 1).padStart(2, '0')}.html`
    const bounds = focusBounds(slide.anchors)
    const visual = normalizeSvgText(slide.bodyHtml)
    const markers = slide.anchors.map(anchor => `<i class="anchor" data-anchor="${escapeHtml(anchor.id)}" data-rect="${anchor.x} ${anchor.y} ${anchor.width} ${anchor.height}" style="left:${anchor.x}px;top:${anchor.y}px;width:${anchor.width}px;height:${anchor.height}px"></i>`).join('')
    slides[file] = `<!doctype html><html lang="${escapeHtml(deck.language)}"><head><meta charset="utf-8">${academicCss}<title>${escapeHtml(slide.title)}</title></head><body><main class="slide ${role}" data-slide-id="${escapeHtml(slide.id)}" data-slide-role="${role}" data-canvas="1280x720" data-style="anthropic-academic"><div class="eyebrow">${role === 'opening' ? 'LECTURE' : role === 'closing' ? 'CONCLUSION' : escapeHtml(slide.title)}</div><h1>${escapeHtml(slide.conclusion || slide.title)}</h1><div class="visual" data-visual="${escapeHtml(slide.visualKind)}" style="left:${bounds.x}px;top:${bounds.y}px;width:${bounds.right - bounds.x}px;height:${bounds.bottom - bounds.y}px">${visual}</div>${markers}<div class="footer">${escapeHtml(deck.title)}<span class="page">${String(index + 1).padStart(2, '0')}</span></div></main></body></html>`
    const anchors = slide.anchors.map(anchor => ({ id: anchor.id, label: slide.steps.find(step => step.anchorIds.includes(anchor.id))?.title ?? anchor.id, rect: { x: anchor.x, y: anchor.y, w: anchor.width, h: anchor.height } }))
    const steps: Record<string, unknown>[] = [{ id: `${slide.id}-overview`, order: 1, kind: 'overview', camera: { mode: 'fit' }, advance: 'manual' }]
    if (role === 'content') slide.steps.forEach((step, stepIndex) => steps.push({ id: step.id, order: stepIndex + 2, kind: 'zoom', camera: { mode: 'anchor', anchorId: step.anchorIds[0] }, highlight: { anchorIds: step.anchorIds, style: 'outline' }, panel: { placement: 'auto', title: step.title, body: step.explanation }, advance: 'manual' }))
    return { id: slide.id, index: index + 1, role, file, title: slide.title, anchors, steps }
  })
  return { lecture: { schemaVersion: 'zoom-lecture/v2', deck: { id: deck.deckId, title: deck.title, language: 'zh-CN', style: 'anthropic-academic', canvas: { width: 1280, height: 720, format: 'ppt169' }, slideDir: 'slides', createdAt: deck.createdAt }, defaults: { camera: { padding: 28 }, transition: { inMs: 980, outMs: 700, easing: 'zoomOut' }, highlight: { style: 'outline' }, panel: { placement: 'auto', width: 420 } }, slides: lectureSlides }, slides }
}

export function buildStandaloneLecture(input: DeckManifest, maxBytes = 16 * 1024 * 1024): LectureArtifact {
  const deck = assertValidDeck(structuredClone(input))
  const html = runtime.replace(bundleSlot, `<script id="zoomLectureBundle" type="application/json">${safeJson(standaloneBundle(deck))}</script>`)
  const bytes = new TextEncoder().encode(html)
  if (bytes.length > maxBytes) throw new Error(`lecture artifact exceeds the ${maxBytes} byte budget`)
  return { filename: 'lecture.html', mime: 'text/html', size: bytes.length, sha256: createHash('sha256').update(bytes).digest('hex'), bytes }
}

export function bindingSources(deck: DeckManifest, binding: EvidenceBinding) {
  return deck.evidence.find(snapshot => snapshot.id === binding.snapshotId)?.items.filter(item => binding.evidenceMarkers.includes(item.marker)) ?? []
}

export function focusBounds(anchors: readonly LectureAnchor[]) {
  if (!anchors.length) throw new Error('at least one anchor is required')
  return { x: Math.min(...anchors.map(item => item.x)), y: Math.min(...anchors.map(item => item.y)),
    right: Math.max(...anchors.map(item => item.x + item.width)), bottom: Math.max(...anchors.map(item => item.y + item.height)) }
}
