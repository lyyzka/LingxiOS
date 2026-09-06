import { createHash } from 'node:crypto'
import type { DeckManifest, EvidenceBinding, LectureAnchor } from './contracts.js'
import { assertValidDeck } from './validation.js'

export interface LectureArtifact { filename: 'lecture.html'; mime: 'text/html'; size: number; sha256: string; bytes: Uint8Array }

const escapeHtml = (value: string) => value.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;').replaceAll("'", '&#39;')
const safeJson = (value: unknown) => JSON.stringify(value).replaceAll('<', '\\u003c').replaceAll('\u2028', '\\u2028').replaceAll('\u2029', '\\u2029')

export function buildStandaloneLecture(input: DeckManifest, maxBytes = 16 * 1024 * 1024): LectureArtifact {
  const deck = assertValidDeck(structuredClone(input))
  const html = `<!doctype html>
<html lang="${escapeHtml(deck.language)}"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="color-scheme" content="dark"><title>${escapeHtml(deck.title)}</title><style>
:root{--bg:${deck.theme.background};--surface:${deck.theme.surface};--text:${deck.theme.text};--muted:${deck.theme.muted};--accent:${deck.theme.accent};font-family:${deck.theme.fontFamily ?? 'system-ui,sans-serif'}}
*{box-sizing:border-box}html,body{width:100%;height:100%;margin:0;overflow:hidden;background:var(--bg);color:var(--text)}button{font:inherit;color:inherit}
#viewport{position:absolute;inset:0;perspective:1400px;touch-action:none;overflow:hidden}.fit{position:absolute;left:50%;top:50%;width:1280px;height:720px;transform-origin:center}.interaction,.spatial,.camera{width:100%;height:100%;transform-style:preserve-3d;transition:transform .55s cubic-bezier(.2,.8,.2,1)}
.slide{position:absolute;inset:0;padding:56px 68px;background:var(--bg);overflow:hidden}.slide h1{font-size:44px;line-height:1.08;margin:0 0 22px}.slide .conclusion{font-size:22px;color:var(--accent);max-width:1050px}.slide-body{position:absolute;left:68px;right:68px;top:150px;bottom:54px}.slide svg{max-width:100%;max-height:100%}
#hud{position:absolute;inset:0;pointer-events:none}#progress{position:absolute;left:20px;bottom:16px;color:var(--muted);font-size:14px}.controls{position:absolute;right:18px;bottom:16px;display:flex;gap:8px;opacity:.18;transition:opacity .2s;pointer-events:auto}.controls:hover,.controls:focus-within{opacity:1}.controls button,#close-cite{border:1px solid color-mix(in srgb,var(--text) 25%,transparent);background:color-mix(in srgb,var(--surface) 90%,transparent);border-radius:10px;padding:8px 12px;cursor:pointer}.controls button:focus-visible,#close-cite:focus-visible{outline:3px solid var(--accent)}
#notes{position:absolute;right:22px;top:22px;width:min(390px,40vw);max-height:calc(100% - 90px);overflow:auto;padding:18px 20px;border:1px solid color-mix(in srgb,var(--text) 20%,transparent);border-radius:14px;background:color-mix(in srgb,var(--surface) 94%,transparent);box-shadow:0 18px 60px #0008;transform:translateX(calc(100% + 40px));transition:transform .3s;pointer-events:auto}#notes.open{transform:none}#notes h2{font-size:19px;margin:0 36px 8px 0}#notes p{line-height:1.5;color:var(--muted)}
.cite{border:0;border-bottom:1px dotted var(--accent);background:none;color:var(--accent);padding:0;cursor:pointer}.source{padding:10px 0;border-top:1px solid #ffffff1f}.source a{color:var(--accent)}
@media(prefers-reduced-motion:reduce){.interaction,.spatial,.camera,#notes{transition:none}}
</style></head><body><main id="viewport" aria-label="${escapeHtml(deck.title)}"><div class="fit"><div class="interaction"><div class="spatial"><div class="camera" id="camera"></div></div></div></div></main>
<div id="hud"><div id="progress" aria-live="polite"></div><div class="controls" aria-label="课件导航"><button id="prev" aria-label="上一页">←</button><button id="step" aria-label="下一讲解步骤">聚焦</button><button id="next" aria-label="下一页">→</button><button id="reset" aria-label="复位视图">复位</button></div><aside id="notes" aria-live="polite"><button id="close-cite" style="float:right" aria-label="关闭说明">×</button><div id="note-content"></div></aside></div>
<script type="application/json" id="lecture-data">${safeJson(deck)}</script><script>
(()=>{'use strict';const deck=JSON.parse(document.getElementById('lecture-data').textContent);const viewport=document.getElementById('viewport'),fit=document.querySelector('.fit'),interaction=document.querySelector('.interaction'),spatial=document.querySelector('.spatial'),camera=document.getElementById('camera'),notes=document.getElementById('notes'),noteContent=document.getElementById('note-content'),progress=document.getElementById('progress');let page=0,step=-1,panX=0,panY=0,zoom=1,drag=null,clickStart=null;
const esc=s=>String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function resize(){const s=Math.min(innerWidth/1280,innerHeight/720);fit.style.transform='translate(-50%,-50%) scale('+s+')';if(step>=0)focus(step)}
function claims(slide){const map=new Map;for(const b of slide.bindings)map.set(b.claimId,b);return map}
function render(){const s=deck.slides[page];step=-1;camera.innerHTML='<article class="slide" data-page-id="'+esc(s.id)+'"><h1>'+esc(s.title)+'</h1><div class="conclusion">'+esc(s.conclusion)+'</div><div class="slide-body">'+s.bodyHtml+'</div></article>';progress.textContent=(page+1)+' / '+deck.slides.length+' · '+s.title;reset();notes.classList.remove('open');document.title=s.title+' — '+deck.title}
function reset(){panX=panY=0;zoom=1;interaction.style.transform='';spatial.style.transform='';camera.style.transform=''}
function focus(index){const s=deck.slides[page];if(!s.steps.length)return;step=index===undefined?(step+1)%s.steps.length:index;const item=s.steps[step],slide=camera.querySelector('.slide'),base=slide.getBoundingClientRect(),sx=1280/base.width,sy=720/base.height,targets=item.anchorIds.map(id=>[...slide.querySelectorAll('[data-anchor-id]')].find(el=>el.getAttribute('data-anchor-id')===id)).filter(Boolean).map(el=>{const r=el.getBoundingClientRect();return{x:(r.left-base.left)*sx,y:(r.top-base.top)*sy,width:r.width*sx,height:r.height*sy}});if(!targets.length)return;let x=Math.min(...targets.map(a=>a.x)),y=Math.min(...targets.map(a=>a.y)),r=Math.max(...targets.map(a=>a.x+a.width)),b=Math.max(...targets.map(a=>a.y+a.height));const panel=innerWidth<900?0:390,scale=Math.min(2.4,Math.max(1,Math.min((1280-panel-130)/(r-x),(720-130)/(b-y))));camera.style.transform='translate('+(640-(x+r)/2-panel/3)+'px,'+(360-(y+b)/2)+'px) scale('+scale+')';spatial.style.transform='rotateY('+(step%2?'-2.5':'2.5')+'deg) translateZ(12px)';noteContent.innerHTML='<h2>'+esc(item.title)+'</h2><p>'+esc(item.explanation)+'</p>'+item.claimIds.map(id=>'<button class="cite" data-claim="'+esc(id)+'">查看依据：'+esc(claims(s).get(id)?.statement||id)+'</button>').join('<br>');notes.classList.add('open')}
function cite(id){const s=deck.slides[page],binding=claims(s).get(id);if(!binding)return;const snapshot=deck.evidence.find(x=>x.id===binding.snapshotId),items=snapshot?snapshot.items.filter(x=>binding.evidenceMarkers.includes(x.marker)):[];noteContent.innerHTML='<h2>'+esc(binding.statement)+'</h2>'+(binding.kind==='teaching-example'?'<p>教学示例数据，不代表实测结果。</p>':items.map(x=>'<div class="source"><strong>'+esc(x.title)+'</strong><p>'+esc(x.excerpt)+'</p><small>'+esc(x.sourceVersion)+' · '+esc(x.chunkId)+'</small>'+(x.url?'<br><a target="_blank" rel="noopener noreferrer" href="'+esc(x.url)+'">打开原始来源</a>':'')+'</div>').join(''));notes.classList.add('open')}
function go(delta){page=Math.max(0,Math.min(deck.slides.length-1,page+delta));render()}
document.getElementById('prev').onclick=()=>go(-1);document.getElementById('next').onclick=()=>go(1);document.getElementById('step').onclick=focus;document.getElementById('reset').onclick=reset;document.getElementById('close-cite').onclick=()=>notes.classList.remove('open');noteContent.onclick=e=>{const button=e.target.closest('[data-claim]');if(button)cite(button.dataset.claim)};
addEventListener('keydown',e=>{if(e.key==='ArrowRight'||e.key==='PageDown'){e.preventDefault();go(1)}else if(e.key==='ArrowLeft'||e.key==='PageUp'){e.preventDefault();go(-1)}else if(e.key===' '||e.key==='Enter'){e.preventDefault();focus()}else if(e.key==='Escape')notes.classList.remove('open')});
viewport.addEventListener('wheel',e=>{e.preventDefault();zoom=Math.max(.5,Math.min(3,zoom*Math.exp(-e.deltaY*.001)));interaction.style.transform='translate('+panX+'px,'+panY+'px) scale('+zoom+')'},{passive:false});viewport.addEventListener('pointerdown',e=>{if(e.target.closest('button,a,#notes'))return;drag={x:e.clientX,y:e.clientY,panX,panY};clickStart={x:e.clientX,y:e.clientY};viewport.setPointerCapture(e.pointerId)});viewport.addEventListener('pointermove',e=>{if(!drag)return;panX=drag.panX+(e.clientX-drag.x)/Math.max(.1,Math.min(innerWidth/1280,innerHeight/720));panY=drag.panY+(e.clientY-drag.y)/Math.max(.1,Math.min(innerWidth/1280,innerHeight/720));interaction.style.transform='translate('+panX+'px,'+panY+'px) scale('+zoom+')'});viewport.addEventListener('pointerup',e=>{if(drag&&clickStart&&Math.hypot(e.clientX-clickStart.x,e.clientY-clickStart.y)<8)go(e.clientX<innerWidth/2?-1:1);drag=null;clickStart=null});viewport.ondblclick=reset;addEventListener('resize',resize);resize();render()})();
</script></body></html>`
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
