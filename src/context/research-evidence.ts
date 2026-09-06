import { createHash } from 'node:crypto'
import type { HostActionResult } from '../protocol/types.js'
import { snapshotEvidence, type EvidenceSnapshot } from './evidence.js'

/** Promote only the text actually returned by a successful recorded read. */
export function appendResearchEvidence(snapshot: EvidenceSnapshot, actionKey: string, result: HostActionResult): EvidenceSnapshot {
  if (!result.ok || result.executionState === 'unknown' || result.directive || !result.value || typeof result.value !== 'object') return snapshot
  const value = result.value as Record<string, unknown>
  if (typeof value['text'] !== 'string' || !value['text'].trim() || value['text'].length > 65_536
    || typeof value['finalUrl'] !== 'string' || !/^https?:\/\//.test(value['finalUrl'])
    || typeof value['sha256'] !== 'string' || !/^[a-f0-9]{64}$/.test(value['sha256'])) return snapshot
  if (snapshot.items.some(item => item.actionKey === actionKey
    || item.sourceId === value['finalUrl'] && item.sourceVersion === `sha256:${value['sha256']}` && item.excerpt === value['text'] && Boolean(item.truncated) === (value['truncated'] === true))
    || snapshot.items.length >= 200) return snapshot
  let number = 1
  while (snapshot.items.some(item => item.marker === `S${number}`)) number++
  const items = [...snapshot.items, { marker: `S${number}`, actionKey, sourceId: value['finalUrl'],
    sourceVersion: `sha256:${value['sha256']}`, chunkId: actionKey, title: value['finalUrl'].slice(0, 2000),
    excerpt: value['text'], url: value['finalUrl'], ...(value['truncated'] === true ? { truncated: true } : {}) }]
  return snapshotEvidence(`evidence:${createHash('sha256').update(JSON.stringify(items)).digest('hex')}`, items)
}
