import type { EvidenceItem } from '../context/evidence.js'
import type { LectureEvidenceProvider } from './service.js'

export interface OpenNotebookSearchHit {
  sourceId: string; sourceVersion: string; chunkId: string; title: string; text: string; url?: string; sourceType?: 'source' | 'note' | 'source_insight'; truncated?: boolean
}
export interface OpenNotebookClient {
  search(input: { tenantId: string; principalId: string; sourceIds: string[]; query: string; limit: number; signal?: AbortSignal }): Promise<OpenNotebookSearchHit[]>
}
export interface LectureSourceAuthorizer { allowedSourceIds(tenantId: string, principalId: string, requested: readonly string[]): Promise<string[]> }

export class OpenNotebookLectureAdapter implements LectureEvidenceProvider {
  constructor(private readonly client: OpenNotebookClient, private readonly authorizer: LectureSourceAuthorizer) {}
  async search(input: Parameters<LectureEvidenceProvider['search']>[0]): Promise<EvidenceItem[]> {
    const allowed = await this.authorizer.allowedSourceIds(input.tenantId, input.principalId, input.sourceIds)
    if (new Set(allowed).size !== allowed.length || input.sourceIds.some(id => !allowed.includes(id))) throw new Error('one or more lecture sources are not authorized')
    const hits = (await Promise.all(input.queries.map(query => this.client.search({ ...input, sourceIds: allowed, query, limit: Math.max(1, Math.ceil(input.limit / input.queries.length)) })))).flat()
    const seen = new Set<string>(), items: EvidenceItem[] = []
    for (const hit of hits) {
      if (!allowed.includes(hit.sourceId)) throw new Error('Open Notebook returned an out-of-scope source')
      const key = `${hit.sourceId}\0${hit.sourceVersion}\0${hit.chunkId}`
      if (seen.has(key)) continue
      seen.add(key)
      items.push({ marker: `S${items.length + 1}`, sourceId: hit.sourceId, sourceVersion: hit.sourceVersion, chunkId: hit.chunkId,
        title: hit.sourceType && hit.sourceType !== 'source' ? `${hit.title} (${hit.sourceType})` : hit.title, excerpt: hit.text,
        ...(hit.url ? { url: hit.url } : {}), ...(hit.truncated ? { truncated: true } : {}) })
      if (items.length === input.limit) break
    }
    return items
  }
}
