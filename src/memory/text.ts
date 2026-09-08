import { createHash } from 'node:crypto'

export const MEMORY_BODY_BYTES = 16 * 1024
const segmenter = new Intl.Segmenter('zh', { granularity: 'word' })

/** Segment before PostgreSQL's simple dictionary so Chinese does not become a whole-sentence token. */
export function memorySearchText(text: string): string {
  return [...segmenter.segment(text.normalize('NFKC').toLowerCase())]
    .filter(item => item.isWordLike).map(item => item.segment).join(' ')
}

export function memoryQuery(query: string): string {
  if (typeof query !== 'string' || query.length > 2000) throw new Error('invalid memory query')
  return [...new Set(memorySearchText(query).split(' ').filter(Boolean))].slice(0, 64)
    .map(word => `'${word.replaceAll("'", "''")}'`).join(' | ')
}

export function memoryDigest(value: unknown): string {
  return createHash('sha256').update(JSON.stringify(value)).digest('hex')
}

export function validateMemoryPath(value: unknown): asserts value is string {
  if (typeof value !== 'string' || !value.endsWith('.md') || value.length > 512
    || value.normalize('NFC') !== value || /[\\\x00-\x20:#?%\[\]<>|]/u.test(value)
    || value.split('/').some(part => !part || part === '.' || part === '..' || part.startsWith('.'))
    || value.split('/').length > 8) throw new Error('invalid relative memory Markdown path')
}

export function excerpt(text: string, bytes: number): string {
  const buffer = Buffer.from(text)
  if (buffer.length <= bytes) return text
  let end = bytes
  while (end > 0 && (buffer[end]! & 0xc0) === 0x80) end--
  return buffer.subarray(0, end).toString('utf8')
}

export function pageLimit(value = 32): number {
  if (!Number.isSafeInteger(value) || value < 1 || value > 64) throw new Error('memory page limit must be 1-64')
  return value
}
