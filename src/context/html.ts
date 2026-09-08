/** Linear tag scan. Malformed '<' runs never cause overlapping suffix searches. */
export function decodeHtml(value: string): string {
  const parts: string[] = []
  const tags = /<[^<>]*>/g
  let cursor = 0
  for (let match = tags.exec(value); match; match = tags.exec(value)) {
    parts.push(value.slice(cursor, match.index), ' ')
    cursor = tags.lastIndex
    const hidden = /^<\s*(script|style|noscript)\b/i.exec(match[0])
    if (hidden) {
      const closing = new RegExp(`</${hidden[1]}\\s*>`, 'gi')
      closing.lastIndex = cursor
      const end = closing.exec(value)
      // An unclosed raw-text element consumes the rest, rather than exposing its contents.
      cursor = end ? closing.lastIndex : value.length
      tags.lastIndex = cursor
    }
  }
  parts.push(value.slice(cursor))
  return parts.join('')
    .replace(/&(?:nbsp|amp|lt|gt|quot|apos|#39|#\d+);/gi, entity => {
      const name = entity.slice(1, -1).toLowerCase()
      const named: Record<string, string> = { nbsp: ' ', amp: '&', lt: '<', gt: '>', quot: '"', apos: "'", '#39': "'" }
      if (name in named) return named[name]!
      const code = Number(name.slice(1))
      return code <= 0x10ffff ? String.fromCodePoint(code) : '\uFFFD'
    })
    .replace(/\s+/g, ' ').trim()
}
