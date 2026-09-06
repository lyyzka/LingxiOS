export interface RequestAttachment {
  id: string
  sourceVersion: string
  name: string
  mimeType: string
  size: number
  /** Text extracted by the authenticated ingress; absence means content is not available. */
  text?: string
}

export function snapshotAttachments(value: unknown): RequestAttachment[] {
  if (!Array.isArray(value) || value.length > 20) throw new Error('request attachments must be an array of at most 20 items')
  const ids = new Set<string>()
  for (const item of value) {
    if (!item || typeof item !== 'object' || Array.isArray(item)
      || Object.keys(item).some(key => !['id', 'sourceVersion', 'name', 'mimeType', 'size', 'text'].includes(key))
      || !['id', 'sourceVersion', 'name', 'mimeType'].every(key => typeof item[key] === 'string' && item[key].trim() && item[key].length <= 2_000)
      || !Number.isSafeInteger(item.size) || item.size < 0
      || (item.text !== undefined && (typeof item.text !== 'string' || item.text.length > 1_000_000))) throw new Error('invalid request attachment')
    if (ids.has(item.id)) throw new Error('duplicate request attachment identity')
    ids.add(item.id)
  }
  return structuredClone(value)
}
