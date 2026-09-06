import { decodeSourceText } from '../../context/text.js'
import { extractDocumentText } from '../../context/document-text.js'
import { createHash } from 'node:crypto'
import { snapshotAttachments, type RequestAttachment } from '../../context/attachments.js'
import type { LingxiLoopServices, NativeMessage } from './service-contracts.js'

export async function readRequestAttachments(services: Pick<LingxiLoopServices, 'storage'>, messages: NativeMessage[], input: {
  companyId: string; channelId: string; channelType: number; clientMsgNos: string[]
}): Promise<RequestAttachment[]> {
  if (!Array.isArray(input.clientMsgNos) || input.clientMsgNos.length > 20
    || input.clientMsgNos.some(id => typeof id !== 'string' || !id.trim())
    || new Set(input.clientMsgNos).size !== input.clientMsgNos.length) throw new Error('invalid attachment message references')
  if (!input.clientMsgNos.length) return []
  if (!services.storage?.readObjectBounded) throw new Error('native bounded storage reader is required for request attachments')
  const attachments: RequestAttachment[] = []
  for (const id of input.clientMsgNos) {
    const message = messages.find(item => item.clientMsgNo === id && item.channelId === input.channelId && item.channelType === input.channelType)
    const data = message?.payload.data
    if (message?.payload.version !== 1 || message.payload.kind !== 'attachment' || !data
      || typeof data['key'] !== 'string' || !data['key'].startsWith(`attachments/${input.companyId}/`)
      || typeof data['name'] !== 'string' || !data['name'].trim() || typeof data['mime'] !== 'string'
      || !Number.isSafeInteger(data['size']) || Number(data['size']) < 0 || Number(data['size']) > 16 * 1024 * 1024) throw new Error('invalid or unavailable committed request attachment')
    const received = await services.storage.readObjectBounded(data['key'], 16 * 1024 * 1024)
    if (!(received instanceof Uint8Array) || received.byteLength !== data['size']) throw new Error('attachment bytes do not match committed size')
    // Storage may reuse its buffer while asynchronous parsing is in flight.
    const bytes = Uint8Array.from(received)
    const mime = data['mime'].split(';')[0]!.trim().toLowerCase()
    const text = mime.startsWith('text/') || ['application/json', 'application/xml'].includes(mime)
      ? decodeSourceText(bytes)
      : mime === 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' ? await extractDocumentText(bytes, 'docx')
      : mime === 'application/pdf' ? await extractDocumentText(bytes, 'pdf') : undefined
    attachments.push({ id, sourceVersion: `sha256:${createHash('sha256').update(bytes).digest('hex')}`, name: data['name'],
      mimeType: data['mime'], size: bytes.byteLength, ...(text === undefined ? {} : { text }) })
  }
  return snapshotAttachments(attachments)
}
