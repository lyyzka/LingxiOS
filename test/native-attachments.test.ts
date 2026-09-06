import { pdfFixture as pdf } from './pdf-fixture.js'
import mammoth from 'mammoth'
import { extractDocumentText } from '../src/context/document-text.js'
const extractDocxText = (bytes: Uint8Array) => extractDocumentText(bytes, 'docx')
import assert from 'node:assert/strict'
import { createHash } from 'node:crypto'
import { it } from 'node:test'
import { readRequestAttachments } from '../src/integrations/lingxiloop/attachments.js'
import type { NativeMessage } from '../src/integrations/lingxiloop/service-contracts.js'

it('captures bounded committed attachment bytes and rejects foreign or inconsistent references', async () => {
  const bytes = Buffer.from('附件内容')
  const message: NativeMessage = { clientMsgNo: 'file', messageSeq: 1, channelId: 'room', channelType: 2, fromUid: 'user',
    payload: { version: 1, kind: 'attachment', data: { key: 'attachments/company/file', name: 'notes.txt', mime: 'text/plain', size: bytes.length } } }
  const input = { companyId: 'company', channelId: 'room', channelType: 2, clientMsgNos: ['file'] }
  let reads = 0
  const services = { storage: { readObjectBounded: async (key: string, maxBytes: number) => {
    reads++
    assert.equal(key, 'attachments/company/file')
    assert.equal(maxBytes, 16 * 1024 * 1024)
    return bytes
  } } }
  assert.deepEqual(await readRequestAttachments(services, [message], input), [{ id: 'file',
    sourceVersion: `sha256:${createHash('sha256').update(bytes).digest('hex')}`, name: 'notes.txt', mimeType: 'text/plain', size: bytes.length, text: '附件内容' }])
  for (const override of [{ channelId: 'other' }, { companyId: 'other' }, { channelType: 1 }, { clientMsgNos: ['missing'] }, { clientMsgNos: ['file', 'file'] }]) {
    await assert.rejects(readRequestAttachments(services, [message], { ...input, ...override }))
  }
  assert.equal(reads, 1)
  await assert.rejects(readRequestAttachments({}, [message], input), /bounded storage reader/)
  await assert.rejects(readRequestAttachments({ storage: { readObjectBounded: async () => Buffer.alloc(0) } }, [message], input), /committed size/)
  const binary = structuredClone(message)
  binary.payload.data!['mime'] = 'application/octet-stream'
  assert.equal('text' in (await readRequestAttachments(services, [binary], input))[0]!, false)
  await assert.rejects(readRequestAttachments({ storage: { readObjectBounded: async () => Buffer.alloc(bytes.length, 255) } }, [message], input), /encoded data/)
  assert.deepEqual(await readRequestAttachments({}, [], { ...input, clientMsgNos: [] }), [])
})

it('decodes BOM-tagged UTF-16 text without guessing malformed encodings', async () => {
  const input = { companyId: 'company', channelId: 'room', channelType: 2, clientMsgNos: ['file'] }
  const read = async (bytes: Buffer) => readRequestAttachments({ storage: { readObjectBounded: async () => bytes } }, [{
    clientMsgNo: 'file', messageSeq: 1, channelId: 'room', channelType: 2, fromUid: 'user',
    payload: { version: 1, kind: 'attachment', data: { key: 'attachments/company/file', name: 'notes.txt', mime: 'text/plain', size: bytes.length } },
  }], input)
  const littleEndian = Buffer.from('\ufeff附件😀', 'utf16le')
  const bigEndian = Buffer.from(littleEndian).swap16()
  for (const bytes of [littleEndian, bigEndian, Buffer.from('\ufeff附件😀', 'utf8')]) {
    const [attachment] = await read(bytes)
    assert.equal(attachment!.text, '附件😀')
    assert.equal(attachment!.sourceVersion, `sha256:${createHash('sha256').update(bytes).digest('hex')}`)
  }
  for (const bytes of [Buffer.from([0xff, 0xfe, 0x41]), Buffer.from([0xfe, 0xff, 0xd8, 0x00]), Buffer.from([0xff, 0xff])]) {
    await assert.rejects(read(bytes), /encoded data/)
  }
})

it('extracts DOCX paragraphs through the isolated parser and rejects corrupt files', async () => {
  const bytes = Buffer.from('UEsDBBQAAAAIAOudJV10JJxTuwAAAD4BAAATAAAAW0NvbnRlbnRfVHlwZXNdLnhtbJWQuQ7CMAyGX6XKiqgRAwNquwArMPACVuq2EbkUuxxvT8o1sDHa//FZrk73SFzcnPVcq0EkrgFYD+SQyxDJZ6ULyaHkMfUQUZ+xJ1guFivQwQt5mcvUoZpqSx2OVordLa/ZBF+rRJZVsXkZJ1atMEZrNErW4eLbH8r8TShz8unhwUSeZYOCpjpcKCXTUnHEJHt0uQ6uIbXQBj26jCgn41+80HVG0zc/tcUUNDEb3ztbfhWHxn/ugOfbmgdQSwMEFAAAAAgA650lXWF7L0OJAAAA8gAAAAsAAABfcmVscy8ucmVsc43POw4CIRAG4KsQDrCzWlgYoLLZ1ngBAsMjLo8MGPX2UlisxsJy5p98f0accdU9ltxCrI090pqb5KH3egRoJmDSbSoV80hcoaT7GMlD1eaqPcJ+ng9AW4MrsTXZYiWnxe44uzwr/mMX56LBUzG3hLn/qPi6GLImj13yeyEL9r2eBstBCfh4Ub0AUEsDBBQAAAAIAOudJV1Jzd5OowAAAN4AAAARAAAAd29yZC9kb2N1bWVudC54bWyzKbdKyU8uzU3NK1GoyM3JK7Yqt1XKKCkpsNLXL07OSM1NLNbLL0jNA8ql5RflJpYAuUXp+uX5RSkFRfnJqcXFmXnpuTn6RgYGZvq5iZl5SnY25VZJ+SmVILoARBSBiBK7lzNbnuze9mzt4mfT2hXUEnMLrBVSyzJTUvOSU230QQpAZBGYLEDXG5yanJ+XolCQWJSYXpRYkIFFgz7MVn2Ej+wAUEsBAhQAFAAAAAgA650lXXQknFO7AAAAPgEAABMAAAAAAAAAAAAAAIABAAAAAFtDb250ZW50X1R5cGVzXS54bWxQSwECFAAUAAAACADrnSVdYXsvQ4kAAADyAAAACwAAAAAAAAAAAAAAgAHsAAAAX3JlbHMvLnJlbHNQSwECFAAUAAAACADrnSVdSc3eTqMAAADeAAAAEQAAAAAAAAAAAAAAgAGeAQAAd29yZC9kb2N1bWVudC54bWxQSwUGAAAAAAMAAwC5AAAAcAIAAAAA', 'base64')
  const input = { companyId: 'company', channelId: 'room', channelType: 2, clientMsgNos: ['file'] }
  const read = async (data: Buffer) => readRequestAttachments({ storage: { readObjectBounded: async () => data } }, [{
    clientMsgNo: 'file', messageSeq: 1, channelId: 'room', channelType: 2, fromUid: 'user',
    payload: { version: 1, kind: 'attachment', data: { key: 'attachments/company/file', name: 'notes.docx', mime: 'application/vnd.openxmlformats-officedocument.wordprocessingml.document', size: data.length } },
  }], input)
  const [attachment] = await read(bytes)
  assert.equal(attachment!.sourceVersion, `sha256:${createHash('sha256').update(bytes).digest('hex')}`)
  assert.match(attachment!.text!, /formatting and images are not represented/)
  assert.match(attachment!.text!, /附件正文 & evidence\n\nSecond paragraph/)
  await assert.rejects(read(Buffer.from('not a zip file')), /Document text extraction failed/)
  assert.match((await read(bytes))[0]!.text!, /Second paragraph/)
})

it('bounds parser concurrency and rejects oversized input and expanded text', async () => {
  const failed = () => extractDocxText(Buffer.from('bad')).then(() => assert.fail('corrupt input accepted'), error => error)
  const first = failed(), second = failed()
  await assert.rejects(extractDocxText(Buffer.from('bad')), /parser is busy/)
  assert.ok((await Promise.all([first, second])).every(error => error instanceof Error))
  await assert.rejects(extractDocxText(Buffer.alloc(16 * 1024 * 1024 + 1)), /input exceeds/)
  const expanded = Buffer.from('UEsDBBQAAAAIAEaeJV0FHMMkRgQAAMMbDwARAAAAd29yZC9kb2N1bWVudC54bWztyUFuwjAQQNGrVBygRl10EYXcJSQpIGE7cowCty/JEVi/t/gzo2nXZszDI06pfj3jPS3Nejpca52bEJbhOsV++c7zlN6/v1xiX99nuYQ1l3EueZiW5ZYu8R5+jsffEPtbOnTt2pzz+NrmvKVsqd0TAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIDPtGFtare17J33nvP42pcxD484pdr9A1BLAQIUABQAAAAIAEaeJV0FHMMkRgQAAMMbDwARAAAAAAAAAAAAAACAAQAAAAB3b3JkL2RvY3VtZW50LnhtbFBLBQYAAAAAAQABAD8AAAB1BAAAAAA=', 'base64')
  assert.ok((await mammoth.extractRawText({ buffer: expanded })).value.length > 990_000)
  await assert.rejects(extractDocxText(expanded), /extraction failed or exceeded/)
  await assert.rejects(extractDocxText(Buffer.from('bad')), /extraction failed or exceeded/)
})

it('extracts PDF page text, identifies pages without extractable text and rejects oversized page counts', async () => {
  const bytes = pdf(['Hello PDF', 'Second page', ''])
  const expectedVersion = `sha256:${createHash('sha256').update(bytes).digest('hex')}`
  const [attachment] = await readRequestAttachments({ storage: { readObjectBounded: async () => {
    setTimeout(() => bytes.fill(0), 0)
    return bytes
  } } }, [{ clientMsgNo: 'pdf', messageSeq: 1, channelId: 'room', channelType: 2, fromUid: 'user',
    payload: { version: 1, kind: 'attachment', data: { key: 'attachments/company/pdf', name: 'notes.pdf', mime: 'application/pdf', size: bytes.length } },
  }], { companyId: 'company', channelId: 'room', channelType: 2, clientMsgNos: ['pdf'] })
  assert.ok(bytes.every(value => value === 0), 'storage reused its returned buffer during parsing')
  assert.equal(attachment!.sourceVersion, expectedVersion)
  const text = attachment!.text!
  assert.match(text, /\[Page 1\]\nHello PDF/)
  assert.match(text, /\[Page 2\]\nSecond page/)
  assert.match(text, /\[Page 3\]\n\[No extractable text; OCR was not performed.\]/)
  assert.match(await extractDocumentText(pdf(['中文证据'], true), 'pdf'), /中文证据/)
  await assert.rejects(extractDocumentText(pdf(Array(201).fill('Page')), 'pdf'), /extraction failed/)
  await assert.rejects(extractDocumentText(Buffer.from('not a PDF'), 'pdf'), /extraction failed/)
})
