import assert from 'node:assert/strict'
import { it } from 'node:test'
import { readRequestAttachment, requestItems, type RequestSnapshot } from '../src/context/request.js'
import { parseTaskArgs } from '../src/tools/catalog.js'

it('bounds model attachment previews while retaining exact versioned source ranges', () => {
  const text = '正文'.repeat(20_000)
  const request = { originalText: 'Inspect the attachment', sourceRef: 'r', revisions: [],
    attachments: [{ id: 'a', sourceVersion: 'v1', name: 'a.txt', mimeType: 'text/plain', size: text.length * 3, text }] } as unknown as RequestSnapshot
  const before = structuredClone(request)
  assert.ok(JSON.stringify(requestItems(request, true)).length < 2000)
  assert.ok(JSON.stringify(requestItems(request)).includes(text))
  const args = { id: 'a', sourceVersion: 'v1', offset: 512, limit: 16_000 }
  parseTaskArgs('task.read_attachment', args)
  assert.deepEqual(readRequestAttachment(request, args), { id: 'a', sourceVersion: 'v1', offset: 512,
    text: text.slice(512, 16_512), textLength: text.length, nextOffset: 16_512, truncated: true })
  assert.throws(() => readRequestAttachment(request, { ...args, sourceVersion: 'v2' }), /missing/)
  assert.throws(() => readRequestAttachment(request, { ...args, offset: -1 }), /range/)
  assert.deepEqual(request, before)
})
