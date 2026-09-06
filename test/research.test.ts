import { createHash } from 'node:crypto'
import { pdfFixture } from './pdf-fixture.js'
import assert from 'node:assert/strict'
import { it } from 'node:test'
import http from 'node:http'
import { syncBuiltinESMExports } from 'node:module'
import { PassThrough } from 'node:stream'
import { EventEmitter } from 'node:events'
import { isPublicAddress, researchUrl } from '../src/research/address.js'
import { fetchResearch, pinnedRequestOptions } from '../src/research/fetch.js'
import { decodeHtml, parseOpenAlex, readResearch, searchResearch } from '../src/research/index.js'
import { executeResearch } from '../src/integrations/lingxiloop/research.js'

it('rejects local, mapped, transition and reserved research addresses', () => {
  for (const address of ['127.0.0.1', '10.1.2.3', '100.64.0.1', '169.254.169.254', '192.168.1.2', '198.18.0.1', '224.0.0.1', '::1', '::ffff:127.0.0.1', '::ffff:8.8.8.8', 'fc00::1', 'fe80::1', '2002:7f00:1::', '2001:db8::1']) {
    assert.equal(isPublicAddress(address), false, address)
  }
  for (const address of ['8.8.8.8', '1.1.1.1', '2606:4700:4700::1111']) assert.equal(isPublicAddress(address), true, address)
  for (const url of ['http://2130706433', 'http://0x7f000001', 'http://[::1]', 'http://localhost', 'https://u:p@example.com', 'file:///tmp/file']) assert.throws(() => researchUrl(url))
  assert.equal(researchUrl('https://example.com/page').href, 'https://example.com/page')
})

it('bounds response bodies and revalidates redirects before making another request', async (context) => {
  let calls = 0
  let status = 200
  let headers: Record<string, string> = { 'content-type': 'text/plain' }
  let body: Buffer = Buffer.from('Public response')
  context.mock.method(http, 'request', (_url: URL, options: http.RequestOptions, callback: (response: http.IncomingMessage) => void) => {
    calls++
    assert.equal(options.hostname, '1.1.1.1')
    const request = new EventEmitter() as EventEmitter & { end(): void }
    request.end = () => {
      const response = new PassThrough() as PassThrough & { statusCode: number; headers: Record<string, string> }
      response.statusCode = status
      response.headers = headers
      callback(response as unknown as http.IncomingMessage)
      if (!response.destroyed) response.end(body)
    }
    return request
  })
  syncBuiltinESMExports()
  try {
    const result = await fetchResearch('http://1.1.1.1/public')
    assert.equal(result.body.toString(), 'Public response')
    status = 302
    headers = { location: 'http://127.0.0.1/private' }
    await assert.rejects(fetchResearch('http://1.1.1.1/redirect'), /blocked/)
    assert.equal(calls, 2)
    status = 200
    headers = { 'content-encoding': 'gzip' }
    await assert.rejects(fetchResearch('http://1.1.1.1/compressed'), /encoding/)
    headers = { 'content-type': 'text/plain' }
    body = Buffer.from([0xff, 0xff])
    await assert.rejects(readResearch('http://1.1.1.1/bad-text'), /encoded data/)
    body = Buffer.from('\ufeff中文来源', 'utf16le')
    assert.equal((await readResearch('http://1.1.1.1/text')).text, '中文来源')
    headers = { 'content-type': 'application/pdf' }
    body = pdfFixture(['Research PDF content'])
    const pdf = await readResearch('http://1.1.1.1/paper.pdf')
    assert.deepEqual({ url: pdf.url, finalUrl: pdf.finalUrl, contentType: pdf.contentType, bytes: pdf.bytes, sha256: pdf.sha256, truncated: pdf.truncated },
      { url: 'http://1.1.1.1/paper.pdf', finalUrl: 'http://1.1.1.1/paper.pdf', contentType: 'application/pdf', bytes: body.length, sha256: createHash('sha256').update(body).digest('hex'), truncated: false })
    assert.match(pdf.text, /Research PDF content/)
    body = Buffer.from('corrupt PDF')
    await assert.rejects(readResearch('http://1.1.1.1/corrupt.pdf'), /extraction failed/)
    headers = {}
    body = Buffer.alloc(2 * 1024 * 1024 + 1)
    await assert.rejects(fetchResearch('http://1.1.1.1/large'), /2 MiB/)
  } finally {
    context.mock.restoreAll()
    syncBuiltinESMExports()
  }
})

it('authorizes research using the persisted human and conversation', async () => {
  const work = { id: 'w', tenantId: 't', agentId: 'a', principalId: 'u', sessionId: 's', kind: 'turn', lane: 'interactive' as const, triggerRef: 'm', fence: 1, homeEpoch: 1 }
  const action = { runId: 'w', cellId: 'c', callIndex: 0, action: 'research.read', args: { url: 'http://127.0.0.1/private' }, idempotencyKey: 'key' }
  const requests: unknown[] = []
  const services = { permissionService: { assertCan: async (request: unknown) => { requests.push(request) } } }
  await assert.rejects(executeResearch(work, action, services), /blocked/)
  assert.deepEqual(requests, [{ actorUserId: 'u', companyId: 't', action: 'agent:read', resource: { type: 'conversation', id: 's' } }])
  await assert.rejects(executeResearch(work, { ...action, args: { ...action.args, userId: 'forged' } }, services), /unknown/)
  await assert.rejects(executeResearch(work, action, { permissionService: { assertCan: async () => { throw new Error('denied') } } }), /denied/)
  assert.equal(requests.length, 1)
})

it('extracts bounded research text and rejects invalid requests before fetching', async () => {
  const result = { display_name: 'Paper', doi: 'https://doi.org/10.1/example', abstract_inverted_index: { Hello: [0], world: [1] } }
  assert.deepEqual(parseOpenAlex({ results: [result, { ...result, doi: 'javascript:alert(1)' }, { ...result, doi: 'http://127.0.0.1/private' }] }, 8), [
    { title: 'Paper', url: 'https://doi.org/10.1/example', doi: 'https://doi.org/10.1/example', authors: [], abstract: 'Hello world', source: 'OpenAlex' },
  ])
  assert.throws(() => parseOpenAlex(null, 8), /invalid results/)
  assert.equal(decodeHtml('<script>ignore()</script><style>ignore</style><p>A &amp; B &#999999999999;</p>'), 'A & B \uFFFD')
  await assert.rejects(searchResearch('', 8), /query/)
  await assert.rejects(searchResearch('Topic', NaN), /limit/)
  await assert.rejects(searchResearch('Topic', 21), /limit/)
  await assert.rejects(readResearch('http://127.0.0.1/private'), /blocked/)
})

it('pins the connection address while retaining HTTP and TLS authority', async () => {
  const options = pinnedRequestOptions(new URL('https://example.com:8443/page'), '1.1.1.1')
  assert.equal(options.hostname, '1.1.1.1')
  assert.equal(options.servername, 'example.com')
  assert.equal((options.headers as Record<string, string>)['host'], 'example.com:8443')
  assert.equal(options.agent, false)
  assert.throws(() => pinnedRequestOptions(new URL('https://example.com'), '127.0.0.1'), /blocked/)
  await assert.rejects(fetchResearch('http://127.0.0.1/private'), /blocked/)
})
