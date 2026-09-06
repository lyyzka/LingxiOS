import assert from 'node:assert/strict'
import http from 'node:http'
import { it } from 'node:test'
import { OpenAIEmbeddingDriver } from '../src/model/embeddings.js'

it('bounds embedding requests, aligns response indices and rejects corrupt vectors and leaked errors', async () => {
  let payload: unknown
  let status = 200
  let slow = false
  let received: unknown
  let requests = 0
  const server = http.createServer((req, res) => {
    requests++
    const buffers: Buffer[] = []
    req.on('data', (chunk: Buffer) => buffers.push(chunk))
    req.on('end', () => {
      received = JSON.parse(Buffer.concat(buffers).toString('utf8'))
      if (slow) return
      res.writeHead(status, { 'content-type': 'application/json' })
      res.end(JSON.stringify(payload))
    })
  })
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve))
  const address = server.address()
  assert.ok(address && typeof address === 'object')
  const options = { id: 'embedding-test', apiKey: 'secret', baseUrl: `http://127.0.0.1:${address.port}/v1`, dimensions: 2 }
  const driver = new OpenAIEmbeddingDriver(options)
  try {
    payload = { model: 'embedding-test', data: [{ index: 1, embedding: [0, 5] }, { index: 0, embedding: [3, 4] }], usage: { prompt_tokens: 7, total_tokens: 7 } }
    assert.deepEqual(await driver.embed(['text', '文字']), { model: 'embedding-test', vectors: [[0.6, 0.8], [0, 1]], usage: { available: true, inputTokens: 7, totalTokens: 7 } })
    assert.deepEqual(received, { model: 'embedding-test', input: ['text', '文字'], encoding_format: 'float', dimensions: 2 })
    assert.equal(driver.cacheKey, new OpenAIEmbeddingDriver({ ...options, apiKey: 'rotated' }).cacheKey)
    assert.notEqual(driver.cacheKey, new OpenAIEmbeddingDriver({ ...options, dimensions: 3 }).cacheKey)
    const count = requests
    for (const input of [[], [''], ['字'.repeat(3000)], Array(33).fill('text')]) await assert.rejects(driver.embed(input), /embedding input/)
    assert.equal(requests, count)
    for (const data of [[], [{ index: 0, embedding: [0, 0] }], [{ index: 0, embedding: [1, null] }],
      [{ index: 0, embedding: [1, 2, 3] }], [{ index: 1, embedding: [1, 0] }]]) {
      payload = { model: 'embedding-test', data }
      await assert.rejects(driver.embed(['text']), /embedding/)
    }
    payload = { model: 'embedding-test', data: [{ index: 0, embedding: [1, 0] }, { index: 0, embedding: [0, 1] }] }
    await assert.rejects(driver.embed(['a', 'b']), /index/)
    payload = 'x'.repeat(4 * 1024 * 1024)
    await assert.rejects(driver.embed(['text']), /4 MiB/)
    status = 401
    payload = { error: 'secret user input must not appear in the error' }
    await assert.rejects(driver.embed(['text']), error => error instanceof Error && error.message === 'embedding provider returned HTTP 401')
    slow = true
    await assert.rejects(new OpenAIEmbeddingDriver({ ...options, requestTimeoutMs: 30 }).embed(['text']), /timeout|aborted/i)
    const controller = new AbortController()
    controller.abort(new Error('caller cancelled'))
    await assert.rejects(driver.embed(['text'], controller.signal), /caller cancelled/)
  } finally {
    server.closeAllConnections()
    await new Promise<void>(resolve => server.close(() => resolve()))
  }
})
