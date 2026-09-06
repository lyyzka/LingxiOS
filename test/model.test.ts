import assert from 'node:assert/strict'
import { it } from 'node:test'
import { DEFAULT_MODEL, OpenAIChatDriver } from '../src/model/openai.js'
import { loadWorkerConfig } from '../src/config.js'

it('uses DeepSeek Flash with high reasoning for agent, review and compaction calls', async () => {
  const config = loadWorkerConfig({ AGENT_OS_CONTROL_PLANE_URL: 'http://localhost', AGENT_OS_SERVICE_TOKEN: 'test', AGENT_OS_MODEL_API_KEY: 'test' })
  assert.deepEqual(config.model, { ...DEFAULT_MODEL, apiKey: 'test' })
  assert.throws(() => loadWorkerConfig({ AGENT_OS_REASONING_EFFORT: 'invalid' }), /REASONING_EFFORT/)
  let calls = 0
  const model = new OpenAIChatDriver(config.model.id, { ...config.model, fetchImpl: async (url, init) => {
    assert.equal(url, 'https://api.siliconflow.cn/v1/chat/completions')
    const body = JSON.parse(String(init?.body))
    assert.equal(body.model, DEFAULT_MODEL.id)
    assert.equal(body.reasoning_effort, 'high')
    assert.equal(body.enable_thinking, true)
    calls++
    return body.stream ? new Response('data: {"choices":[{"delta":{"content":"{}"},"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n')
      : Response.json({ choices: [{ finish_reason: 'stop', message: { content: '{"summary":"Context."}' } }] })
  } })
  await model.run({ instructions: '', items: [] })
  await model.structured({ instructions: '', input: {} })
  await model.compact({ instructions: '', items: [] })
  assert.equal(calls, 3)
})

function driver(delta: unknown, finishReason: string | null = 'stop') {
  return new OpenAIChatDriver('test', {
    apiKey: 'test',
    fetchImpl: async () => new Response(
      `data: ${JSON.stringify({ choices: [{ delta, finish_reason: finishReason }] })}\n\ndata: [DONE]\n\n`,
      { headers: { 'content-type': 'text/event-stream' } },
    ),
  })
}

it('does not send provider-specific reasoning settings unless configured', async () => {
  const model = new OpenAIChatDriver('compatible-model', { apiKey: 'test', fetchImpl: async (_url, init) => {
    const body = JSON.parse(String(init?.body))
    assert.equal(body.reasoning_effort, undefined)
    assert.equal(body.enable_thinking, undefined)
    return new Response('data: {"choices":[{"delta":{"content":"answer"},"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n')
  } })
  await model.run({ instructions: '', items: [] })
})

it('keeps incremental text and rejects truncated or unfinished streams', async () => {
  const deltas: string[] = []
  const result = await driver({ content: 'hello' }).run({ instructions: '', items: [], onTextDelta: (text) => deltas.push(text) })
  assert.equal(result.text, 'hello')
  assert.equal(result.finalCandidate, 'hello', 'plain final text must enter runtime protocol correction too')
  assert.deepEqual(deltas, ['hello'])
  for (const reason of ['length', 'content_filter', null]) {
    await assert.rejects(driver({ content: 'partial' }, reason).run({ instructions: '', items: [] }), /did not finish normally/)
  }
})

it('never invents tool names or call identities', async () => {
  for (const call of [
    { function: { name: 'ipython', arguments: '{"code":"1"}' } },
    { id: 'call-1', function: { arguments: '{"code":"1"}' } },
    { id: 'call-1', function: { name: 'shell', arguments: '{"code":"1"}' } },
  ]) {
    await assert.rejects(driver({ tool_calls: [call] }, 'tool_calls').run({ instructions: '', items: [] }), /invalid tool identity/)
  }
  const result = await driver({ content: 'Checking.', tool_calls: [{ id: 'call-1', function: { name: 'ipython', arguments: '{"code":"1"}' } }] }, 'tool_calls')
    .run({ instructions: '', items: [] })
  assert.equal(result.output.length, 2)
  assert.equal(result.finalCandidate, undefined, 'progress accompanying a tool call is not a final candidate')
})

it('rejects truncated auxiliary calls and keeps compaction instructions independent', async () => {
  const model = new OpenAIChatDriver('test', { apiKey: 'test', fetchImpl: async (_url, init) => {
    assert.doesNotMatch(String(init?.body), /SECRET_PERSONA/)
    return Response.json({ choices: [{ finish_reason: 'length', message: { content: '{}' } }] })
  } })
  await assert.rejects(model.compact({ instructions: 'SECRET_PERSONA', items: [] }), /did not finish normally/)
  await assert.rejects(model.structured({ instructions: '', input: {} }), /did not finish normally/)
})

it('bounds streaming and auxiliary response bodies and cancels oversized readers', async () => {
  for (const method of ['run', 'structured', 'compact'] as const) {
    let cancelled = false
    let chunks = 0
    const model = new OpenAIChatDriver('test', { apiKey: 'test', fetchImpl: async () => new Response(new ReadableStream({
      pull(controller) { chunks++; controller.enqueue(new Uint8Array(1024 * 1024).fill(120)) },
      cancel() { cancelled = true },
    })) })
    const call = method === 'run' ? model.run({ instructions: '', items: [] })
      : method === 'structured' ? model.structured({ instructions: '', input: {} }) : model.compact({ instructions: '', items: [] })
    await assert.rejects(call, /exceeds 16 MiB/)
    assert.equal(cancelled, true)
    assert.ok(chunks <= 18)
  }
})
