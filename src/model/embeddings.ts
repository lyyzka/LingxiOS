import { createHash } from 'node:crypto'

export interface EmbeddingOptions {
  inputCostMicrosPerMillion?: number
  id: string
  apiKey: string
  baseUrl?: string
  dimensions?: number
  requestTimeoutMs?: number
}

/** One bounded request. Durable indexing work, rather than the HTTP client, owns retries. */
export class OpenAIEmbeddingDriver {
  readonly cacheKey: string
  private readonly endpoint: string
  private readonly timeoutMs: number

  constructor(private readonly options: EmbeddingOptions) {
    this.options = { ...options }
    if (!options.id?.trim() || !options.apiKey?.trim()) throw new Error('embedding model id and apiKey are required')
    let url: URL
    try { url = new URL(options.baseUrl ?? 'https://api.openai.com/v1') }
    catch { throw new Error('invalid embedding base URL') }
    if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search || url.hash) throw new Error('invalid embedding base URL')
    this.endpoint = `${url.href.replace(/\/+$/, '')}/embeddings`
    this.timeoutMs = options.requestTimeoutMs ?? 15_000
    if (!Number.isSafeInteger(this.timeoutMs) || this.timeoutMs < 1 || this.timeoutMs > 60_000) throw new Error('invalid embedding timeout')
    if (options.dimensions !== undefined && (!Number.isSafeInteger(options.dimensions) || options.dimensions < 1 || options.dimensions > 4096)) throw new Error('invalid embedding dimensions')
    this.cacheKey = createHash('sha256').update(JSON.stringify([this.endpoint, options.id, options.dimensions ?? null])).digest('hex')
  }

  async embed(input: readonly string[], signal?: AbortSignal) {
    // UTF-8 bytes conservatively bound input tokens without a tokenizer dependency.
    if (!Array.isArray(input) || !input.length || input.length > 32
      || input.some(text => typeof text !== 'string' || !text.trim() || Buffer.byteLength(text) > 8000)) throw new Error('embedding input requires 1-32 nonempty strings of at most 8000 UTF-8 bytes')
    const combined = AbortSignal.any([...(signal ? [signal] : []), AbortSignal.timeout(this.timeoutMs)])
    combined.throwIfAborted()
    const response = await fetch(this.endpoint, { method: 'POST', redirect: 'error', signal: combined,
      headers: { 'content-type': 'application/json', authorization: `Bearer ${this.options.apiKey}` },
      body: JSON.stringify({ model: this.options.id, input, encoding_format: 'float',
        ...(this.options.dimensions === undefined ? {} : { dimensions: this.options.dimensions }) }) })
    if (!response.ok) {
      await response.body?.cancel()
      // Provider error bodies may echo credentials or input; never persist them in work errors.
      throw new Error(`embedding provider returned HTTP ${response.status}`)
    }
    if (!response.body) throw new Error('embedding provider returned no response body')
    const reader = response.body.getReader()
    const chunks: Uint8Array[] = []
    let size = 0
    try {
      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        size += value.byteLength
        if (size > 4 * 1024 * 1024) throw new Error('embedding response exceeds 4 MiB')
        chunks.push(value)
      }
    } finally { await reader.cancel().catch(() => {}); reader.releaseLock() }
    combined.throwIfAborted()
    let result: { model?: unknown; data?: unknown; usage?: { prompt_tokens?: unknown; total_tokens?: unknown } }
    try { result = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(chunks))) }
    catch { throw new Error('embedding provider returned invalid JSON') }
    if (!result || typeof result !== 'object' || typeof result.model !== 'string' || !result.model.trim() || result.model.length > 256
      || !Array.isArray(result.data) || result.data.length !== input.length) throw new Error('invalid embedding response envelope')
    const vectors: number[][] = new Array(input.length)
    let dimensions = this.options.dimensions
    for (const item of result.data) {
      if (!item || !Number.isSafeInteger(item.index) || item.index < 0 || item.index >= input.length || vectors[item.index]
        || !Array.isArray(item.embedding) || item.embedding.length < 1 || item.embedding.length > 4096
        || item.embedding.some((value: unknown) => typeof value !== 'number' || !Number.isFinite(value))) throw new Error('invalid embedding vector or index')
      dimensions ??= item.embedding.length
      if (item.embedding.length !== dimensions) throw new Error('embedding dimensions do not match')
      const norm = Math.hypot(...item.embedding)
      if (!Number.isFinite(norm) || norm === 0) throw new Error('embedding vector has invalid magnitude')
      vectors[item.index] = item.embedding.map((value: number) => value / norm)
    }
    const prompt = result.usage?.prompt_tokens, total = result.usage?.total_tokens
    const available = Number.isSafeInteger(prompt) && Number(prompt) >= 0 && Number.isSafeInteger(total) && Number(total) >= Number(prompt)
    return { model: result.model, vectors, usage: { available, inputTokens: available ? Number(prompt) : 0, totalTokens: available ? Number(total) : 0 } }
  }
}
