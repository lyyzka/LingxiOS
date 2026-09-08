/**
 * HTTP adapter over {@link ControlPlaneService}. Dependency-free node:http
 * with a small explicit route table; every route is authenticated with a
 * timing-safe service-token comparison.
 */
import { AGENT_OS_PROTOCOL_VERSION } from '../protocol/constants.js'
import { ByteBudget } from '../resource-quota.js'
const uploadBytes = new ByteBudget()
const jsonBytes = new ByteBudget(64 * 1024 * 1024, 64 * 1024 * 1024)

import { timingSafeEqual } from 'node:crypto'
import http from 'node:http'
import type { AddressInfo } from 'node:net'
import { nullLogger, type Logger } from '../logging.js'
import type { MetricsRegistry } from '../metrics.js'
import type { AssistantMessage, RunEvent, SessionRecord, WorkCompletion } from '../protocol/types.js'
import type { EnqueueWorkInput } from './stores.js'
import { ControlPlaneError, ControlPlaneService, type LeaseProof } from './service.js'
import { readPreviewFrames } from '../host/preview-stream.js'

export interface ControlPlaneServerOptions {
  service: ControlPlaneService
  claimWork: (workerId: string, requestId?: string, workKinds?: readonly string[], lanes?: readonly import('../protocol/types.js').WorkLane[], executionClass?: import('../protocol/types.js').ExecutionClass) => Promise<import('../protocol/types.js').WorkItem | null>
  recoverWork?: (proof: LeaseProof, signal?: AbortSignal) => Promise<boolean>
  waitForWork?: NonNullable<import('../host/port.js').HostPort['waitForWork']>
  streamPreview?: (proof: LeaseProof, frames: AsyncIterable<import('../protocol/preview.js').PreviewFrame>, signal: AbortSignal) => Promise<void>
  serviceToken: string
  logger?: Logger
  metrics?: MetricsRegistry
  maxBodyBytes?: number
  ready?: () => Promise<boolean>
}

async function readBody(req: http.IncomingMessage, maxBytes: number, signal: AbortSignal): Promise<unknown> {
  const release = jsonBytes.acquire('json', maxBytes * 2)
  const cancel = () => req.destroy(new Error('request body deadline exceeded'))
  signal.addEventListener('abort', cancel, { once: true })
  try {
    signal.throwIfAborted()
    const chunks: Buffer[] = []
    let size = 0
    for await (const chunk of req) {
      size += chunk.length
      if (size > maxBytes) throw new ControlPlaneError(413, 'request body too large')
      chunks.push(chunk)
    }
    if (!size) return {}
    try { return JSON.parse(Buffer.concat(chunks).toString('utf8')) }
    catch { throw new ControlPlaneError(400, 'request body must be JSON') }
  } finally { signal.removeEventListener('abort', cancel); release() }
}

function json(res: http.ServerResponse, status: number, payload: unknown): void {
  const body = JSON.stringify(payload)
  res.writeHead(status, { 'content-type': 'application/json' })
  res.end(body)
}

function stringField(body: Record<string, unknown>, key: string): string {
  const value = body[key]
  if (typeof value !== 'string') throw new ControlPlaneError(400, `${key} must be a string`)
  return value
}

function leaseProofOf(id: string, body: Record<string, unknown>): LeaseProof {
  const fence = body['fence']
  if (!Number.isSafeInteger(fence) || Number(fence) < 1) throw new ControlPlaneError(400, 'fence must be a positive safe integer')
  return { id, fence: fence as number, leaseToken: stringField(body, 'leaseToken') }
}

export class ControlPlaneServer {
  private readiness: Promise<boolean> | undefined
  private readonly server: http.Server
  private readonly logger: Logger

  constructor(private readonly options: ControlPlaneServerOptions) {
    this.logger = options.logger ?? nullLogger
    this.server = http.createServer((req, res) => {
      void this.handle(req, res).catch((error: unknown) => {
        if (error instanceof ControlPlaneError) {
          json(res, error.status, { error: error.message, ...(error.code ? { code: error.code } : {}) })
          return
        }
        this.logger.error('control-plane request failed', { path: req.url?.split('?')[0], error })
        json(res, 500, { error: 'internal error' })
      })
    })
  }

  private authorized(req: http.IncomingMessage): boolean {
    const auth = req.headers.authorization
    const provided = typeof auth === 'string' && auth.startsWith('Bearer ') ? auth.slice(7) : ''
    const expected = this.options.serviceToken
    const providedBuffer = Buffer.from(provided)
    const expectedBuffer = Buffer.from(expected)
    if (providedBuffer.length !== expectedBuffer.length) return false
    return timingSafeEqual(providedBuffer, expectedBuffer)
  }

  private async handle(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    const url = new URL(req.url ?? '/', 'http://internal')
    const path = url.pathname
    const method = req.method ?? 'GET'
    this.options.metrics?.counter('agentos_control_http_requests_total', 'Control-plane HTTP requests including retries').inc({
      operation: path.endsWith('/preview') ? 'preview' : path.endsWith('/claim') ? 'claim' : path.endsWith('/wait') ? 'wait'
        : path === '/v5/sessions' ? 'checkpoint' : path.endsWith('/context') ? 'context' : 'other',
    })
    const service = this.options.service
    const disconnected = new AbortController()
    res.once('close', () => { if (!res.writableFinished) disconnected.abort(new Error('worker connection closed')) })

    if (method === 'GET' && path === '/healthz') {
      json(res, 200, { ok: true })
      return
    }
    if (method === 'GET' && path === '/readyz') {
      this.readiness ??= (this.options.ready?.() ?? Promise.resolve(false)).catch(() => false).finally(() => { this.readiness = undefined })
      let timer: NodeJS.Timeout | undefined
      const ok = await Promise.race([this.readiness, new Promise<false>(resolve => { timer = setTimeout(() => resolve(false), 2_000) })])
      if (timer) clearTimeout(timer)
      json(res, ok ? 200 : 503, { ok }); return
    }
    if (method === 'GET' && path === '/metrics' && this.options.metrics) {
      res.writeHead(200, { 'content-type': 'text/plain; version=0.0.4' })
      res.end(this.options.metrics.expose())
      return
    }
    if (!this.authorized(req)) {
      json(res, 401, { error: 'invalid service identity' })
      return
    }

    const preview = /^\/v5\/work\/([^/]+)\/preview$/.exec(path)
    if (method === 'POST' && preview && this.options.streamPreview) {
      const proof = leaseProofOf(decodeURIComponent(preview[1]!), {
        fence: Number(req.headers['x-lingxios-fence']), leaseToken: req.headers['x-lingxios-lease'],
      })
      await this.options.streamPreview(proof, readPreviewFrames(req, disconnected.signal), disconnected.signal)
      json(res, 200, { ok: true })
      return
    }

    const binary = /^\/v5\/work\/([^/]+)\/artifact-bytes$/.exec(path)
    if (method === 'POST' && binary) {
      const proof = leaseProofOf(decodeURIComponent(binary[1]!), {
        fence: Number(req.headers['x-lingxios-fence']), leaseToken: req.headers['x-lingxios-lease'],
      })
      const metadata = req.headers['x-lingxios-artifact']
      if (typeof metadata !== 'string' || metadata.length > 8192) throw new ControlPlaneError(400, 'invalid artifact metadata')
      let artifact: unknown
      try { artifact = JSON.parse(Buffer.from(metadata, 'base64url').toString('utf8')) }
      catch { throw new ControlPlaneError(400, 'invalid artifact metadata') }
      const work = await service.requireLease(proof, { rejectCancelled: true })
      const size = (artifact as { size?: number })?.size
      if (!Number.isSafeInteger(size) || size! < 0 || size! > 16 * 1024 * 1024) throw new ControlPlaneError(413, 'invalid artifact size')
      const release = uploadBytes.acquire(work.tenantId, Math.max(64 * 1024, size!))
      const signal = AbortSignal.any([disconnected.signal, AbortSignal.timeout(30_000)])
      const cancel = () => req.destroy(new Error('artifact upload cancelled'))
      signal.addEventListener('abort', cancel, { once: true })
      try { await service.stageArtifactStream(proof, artifact as never, req, signal) }
      finally { signal.removeEventListener('abort', cancel); release() }
      json(res, 200, { ok: true }); return
    }

    const maxBody = path.endsWith('/artifacts') ? 24 * 1024 * 1024 : this.options.maxBodyBytes ?? 8 * 1024 * 1024
    const parsed = method === 'GET' ? {} : await readBody(req, maxBody, AbortSignal.any([disconnected.signal, AbortSignal.timeout(30_000)]))
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) throw new ControlPlaneError(400, 'request body must be a JSON object')
    const body = parsed as Record<string, unknown>

    // Route table -----------------------------------------------------------
    if (method === 'POST' && path === '/v5/work/wait' && this.options.waitForWork) {
      if (body['cursor'] !== undefined && typeof body['cursor'] !== 'string'
        || !Number.isSafeInteger(body['timeoutMs']) || Number(body['timeoutMs']) < 1 || Number(body['timeoutMs']) > 25_000) {
        throw new ControlPlaneError(400, 'invalid work wait')
      }
      json(res, 200, await this.options.waitForWork(body['cursor'] as string | undefined, Number(body['timeoutMs']), disconnected.signal))
      return
    }

    if (method === 'POST' && path === '/v5/work') {
      json(res, 200, await service.enqueue(body as unknown as EnqueueWorkInput))
      return
    }
    if (method === 'POST' && path === '/v5/work/claim') {
      if (body['protocol'] !== AGENT_OS_PROTOCOL_VERSION) throw new ControlPlaneError(409, 'upgrade worker: claims require the per-run recovery protocol', 'protocol_mismatch')
      if (body['requestId'] !== undefined && typeof body['requestId'] !== 'string') {
        throw new ControlPlaneError(400, 'requestId must be a string')
      }
      if (!Array.isArray(body['workKinds']) || body['workKinds'].length < 1 || body['workKinds'].length > 64
        || body['workKinds'].some(kind => typeof kind !== 'string' || !/^[a-z][a-z0-9_]{0,63}$/.test(kind))) throw new ControlPlaneError(400, 'workKinds are required')
      json(res, 200, await this.options.claimWork(stringField(body, 'workerId'),
        typeof body['requestId'] === 'string' ? body['requestId'] : undefined, body['workKinds'], body['lanes'] as never, body['executionClass'] as never))
      return
    }

    const workMatch = /^\/v5\/work\/([^/]+)\/([a-z-]+)$/.exec(path)
    if (workMatch) {
      const id = decodeURIComponent(workMatch[1]!)
      const operation = workMatch[2]!
      if (method === 'GET' && operation === 'context') {
        const proof: LeaseProof = {
          id,
          fence: Number(url.searchParams.get('fence')),
          leaseToken: url.searchParams.get('leaseToken') ?? '',
        }
        json(res, 200, await service.loadContext(proof, url.searchParams.get('includeSession') === 'true', disconnected.signal))
        return
      }
      if (method === 'POST') {
        const proof = leaseProofOf(id, body)
        switch (operation) {
          case 'recover':
            json(res, 200, await this.options.recoverWork?.(proof, disconnected.signal) ?? false); return
          case 'heartbeat':
            json(res, 200, await service.heartbeat(proof)); return
          case 'model-budget':
            json(res, 200, await service.reserveModelCall(proof, stringField(body, 'callId'), body['limits'] as never)); return
          case 'model-usage':
            await service.recordModelUsage(proof, stringField(body, 'callId'), body['usage'] as never, body['observation'] as never)
            json(res, 200, { ok: true }); return
          case 'session':
            json(res, 200, { session: await service.getSession(proof, stringField(body, 'key')) }); return
          case 'yield':
            await service.yieldWork(proof); json(res, 200, { ok: true }); return
          case 'actions':
            json(res, 200, await service.executeAction(proof, body['action'] as never, disconnected.signal)); return
          case 'reconcile':
            json(res, 200, await service.recoverCell(proof, stringField(body, 'cellId'), disconnected.signal)); return
          case 'step':
            json(res, 200, await service.recoverStep(proof, stringField(body, 'cellId'))); return
          case 'verify':
            json(res, 200, await service.verifyCandidate(proof, body['candidate'] as never, disconnected.signal)); return
          case 'memory-review':
            json(res,200,await service.prepareMemoryReview(proof,body['action'] as never)); return
          case 'memory-review-result':
            await service.recordMemoryReview(proof,body['action'] as never,stringField(body,'hash'),body['review'] as never)
            json(res,200,{ok:true}); return
          case 'checkpoint':
            await service.saveStep(proof, body['step'] as never); json(res, 200, { ok: true }); return
          case 'artifacts':
            await service.stageArtifact(proof, body['artifact'] as never, stringField(body, 'contentBase64'), disconnected.signal)
            json(res, 200, { ok: true }); return
          case 'events':
            await service.recordEvent(proof, body['event'] as RunEvent); json(res, 200, { ok: true }); return
          case 'result':
            await service.commitResult(proof, body['message'] as AssistantMessage); json(res, 200, { ok: true }); return
          case 'complete':
            await service.complete(proof, {
              status: body['status'] as WorkCompletion['status'],
              ...(typeof body['error'] === 'string' ? { error: body['error'] } : {}),
              ...(body['goalOutcome'] === undefined ? {} : { goalOutcome: body['goalOutcome'] as NonNullable<WorkCompletion['goalOutcome']> }),
            })
            json(res, 200, { ok: true }); return
          case 'wait':
            await service.waitWork(proof, body['goalOutcome'] as import('../protocol/outcome.js').WaitingOutcome)
            json(res, 200, { ok: true }); return
          case 'cancel':
            json(res, 200, { ok: await service.requestCancel(id) }); return
          case 'preempt':
            json(res, 200, { ok: await service.requestPreempt(id) }); return
          case 'steer':
            json(res, 200, { ok: await service.addSteer(id, stringField(body, 'text')) }); return
        }
      }
    }

    if (method === 'PUT' && path === '/v5/sessions') {
      const proof = leaseProofOf(stringField(body, 'workId'), body)
      json(res, 200, await service.saveSession(proof, body['session'] as SessionRecord))
      return
    }

    json(res, 404, { error: 'not found' })
  }

  listen(port: number, host = '0.0.0.0'): Promise<number> {
    return new Promise((resolveListen, rejectListen) => {
      this.server.once('error', rejectListen)
      this.server.listen(port, host, () => {
        resolveListen((this.server.address() as AddressInfo).port)
      })
    })
  }

  async close(): Promise<void> {
    await new Promise<void>((resolveClose) => {
      this.server.close(() => resolveClose())
      this.server.closeAllConnections?.()
    })
  }
}

export { ControlPlaneError, ControlPlaneService }
