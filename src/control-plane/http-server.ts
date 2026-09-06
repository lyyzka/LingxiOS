/**
 * HTTP adapter over {@link ControlPlaneService}. Dependency-free node:http
 * with a small explicit route table; every route is authenticated with a
 * timing-safe service-token comparison.
 */
import { timingSafeEqual } from 'node:crypto'
import http from 'node:http'
import type { AddressInfo } from 'node:net'
import { nullLogger, type Logger } from '../logging.js'
import type { MetricsRegistry } from '../metrics.js'
import type { AssistantMessage, RunEvent, SessionRecord, WorkCompletion } from '../protocol/types.js'
import type { EnqueueWorkInput } from './stores.js'
import { ControlPlaneError, ControlPlaneService, type LeaseProof } from './service.js'

export interface ControlPlaneServerOptions {
  service: ControlPlaneService
  claimWork: (workerId: string, requestId?: string, workKinds?: readonly string[]) => Promise<import('../protocol/types.js').WorkItem | null>
  serviceToken: string
  logger?: Logger
  metrics?: MetricsRegistry
  maxBodyBytes?: number
  ready?: () => Promise<boolean>
}

function readBody(req: http.IncomingMessage, maxBytes: number): Promise<unknown> {
  return new Promise((resolveBody, rejectBody) => {
    const chunks: Buffer[] = []
    let size = 0
    req.on('data', (chunk: Buffer) => {
      size += chunk.length
      if (size > maxBytes) {
        rejectBody(new ControlPlaneError(413, 'request body too large'))
        req.destroy()
        return
      }
      chunks.push(chunk)
    })
    req.on('end', () => {
      if (chunks.length === 0) { resolveBody({}); return }
      try {
        resolveBody(JSON.parse(Buffer.concat(chunks).toString('utf8')))
      } catch {
        rejectBody(new ControlPlaneError(400, 'request body must be JSON'))
      }
    })
    req.on('error', rejectBody)
  })
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
    const service = this.options.service

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

    const maxBody = path.endsWith('/artifacts') || path.endsWith('/lecture') ? 24 * 1024 * 1024 : this.options.maxBodyBytes ?? 8 * 1024 * 1024
    const parsed = method === 'GET' ? {} : await readBody(req, maxBody)
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) throw new ControlPlaneError(400, 'request body must be a JSON object')
    const body = parsed as Record<string, unknown>

    // Route table -----------------------------------------------------------
    if (method === 'POST' && path === '/v3/work') {
      json(res, 200, await service.enqueue(body as unknown as EnqueueWorkInput))
      return
    }
    if (method === 'POST' && path === '/v3/work/claim') {
      if (body['requestId'] !== undefined && typeof body['requestId'] !== 'string') {
        throw new ControlPlaneError(400, 'requestId must be a string')
      }
      if (!Array.isArray(body['workKinds']) || body['workKinds'].length < 1 || body['workKinds'].length > 64
        || body['workKinds'].some(kind => typeof kind !== 'string' || !/^[a-z][a-z0-9_]{0,63}$/.test(kind))) throw new ControlPlaneError(400, 'workKinds are required')
      json(res, 200, await this.options.claimWork(stringField(body, 'workerId'),
        typeof body['requestId'] === 'string' ? body['requestId'] : undefined, body['workKinds']))
      return
    }

    const workMatch = /^\/v3\/work\/([^/]+)\/([a-z-]+)$/.exec(path)
    if (workMatch) {
      const id = decodeURIComponent(workMatch[1]!)
      const operation = workMatch[2]!
      if (method === 'GET' && operation === 'context') {
        const proof: LeaseProof = {
          id,
          fence: Number(url.searchParams.get('fence')),
          leaseToken: url.searchParams.get('leaseToken') ?? '',
        }
        json(res, 200, await service.loadContext(proof))
        return
      }
      if (method === 'POST') {
        const proof = leaseProofOf(id, body)
        switch (operation) {
          case 'lecture':
            json(res, 200, await service.lecture(proof, body['command'] as never)); return
          case 'heartbeat':
            json(res, 200, await service.heartbeat(proof)); return
          case 'model-budget':
            json(res, 200, await service.reserveModelCall(proof, stringField(body, 'callId'), body['limits'] as never)); return
          case 'model-usage':
            await service.recordModelUsage(proof, stringField(body, 'callId'), body['usage'] as never)
            json(res, 200, { ok: true }); return
          case 'session':
            json(res, 200, { session: await service.getSession(proof, stringField(body, 'key')) }); return
          case 'yield':
            await service.yieldWork(proof); json(res, 200, { ok: true }); return
          case 'actions':
            json(res, 200, await service.executeAction(proof, body['action'] as never)); return
          case 'reconcile':
            json(res, 200, await service.recoverCell(proof, stringField(body, 'cellId'))); return
          case 'step':
            json(res, 200, await service.recoverStep(proof, stringField(body, 'cellId'))); return
          case 'artifacts':
            await service.stageArtifact(proof, body['artifact'] as never, stringField(body, 'contentBase64'))
            json(res, 200, { ok: true }); return
          case 'events':
            await service.recordEvent(proof, body['event'] as RunEvent); json(res, 200, { ok: true }); return
          case 'messages':
            await service.commitMessage(proof, body['message'] as AssistantMessage); json(res, 200, { ok: true }); return
          case 'complete':
            await service.complete(proof, {
              status: body['status'] as WorkCompletion['status'],
              ...(typeof body['resultText'] === 'string' ? { resultText: body['resultText'] } : {}),
              ...(typeof body['error'] === 'string' ? { error: body['error'] } : {}),
              ...(body['goalOutcome'] === undefined ? {} : { goalOutcome: body['goalOutcome'] as NonNullable<WorkCompletion['goalOutcome']> }),
            })
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

    if (method === 'PUT' && path === '/v3/sessions') {
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
