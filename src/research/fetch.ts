import { lookup } from 'node:dns/promises'
import { request as httpRequest } from 'node:http'
import { request as httpsRequest, type RequestOptions } from 'node:https'
import { isIP } from 'node:net'
import { isPublicAddress, researchUrl } from './address.js'

const MAX_BYTES = 2 * 1024 * 1024

export function pinnedRequestOptions(url: URL, address: string): RequestOptions {
  if (!isPublicAddress(address)) throw new Error('research address is blocked')
  const host = url.hostname.replace(/^\[|\]$/g, '')
  return { hostname: address, agent: false,
    ...(!isIP(host) ? { servername: host } : {}),
    headers: { host: url.host, accept: 'text/html,application/json,text/plain', 'accept-encoding': 'identity', 'user-agent': 'LingxiOS-Research/1.0' },
  }
}

export async function fetchResearch(raw: string): Promise<{ url: string; contentType: string; body: Buffer }> {
  const controller = new AbortController()
  let timeout: ReturnType<typeof setTimeout> | undefined
  const expired = new Promise<never>((_resolve, reject) => {
    timeout = setTimeout(() => { controller.abort(); reject(new Error('research request timed out')) }, 15_000)
  })
  try {
    return await Promise.race([expired, (async () => {
      let url = researchUrl(raw)
      for (let redirects = 0; redirects <= 5; redirects++) {
        const host = url.hostname.replace(/^\[|\]$/g, '')
        const addresses = isIP(host) ? [{ address: host }] : await lookup(host, { all: true, verbatim: true })
        if (!addresses.length || addresses.some(item => !isPublicAddress(item.address))) throw new Error('research DNS resolved to a blocked address')
        controller.signal.throwIfAborted()
        const result = await new Promise<{ redirect: string } | { url: string; contentType: string; body: Buffer }>((resolve, reject) => {
          const request = (url.protocol === 'https:' ? httpsRequest : httpRequest)(url, {
            ...pinnedRequestOptions(url, addresses[0]!.address), signal: controller.signal,
          }, response => {
            const status = response.statusCode ?? 0
            if (status >= 300 && status < 400) {
              const location = response.headers.location
              response.destroy()
              if (!location) reject(new Error('research redirect has no location'))
              else resolve({ redirect: location })
              return
            }
            if (status < 200 || status >= 300 || Number(response.headers['content-length'] ?? 0) > MAX_BYTES) {
              response.destroy()
              reject(new Error(`research response rejected (HTTP ${status}, size limit ${MAX_BYTES})`))
              return
            }
            if (response.headers['content-encoding'] && response.headers['content-encoding'] !== 'identity') {
              response.destroy()
              reject(new Error('research source returned unsupported content encoding'))
              return
            }
            const chunks: Buffer[] = []
            let size = 0
            response.on('data', (chunk: Buffer) => {
              size += chunk.length
              if (size > MAX_BYTES) response.destroy(new Error('research source exceeds 2 MiB'))
              else chunks.push(chunk)
            })
            response.on('error', reject)
            response.on('end', () => resolve({ url: url.href, contentType: String(response.headers['content-type'] ?? 'application/octet-stream').split(';')[0]!.toLowerCase(), body: Buffer.concat(chunks) }))
          })
          request.on('error', reject)
          request.end()
        })
        if (!('redirect' in result)) return result
        if (redirects === 5) throw new Error('research source redirected too many times')
        url = researchUrl(new URL(result.redirect, url).href)
      }
      throw new Error('research redirect limit exceeded')
    })()])
  } finally {
    clearTimeout(timeout)
  }
}
