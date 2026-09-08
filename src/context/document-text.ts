import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { ByteBudget, ResourceQuota } from '../resource-quota.js'

const parsers = new ResourceQuota(2, 16)
const inputs = new ByteBudget(64 * 1024 * 1024, 64 * 1024 * 1024)
export type DocumentFormat = 'docx' | 'pdf' | 'html' | 'source' | 'text' | 'json'

/** Isolated CPU work; a slot is returned only on process close, never merely on caller abort. */
export async function extractDocumentText(bytes: Uint8Array, format: DocumentFormat, signal?: AbortSignal): Promise<string> {
  signal?.throwIfAborted()
  if (bytes.byteLength > 16 * 1024 * 1024) throw new Error('Document input exceeds 16 MiB')
  const release = inputs.acquire('parsers', bytes.byteLength)
  return parsers.run(() => new Promise<string>((resolve, reject) => {
    const child = spawn(process.execPath, ['--max-old-space-size=128', fileURLToPath(new URL('./document-worker.js', import.meta.url)), format],
      { windowsHide: true, env: {}, stdio: ['pipe', 'pipe', 'ignore'] })
    const chunks: Buffer[] = []
    let size = 0, failure: unknown
    const stop = (reason: unknown) => { failure ??= reason; child.kill('SIGKILL') }
    const cancel = () => stop(signal?.reason ?? new Error('Document extraction cancelled'))
    const timer = setTimeout(() => stop(new Error('Document text extraction failed or exceeded resource limits')), 10_000)
    signal?.addEventListener('abort', cancel, { once: true })
    child.on('error', error => { failure ??= error })
    child.stdout.on('data', (chunk: Buffer) => {
      size += chunk.length
      if (size > 4 * 1024 * 1024) stop(new Error('Document extracted text exceeds limit'))
      else chunks.push(chunk)
    })
    child.on('close', code => {
      clearTimeout(timer); signal?.removeEventListener('abort', cancel)
      if (failure || code !== 0) reject(failure ?? new Error('Document text extraction failed or exceeded resource limits (invalid format or encoded data)'))
      else resolve(Buffer.concat(chunks).toString('utf8'))
    })
    child.stdin.on('error', () => { /* close owns failure and quota release */ })
    if (signal?.aborted) cancel()
    else child.stdin.end(bytes)
  }), signal).finally(release)
}
