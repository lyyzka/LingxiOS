import { execFile } from 'node:child_process'
import { fileURLToPath } from 'node:url'

// ponytail: at most two parsers per process; use a bounded job queue if ingress throughput requires it.
let active = 0
export async function extractDocumentText(bytes: Uint8Array, format: 'docx' | 'pdf'): Promise<string> {
  if (bytes.byteLength > 16 * 1024 * 1024) throw new Error('Document input exceeds 16 MiB')
  if (active >= 2) throw new Error('Document parser is busy; retry request ingestion later')
  active++
  try {
    return await new Promise<string>((resolve, reject) => {
      const child = execFile(process.execPath, ['--max-old-space-size=128', fileURLToPath(new URL('./document-worker.js', import.meta.url)), format],
        { timeout: 10_000, maxBuffer: 4 * 1024 * 1024, windowsHide: true, env: {} }, (error, stdout) => {
          if (error) reject(new Error('Document text extraction failed or exceeded resource limits'))
          else if (stdout.length > 1_000_000) reject(new Error('Document extracted text exceeds limit'))
          else resolve(stdout)
        })
      child.stdin?.on('error', () => { /* Process exit is handled by the callback. */ })
      child.stdin?.end(bytes)
    })
  } finally { active-- }
}
