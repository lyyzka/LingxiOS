import { extractDocumentText } from '../context/document-text.js'
import type { VerificationRecord } from '../outcome/verification.js'
import { createHash, randomUUID, webcrypto } from 'node:crypto'
import { open, realpath, mkdir, link, unlink } from 'node:fs/promises'
import { isAbsolute, relative, resolve } from 'node:path'
import type { SqlPool } from '../control-plane/pg-store.js'
import { kernelHome } from '../kernel/manager.js'
import { snapshotArtifacts } from '../outcome/envelope.js'
import type { AssistantMessage, KernelArtifact, WorkItem } from '../protocol/types.js'
import type { MessageIdentity, RequestInput } from './index.js'
import type { ArtifactInput } from '../tools/definition.js'
import { authorizeRunRead } from '../collaboration/api.js'
import { ByteBudget } from '../resource-quota.js'
const artifactBytes = new ByteBudget()

export async function createNativeArtifact(homesRoot: string, work: Omit<WorkItem, 'leaseToken'>, input: ArtifactInput, signal?: AbortSignal) {
  if (input.bytes.byteLength > 16 * 1024 * 1024) throw new Error('artifact exceeds the 16 MiB limit')
  const release = artifactBytes.acquire(work.tenantId, Math.max(64 * 1024, input.bytes.byteLength * 2))
  let hash: string
  try {
    signal?.throwIfAborted()
    hash = Buffer.from(await webcrypto.subtle.digest('SHA-256', Uint8Array.from(input.bytes))).toString('hex')
  } finally { release() }
  const artifact = snapshotArtifacts([{ path: input.path, mime: input.mime, size: input.bytes.byteLength,
    sha256: hash, ...(input.source ? { source: input.source } : {}) }])[0]!
  await stageArtifact(homesRoot, work, artifact, input.bytes, signal)
  return artifact
}

function artifactDirectory(root: string, identity: MessageIdentity): string {
  const hash = createHash('sha256').update(JSON.stringify([identity.tenantId, identity.agentId, identity.runId])).digest('hex')
  return resolve(root, '.committed-artifacts', hash)
}

const inside = (parent: string, child: string) => {
  const path = relative(parent, child)
  return path !== '..' && !path.startsWith('../') && !path.startsWith('..\\') && !isAbsolute(path)
}

async function checkedFile(root: string, directory: string, filename: string, artifact: KernelArtifact, signal?: AbortSignal, readBytes = true) {
  if (artifact.size > 16 * 1024 * 1024) throw new Error('artifact exceeds the 16 MiB limit')
  const home = await realpath(directory)
  const file = await realpath(resolve(home, filename))
  if (!inside(root, home) || !inside(home, file)) throw new Error('artifact is outside its committed home')
  const handle = await open(file, 'r')
  try {
    const before = await handle.stat()
    if (!before.isFile() || before.size !== artifact.size) throw new Error('artifact changed since commitment')
    const bytes = readBytes ? Buffer.alloc(artifact.size) : Buffer.alloc(0)
    const chunk = Buffer.alloc(64 * 1024), hash = createHash('sha256')
    let length = 0
    for (;;) {
      signal?.throwIfAborted()
      const { bytesRead } = await handle.read(chunk, 0, chunk.length, null)
      if (!bytesRead) break
      if (length + bytesRead > artifact.size) throw new Error('artifact changed since commitment')
      const part = chunk.subarray(0, bytesRead)
      hash.update(part)
      if (readBytes) bytes.set(part, length)
      length += bytesRead
    }
    const after = await handle.stat()
    if (length !== artifact.size || before.mtimeMs !== after.mtimeMs || after.size !== artifact.size
      || hash.digest('hex') !== artifact.sha256.toLowerCase()) throw new Error('artifact changed since commitment')
    return { artifact, bytes }
  } finally { await handle.close() }
}

async function storeArtifactBytes(root: string, directory: string, artifact: KernelArtifact, bytes: AsyncIterable<Uint8Array>, signal?: AbortSignal) {
  await mkdir(directory, { recursive: true, mode: 0o700 })
  if (await realpath(directory) !== directory) throw new Error('artifact storage must not traverse directory links')
  const destination = resolve(directory, artifact.sha256.toLowerCase())
  const temporary = resolve(directory, randomUUID())
  const handle = await open(temporary, 'wx', 0o600)
  try {
    const hash = createHash('sha256')
    let size = 0
    for await (const chunk of bytes) {
      signal?.throwIfAborted()
      size += chunk.byteLength
      if (size > artifact.size) throw new Error('artifact content exceeds its commitment')
      for (let offset = 0; offset < chunk.byteLength; offset += 64 * 1024) {
        signal?.throwIfAborted()
        const part = chunk.subarray(offset, offset + 64 * 1024)
        hash.update(part)
        await handle.writeFile(part)
      }
    }
    if (size !== artifact.size || hash.digest('hex') !== artifact.sha256.toLowerCase()) throw new Error('artifact content does not match its commitment')
    signal?.throwIfAborted()
    await handle.sync()
    await handle.close()
    try { await link(temporary, destination) }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error }
    await checkedFile(root, directory, artifact.sha256.toLowerCase(), artifact, signal, false)
  } finally { await handle.close(); await unlink(temporary) }
}

export async function stageArtifact(homesRoot: string, work: Omit<WorkItem, 'leaseToken'>,
  artifact: KernelArtifact, bytes: Uint8Array | AsyncIterable<Uint8Array>, signal?: AbortSignal) {
  artifact = snapshotArtifacts([artifact])[0]!
  if (artifact.size > 16 * 1024 * 1024) throw new Error('artifact exceeds the 16 MiB limit')
  const release = artifactBytes.acquire(work.tenantId, Math.max(64 * 1024, artifact.size))
  try {
    signal?.throwIfAborted()
    await mkdir(homesRoot, { recursive: true, mode: 0o700 })
    const root = await realpath(homesRoot)
    const stream = bytes instanceof Uint8Array ? (async function* () { yield bytes })() : bytes
    await storeArtifactBytes(root, artifactDirectory(root, { ...work, runId: work.id }), artifact, stream, signal)
  } finally { release() }
}

export async function persistArtifacts(homesRoot: string, work: Omit<WorkItem, 'leaseToken'>, message: AssistantMessage) {
  const artifacts = snapshotArtifacts(message.envelope.artifacts)
  if (!artifacts.length) return
  const root = await realpath(homesRoot)
  const directory = artifactDirectory(root, { ...work, runId: work.id })
  for (const artifact of artifacts) {
    try { await checkedFile(root, directory, artifact.sha256.toLowerCase(), artifact, undefined, false); continue }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error }
    const release = artifactBytes.acquire(work.tenantId, Math.max(64 * 1024, artifact.size))
    try {
      const { bytes } = await checkedFile(root, kernelHome(root, work), artifact.path, artifact)
      await storeArtifactBytes(root, directory, artifact, (async function* () { yield bytes })())
    } finally { release() }
  }
}

export async function readArtifact(database: SqlPool, homesRoot: string,
  identity: MessageIdentity & Pick<RequestInput, 'principalId' | 'threadId'>, path: string) {
  await authorizeRunRead(database, identity)
  if (!identity.principalId?.trim()) throw new Error('authenticated principalId is required')
  const { rows } = await database.query(`SELECT result.message FROM lingxios.agent_work_items work
    JOIN lingxios.agent_results result ON result.id=work.result_id
    WHERE work.id=$1 AND work.tenant_id=$2 AND work.agent_id=$3 AND work.session_id=$4
      AND work.principal_id=$5 AND work.thread_id IS NOT DISTINCT FROM $6`,
  [identity.runId, identity.tenantId, identity.agentId, identity.sessionId, identity.principalId, identity.threadId ?? null])
  if (!rows[0]) return null
  const message = rows[0]['message'] as AssistantMessage
  const artifact = snapshotArtifacts(message.envelope.artifacts).find(item => item.path === path)
  if (!artifact) return null
  try {
    const root = await realpath(homesRoot)
    const directory = artifactDirectory(root, identity)
    if (await realpath(directory) !== directory) throw new Error('artifact storage must not traverse directory links')
    const release = artifactBytes.acquire(identity.tenantId, Math.max(64 * 1024, artifact.size))
    try { return await checkedFile(root, directory, artifact.sha256.toLowerCase(), artifact) }
    finally { release() }
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return null
    throw error
  }
}

/** Read the same committed bytes later served by the download endpoint. Never accept model-supplied file contents. */
export async function inspectArtifacts(homesRoot: string, work: Omit<WorkItem, 'leaseToken'>, artifacts: KernelArtifact[], external?: AbortSignal): Promise<VerificationRecord[]> {
  const signal = AbortSignal.any([AbortSignal.timeout(20_000), ...external ? [external] : []])
  const records: VerificationRecord[] = [], started = Date.now()
  for (const artifact of snapshotArtifacts(artifacts)) {
    const checker = `artifact:${artifact.path}`
    external?.throwIfAborted()
    const release = artifactBytes.acquire(work.tenantId, Math.max(64 * 1024, artifact.size))
    try {
      if (Date.now() - started > 20_000) {
        records.push({ checker, status: 'inconclusive', evidence: { artifact, reason: 'File inspection time budget exhausted' } }); continue
      }
      const root = await realpath(homesRoot), directory = artifactDirectory(root, { ...work, runId: work.id })
      const { bytes } = await checkedFile(root, directory, artifact.sha256.toLowerCase(), artifact, signal)
      let text: string | undefined
      const mime = artifact.mime.split(';')[0]!.toLowerCase()
      if (mime === 'application/pdf') text = await extractDocumentText(bytes, 'pdf', signal)
      else if (mime === 'application/vnd.openxmlformats-officedocument.wordprocessingml.document') text = await extractDocumentText(bytes, 'docx', signal)
      else if (mime.startsWith('text/') || ['application/json','application/javascript','application/xml','image/svg+xml'].includes(mime)) {
        text = await extractDocumentText(bytes, mime === 'application/json' ? 'json' : 'text', signal)
      }
      records.push({ checker, status: text === undefined ? 'inconclusive' : 'passed', evidence: {
        artifact, byteCount: bytes.length, sha256: artifact.sha256,
        ...(text === undefined ? { reason: 'Bytes and hash verified; no content parser is available for this format' }
          : { extractedText: text.slice(0, 12_000), truncated: text.length > 12_000,
            scope: 'Readable bytes and extracted text; layout, images and task semantics are separate checks' }),
      } })
    } catch {
      external?.throwIfAborted()
      records.push({ checker, status: signal.aborted ? 'inconclusive' : 'failed', evidence: { artifact, reason: 'File bytes, hash or declared format could not be verified' } })
    } finally { release() }
  }
  return records
}
