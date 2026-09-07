import { extractDocumentText } from '../context/document-text.js'
import type { VerificationRecord } from '../outcome/verification.js'
import { createHash, randomUUID } from 'node:crypto'
import { open, realpath, mkdir, link, unlink } from 'node:fs/promises'
import { isAbsolute, relative, resolve } from 'node:path'
import type { SqlPool } from '../control-plane/pg-store.js'
import { kernelHome } from '../kernel/manager.js'
import { snapshotArtifacts } from '../outcome/envelope.js'
import type { AssistantMessage, KernelArtifact, WorkItem } from '../protocol/types.js'
import type { MessageIdentity, RequestInput } from './index.js'

function artifactDirectory(root: string, identity: MessageIdentity): string {
  const hash = createHash('sha256').update(JSON.stringify([identity.tenantId, identity.agentId, identity.runId])).digest('hex')
  return resolve(root, '.committed-artifacts', hash)
}

const inside = (parent: string, child: string) => {
  const path = relative(parent, child)
  return path !== '..' && !path.startsWith('../') && !path.startsWith('..\\') && !isAbsolute(path)
}

async function checkedFile(root: string, directory: string, filename: string, artifact: KernelArtifact) {
  // ponytail: bounded buffers; add streaming storage for files above 16 MiB.
  if (artifact.size > 16 * 1024 * 1024) throw new Error('artifact exceeds the 16 MiB limit')
  const home = await realpath(directory)
  const file = await realpath(resolve(home, filename))
  if (!inside(root, home) || !inside(home, file)) throw new Error('artifact is outside its committed home')
  const handle = await open(file, 'r')
  try {
    const before = await handle.stat()
    if (!before.isFile() || before.size !== artifact.size) throw new Error('artifact changed since commitment')
    const bytes = Buffer.alloc(artifact.size + 1)
    let length = 0
    while (length < bytes.length) {
      const read = await handle.read(bytes, length, bytes.length - length, null)
      if (!read.bytesRead) break
      length += read.bytesRead
    }
    const after = await handle.stat()
    const content = bytes.subarray(0, length)
    if (length !== artifact.size || before.mtimeMs !== after.mtimeMs || after.size !== artifact.size
      || createHash('sha256').update(content).digest('hex') !== artifact.sha256.toLowerCase()) throw new Error('artifact changed since commitment')
    return { artifact, bytes: content }
  } finally { await handle.close() }
}

async function storeArtifactBytes(root: string, directory: string, artifact: KernelArtifact, bytes: Uint8Array) {
  if (bytes.length !== artifact.size || createHash('sha256').update(bytes).digest('hex') !== artifact.sha256.toLowerCase()) {
    throw new Error('artifact content does not match its commitment')
  }
  await mkdir(directory, { recursive: true, mode: 0o700 })
  if (await realpath(directory) !== directory) throw new Error('artifact storage must not traverse directory links')
  const destination = resolve(directory, artifact.sha256.toLowerCase())
  try { await checkedFile(root, directory, artifact.sha256.toLowerCase(), artifact); return }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error }
  const temporary = resolve(directory, randomUUID())
  const handle = await open(temporary, 'wx', 0o600)
  try {
    await handle.writeFile(bytes)
    await handle.sync()
    await handle.close()
    try { await link(temporary, destination) }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error }
    await checkedFile(root, directory, artifact.sha256.toLowerCase(), artifact)
  } finally { await handle.close(); await unlink(temporary) }
}

export async function stageArtifact(homesRoot: string, work: Omit<WorkItem, 'leaseToken'>,
  artifact: KernelArtifact, bytes: Uint8Array) {
  if (artifact.size > 16 * 1024 * 1024) throw new Error('artifact exceeds the 16 MiB limit')
  await mkdir(homesRoot, { recursive: true, mode: 0o700 })
  const root = await realpath(homesRoot)
  await storeArtifactBytes(root, artifactDirectory(root, { ...work, runId: work.id }), artifact, bytes)
}

export async function persistArtifacts(homesRoot: string, work: Omit<WorkItem, 'leaseToken'>, message: AssistantMessage) {
  const artifacts = snapshotArtifacts(message.envelope.artifacts)
  if (!artifacts.length) return
  const root = await realpath(homesRoot)
  const directory = artifactDirectory(root, { ...work, runId: work.id })
  for (const artifact of artifacts) {
    try { await checkedFile(root, directory, artifact.sha256.toLowerCase(), artifact); continue }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error }
    const { bytes } = await checkedFile(root, kernelHome(root, work), artifact.path, artifact)
    await storeArtifactBytes(root, directory, artifact, bytes)
  }
}

export async function readArtifact(database: SqlPool, homesRoot: string,
  identity: MessageIdentity & Pick<RequestInput, 'principalId' | 'threadId'>, path: string) {
  if (!identity.principalId?.trim()) throw new Error('authenticated principalId is required')
  const { rows } = await database.query(`SELECT message.message,message.home_epoch FROM lingxios.agent_messages message
    JOIN lingxios.agent_work_items work ON work.id=message.run_id AND work.tenant_id=message.tenant_id
      AND work.agent_id=message.agent_id AND work.session_id=message.session_id
    WHERE message.run_id=$1 AND message.tenant_id=$2 AND message.agent_id=$3 AND message.session_id=$4
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
    return await checkedFile(root, directory, artifact.sha256.toLowerCase(), artifact)
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return null
    throw error
  }
}

/** Read the same committed bytes later served by the download endpoint. Never accept model-supplied file contents. */
export async function inspectArtifacts(homesRoot: string, work: Omit<WorkItem, 'leaseToken'>, artifacts: KernelArtifact[]): Promise<VerificationRecord[]> {
  const records: VerificationRecord[] = [], started = Date.now()
  for (const artifact of snapshotArtifacts(artifacts)) {
    const checker = `artifact:${artifact.path}`
    try {
      if (Date.now() - started > 20_000) {
        records.push({ checker, status: 'inconclusive', evidence: { artifact, reason: 'File inspection time budget exhausted' } }); continue
      }
      const root = await realpath(homesRoot), directory = artifactDirectory(root, { ...work, runId: work.id })
      const { bytes } = await checkedFile(root, directory, artifact.sha256.toLowerCase(), artifact)
      let text: string | undefined
      const mime = artifact.mime.split(';')[0]!.toLowerCase()
      if (mime === 'application/pdf') text = await extractDocumentText(bytes, 'pdf')
      else if (mime === 'application/vnd.openxmlformats-officedocument.wordprocessingml.document') text = await extractDocumentText(bytes, 'docx')
      else if (mime.startsWith('text/') || ['application/json','application/javascript','application/xml','image/svg+xml'].includes(mime)) {
        text = new TextDecoder('utf-8', { fatal: true }).decode(bytes)
        if (mime === 'application/json') JSON.parse(text)
        if (text.includes('\u0000')) throw new Error('Text file contains binary null bytes')
      }
      records.push({ checker, status: text === undefined ? 'inconclusive' : 'passed', evidence: {
        artifact, byteCount: bytes.length, sha256: artifact.sha256,
        ...(text === undefined ? { reason: 'Bytes and hash verified; no content parser is available for this format' }
          : { extractedText: text.slice(0, 12_000), truncated: text.length > 12_000,
            scope: 'Readable bytes and extracted text; layout, images and task semantics are separate checks' }),
      } })
    } catch {
      records.push({ checker, status: 'failed', evidence: { artifact, reason: 'File bytes, hash or declared format could not be verified' } })
    }
  }
  return records
}
