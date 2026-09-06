import { createHash, randomUUID } from 'node:crypto'
import { mkdir, readFile, link, open, unlink, lstat, realpath } from 'node:fs/promises'
import { resolve } from 'node:path'
import { isDeepStrictEqual } from 'node:util'
import type { SqlPool } from '../control-plane/pg-store.js'
import type { LectureArtifact } from './standalone.js'
import type { LectureCheckpoint, LecturePublisher, LectureRecord, LectureRepository } from './service.js'

export class MemoryLectureRepository implements LectureRepository {
  private records = new Map<string, LectureRecord>()
  private checkpoints = new Map<string, LectureCheckpoint>()
  async create(record: LectureRecord) {
    const key = `${record.tenantId}:${record.id}`
    if (this.records.has(key)) throw new Error('lecture already exists')
    this.records.set(key, structuredClone(record))
  }
  async get(tenantId: string, id: string) { return structuredClone(this.records.get(`${tenantId}:${id}`) ?? null) }
  async save(record: LectureRecord, expectedRevision: number) {
    const key = `${record.tenantId}:${record.id}`, current = this.records.get(key)
    if (!current || current.revision !== expectedRevision || record.revision < expectedRevision || record.revision > expectedRevision + 1) throw new Error('lecture revision changed')
    if ((current.stateVersion ?? 0) !== (record.stateVersion ?? 0)) throw new Error('lecture state changed')
    record.stateVersion = (record.stateVersion ?? 0) + 1
    this.records.set(key, structuredClone(record))
  }
  async claimPublication(record: LectureRecord) {
    const key = `${record.tenantId}:${record.id}`, current = this.records.get(key)
    if (!current || current.revision !== record.revision || current.status !== 'validating') return false
    if ((current.stateVersion ?? 0) !== (record.stateVersion ?? 0)) return false
    record.stateVersion = (record.stateVersion ?? 0) + 1
    this.records.set(key, structuredClone(record)); return true
  }
  async checkpoint(deckId: string, revision: number, value: LectureCheckpoint) { this.checkpoints.set(`${deckId}:${revision}:${value.stage}:${value.key}`, structuredClone(value)) }
  async readCheckpoint(deckId: string, revision: number, stage: LectureCheckpoint['stage'], key: string, inputHash: string) {
    const value = this.checkpoints.get(`${deckId}:${revision}:${stage}:${key}`)
    return value?.inputHash === inputHash ? structuredClone(value) : null
  }
}

export class PostgresLectureRepository implements LectureRepository {
  constructor(private readonly database: SqlPool) {}
  async create(record: LectureRecord) {
    await this.database.query(`INSERT INTO lingxios.lecture_decks(id,tenant_id,principal_id,revision,status,record)
      VALUES($1,$2,$3,$4,$5,$6::jsonb)`, [record.id, record.tenantId, record.principalId, record.revision, record.status, JSON.stringify(record)])
  }
  async get(tenantId: string, id: string) {
    const { rows } = await this.database.query('SELECT record FROM lingxios.lecture_decks WHERE tenant_id=$1 AND id=$2', [tenantId, id])
    const value = rows[0]?.['record']
    return value ? structuredClone(typeof value === 'string' ? JSON.parse(value) : value) as LectureRecord : null
  }
  async save(record: LectureRecord, expectedRevision: number) {
    const next = { ...record, stateVersion: (record.stateVersion ?? 0) + 1 }
    const result = await this.database.query(`UPDATE lingxios.lecture_decks SET revision=$1,status=$2,record=$3::jsonb,updated_at=NOW()
      WHERE tenant_id=$4 AND id=$5 AND revision=$6 AND COALESCE((record->>'stateVersion')::integer,0)=$7`,
      [record.revision, record.status, JSON.stringify(next), record.tenantId, record.id, expectedRevision, record.stateVersion ?? 0])
    if (result.rowCount !== 1) {
      const current = await this.get(record.tenantId, record.id)
      if (!current || !isDeepStrictEqual(current, next)) throw new Error('lecture state or revision changed')
    }
    record.stateVersion = next.stateVersion
  }
  async claimPublication(record: LectureRecord) {
    const next = { ...record, stateVersion: (record.stateVersion ?? 0) + 1 }
    const result = await this.database.query(`UPDATE lingxios.lecture_decks SET status='publishing',record=$1::jsonb,updated_at=NOW()
      WHERE tenant_id=$2 AND id=$3 AND revision=$4 AND status='validating' AND COALESCE((record->>'stateVersion')::integer,0)=$5`,
      [JSON.stringify(next), record.tenantId, record.id, record.revision, record.stateVersion ?? 0])
    if (result.rowCount !== 1) return false
    record.stateVersion = next.stateVersion
    return true
  }
  async checkpoint(deckId: string, revision: number, value: LectureCheckpoint) {
    await this.database.query(`INSERT INTO lingxios.lecture_checkpoints(deck_id,revision,stage,stage_key,input_hash,output_hash,attempts,result,completed_at)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9) ON CONFLICT(deck_id,revision,stage,stage_key) DO UPDATE SET
      input_hash=EXCLUDED.input_hash,output_hash=EXCLUDED.output_hash,attempts=lecture_checkpoints.attempts+1,result=EXCLUDED.result,completed_at=EXCLUDED.completed_at`,
    [deckId, revision, value.stage, value.key, value.inputHash, value.outputHash, value.attempts, JSON.stringify(value.output), value.completedAt])
  }
  async readCheckpoint(deckId: string, revision: number, stage: LectureCheckpoint['stage'], key: string, inputHash: string) {
    const { rows } = await this.database.query(`SELECT input_hash,output_hash,attempts,result,completed_at FROM lingxios.lecture_checkpoints
      WHERE deck_id=$1 AND revision=$2 AND stage=$3 AND stage_key=$4 AND input_hash=$5`, [deckId, revision, stage, key, inputHash])
    const row = rows[0]
    return row ? { stage, key, inputHash: String(row['input_hash']), outputHash: String(row['output_hash']), attempts: Number(row['attempts']), output: structuredClone(typeof row['result'] === 'string' ? JSON.parse(row['result']) : row['result']), completedAt: new Date(String(row['completed_at'])).toISOString() } : null
  }
}

/** Atomic local publisher; storage services can implement LecturePublisher directly. */
export class FileLecturePublisher implements LecturePublisher {
  constructor(private readonly root: string) {}
  private async directory(record: LectureRecord, create: boolean) {
    if (!Number.isSafeInteger(record.revision) || record.revision < 1) throw new Error('invalid lecture revision')
    let directory = resolve(this.root)
    for (const part of ['', createHash('sha256').update(`${record.tenantId}\0${record.id}`).digest('hex'), String(record.revision)]) {
      directory = resolve(directory, part)
      if (create) await mkdir(directory, { recursive: true, mode: 0o700 })
      if (!(await lstat(directory)).isDirectory() || resolve(await realpath(directory)) !== directory) throw new Error('lecture publication directory must not traverse a link')
    }
    return directory
  }
  async publish(record: LectureRecord, artifact: LectureArtifact) {
    if (artifact.filename !== 'lecture.html' || artifact.bytes.length !== artifact.size || artifact.size > 16 * 1024 * 1024
      || createHash('sha256').update(artifact.bytes).digest('hex') !== artifact.sha256) throw new Error('invalid lecture artifact')
    const directory = await this.directory(record, true)
    const temporary = resolve(directory, `${randomUUID()}.tmp`), destination = resolve(directory, artifact.filename)
    const file = await open(temporary, 'wx', 0o600)
    try { await file.writeFile(artifact.bytes); await file.sync() } finally { await file.close() }
    try {
      try { await link(temporary, destination) }
      catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error
        const existing = await this.read(record)
        if (!existing || existing.length !== artifact.size || createHash('sha256').update(existing).digest('hex') !== artifact.sha256) throw new Error('lecture publication already contains different bytes')
      }
      if (process.platform !== 'win32') { const dir = await open(directory, 'r'); try { await dir.sync() } finally { await dir.close() } }
    } finally { await unlink(temporary) }
  }
  async read(record: LectureRecord) {
    try {
      const path = resolve(await this.directory(record, false), 'lecture.html'), stat = await lstat(path)
      if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 16 * 1024 * 1024) throw new Error('invalid published lecture file')
      return await readFile(path)
    }
    catch (error) { if ((error as NodeJS.ErrnoException).code === 'ENOENT') return null; throw error }
  }
}
