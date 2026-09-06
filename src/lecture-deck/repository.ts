import { createHash, randomUUID } from 'node:crypto'
import { mkdir, rename, writeFile } from 'node:fs/promises'
import { resolve } from 'node:path'
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
    this.records.set(key, structuredClone(record))
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
    return rows[0]?.['record'] ? structuredClone(rows[0]['record']) as LectureRecord : null
  }
  async save(record: LectureRecord, expectedRevision: number) {
    const result = await this.database.query(`UPDATE lingxios.lecture_decks SET revision=$1,status=$2,record=$3::jsonb,updated_at=NOW()
      WHERE tenant_id=$4 AND id=$5 AND revision=$6`, [record.revision, record.status, JSON.stringify(record), record.tenantId, record.id, expectedRevision])
    if (result.rowCount !== 1) throw new Error('lecture revision changed')
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
    return row ? { stage, key, inputHash: String(row['input_hash']), outputHash: String(row['output_hash']), attempts: Number(row['attempts']), output: structuredClone(row['result']), completedAt: new Date(String(row['completed_at'])).toISOString() } : null
  }
}

/** Atomic local publisher; storage services can implement LecturePublisher directly. */
export class FileLecturePublisher implements LecturePublisher {
  constructor(private readonly root: string) {}
  async publish(record: LectureRecord, artifact: LectureArtifact) {
    const directory = resolve(this.root, createHash('sha256').update(`${record.tenantId}\0${record.id}`).digest('hex'), String(record.revision))
    await mkdir(directory, { recursive: true, mode: 0o700 })
    const temporary = resolve(directory, `${randomUUID()}.tmp`), destination = resolve(directory, artifact.filename)
    await writeFile(temporary, artifact.bytes, { mode: 0o600, flag: 'wx' })
    await rename(temporary, destination)
  }
}
