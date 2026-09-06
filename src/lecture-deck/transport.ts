import type { HostPort } from '../host/port.js'
import type { WorkItem } from '../protocol/types.js'
import type { ModelDriver } from '../model/driver.js'
import { LectureDeckService, ModelLectureAuthor, ModelLectureReviewer, type LectureCheckpoint, type LectureRecord, type LectureEvidenceProvider } from './service.js'
import type { LectureArtifact } from './standalone.js'

export type LectureCommand =
  | { operation: 'get' }
  | { operation: 'save'; record: LectureRecord; expectedRevision: number }
  | { operation: 'claimPublication'; record: LectureRecord }
  | { operation: 'checkpoint'; value: LectureCheckpoint }
  | { operation: 'readCheckpoint'; stage: LectureCheckpoint['stage']; key: string; inputHash: string }
  | { operation: 'search'; input: Omit<Parameters<LectureEvidenceProvider['search']>[0], 'signal'> }
  | { operation: 'publish'; record: LectureRecord; artifact: Omit<LectureArtifact, 'bytes'>; bytes: string }
  | { operation: 'readArtifact'; record: LectureRecord }

/** The same processor uses local or HTTP host calls; it never receives a database pool. */
export function workerLectureService(host: HostPort, work: WorkItem, model: ModelDriver, configured?: LectureDeckService): LectureDeckService {
  const call = async <T>(command: LectureCommand): Promise<T> => {
    if (!host.lecture) throw new Error('lecture transport is unavailable')
    return await host.lecture(work, command) as T
  }
  return new LectureDeckService({
    repository: {
      create: async () => { throw new Error('lecture creation belongs to authenticated ingress') },
      get: () => call<LectureRecord>({ operation: 'get' }),
      async save(record, expectedRevision) { record.stateVersion = await call<number>({ operation: 'save', record, expectedRevision }) },
      async claimPublication(record) {
        const stateVersion = await call<number | null>({ operation: 'claimPublication', record })
        if (stateVersion === null) return false
        record.stateVersion = stateVersion; return true
      },
      checkpoint: (_id, _revision, value) => call<void>({ operation: 'checkpoint', value }),
      readCheckpoint: (_id, _revision, stage, key, inputHash) => call<LectureCheckpoint | null>({ operation: 'readCheckpoint', stage, key, inputHash }),
    },
    evidence: { async search({ signal: _signal, ...input }) { return call({ operation: 'search', input }) } },
    author: configured && !(configured.dependencies.author instanceof ModelLectureAuthor) ? configured.dependencies.author : new ModelLectureAuthor(model),
    reviewer: configured && !(configured.dependencies.reviewer instanceof ModelLectureReviewer) ? configured.dependencies.reviewer : new ModelLectureReviewer(model),
    publisher: {
      async publish(record, { bytes, ...artifact }) { await call({ operation: 'publish', record, artifact, bytes: Buffer.from(bytes).toString('base64') }) },
      async read(record) { const bytes = await call<string | null>({ operation: 'readArtifact', record }); return bytes === null ? null : Buffer.from(bytes, 'base64') },
    },
  })
}
