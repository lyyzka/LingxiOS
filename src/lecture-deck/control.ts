import { createHash } from 'node:crypto'
import { isDeepStrictEqual } from 'node:util'
import { withTransaction, type SqlPool } from '../control-plane/pg-store.js'
import { ControlPlaneError } from '../control-plane/service.js'
import type { WorkItem } from '../protocol/types.js'
import { PostgresLectureRepository } from './repository.js'
import type { LectureDeckService, LectureRecord } from './service.js'
import type { LectureCommand } from './transport.js'
import { contentHash } from './contracts.js'
import { buildStandaloneLecture } from './standalone.js'

export function lectureControl(database: SqlPool, service: LectureDeckService) {
  return async (work: WorkItem, command: LectureCommand): Promise<unknown> => withTransaction(database, async client => {
    const leased = await client.query(`SELECT id FROM lingxios.agent_work_items
      WHERE id=$1 AND fence=$2 AND lease_token_hash=$3 AND status='leased' AND lease_expires_at>NOW()
        AND cancel_requested_at IS NULL FOR UPDATE`, [work.id, work.fence, createHash('sha256').update(work.leaseToken).digest('hex')])
    if (!leased.rows.length) throw new ControlPlaneError(409, 'work lease lost or expired', 'lease_lost')
    if (work.kind !== 'lecture_deck' || !work.principalId || typeof work.meta?.['deckId'] !== 'string') throw new ControlPlaneError(403, 'not a lecture work item')
    const repository = new PostgresLectureRepository({ query: client.query.bind(client), connect: async () => client })
    await client.query('SELECT id FROM lingxios.lecture_decks WHERE tenant_id=$1 AND id=$2 FOR UPDATE', [work.tenantId, work.meta['deckId']])
    const current = await repository.get(work.tenantId, work.meta['deckId'])
    if (!current || current.principalId !== work.principalId || current.revision !== work.meta['deckRevision']) throw new ControlPlaneError(409, 'lecture identity or revision changed')
    const assertRecord = (record: LectureRecord) => {
      if (!record || record.id !== current.id || record.tenantId !== current.tenantId || record.principalId !== current.principalId
        || record.revision !== current.revision || !isDeepStrictEqual(record.request, current.request)
        || record.rootWorkId !== current.rootWorkId
        || record.outlineApproved !== current.outlineApproved && record.outlineApproved !== false) throw new ControlPlaneError(400, 'invalid lecture state update')
    }
    switch (command?.operation) {
      case 'get': return current
      case 'save': {
        assertRecord(command.record)
        if (isDeepStrictEqual(current, { ...command.record, stateVersion: (command.record.stateVersion ?? 0) + 1 })) return current.stateVersion
        const allowed: Record<string, string[]> = {
          planning: ['awaiting_outline_approval', 'failed'], generating: ['generating', 'validating', 'failed'],
          validating: ['validating', 'generating', 'failed'], publishing: ['ready'], ready: ['ready'],
        }
        if (!allowed[current.status]?.includes(command.record.status)) throw new ControlPlaneError(409, 'invalid lecture transition')
        if (current.status === 'publishing') {
          if (!isDeepStrictEqual(command.record.artifact, current.artifact) || !isDeepStrictEqual(command.record.manifest, current.manifest)) throw new ControlPlaneError(409, 'publication commitment changed')
          const bytes = await service.dependencies.publisher.read?.(current)
          if (!bytes || bytes.length !== current.artifact?.size || createHash('sha256').update(bytes).digest('hex') !== current.artifact.sha256) throw new ControlPlaneError(409, 'published artifact is not available')
        }
        await repository.save(command.record, command.expectedRevision)
        return command.record.stateVersion
      }
      case 'claimPublication': {
        assertRecord(command.record)
        if (command.record.status !== 'publishing' || !command.record.manifest || !command.record.report?.passed || command.record.report.issues.length) throw new ControlPlaneError(400, 'invalid publication intent')
        if (!current.outlineApproved || !isDeepStrictEqual(command.record.manifest.course, current.outline)) throw new ControlPlaneError(409, 'publication does not match approved outline')
        const artifact = buildStandaloneLecture(command.record.manifest)
        if (artifact.sha256 !== command.record.artifact?.sha256 || artifact.size !== command.record.artifact.size) throw new ControlPlaneError(400, 'publication hash mismatch')
        if (current.status === 'publishing' && isDeepStrictEqual(current.manifest, command.record.manifest)) return current.stateVersion
        return await repository.claimPublication(command.record) ? command.record.stateVersion : null
      }
      case 'checkpoint': {
        const value = command.value
        if (!value || !['plan-course', 'plan-chapter', 'author-slide', 'validate-slide', 'repair-slide', 'validate-deck', 'publish-deck'].includes(value.stage)
          || typeof value.key !== 'string' || value.key.length > 160 || !/^[a-f0-9]{64}$/.test(value.inputHash)
          || value.outputHash !== contentHash(value.output) || !Number.isFinite(Date.parse(value.completedAt))) throw new ControlPlaneError(400, 'invalid lecture checkpoint')
        await repository.checkpoint(current.id, current.revision, value); return null
      }
      case 'readCheckpoint': return repository.readCheckpoint(current.id, current.revision, command.stage, command.key, command.inputHash)
      case 'search': {
        const input = command.input
        if (!input || input.tenantId !== current.tenantId || input.principalId !== current.principalId
          || !Array.isArray(input.sourceIds) || input.sourceIds.some(id => !current.request.sourceIds?.includes(id))
          || !Array.isArray(input.queries) || input.queries.length > 100 || input.queries.some(query => typeof query !== 'string' || query.length > 20_000)
          || !Number.isInteger(input.limit) || input.limit < 1 || input.limit > 200) throw new ControlPlaneError(400, 'invalid lecture evidence scope')
        return service.dependencies.evidence.search({ ...input, signal: AbortSignal.timeout(15_000) })
      }
      case 'publish': {
        assertRecord(command.record)
        if (current.status !== 'publishing' || !isDeepStrictEqual(current.artifact, command.artifact)) throw new ControlPlaneError(409, 'publication not reserved')
        const bytes = Buffer.from(command.bytes, 'base64')
        if (bytes.length > 16 * 1024 * 1024 || bytes.length !== command.artifact.size || createHash('sha256').update(bytes).digest('hex') !== command.artifact.sha256) throw new ControlPlaneError(400, 'artifact hash mismatch')
        await service.dependencies.publisher.publish(current, { ...command.artifact, bytes }); return null
      }
      case 'readArtifact': {
        assertRecord(command.record)
        const bytes = await service.dependencies.publisher.read?.(current)
        return bytes ? Buffer.from(bytes).toString('base64') : null
      }
      default: throw new ControlPlaneError(400, 'unknown lecture operation')
    }
  })
}
