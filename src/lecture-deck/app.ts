import { randomUUID } from 'node:crypto'
import { isDeepStrictEqual } from 'node:util'
import type { RequestInput } from '../app/index.js'
import { PgWorkStore, withTransaction, type SqlPool, type SqlClient } from '../control-plane/pg-store.js'
import { contentHash } from './contracts.js'
import { LectureDeckService, type LectureRecord } from './service.js'
import { PostgresLectureRepository } from './repository.js'

export interface LectureRequestInput extends Omit<RequestInput, 'text' | 'attachments' | 'authorName'> { request: unknown }
export interface LectureOperationInput extends Omit<LectureRequestInput, 'request' | 'id'> {
  deckId: string; operation: 'retry' | 'revise' | 'approve_outline' | 'revise_outline'; idempotencyKey: string; request?: unknown
}

/** Authenticated ingress only. State changes and work enqueue share one transaction. */
export function createLectureDeckApp(database: SqlPool, configured: LectureDeckService) {
  async function enqueue(input: LectureRequestInput | LectureOperationInput, transaction?: SqlClient) {
    for (const value of [input.tenantId, input.principalId, input.agentId, input.sessionId]) {
      if (typeof value !== 'string' || !value.trim()) throw new Error('authenticated lecture identity is required')
    }
    const operation = 'operation' in input ? input.operation : 'create'
    const key = 'idempotencyKey' in input ? input.idempotencyKey : input.id ?? randomUUID()
    if (typeof key !== 'string' || !key.trim()) throw new Error('idempotencyKey is required')
    const id = `lecture_${contentHash([input.tenantId, input.principalId, operation, 'deckId' in input ? input.deckId : null, key])}`
    const execute = async (client: SqlClient) => {
      const pool: SqlPool = { query: client.query.bind(client), connect: async () => client }
      const fingerprint = { operation, input: { ...input, ...('operation' in input ? {} : { id: key }) } }
      const inserted = await client.query(`INSERT INTO lingxios.agent_inbox_events(event_id,work_input) VALUES($1,$2::jsonb)
        ON CONFLICT DO NOTHING RETURNING event_id`, [id, JSON.stringify(fingerprint)])
      if (!inserted.rows.length) {
        const { rows } = await client.query('SELECT work_input FROM lingxios.agent_inbox_events WHERE event_id=$1 FOR UPDATE', [id])
        const prior = rows[0]?.['work_input'] as { operation: string; input: unknown; result: LectureEnqueueResult }
        if (!prior || !isDeepStrictEqual(prior.input, fingerprint.input) || prior.operation !== operation) throw new Error('lecture idempotency key reused with different input')
        return { ...prior.result, deduplicated: true }
      }
      const repository = new PostgresLectureRepository(pool)
      const service = new LectureDeckService({ ...configured.dependencies, repository })
      let deck: LectureRecord
      if (!('operation' in input)) {
        deck = await service.begin(input, input.request, `deck_${contentHash([input.tenantId, input.principalId, key]).slice(0, 32)}`)
        deck.rootWorkId = id
        await repository.save(deck, deck.revision)
      } else {
        await client.query('SELECT id FROM lingxios.lecture_decks WHERE tenant_id=$1 AND id=$2 FOR UPDATE', [input.tenantId, input.deckId])
        const current = await service.get(input, input.deckId)
        if (input.operation === 'revise') deck = await service.revise(input, input.deckId, input.request)
        else if (input.operation === 'approve_outline') {
          if (current.outlineApproved) throw new Error('lecture outline already approved; reuse the original idempotency key')
          const request = input.request as { expectedRevision?: number }
          if (!Number.isSafeInteger(request?.expectedRevision)) throw new Error('expectedRevision is required')
          deck = await service.approveOutline(input, input.deckId, request.expectedRevision!)
        } else if (input.operation === 'revise_outline') {
          deck = await service.reviseOutline(input, input.deckId, input.request as Parameters<typeof service.reviseOutline>[2])
        } else if (input.operation === 'retry') {
          if (!['failed', 'cancelled', 'publishing'].includes(current.status)) throw new Error('lecture is not retryable')
          const { error: _error, ...record } = current
          deck = { ...record, status: current.status === 'publishing' ? 'publishing' : current.outlineApproved ? 'generating' : 'planning' }
          await repository.save(deck, current.revision)
        } else throw new Error('unknown lecture operation')
      }
      const meta = { operation: 'run', deckId: deck.id, deckRevision: deck.revision,
        ...(deck.rootWorkId && deck.rootWorkId !== id ? { rootWorkId: deck.rootWorkId, parentWorkId: deck.rootWorkId } : {}) }
      const queued = await new PgWorkStore(pool).enqueue({ id, tenantId: input.tenantId, principalId: input.principalId,
        agentId: input.agentId, sessionId: input.sessionId, ...(input.threadId ? { threadId: input.threadId } : {}),
        kind: 'lecture_deck', lane: 'background', triggerRef: input.sourceRef ?? key, meta })
      const result = { ...queued, deckId: deck.id, revision: deck.revision, status: deck.status }
      await client.query('UPDATE lingxios.agent_inbox_events SET work_input=$2::jsonb WHERE event_id=$1', [id, JSON.stringify({ ...fingerprint, result })])
      return result
    }
    return transaction ? execute(transaction) : withTransaction(database, execute)
  }
  return {
    enqueueLecture: (input: LectureRequestInput) => enqueue(input),
    enqueueLectureOperation: (input: LectureOperationInput, transaction?: SqlClient) => enqueue(input, transaction),
    readLecture: (input: { tenantId: string; principalId: string; deckId: string }) => configured.get(input, input.deckId),
    readLectureHtml: (input: { tenantId: string; principalId: string; deckId: string }) => configured.readHtml(input, input.deckId),
    cancelLecture: (input: { tenantId: string; principalId: string; deckId: string }) => withTransaction(database, async client => {
      // Lock works before the deck, matching worker writes and avoiding a cancellation deadlock.
      await client.query(`SELECT id FROM lingxios.agent_work_items WHERE tenant_id=$1 AND principal_id=$2 AND meta->>'deckId'=$3 FOR UPDATE`, [input.tenantId, input.principalId, input.deckId])
      await client.query('SELECT id FROM lingxios.lecture_decks WHERE tenant_id=$1 AND id=$2 FOR UPDATE', [input.tenantId, input.deckId])
      const repository = new PostgresLectureRepository({ query: client.query.bind(client), connect: async () => client })
      const result = await new LectureDeckService({ ...configured.dependencies, repository }).cancel(input, input.deckId)
      if (result.status === 'cancelled') await client.query(`UPDATE lingxios.agent_work_items SET cancel_requested_at=NOW(),
        status=CASE WHEN status='queued' THEN 'cancelled' ELSE status END
        WHERE tenant_id=$1 AND principal_id=$2 AND meta->>'deckId'=$3 AND status IN ('queued','leased')`, [input.tenantId, input.principalId, input.deckId])
      return result
    }),
  }
}

export interface LectureEnqueueResult { id: string; deckId: string; revision: number; status: string; deduplicated: boolean }
