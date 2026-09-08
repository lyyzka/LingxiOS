import type { SqlQueryable } from '../control-plane/pg-store.js'
import { actionKeyOf, type HostAction, type WorkItem } from '../protocol/types.js'
import { requestSnapshot } from '../app/jobs.js'
import { authorizeScope, identityOf, type MemoryOptions } from './access.js'
import { lockMemoryScopes, currentMemoryScopes } from './forget.js'
import { parseDocumentChanges, readMemory, memoryDocument } from './store.js'
import { MEMORY_REVIEW_ACTIONS, parseMemoryInput, type MemoryMethod } from './contracts.js'
import { memoryDigest } from './text.js'
import type { MemoryDocument, MemoryReview, MemoryReviewRequest, MemoryScope } from './types.js'

export function parseMemoryReview(value: unknown): MemoryReview {
  if (!value || typeof value!=='object' || Array.isArray(value)) throw new Error('invalid memory review')
  const review = value as MemoryReview
  if (Object.keys(value).some(key => !['approved','explicit','confidence'].includes(key)) || typeof review.approved!=='boolean'
    || typeof review.explicit!=='boolean' || typeof review.confidence!=='number' || !Number.isFinite(review.confidence)
    || review.confidence<0 || review.confidence>1) throw new Error('invalid memory review')
  return review
}

/** Private worker API. Only the independent reviewer can attest; these fields never enter a tool schema. */
export async function prepareMemoryReview(database: SqlQueryable,options: MemoryOptions,work: Omit<WorkItem,'leaseToken'>,action: HostAction) {
  if (!MEMORY_REVIEW_ACTIONS.has(action.action)) return null
  if (action.runId!==work.id || action.idempotencyKey!==actionKeyOf(action)) throw new Error('invalid memory action identity')
  const input = parseMemoryInput(action.action.split('.')[1] as MemoryMethod,action.args)
  const identity = identityOf(work),scope: MemoryScope = { tenantId: work.tenantId,scopeType: String(input['scopeType']),scopeId: String(input['scopeId']) }
  await authorizeScope(options,identity,database,scope)
  const [epoch] = await lockMemoryScopes(database,[scope])
  const live=await database.query(`SELECT id,jsonb_array_length(steer_inputs)+1 AS request_version FROM lingxios.agent_work_items WHERE id=$1 AND fence=$2 AND tenant_id=$3 AND agent_id=$4
    AND principal_id=$5 AND status='leased' AND lease_expires_at>NOW() AND cancel_requested_at IS NULL FOR UPDATE`,
    [work.id,work.fence,work.tenantId,work.agentId,work.principalId])
  if (!live.rows.length) throw new Error('memory review requires the current worker lease')
  if (!(await currentMemoryScopes(database,[epoch!],work.id)).length) throw new Error('memory source predates forgetting')
  const request = await requestSnapshot(database,work.id,Number(live.rows[0]!['request_version']))
  if (request.authorId!==work.principalId || request.tenantId!==work.tenantId) throw new Error('memory request identity mismatch')
  const documents: MemoryDocument[]=[]
  const add = async (id: string,expectedVersion: number) => {
    const document = await readMemory(database,scope,id)
    if (!document || document.version!==expectedVersion) throw new Error('memory is unavailable or stale')
    documents.push(document)
  }
  if (action.action==='memory.apply') {
    for (const change of parseDocumentChanges(input['changes'])) {
      if (change.action!=='create') await add(change.id,change.expectedVersion)
      if (change.action==='merge') for (const source of change.from) await add(source.id,source.expectedVersion)
    }
  } else if (action.action==='memory.restore') {
    await add(String(input['id']),Number(input['expectedVersion']))
    if (Number(input['version'])!==Number(input['expectedVersion'])) {
      const row = (await database.query('SELECT snapshot FROM lingxios.agent_memory_versions WHERE tenant_id=$1 AND memory_id=$2 AND version=$3',
        [scope.tenantId,input['id'],input['version']])).rows[0]
      if (!row) throw new Error('memory version is unavailable')
      documents.push(memoryDocument(row['snapshot'] as Record<string,unknown>))
    }
  }
  const preview: MemoryReviewRequest['input'] = {
    request:{ originalText:request.originalText,revisions:[...request.inheritedRevisions ?? [],...request.revisions]
      .filter(item => !item.author || item.author.kind==='human').map(item => ({ text:item.text })),delegated:!!request.instructionAuthor },
    action:action.action,args:input,documents,
  }
  const hash = memoryDigest({ preview,scope,epoch:epoch!.epoch,workId:work.id,fence:work.fence,requestVersion:request.revisions.length+1 })
  return { hash,input:preview,epoch:epoch!.epoch,scope,requestVersion:request.revisions.length+1 }
}

export async function recordMemoryReview(database: SqlQueryable,options: MemoryOptions,work: Omit<WorkItem,'leaseToken'>,
  action: HostAction,hash: string,value: unknown): Promise<void> {
  const review = parseMemoryReview(value),prepared = await prepareMemoryReview(database,options,work,action)
  if (!prepared || prepared.hash!==hash) throw new Error('memory review was superseded; read and review the current request again')
  await database.query(`INSERT INTO lingxios.agent_memory_reviews(action_id,tenant_id,scope_type,scope_id,work_id,fence,request_version,epoch,preview_hash,review)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb) ON CONFLICT(action_id) DO UPDATE SET fence=EXCLUDED.fence,
      request_version=EXCLUDED.request_version,epoch=EXCLUDED.epoch,preview_hash=EXCLUDED.preview_hash,review=EXCLUDED.review
    WHERE lingxios.agent_memory_reviews.work_id=EXCLUDED.work_id AND lingxios.agent_memory_reviews.tenant_id=EXCLUDED.tenant_id`,
    [action.idempotencyKey,work.tenantId,prepared.scope.scopeType,prepared.scope.scopeId,work.id,work.fence,prepared.requestVersion,prepared.epoch,hash,JSON.stringify(review)])
}

export async function requireMemoryReview(database: SqlQueryable,options: MemoryOptions,work: Omit<WorkItem,'leaseToken'>,action: HostAction) {
  const prepared = await prepareMemoryReview(database,options,work,action)
  if (!prepared) throw new Error('memory action cannot be reviewed')
  const row = (await database.query(`SELECT review FROM lingxios.agent_memory_reviews WHERE action_id=$1 AND work_id=$2 AND fence=$3
    AND tenant_id=$4 AND epoch=$5 AND request_version=$6 AND preview_hash=$7`,
    [action.idempotencyKey,work.id,work.fence,work.tenantId,prepared.epoch,prepared.requestVersion,prepared.hash])).rows[0]
  if (!row) throw new Error('memory action requires an independent worker review')
  const review = parseMemoryReview(row['review'])
  if (!review.approved || review.confidence<0.6) throw new Error('independent memory review rejected the change')
  if (prepared.input.request.delegated && review.explicit) throw new Error('delegated instructions cannot authorize protected memory changes')
  if ((action.action==='memory.restore' || action.action==='memory.forget') && !review.explicit) throw new Error('memory restore and forgetting require an explicit human request')
  return { ...prepared,review }
}
