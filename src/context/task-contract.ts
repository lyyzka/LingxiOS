import { createHash } from 'node:crypto'

export interface TaskContract {
  version: 1
  requestVersion: number
  originalInputSha256: string
  deliverables: string[]
  constraints: string[]
  actions: string[]
  acceptance: string[]
}

/** Model-authored interpretation, never a replacement for the original input. */
export function createTaskContract(originalInput: string, requestVersion: number, draft: unknown): TaskContract {
  if (!Number.isSafeInteger(requestVersion) || requestVersion < 1) throw new Error('invalid contract request version')
  if (!draft || typeof draft !== 'object' || Array.isArray(draft)) throw new Error('task contract draft must be an object')
  const fields = ['deliverables', 'constraints', 'actions', 'acceptance'] as const
  const input = draft as Record<string, unknown>
  if (Object.keys(input).some(key => !fields.includes(key as typeof fields[number]))) throw new Error('unknown task contract field')
  const result: TaskContract = { version: 1, requestVersion, originalInputSha256: createHash('sha256').update(originalInput).digest('hex'), deliverables: [], constraints: [], actions: [], acceptance: [] }
  for (const field of fields) {
    const values = input[field]
    if (!Array.isArray(values) || values.length > 64 || !values.every(value => typeof value === 'string' && value.trim().length > 0 && value.length <= 2000)) throw new Error(`invalid task contract ${field}`)
    result[field] = [...values]
  }
  if (!result.deliverables.length || !result.acceptance.length) throw new Error('task contract requires deliverables and acceptance conditions')
  return result
}
