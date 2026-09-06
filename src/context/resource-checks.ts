import type { HostActionResult } from '../protocol/types.js'

export interface ResourceCheckRecord {
  actionKey: string
  result: HostActionResult
}

/** Immutable observations; a replay never replaces an earlier read with a fresh one. */
export function appendResourceCheck(records: readonly ResourceCheckRecord[], actionKey: string,
  result: HostActionResult, requestVersion: number): ResourceCheckRecord[] {
  if (!result.ok || result.executionState === 'unknown' || result.directive || !result.value || typeof result.value !== 'object') return [...records]
  const value = result.value as Record<string, unknown>
  if (value['scope'] !== 'observed_resource_fields' || value['requestVersion'] !== requestVersion
    || !['pass', 'fail', 'not_observed'].includes(String(value['status']))) throw new Error('invalid resource observation')
  if (records.some(record => record.actionKey === actionKey)) return [...records]
  if (records.length >= 64) throw new Error('resource observation limit reached')
  return [...records, { actionKey, result: structuredClone(result) }]
}
