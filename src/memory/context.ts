import type { MemorySnapshot } from './types.js'
import { memoryDigest } from './text.js'

export function snapshotMemories(input: Omit<MemorySnapshot,'id'>): MemorySnapshot {
  return { ...input,id: `memory:${memoryDigest(input)}` }
}

/** Whole records only. Core is retained before directory entries, recall excerpts, and optional strategy hints. */
export function fitMemorySnapshot(source: MemorySnapshot, contextWindowTokens: number, availableTokens = Infinity): MemorySnapshot {
  const max = Math.max(0,Math.min(source.budget.maxTokens,Math.floor(contextWindowTokens*source.budget.ratio),availableTokens))
  const result = structuredClone(source)
  result.core=[]; result.directory=[]; result.recalled=[]; result.strategies=[]
  const base = Buffer.byteLength(JSON.stringify(result))+128
  let remaining = Math.max(0,max-base)
  for (const field of ['core','directory','recalled','strategies'] as const) {
    for (const item of source[field]) {
      const bytes = Buffer.byteLength(JSON.stringify(item))+1
      if (bytes>remaining) { result.omitted[field]++; continue }
      remaining-=bytes
      // The field and item are correlated by the loop; no body is partially copied.
      ;(result[field] as unknown[]).push(item)
    }
  }
  const { id: _id,...value } = result
  return snapshotMemories(value)
}

export function roundRobin<T>(groups: T[][]): T[] {
  const items: T[] = []
  for (let index=0;index<Math.max(0,...groups.map(group => group.length));index++) {
    for (const group of groups) if (index<group.length) items.push(group[index]!)
  }
  return items
}
