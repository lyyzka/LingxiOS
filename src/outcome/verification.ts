import { createHash } from 'node:crypto'
import type { KernelArtifact } from '../protocol/types.js'
import { snapshotArtifacts } from './envelope.js'

export interface Candidate {
  body: string
  requestVersion: number
  artifacts: KernelArtifact[]
}
export interface VerificationRecord {
  checker: string
  status: 'passed' | 'failed' | 'inconclusive'
  evidence: Record<string, unknown>
}
export interface CandidateVerification {
  candidateHash: string
  requestVersion: number
  records: VerificationRecord[]
}
export function candidateHash(candidate: Candidate): string {
  return createHash('sha256').update(JSON.stringify({ body: candidate.body,
    artifacts: snapshotArtifacts(candidate.artifacts) })).digest('hex')
}
