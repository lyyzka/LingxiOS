import { canonicalJson, textSha256, type PromptManifest } from '../context/compiler.js'
import type { ExecutionSnapshot } from '../runtime/execution-policy.js'
import type { ModelDriver } from '../model/driver.js'
import { modelProfile } from '../model/profile.js'
import { toolContractHash } from '../tools/contracts.js'
import { releaseVersions } from '../versions.js'

/** Content-free call artifact. Input bodies remain subject to the configured trace policy. */
export function promptProgram(manifest: PromptManifest, execution: ExecutionSnapshot, model: ModelDriver, inputSha256: string, harnessHash?: string) {
  if (!/^[a-f0-9]{64}$/.test(inputSha256)) throw new Error('invalid prompt program input hash')
  const value = { version: 1, runtime: releaseVersions.runtime, prompt: manifest.fingerprint, instructions: manifest.instructionsSha256,
    harness: harnessHash ?? null, execution: execution.hash, mode: execution.mode, model: modelProfile(model),
    provider: model.configurationFingerprint ?? null, inputSha256, tools: execution.tools.map(tool => ({ action: tool.action, hash: toolContractHash(tool) })) }
  return { ...value, hash: textSha256(canonicalJson(value)) }
}
