import { AGENT_OS_PROTOCOL_VERSION, KERNEL_PROTOCOL_VERSION } from './protocol/constants.js'

/** Release compatibility identifiers; a matching number does not replace startup checks. */
export const releaseVersions = Object.freeze({
  runtime: '1.1.0',
  controlPlane: AGENT_OS_PROTOCOL_VERSION,
  kernel: KERNEL_PROTOCOL_VERSION,
  schema: 5,
  lingxiLoop: 3,
  assistantMessage: 2,
} as const)
