/**
 * Environment configuration, parsed once at process start and validated
 * eagerly so a misconfigured service fails at boot, not mid-run.
 */
import { ConfigError } from './errors.js'
import { DEFAULT_MODEL } from './model/openai.js'

export function requiredEnv(name: string, env: NodeJS.ProcessEnv = process.env): string {
  const value = env[name]?.trim()
  if (!value) throw new ConfigError(`missing required environment variable: ${name}`)
  return value
}

export function intEnv(name: string, fallback: number, options: { min?: number; max?: number } = {}, env: NodeJS.ProcessEnv = process.env): number {
  const raw = env[name]?.trim()
  if (!raw) return fallback
  const value = Number(raw)
  if (!Number.isSafeInteger(value)) throw new ConfigError(`${name} must be an integer, got '${raw}'`)
  if (options.min !== undefined && value < options.min) throw new ConfigError(`${name} must be >= ${options.min}`)
  if (options.max !== undefined && value > options.max) throw new ConfigError(`${name} must be <= ${options.max}`)
  return value
}

export function boolEnv(name: string, fallback: boolean, env: NodeJS.ProcessEnv = process.env): boolean {
  const raw = env[name]?.trim().toLowerCase()
  if (!raw) return fallback
  if (['1', 'true', 'yes', 'on'].includes(raw)) return true
  if (['0', 'false', 'no', 'off'].includes(raw)) return false
  throw new ConfigError(`${name} must be a boolean, got '${raw}'`)
}

export interface WorkerConfig {
  controlPlaneUrl: string
  serviceToken: string
  workerId: string
  healthPort: number
  maxConcurrentRuns: number
  shutdownGraceMs: number
  pollIdleMs: number
  model: {
    id: string
    apiKey: string
    baseUrl: string
    reasoningEffort?: 'high' | 'max'
  }
}

export function loadWorkerConfig(env: NodeJS.ProcessEnv = process.env): WorkerConfig {
  const reasoningEffort = env['AGENT_OS_REASONING_EFFORT']?.trim()
  if (reasoningEffort !== undefined && reasoningEffort !== 'high' && reasoningEffort !== 'max') throw new ConfigError('AGENT_OS_REASONING_EFFORT must be high or max')
  return {
    controlPlaneUrl: requiredEnv('AGENT_OS_CONTROL_PLANE_URL', env),
    serviceToken: requiredEnv('AGENT_OS_SERVICE_TOKEN', env),
    workerId: env['AGENT_OS_WORKER_ID']?.trim() || `agent-os-${process.pid}`,
    healthPort: intEnv('AGENT_OS_WORKER_PORT', 5190, { min: 0, max: 65_535 }, env),
    maxConcurrentRuns: intEnv('AGENT_OS_MAX_CONCURRENT_RUNS', 2, { min: 1, max: 1_024 }, env),
    shutdownGraceMs: intEnv('AGENT_OS_SHUTDOWN_GRACE_MS', 20_000, { min: 1_000 }, env),
    pollIdleMs: intEnv('AGENT_OS_POLL_IDLE_MS', 750, { min: 50 }, env),
    model: {
      id: env['AGENT_OS_MODEL']?.trim() || DEFAULT_MODEL.id,
      apiKey: requiredEnv('AGENT_OS_MODEL_API_KEY', env),
      baseUrl: env['AGENT_OS_MODEL_BASE_URL']?.trim() || DEFAULT_MODEL.baseUrl,
      ...(reasoningEffort ? { reasoningEffort } : {}),
    },
  }
}
