/**
 * Worker entrypoint: wire the HTTP host client, model driver, kernel manager,
 * and runtime together from environment configuration, then run until
 * SIGINT/SIGTERM drains the process.
 */
import { boolEnv, loadWorkerConfig } from '../config.js'
import { HttpHostClient } from '../host/http-client.js'
import { KernelManager, type KernelHostBridge, type ManagedKernelExecutor } from '../kernel/manager.js'
import { createLogger } from '../logging.js'
import { MetricsRegistry } from '../metrics.js'
import { OpenAIChatDriver } from '../model/openai.js'
import { AgentRuntime } from '../runtime/runtime.js'
import { AgentWorker } from './worker.js'
import { memorySynthesisProcessor, memoryIndexProcessor } from '../memory/processor.js'
import { ConfigError } from '../errors.js'
import type { RuntimePolicy } from '../runtime/policy.js'
import { createLingxiLoopRuntimePolicy } from '../integrations/lingxiloop/policy.js'

export async function startWorker(env: NodeJS.ProcessEnv = process.env, options: {
  kernelFactory?: (bridge: KernelHostBridge) => ManagedKernelExecutor
  policy?: RuntimePolicy
} = {}): Promise<AgentWorker> {
  const config = loadWorkerConfig(env)
  const logger = createLogger().child({ service: 'agent-os-worker' })
  const metrics = new MetricsRegistry()

  const host = new HttpHostClient({
    baseUrl: config.controlPlaneUrl,
    serviceToken: config.serviceToken,
    workerId: config.workerId,
  })
  const model = new OpenAIChatDriver(config.model.id, {
    apiKey: config.model.apiKey,
    baseUrl: config.model.baseUrl,
    ...(config.model.reasoningEffort ? { reasoningEffort: config.model.reasoningEffort } : {}),
  })
  const bridge: KernelHostBridge = { execute: (work, action) => host.executeAction(work, action) }
  if (!options.kernelFactory && env['NODE_ENV'] === 'production'
    && !boolEnv('AGENT_OS_TRUST_PROCESS_KERNEL', false, env)) {
    throw new ConfigError('production worker requires an OS-isolated kernelFactory; set AGENT_OS_TRUST_PROCESS_KERNEL=true only for trusted model code')
  }
  const kernels = options.kernelFactory?.(bridge) ?? new KernelManager(
    bridge, { logger, maxKernels: config.maxConcurrentRuns }, env,
  )
  const policyName = env['AGENT_OS_RUNTIME_POLICY']?.trim()
  if (policyName && policyName !== 'lingxiloop') throw new ConfigError('AGENT_OS_RUNTIME_POLICY must be lingxiloop when set')
  const policy = options.policy ?? (policyName === 'lingxiloop' ? createLingxiLoopRuntimePolicy() : undefined)
  const runtime = new AgentRuntime(host, model, kernels, { logger, ...(policy ? { policy } : {}),
    recordModelPayloads: boolEnv('AGENT_OS_RECORD_MODEL_PAYLOADS', false, env) })
  runtime.registerProcessor('memory_synthesis', memorySynthesisProcessor)
  runtime.registerProcessor('memory_index', memoryIndexProcessor)
  runtime.registerProcessor('teacher_digest', 'conversation')
  runtime.registerProcessor('routine', 'conversation')
  runtime.registerProcessor('mission_coordinator', 'conversation')
  const worker = new AgentWorker({
    host,
    runtime,
    kernels,
    workerId: config.workerId,
    maxConcurrentRuns: config.maxConcurrentRuns,
    shutdownGraceMs: config.shutdownGraceMs,
    pollIdleMs: config.pollIdleMs,
    healthPort: config.healthPort,
    logger,
    metrics,
  })

  try {
    await worker.start()
  } catch (error) {
    kernels.close()
    throw error
  }

  return worker
}

export type { AgentWorker } from './worker.js'
export type { KernelExecutor, ManagedKernelExecutor, KernelHostBridge } from '../kernel/manager.js'
