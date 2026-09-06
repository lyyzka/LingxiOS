/**
 * Worker entrypoint: wire the HTTP host client, model driver, kernel manager,
 * and runtime together from environment configuration, then run until
 * SIGINT/SIGTERM drains the process.
 */
import { loadWorkerConfig } from '../config.js'
import { HttpHostClient } from '../host/http-client.js'
import { KernelManager } from '../kernel/manager.js'
import { createLogger } from '../logging.js'
import { MetricsRegistry } from '../metrics.js'
import { OpenAIChatDriver } from '../model/openai.js'
import { AgentRuntime } from '../runtime/runtime.js'
import { AgentWorker } from './worker.js'
import { memorySynthesisProcessor, memoryIndexProcessor } from '../memory/processor.js'

export async function startWorker(env: NodeJS.ProcessEnv = process.env): Promise<AgentWorker> {
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
    reasoningEffort: config.model.reasoningEffort,
  })
  const kernels = new KernelManager(
    { execute: (work, action) => host.executeAction(work, action) },
    { logger, maxKernels: config.maxConcurrentRuns },
    env,
  )
  const runtime = new AgentRuntime(host, model, kernels, { logger })
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
