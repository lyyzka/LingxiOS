import { boolEnv, loadWorkerConfig, loadModelBudget } from '../config.js'
import { KernelManager } from '../kernel/manager.js'
import { kernelIsolation } from '../kernel/isolation.js'
import { createWorker, type WorkerOptions } from './factory.js'

export { createWorker, type WorkerConnection, type WorkerOptions } from './factory.js'
export type { EvolutionEvaluator } from '../memory/processor.js'
export type { WorkProcessor, WorkProcessorContext } from '../runtime/runtime.js'
export type { ModelDriver, ModelUsage, ModelTurnRequest, ModelTurnResult } from '../model/driver.js'
export type { HostPort } from '../host/port.js'
export type { AgentWorker } from './worker.js'
export type { KernelExecutor, ManagedKernelExecutor, KernelHostBridge } from '../kernel/manager.js'

/** Standalone HTTP worker; products register their processors through createWorker. */
export async function startWorker(env: NodeJS.ProcessEnv = process.env,
  options: Pick<WorkerOptions, 'kernelFactory' | 'policy' | 'processors'> = {}) {
  const config = loadWorkerConfig(env)
  const worker = createWorker({ ...options,
    controlPlane: { url: config.controlPlaneUrl, serviceToken: config.serviceToken },
    model: config.model, smallModel: config.smallModel, modelBudget: loadModelBudget(env),
    recordModelPayloads: boolEnv('AGENT_OS_RECORD_MODEL_PAYLOADS', false, env),
    kernelFactory: options.kernelFactory ?? (bridge => new KernelManager(bridge, {
      maxKernels: config.maxConcurrentRuns,
      isolation: kernelIsolation(env['AGENT_OS_KERNEL_ISOLATION'], env['NODE_ENV'] === 'production', boolEnv('AGENT_OS_TRUST_PROCESS_KERNEL', false, env)),
    }, env)),
    worker: { id: config.workerId, concurrency: config.maxConcurrentRuns, shutdownGraceMs: config.shutdownGraceMs,
      pollIdleMs: config.pollIdleMs, healthPort: config.healthPort },
  })
  try { await worker.start(); return worker }
  catch (error) { await worker.stop(); throw error }
}
