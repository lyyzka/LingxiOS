import { randomUUID } from 'node:crypto'
import { ConfigError } from '../errors.js'
import type { HostPort } from '../host/port.js'
import { HttpHostClient } from '../host/http-client.js'
import { KernelManager, type KernelHostBridge, type KernelManagerOptions, type ManagedKernelExecutor } from '../kernel/manager.js'
import { kernelIsolation } from '../kernel/isolation.js'
import { createLogger, type Logger } from '../logging.js'
import { MetricsRegistry } from '../metrics.js'
import { OpenAIChatDriver, DEFAULT_MODEL, DEFAULT_SMALL_MODEL } from '../model/openai.js'
import type { ModelDriver } from '../model/driver.js'
import { AgentRuntime, type AgentRuntimeOptions, type WorkProcessor } from '../runtime/runtime.js'
import { memorySynthesisProcessor, memoryIndexProcessor, memoryEvaluationProcessor, type EvolutionEvaluator } from '../memory/processor.js'
import { AgentWorker } from './worker.js'

export interface WorkerConnection {
  connectWorker(input: { workerId: string; workKinds: readonly string[] }): HostPort
}
export type ModelConfiguration = { id?: string; apiKey: string; baseUrl?: string; reasoningEffort?: 'high' | 'max'; maxOutputTokens?: number; maxThinkingTokens?: number; contextWindowTokens?: number }
export interface WorkerOptions extends Omit<AgentRuntimeOptions, 'rootModelBudget' | 'smallModel'> {
  controlPlane: WorkerConnection | { url: string; serviceToken: string }
  model: ModelDriver | ModelConfiguration
  smallModel?: ModelDriver | ModelConfiguration
  modelBudget?: AgentRuntimeOptions['rootModelBudget']
  processors?: Readonly<Record<string, WorkProcessor | 'conversation'>>
  evolutionEvaluator?: EvolutionEvaluator
  kernel?: Omit<KernelManagerOptions, 'runnerPath' | 'logger' | 'maxKernels'>
  kernelFactory?: (bridge: KernelHostBridge) => ManagedKernelExecutor
  trustProcessKernel?: boolean
  worker?: { id?: string; concurrency?: number; shutdownGraceMs?: number; pollIdleMs?: number; healthPort?: number }
  logger?: Logger
  metrics?: MetricsRegistry
}

/** The only factory that may claim and execute work; it owns its kernel lifecycle. */
export function createWorker(options: WorkerOptions): AgentWorker {
  const concurrency = options.worker?.concurrency ?? 2
  if (!Number.isSafeInteger(concurrency) || concurrency < 1 || concurrency > 1024) throw new ConfigError('worker concurrency must be 1-1024')
  const workerId = options.worker?.id ?? `lingxios-${randomUUID()}`
  const logger = options.logger ?? createLogger()
  const metrics = options.metrics ?? new MetricsRegistry()
  const workKinds = ['turn', 'resume', 'memory_synthesis', 'memory_index', ...options.evolutionEvaluator ? ['memory_evaluation'] : [], ...Object.keys(options.processors ?? {})]
  const connection = options.controlPlane
  const host = 'connectWorker' in connection ? connection.connectWorker({ workerId, workKinds })
    : new HttpHostClient({ baseUrl: connection.url, serviceToken: connection.serviceToken, workerId, workKinds })
  const model = 'run' in options.model ? options.model : new OpenAIChatDriver(options.model.id ?? DEFAULT_MODEL.id, options.model)
  const smallModel = options.smallModel ? 'run' in options.smallModel ? options.smallModel
    : new OpenAIChatDriver(options.smallModel.id ?? DEFAULT_SMALL_MODEL.id, { ...DEFAULT_SMALL_MODEL, ...options.smallModel })
    : 'run' in options.model ? model : new OpenAIChatDriver(DEFAULT_SMALL_MODEL.id, { ...DEFAULT_SMALL_MODEL,
      apiKey: options.model.apiKey, ...(options.model.baseUrl ? { baseUrl: options.model.baseUrl } : {}) })
  const bridge: KernelHostBridge = { execute: (work, action, signal) => host.executeAction(work, action, signal) }
  const kernels = options.kernelFactory?.(bridge) ?? new KernelManager(bridge, { ...options.kernel, logger, maxKernels: concurrency,
    isolation: kernelIsolation(options.kernel?.isolation ?? process.env['AGENT_OS_KERNEL_ISOLATION'], process.env['NODE_ENV'] === 'production', options.trustProcessKernel) })
  const runtime = new AgentRuntime(host, model, kernels, { ...options, smallModel, ...(options.modelBudget ? { rootModelBudget: options.modelBudget } : {}) })
  runtime.registerProcessor('memory_synthesis', memorySynthesisProcessor)
  runtime.registerProcessor('memory_index', memoryIndexProcessor)
  if (options.evolutionEvaluator) runtime.registerProcessor('memory_evaluation', memoryEvaluationProcessor(options.evolutionEvaluator))
  for (const [kind, processor] of Object.entries(options.processors ?? {})) {
    if (['turn', 'resume', 'memory_synthesis', 'memory_index', 'memory_evaluation'].includes(kind)) throw new ConfigError('built-in work kind cannot be replaced')
    runtime.registerProcessor(kind, processor)
  }
  return new AgentWorker({ host, runtime, kernels, workerId, logger, metrics, maxConcurrentRuns: concurrency,
    shutdownGraceMs: options.worker?.shutdownGraceMs ?? 20_000,
    ...(options.worker?.pollIdleMs === undefined ? {} : { pollIdleMs: options.worker.pollIdleMs }),
    ...(options.worker?.healthPort === undefined ? {} : { healthPort: options.worker.healthPort }),
    ...(host instanceof HttpHostClient ? { lastContactAt: () => host.lastContactAt } : {}),
  })
}
