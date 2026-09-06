import type { AgentRuntime } from '../runtime/runtime.js'
import { memorySynthesisProcessor, memoryIndexProcessor } from '../memory/processor.js'
import { lectureDeckProcessor, type LectureDeckService } from '../lecture-deck/service.js'
import { workerLectureService } from '../lecture-deck/transport.js'

export function registerProcessors(runtime: AgentRuntime, lecture?: LectureDeckService) {
  runtime.registerProcessor('memory_synthesis', memorySynthesisProcessor)
  runtime.registerProcessor('memory_index', memoryIndexProcessor)
  for (const kind of ['teacher_digest', 'routine', 'mission_coordinator']) runtime.registerProcessor(kind, 'conversation')
  runtime.registerProcessor('lecture_deck', { async process(work, context) {
    return lectureDeckProcessor(workerLectureService(context.host, work, context.model, lecture)).process(work, context)
  } })
}
