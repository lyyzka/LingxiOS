#!/usr/bin/env node
import { startWorker } from './index.js'

const worker = await startWorker()
for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.once(signal, () => {
    void worker.stop().then(({ timedOut }) => process.exit(timedOut ? 1 : 0))
  })
}
