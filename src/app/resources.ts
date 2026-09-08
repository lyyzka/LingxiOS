import { fileURLToPath } from 'node:url'

/** Locate packaged assets independently of the application's working directory. */
export function packageResources() {
  return {
    schema: fileURLToPath(new URL('../../../db/schema.sql', import.meta.url)),
    migration008: fileURLToPath(new URL('../../../db/migrations/008-governance.sql', import.meta.url)),
    memoryReset009: fileURLToPath(new URL('../../../db/migrations/009-cognitive-memory-reset.sql', import.meta.url)),
    runner: fileURLToPath(new URL('../../../kernel/runner.py', import.meta.url)),
  }
}
