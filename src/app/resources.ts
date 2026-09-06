import { fileURLToPath } from 'node:url'

/** Locate packaged assets independently of the application's working directory. */
export function packageResources() {
  return {
    schema: fileURLToPath(new URL('../../../db/schema.sql', import.meta.url)),
    runner: fileURLToPath(new URL('../../../kernel/runner.py', import.meta.url)),
  }
}
