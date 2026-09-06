import { rmSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { spawnSync } from 'node:child_process'

const root = new URL('../', import.meta.url)
rmSync(new URL('dist/', root), { recursive: true, force: true })
const result = spawnSync(process.execPath, ['node_modules/typescript/bin/tsc', '-p', 'tsconfig.json'], {
  cwd: fileURLToPath(root), stdio: 'inherit',
})
if (result.error) throw result.error
process.exitCode = result.status ?? 1
