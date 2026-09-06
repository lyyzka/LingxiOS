import { cpSync, rmSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { spawnSync } from 'node:child_process'

const root = new URL('../', import.meta.url)
rmSync(new URL('dist/', root), { recursive: true, force: true })
const result = spawnSync(process.execPath, ['node_modules/typescript/bin/tsc', '-p', 'tsconfig.json'], {
  cwd: fileURLToPath(root), stdio: 'inherit',
})
if (result.error) throw result.error
if (result.status === 0) cpSync(new URL('src/lecture-deck/runtime.html', root), new URL('dist/src/lecture-deck/runtime.html', root))
process.exitCode = result.status ?? 1
