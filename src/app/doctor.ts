import { execFile } from 'node:child_process'
import { access } from 'node:fs/promises'
import { promisify } from 'node:util'
import type { SqlQueryable } from '../control-plane/pg-store.js'
import { packageResources } from './resources.js'
import { checkStorage } from './storage.js'
import { releaseVersions } from '../versions.js'

export interface DoctorCheck {
  name: string
  status: 'passed' | 'failed' | 'not_run'
  detail: string
}

export async function doctor(options: { database?: SqlQueryable; pythonCommand?: string; env?: NodeJS.ProcessEnv } = {}) {
  const env = options.env ?? process.env
  const checks: DoctorCheck[] = []
  const [nodeMajor = 0, nodeMinor = 0] = process.versions.node.split('.').map(Number)
  checks.push({ name: 'node', status: (nodeMajor > 22 || nodeMajor === 22 && nodeMinor >= 13) ? 'passed' : 'failed', detail: process.versions.node })
  const python = options.pythonCommand ?? env['AGENT_OS_PYTHON'] ?? (process.platform === 'win32' ? 'python' : 'python3')
  try {
    const { stdout } = await promisify(execFile)(python, ['-I', '-c', 'import sys; assert sys.version_info >= (3, 10); print(sys.version.split()[0])'], { timeout: 10_000 })
    checks.push({ name: 'python', status: 'passed', detail: stdout.trim() })
  } catch {
    checks.push({ name: 'python', status: 'failed', detail: 'Python >=3.10 must be executable; set AGENT_OS_PYTHON if needed' })
  }
  for (const [name, path] of Object.entries(packageResources())) {
    try {
      await access(path)
      checks.push({ name, status: 'passed', detail: path })
    } catch {
      checks.push({ name, status: 'failed', detail: 'required packaged resource is missing' })
    }
  }
  checks.push({ name: 'model_configuration', status: env['AGENT_OS_MODEL_API_KEY']?.trim() ? 'passed' : 'failed', detail: 'Checks credential configuration presence only; main conversations default to DeepSeek-V4-Flash and small tasks to Qwen3.5-4B. Provider access and quality are not tested.' })
  if (options.database) {
    try {
      await checkStorage(options.database)
      checks.push({ name: 'database', status: 'passed', detail: `Schema version ${releaseVersions.schema}, required columns and memory triggers are readable; write permissions and all constraints are not verified` })
    } catch {
      checks.push({ name: 'database', status: 'failed', detail: 'Database unavailable or explicit schema installation is required' })
    }
  } else {
    checks.push({ name: 'database', status: 'not_run', detail: 'Pass the existing database pool to doctor to verify storage' })
  }
  return { ready: checks.every((check) => check.status === 'passed'), versions: releaseVersions, checks }
}
