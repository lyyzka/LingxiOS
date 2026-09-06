import { execFile } from 'node:child_process'
import { existsSync } from 'node:fs'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, isAbsolute } from 'node:path'
import { promisify } from 'node:util'
import { ConfigError } from '../errors.js'

export interface KernelLimits {
  memoryBytes: number
  cpuSeconds: number
  maxProcesses: number
  tmpBytes: number
}

export function kernelIsolation(value: unknown, production: boolean, trusted = false): 'process' | 'bubblewrap' {
  const isolation = value ?? (production && !trusted ? 'bubblewrap' : 'process')
  if (isolation !== 'process' && isolation !== 'bubblewrap') throw new ConfigError('kernel isolation must be process or bubblewrap')
  if (production && isolation === 'process' && !trusted) throw new ConfigError('production worker requires an OS-isolated kernel; process execution requires explicit trustProcessKernel')
  if (isolation === 'bubblewrap' && process.platform !== 'linux') throw new ConfigError('bubblewrap kernel isolation requires Linux')
  return isolation
}

/** Start from an empty root. No application, credentials or sibling homes are mounted. */
export function sandboxCommand(home: string, runner: string, python: string, limits: KernelLimits, allowNetwork: boolean, args: string[] = ['-I', '/runner.py']) {
  if (isAbsolute(python) && !python.startsWith('/usr/')) throw new ConfigError('isolated Python must be installed under /usr')
  return { command: 'prlimit', args: [
    `--as=${limits.memoryBytes}`, `--cpu=${limits.cpuSeconds}`, `--nproc=${limits.maxProcesses}`, '--core=0', '--',
    'bwrap', '--die-with-parent', '--unshare-all', '--new-session', '--cap-drop', 'ALL',
    ...(allowNetwork ? ['--share-net'] : []),
    '--ro-bind', '/usr', '/usr',
    ...['/lib', '/lib64'].filter(existsSync).flatMap(path => ['--ro-bind', path, path]),
    ...(existsSync('/etc/ld.so.cache') ? ['--ro-bind', '/etc/ld.so.cache', '/etc/ld.so.cache'] : []),
    '--ro-bind', runner, '/runner.py',
    '--proc', '/proc', '--dev', '/dev', '--size', String(limits.tmpBytes), '--tmpfs', '/tmp', '--bind', home, home,
    '--chdir', home, '--', python, ...args,
  ] }
}

export async function checkKernelIsolation(runner: string, python: string, limits: KernelLimits): Promise<void> {
  const directory = await mkdtemp(join(tmpdir(), 'lingxios-isolation-'))
  try {
    const launch = sandboxCommand(directory, runner, python, limits, false, ['-I', '-c',
      'import os,socket; assert not os.path.exists("/app"); assert os.path.isfile("/runner.py"); print("isolated")'])
    const { stdout } = await promisify(execFile)(launch.command, launch.args, { timeout: 10_000, env: { PATH: '/usr/bin:/bin' }, windowsHide: true })
    if (stdout.trim() !== 'isolated') throw new Error('unexpected isolation probe output')
  } catch (cause) { throw new ConfigError(`kernel isolation self-check failed: ${cause instanceof Error ? cause.message : String(cause)}`) }
  finally { await rm(directory, { recursive: true, force: true }) }
}
