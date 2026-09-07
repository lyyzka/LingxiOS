import { opendir, lstat, realpath, unlink } from 'node:fs/promises'
import { resolve } from 'node:path'
import type { SqlQueryable } from '../control-plane/pg-store.js'

/** Retain committed hashes and receipts. Only expired traces and old unpublished temporary names are removed. */
export async function maintainStorage(database: SqlQueryable, homesRoot: string) {
  const expired = await database.query(`DELETE FROM lingxios.agent_run_events WHERE (run_id,seq) IN
    (SELECT run_id,seq FROM lingxios.agent_run_events WHERE expires_at<NOW() AND (delivery_work IS NULL OR delivered_at IS NOT NULL) ORDER BY expires_at LIMIT 500)`)
  let inspected = 0, removed = 0
  const root = resolve(homesRoot, '.committed-artifacts')
  try {
    if (await realpath(root) !== root) throw new Error('artifact maintenance cannot traverse links')
    for await (const entry of await opendir(root)) {
      if (++inspected > 500) break
      if (!entry.isDirectory() || !/^[a-f0-9]{64}$/.test(entry.name)) continue
      for await (const item of await opendir(resolve(root, entry.name))) {
        if (++inspected > 500) break
        // Published files are SHA-256 names. UUID files are temporary and never referenced by messages.
        if (!item.isFile() || !/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(item.name)) continue
        const path = resolve(root, entry.name, item.name), stat = await lstat(path)
        if (!stat.isSymbolicLink() && stat.mtimeMs < Date.now() - 86_400_000) { await unlink(path); removed++ }
      }
    }
  } catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error }
  return { expiredEvents: expired.rowCount ?? 0, removedTemporaryArtifacts: removed }
}
