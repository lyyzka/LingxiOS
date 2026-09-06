#!/usr/bin/env node
import { resolve } from 'node:path'
import { pathToFileURL } from 'node:url'
import { doctor } from '../app/doctor.js'
import type { SqlQueryable } from '../control-plane/pg-store.js'

const args = process.argv.slice(2)
if (args.length > 0 && (args.length !== 2 || args[0] !== '--database-module')) {
  throw new Error('usage: lingxios-doctor [--database-module existing-pool-module.js]')
}
let database: SqlQueryable | undefined
if (args[1]) {
  const module = await import(pathToFileURL(resolve(args[1])).href) as { pool?: SqlQueryable }
  if (!module.pool || typeof module.pool.query !== 'function') throw new Error('database module must export its existing pool resource')
  database = module.pool
}
const result = await doctor(database ? { database } : {})
console.log(JSON.stringify(result, null, 2))
// Resource modules may keep database connections open; the CLI owns its process lifetime.
process.exit(result.ready ? 0 : 1)
