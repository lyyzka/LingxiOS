# Production deployment and recovery

Run one control-plane process with PostgreSQL and its persistent artifact directory, then run two worker replicas. Each worker has `AGENT_OS_MAX_CONCURRENT_RUNS=2`; that is the supported four-task execution baseline. Keep upstream admission at 100 queued work items or fewer.

Workers require Linux Bubblewrap isolation. Use the supplied `worker-seccomp.json`, a read-only root filesystem, `--cap-drop ALL`, `no-new-privileges`, `/tmp` as a bounded no-exec tmpfs, and an isolated writable `/data/homes`. Do not run privileged or unconfined workers.

The Python runner communicates over inherited stdio and has no `/proc` filesystem. Code requiring procfs (including `/dev/stdin` aliases through `/proc/self/fd`) is outside the isolated Kernel contract. Docker's masked system paths remain enabled; the sandbox never binds the Worker's process tree into Python.

On hosts where `docker info` lists AppArmor, load the dedicated profile below and pass `--security-opt apparmor=lingxios-worker`. Docker's default profile denies the mounts needed to construct the sandbox. This profile permits Bubblewrap's temporary root setup while keeping mount targets restricted and `/proc` and `/sys` protected. Hosts without AppArmor omit only that AppArmor option; all other sandbox restrictions and readiness checks remain required. See [Docker's AppArmor instructions](https://docs.docker.com/engine/security/apparmor/).

```sh
sudo apparmor_parser --replace --skip-cache deploy/worker-apparmor
docker run --read-only --tmpfs /tmp:rw,noexec,nosuid,size=128m \
  --tmpfs /data/homes:rw,nosuid,size=256m,uid=1000,gid=1000 \
  --cpus=2 --memory=2g --pids-limit=128 --cap-drop ALL \
  --security-opt no-new-privileges --security-opt seccomp=deploy/worker-seccomp.json \
  --security-opt apparmor=lingxios-worker \
  --env-file worker.env lingxios-worker:release
```

Expose control-plane `/healthz` for process liveness and `/readyz` for database, artifact storage, isolation and drain state. Worker `/readyz` additionally requires successful recent control-plane contact. Do not route work to a draining worker.

Back up PostgreSQL and committed artifacts from the same point in time: drain workers, wait for leases to finish or expire, snapshot the database and artifact volume, then restart workers. Restore both snapshots to an isolated environment and verify the matching schema and application before directing traffic there. Schema 7 upgrades explicitly through `db/migrations/008-governance.sql`; drain and resolve pending approvals and unknown effects first. Rollback requires a compatible retained deployment or a consistent backup, as described in [packaged runtime](../docs/packaged-runtime.md).

Run `npm run check:release` for deterministic release checks, including the confined Linux image. It emits source-bound JSON evidence in `release-results/` only when all checks pass. Real-model quality, latency and cost, consuming-product/browser integration and production backup/restore acceptance remain separate release requirements; this command records them as not run and does not accept a signed acceptance JSON.

Version 3.2 uses schema 10 and protocol 8. Existing 3.1/schema-9 databases apply `packageResources().migration010` after ingress is paused and old Workers are drained and stopped. This additive migration preserves memory and business records; retain the matching database/artifact backup for rollback. The release workflow requires Windows tests and Linux PostgreSQL, IM/DAG process recovery, capacity and sandbox checks. Publishing a tag additionally requires successful gates for that exact commit on `main`; qualification JSON is retained as a workflow artifact.
