# Production deployment and recovery

Run one control-plane process with PostgreSQL and its persistent artifact directory, then run two worker replicas. Each worker has `AGENT_OS_MAX_CONCURRENT_RUNS=2`; that is the supported four-task execution baseline. Keep upstream admission at 100 queued work items or fewer.

Workers require Linux Bubblewrap isolation. Use the supplied `worker-seccomp.json`, a read-only root filesystem, `--cap-drop ALL`, `no-new-privileges`, `/tmp` as a bounded no-exec tmpfs, and an isolated writable `/data/homes`. Do not run privileged or unconfined workers.

```sh
docker run --read-only --tmpfs /tmp:rw,noexec,nosuid,size=128m \
  --tmpfs /data/homes:rw,nosuid,size=256m,uid=1000,gid=1000 \
  --cpus=2 --memory=2g --pids-limit=128 --cap-drop ALL \
  --security-opt no-new-privileges --security-opt seccomp=deploy/worker-seccomp.json \
  --env-file worker.env lingxios-worker:release
```

Expose control-plane `/healthz` for process liveness and `/readyz` for database, artifact storage, isolation and drain state. Worker `/readyz` additionally requires successful recent control-plane contact. Do not route work to a draining worker.

Back up PostgreSQL and committed artifacts from the same point in time: drain workers, wait for leases to finish or expire, snapshot the database and artifact volume, then restart workers. Restore to an isolated environment, apply the matching release schema only to an empty database, restore both snapshots, and run `node scripts/check-release.mjs --deterministic` before directing traffic there. To roll back code, drain workers and deploy the prior image only when its recorded schema version matches; the initial schema intentionally has no in-place migration path.

Run `npm run check:release` with model credentials and a signed acceptance JSON before a production release. It preserves logs and JSON evidence in `release-results/`; missing live-model credentials or browser/content, delivery, permission-revocation, or backup/restore acceptance evidence fails the gate.
