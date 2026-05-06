# Remote Autonomy Gateway

Thin Elysia HTTP gateway for running Codex and sync workers on a remote server
while keeping Jazz2 as the durable control plane.

The gateway does not replace `git-sync` or `rsync`. Those tools move bytes. This
service records intent, leases, session presence, receipts, and current state so
Mac-side launchers and server-side workers can restart without losing the graph
of what happened.

## Endpoints

- `GET /health` checks the configured Jazz2 sync server and local Jazz2 stores.
- `GET /v1/bootstrap` returns the sync server URL, app id, data paths, and API
  route names for launchers.
- `POST /v1/codex/presence` records a Codex terminal/session heartbeat.
- `GET /v1/codex/sessions?active=1` lists active Codex sessions.
- `POST /v1/codex/stream-events` records file-backed Codex tail events for
  remote replication through Jazz2.
- `GET /v1/codex/stream-events?sessionId=...` lists replicated Codex stream
  events by session and sequence cursor.
- `POST /v1/executor/traces` records a Nullclaw/Codex/worker executor result as
  an idempotent Jazz2 semantic event.
- `GET /v1/executor/traces?executor=...&traceId=...` lists retained executor
  traces for ORACLE review.
- `POST /v1/sync/jobs` creates idempotent `rsync`, `git-sync`, or worker jobs.
- `POST /v1/sync/jobs/:jobId/claim` claims a queued job for a worker.
- `POST /v1/sync/jobs/:jobId/status` updates job state.
- `POST /v1/sync/receipts` records a transport receipt and updates the job.
- `POST /v1/claims` records a remote workspace ownership lease.
- `POST /v1/spaces` registers a Designer space by slug, local mirror path,
  remote source path, object-storage prefix, and a `space-rsync-mirror` worker
  job.
- `GET /v1/spaces` lists registered Designer spaces from the latest
  `space-rsync-mirror` jobs.
- `GET /v1/spaces/:slug/files?includeContent=1` lists the latest file records
  for a space and, when the gateway can verify cached or materialized bytes
  against `contentHash`, includes `contentBase64` for hydration.
- `POST /v1/spaces/:slug/files` records a Designer space file, object-storage
  descriptor, upload job, and materialization job. Inline `contentBase64` or
  `content` payloads are verified, cached, and materialized immediately.
- `GET /v1/state` returns the current active sessions, jobs, claims, and
  Designer spaces.

## Local Run

```sh
pnpm --filter ./examples/remote-autonomy-gateway dev
```

To run the gateway and the local server-side worker loop in one durable process:

```sh
REMOTE_AUTONOMY_WORKER=1 pnpm --filter ./examples/remote-autonomy-gateway start
```

## Server Environment

```sh
export REMOTE_AUTONOMY_SYNC_SERVER_URL="https://nikitavoloboev-jazz2-sync-ingress.tailbf2c6c.ts.net"
export REMOTE_AUTONOMY_SYNC_SERVER_APP_ID="313aa802-8598-5165-bb91-dab72dcb9d46"
export REMOTE_AUTONOMY_HOST_ID="$(hostname)"
export REMOTE_AUTONOMY_PORT="7474"
export REMOTE_AUTONOMY_WORKER="1"
export REMOTE_AUTONOMY_WORKER_ID="$(hostname):designer-space-worker"
export REMOTE_AUTONOMY_OBJECT_STORAGE_MODE="cache-only" # or oci-cli
export REMOTE_AUTONOMY_SYNC_PROBE_TIMEOUT_MS="3000"
export REMOTE_AUTONOMY_LOCAL_SPACES_ROOT="$HOME/.designer/spaces"
export REMOTE_AUTONOMY_REMOTE_SPACES_ROOT="/users/nikiv/.designer/spaces"
export REMOTE_AUTONOMY_OBJECT_STORAGE_REGION="us-sanjose-1"
export REMOTE_AUTONOMY_OBJECT_STORAGE_BUCKET="x-sanjose"
export REMOTE_AUTONOMY_DESIGNER_SPACES_PREFIX="nikiv/designer"
```

By default the local Jazz2 stores connect to the configured sync server. Set
`REMOTE_AUTONOMY_CONNECT_SYNC=0` for isolated local tests.

## Compute/File Server Deployment

For shared Designer/CAD work, run one gateway on the same Tailscale-reachable
machine that owns the compute workers and the remote workspace filesystem. Point
`REMOTE_AUTONOMY_REMOTE_SPACES_ROOT` at the filesystem path those workers read
and write, and keep `REMOTE_AUTONOMY_LOCAL_SPACES_ROOT` on durable server-local
storage for the object cache and gateway mirror state. The gateway materializes
each shared space as `<spaces-root>/<space-id>/work` on both sides.

Two developers should use the same gateway URL in Designer bootstrap. They
should not each run an independent `127.0.0.1` gateway, because that creates
separate object caches and separate materialized files. With a shared gateway,
one user saves a file through `POST /v1/spaces/:slug/files`; the gateway
hash-checks the bytes, writes the object cache and remote file, records the Jazz2
event, and another user hydrates the latest verified bytes through
`GET /v1/spaces/:slug/files?includeContent=1`.

## Workflow Shape

1. `start` calls `/health` and `/v1/bootstrap`.
2. Server Codex launchers call `/v1/codex/presence` on start and heartbeat.
3. Session tail workers run `codex-sessions-backend replicate-rollout-events
   --follow true ...` so local rollout appends are recorded into
   `codex_stream_events` with Jazz sync durability.
4. Mac or server workers create `/v1/sync/jobs` for `git-sync` and `rsync`.
5. The gateway worker, when `REMOTE_AUTONOMY_WORKER=1`, claims queued
   `space-rsync-mirror`, `space-file-object-upload`, and
   `space-file-materialize` jobs, runs the transport, and records receipts.
   `cache-only` mode writes object bytes under
   `$REMOTE_AUTONOMY_LOCAL_SPACES_ROOT/.object-cache`; `oci-cli` mode also
   shells out to the `oci` CLI for object put/get.
6. Executor workers write `/v1/executor/traces` for every model/tool result
   packet they produce.
7. Review/promotion automation reads `/v1/state` and the Jazz2 records instead
   of scraping logs.

## Designer Spaces

`/v1/spaces` is a control-plane registration surface. It creates a durable
`space-rsync-mirror` job with:

- `payloadJson.sourcePath`: remote work path under the remote spaces root.
- `payloadJson.targetPath`: local mirror work path under the local spaces root.
- `payloadJson.transport`: `rsync`.
- `payloadJson.space.objectStoragePrefix`: OCI object-key prefix under
  `nikiv/designer/<slug>`.

The expected mirror movement is: the gateway worker claims the
`space-rsync-mirror` job, runs `rsync -a --delete` from `sourcePath` to
`targetPath`, then records `/v1/sync/receipts` with the final status and
transfer metadata. This is intentionally async: Designer and Codex do not wait
on long filesystem movement in prompt or launch hooks.

File bytes for active Designer saves use `/v1/spaces/:slug/files`. Without
inline bytes, the gateway records queued `space-file-object-upload` and
`space-file-materialize` jobs for external workers. With `contentBase64` or
`content`, the gateway verifies `contentHash` and `sizeBytes`, writes an object
cache entry under `$REMOTE_AUTONOMY_LOCAL_SPACES_ROOT/.object-cache`, writes the
file to the requested local or remote materialization target, and marks both
jobs completed with receipts. Jazz2 syncs the metadata and receipts between
machines; object storage or the inline object cache owns the file bytes.
