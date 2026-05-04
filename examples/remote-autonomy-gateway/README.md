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
- `GET /v1/state` returns the current active sessions, jobs, claims, and
  Designer spaces.

## Local Run

```sh
pnpm --filter ./examples/remote-autonomy-gateway dev
```

## Server Environment

```sh
export REMOTE_AUTONOMY_SYNC_SERVER_URL="https://nikitavoloboev-jazz2-sync-ingress.tailbf2c6c.ts.net"
export REMOTE_AUTONOMY_SYNC_SERVER_APP_ID="313aa802-8598-5165-bb91-dab72dcb9d46"
export REMOTE_AUTONOMY_HOST_ID="$(hostname)"
export REMOTE_AUTONOMY_PORT="7474"
export REMOTE_AUTONOMY_SYNC_PROBE_TIMEOUT_MS="3000"
export REMOTE_AUTONOMY_LOCAL_SPACES_ROOT="$HOME/spaces"
export REMOTE_AUTONOMY_REMOTE_SPACES_ROOT="/users/nikiv/spaces"
export REMOTE_AUTONOMY_OBJECT_STORAGE_REGION="us-dallas-1"
export REMOTE_AUTONOMY_OBJECT_STORAGE_BUCKET="reactron-updates-dev"
export REMOTE_AUTONOMY_DESIGNER_SPACES_PREFIX="x/nikiv/designer/spaces"
```

By default the local Jazz2 stores connect to the configured sync server. Set
`REMOTE_AUTONOMY_CONNECT_SYNC=0` for isolated local tests.

## Workflow Shape

1. `start` calls `/health` and `/v1/bootstrap`.
2. Server Codex launchers call `/v1/codex/presence` on start and heartbeat.
3. Session tail workers run `codex-sessions-backend replicate-rollout-events
   --follow true ...` so local rollout appends are recorded into
   `codex_stream_events` with Jazz sync durability.
4. Mac or server workers create `/v1/sync/jobs` for `git-sync` and `rsync`.
5. Workers claim jobs, run the transport, then write `/v1/sync/receipts`.
6. Review/promotion automation reads `/v1/state` and the Jazz2 records instead
   of scraping logs.

## Designer Spaces

`/v1/spaces` is a control-plane registration surface. It does not copy files.
Each space record creates a durable `space-rsync-mirror` job with:

- `payloadJson.sourcePath`: remote path under the remote spaces root.
- `payloadJson.targetPath`: local mirror path under the local spaces root.
- `payloadJson.transport`: `rsync`.
- `payloadJson.space.objectStoragePrefix`: OCI object-key prefix under
  `x/nikiv/designer/spaces/<slug>`.

The expected byte movement is: a worker claims the `space-rsync-mirror` job,
runs `rsync` from `sourcePath` to `targetPath`, then records `/v1/sync/receipts`
with the final status and transfer metadata. Jazz2 syncs the metadata and
receipts between machines; the worker is responsible for file bytes.
