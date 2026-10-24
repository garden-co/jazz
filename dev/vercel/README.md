# Jazz on Vercel (spike)

Spike code for running a Jazz shard on Vercel. Not a product surface.

- `app/` is a Vercel project.
  - `lib/core.js` owns the shard core: one persistent Vercel Sandbox per
    shard. The shard's SQLite lives on a read-write Drive at `/data`. The
    `jazz-tools` binary sits on a second Drive mounted as a read-only
    snapshot at `/opt/jazz`.
  - `api/gw.js` is a thin WebSocket proxy on Fluid compute. It wakes the
    Sandbox on first connect and lets the Sandbox stop once clients go idle.
    It parks idle clients while the Sandbox sleeps, and makes them reconnect
    and resubscribe once it runs again.
  - `api/ctl.js` is a control endpoint for experiments, guarded by
    `SPIKE_CTL_SECRET`.
- `spike/` is a Rust load and durability client. It prints JSON lines.
  - `setup` publishes the schema.
  - `write` inserts rows and waits for server acks.
  - `count` checks for gaps.
  - `first-sync` times a cold read.
  - `watch` holds an idle subscription.

Environment for the Vercel project:

- `SPIKE_CTL_SECRET` and `SPIKE_ADMIN_SECRET` must be set.
- `SPIKE_SHARD`, `SPIKE_IDLE_MS` and `SPIKE_POLL_MS` are optional.

The Sandbox SDK authenticates with the function's OIDC token.

```sh
cargo run --release -p jazz-vercel-spike -- \
  --url https://<deployment> --admin-secret "$SPIKE_ADMIN_SECRET" watch
```
