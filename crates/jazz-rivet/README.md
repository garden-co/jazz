# Jazz Rivet actor host

This crate runs the Jazz server in a Rivet actor using Rivet 3.0.7's unmodified
async SQLite worker. It includes the SQL adapter, actor lifecycle, Envoy HTTP and
WebSocket transport, deployment image, and integration tests. The TypeScript
`rivet()` provider lives in `jazz-tools/backend`.

See [the runnable todo example](../../examples/rivet-todos/README.md) for setup,
local execution, and Cloud provisioning through `createJazz`.

The crate is intentionally an independent Cargo workspace: Rivet's SQLite
linkage and Rust toolchain differ from the other Jazz providers. The actor does
not link `rusqlite`. Native Jazz providers retain their existing inline SQLite
implementation. Run checks from the repository root:

```sh
sh crates/jazz-rivet/check.sh
sh crates/jazz-rivet/build-image.sh
```

The check script uses Rust 1.99.1 and sets SQLite's required atomic-write compile
options. Tests exercise scalar bindings, transaction cleanup, cancellation,
replica sync, full HTTP schema deployment, authenticated sockets, policy denial,
and reopening persisted pages. Their deterministic page service is not a live
Rivet cluster.

One actor owns one database and executes complete admitted Jazz operations on
its server owner thread. Shutdown drains those operations and closes Rivet's
worker before releasing the generation. Initial deployment validation uses a
separate disposable worker; it never writes to the actor's persisted pages.
The scratch service supports at most 256 SQLite pages and refuses staged commits.

Host discovery returns the actor's Jazz app ID and image artifact digest.
Jazz database encodings and the `JazzRivetAppIdV1` actor-to-app mapping are unchanged.
