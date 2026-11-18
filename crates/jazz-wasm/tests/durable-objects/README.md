# Durable Object smoke test

Jazz on a Durable Object's own SQL (`ctx.storage.sql`) in local workerd,
through Miniflare: no Cloudflare account, nothing deployed.

```sh
crates/jazz-wasm/tests/durable-objects/run.sh
```

It builds jazz-wasm with the test-only `durable-object-tests` feature into
`./pkg` and runs `smoke.test.mjs`: a Durable Object writes a short and a
300 KiB note, reads them back, restarts (a new workerd over the same
persisted storage), reads them again, and has a callback transaction
refused (a core there accepts finished transactions only).

The full Durable Object host (sessions, sockets, admission, publication)
builds on this once jazz-server's runtime compiles for wasm.
