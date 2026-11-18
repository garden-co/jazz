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

`host.test.mjs` runs the Jazz core a Durable Object hosts
(`durable-object/host.js` over `DurableObjectCore`), spoken to over a
WebSocket as the app's backend speaks to a core: a connection without the
app's core secret is refused; the backend's connection says Hello,
declares its schema, writes and reads a note; after a restart (same
storage) the note is read back. Publication on the object (the admin
routes) comes next; until then the object serves `JAZZ_SCHEMA`.
