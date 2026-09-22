# Wequencer

Wequencer is a collaborative step sequencer: bandmates edit a shared pattern,
watch its transport state, and see recently observed peers. It is an
example family rather than a production DAW. Its narrow musical model makes
several difficult local-first shapes concrete:

- an ordered, windowed step grid with many small subscriptions;
- independent concurrent edits to nearby and identical steps;
- creator-managed editor/viewer permissions on one shared session;
- an explicit reconnect/partition and permission test contract while transport
  observations remain deliberately ephemeral; and
- a deterministic native benchmark and soak fixture that mirrors the public
  schema and key query shapes rather than importing application helpers.

## Variants

- `apps/next-betterauth/` is the self-contained Next.js + Better Auth app.
  It follows the `create-jazz` Next/Better Auth structure: the server owns
  account bootstrap and the client owns only local-first interaction.
- `benchmarks/` is the matching native Divan/CodSpeed model. It exercises
  ordered pattern windows, subscription refreshes, and deterministic local
  edit bursts; the browser scenario covers two-client contention and recovery.

The app models clock and presence as observations. They are useful for a UI but
may remain stale and are not used to authorize or deterministically schedule audio.
Actual sample-accurate synchronization is intentionally an open extension;
the current example makes convergence and ordering pressure visible without
claiming an impossible distributed clock guarantee.

## Reliability surface

[`SCENARIOS.md`](./SCENARIOS.md) fixes the seed, topology, fault, and expected
outcome for the 16-track, 64-step soak profiles. The native benchmark covers
bounded ordered window reads, local subscription delivery, deterministic edit
bursts, and the same latest-transport receipt query used by the UI. The browser
E2E covers two authenticated browser clients, disjoint concurrent edits, a
bounded offline/reconnect phase, transport receipt delivery, and a viewer
authorization rejection. It deliberately does not name a winner for
simultaneous writes to the same field.

Run the native checks with:

```sh
cargo test -p jazz-example-wequencer-benchmark
cargo bench -p jazz-example-wequencer-benchmark --bench loads
```
