# Wequencer

Wequencer is a collaborative step sequencer: bandmates edit a shared pattern,
watch its transport state, and see which peers are currently playing. It is an
example family rather than a production DAW. Its narrow musical model makes
several difficult local-first shapes concrete:

- an ordered, windowed step grid with many small subscriptions;
- independent concurrent edits to nearby and identical steps;
- owner/editor/viewer permissions on one shared session;
- an explicit reconnect/partition and permission test contract while transport
  observations remain deliberately ephemeral; and
- a deterministic native benchmark and soak fixture that duplicate the public
  schema and query shapes rather than importing application helpers.

## Variants

- `apps/next-betterauth/` is the self-contained Next.js + Better Auth app.
  It follows the `create-jazz` Next/Better Auth structure: the server owns
  account bootstrap and the client owns only local-first interaction.
- `benchmarks/` is the matching native Divan/CodSpeed model. It exercises
  ordered pattern windows, subscription refreshes, and concurrent step edits.

The app models clock and presence as short-lived observations. They are useful
for a UI but are not used to authorize or deterministically schedule audio.
Actual sample-accurate synchronization is intentionally an open extension;
the current example makes convergence and ordering pressure visible without
claiming an impossible distributed clock guarantee.

## Reliability surface

[`SCENARIOS.md`](./SCENARIOS.md) fixes the seed, topology, fault, and expected
outcome for the 16-track, 64-step soak profiles. The native benchmark already
covers ordered cursor-window reads, local subscription delivery, and
deterministic edit bursts. Browser E2E will add the specified real-topology
profiles; networked convergence is intentionally not claimed before then.

Run the native checks with:

```sh
cargo test -p jazz-example-wequencer-benchmark
cargo bench -p jazz-example-wequencer-benchmark --bench loads
```
