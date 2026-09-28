# MusicAgent benchmark variant

This native package duplicates the small MusicAgent schema and deterministic
fixture. It does not import the TypeScript application or its fake provider.

[metadata.ts](metadata.ts) owns the wall-clock descriptions, timing boundaries
and work denominators used by the examples page.

`benches/walltime.rs` measures what a user of an agent chat notices: streaming
a 1,000-chunk assistant reply onto a turn that is already a large value,
opening a 200-turn conversation, reopening it after an app restart, and a
64 KiB seek into an 8 MiB audio attachment. Correctness tests also reopen the
same durable fixture and verify that the transcript remains readable after
restart.

```sh
cargo test -p jazz-example-music-agent-benchmark
cargo bench -p jazz-example-music-agent-benchmark --bench walltime
```
