# MusicAgent benchmark variant

This native package duplicates the small MusicAgent schema and deterministic
fixture. It does not import the TypeScript application or its fake provider.

It measures the large-value shapes that matter for an LLM harness: append to a
streamed assistant turn, a bounded byte-range read from an audio attachment,
and ordinary transcript query materialization. Correctness tests also reopen
the same durable fixture and verify that the transcript remains readable after
restart.

```sh
cargo test -p jazz-example-music-agent-benchmark
cargo bench -p jazz-example-music-agent-benchmark --bench loads
```
