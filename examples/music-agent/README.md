# MusicAgent

MusicAgent is a deliberately provider-free LLM harness example. It records a
conversation, streams a long assistant turn, records tool invocations, and
keeps uploaded audio attachments as bytes. Its deterministic fake agent makes
the complete flow useful in tests and demos without an API key.

The family is split by concern:

- `apps/ts-localfirst/` is the small application built on the `create-jazz`
  TypeScript local-first shape.
- `benchmarks/` is a self-contained native benchmark model. It measures the
  append, range-read, and materialization shapes that make an agent transcript
  different from an ordinary chat timeline.

The application intentionally uses the public typed `Db` API for schema,
queries, and streaming writes. The lower-level range/append methods are public
on `JazzClient`, but are not yet promoted through typed `Db`; this example does
not reach through `Db` internals to work around that gap.

Run the app checks with:

```sh
pnpm --dir examples/music-agent/apps/ts-localfirst test
pnpm --dir examples/music-agent/apps/ts-localfirst build
```

Run benchmark correctness checks with:

```sh
cargo test -p jazz-example-music-agent-benchmark
```
