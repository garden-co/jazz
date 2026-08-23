# jazz-napi Fast Local Loop

For local real-data harness iteration, build the native binding with Cargo's
`perf` profile instead of the default release profile:

```sh
cd "$JAZZ_CORE"
pnpm --dir crates/jazz-napi build:perf
```

The build writes the usual platform `.node` file into `crates/jazz-napi`, so a
workspace that depends on `jazz-napi` via a direct `file:` link to
`crates/jazz-napi` loads the perf-profile binding without an artifact copy step.

Sccache remains opt-in for local iteration because it disables Rust incremental
compilation and can slow down single-branch edit/rebuild loops:

```sh
RUSTC_WRAPPER=sccache pnpm --dir crates/jazz-napi build:perf
# or
pnpm --dir crates/jazz-napi build:perf:sccache
```

Use sccache for clean or CI-style builds where cache reuse matters more than
incremental compilation. Do not set a global `rustc-wrapper` in `.cargo/config.toml`
for the local harness loop unless incremental measurements show it is not slower.
