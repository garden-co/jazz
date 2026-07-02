# CPU Profile Smoke Harness

`dev/benchmarks/profile.sh` runs the S1, S3, and S4 jazz-sim smoke scenarios
with the optional `profiling` feature enabled and writes pprof flamegraphs to
`dev/benchmarks/profiles/<run-id>/`.

```sh
dev/benchmarks/profile.sh
```

Each profiled phase writes:

- `<scenario>__<phase>.svg`: flamegraph
- `<scenario>__<phase>.top.txt`: top-10 self-sample table appended to
  `dev/benchmarks/SMOKE_LEDGER.md`

The `profiling` feature is off by default, so normal builds and smoke runs do
not compile pprof. For ad-hoc captures, the same bench binaries can be profiled
directly:

```sh
JAZZ_SMOKE=1 JAZZ_PROFILE_OUT=dev/benchmarks/profiles/manual \
  cargo bench -p jazz-sim --features profiling --bench s4_order_processing
```

On macOS, `samply record` also works well for plain bench binaries when you want
system-level profiles without compiling the pprof feature.
