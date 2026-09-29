# Example benchmarks

Every catalogue app owns a **self-contained benchmark variant** beside its
public schema, deterministic fixture generator, and scenario driver. The
benchmark variant intentionally duplicates the app's schema and workload shape
instead of importing a shared app-runtime helper: it must stay understandable
when opened in isolation and keep its measurement inputs explicit.

## Ownership

Each measured area has exactly one owner example: the app where it is most
semantically relevant. Before adding a case, check whether another app already
owns the area; if it does, extend that app's suite instead of duplicating it.
A new area gets a new benchmark in the app that owns it. Only what no product
owns (engine internals such as Groove's IVM) and the anonymized adopter
workload (`examples/permissioned-resources`) live outside the hero apps.

Cases that are useful as diagnostics but do not need a CodSpeed receipt on
every PR go in a separate `benches/nightly.rs` target. It is compiled by
`dev/gates/benchmark-smoke.sh --compile-ci` and can be run locally with
`cargo bench -p <package> --bench nightly`, but it is not in the CodSpeed
workload table.

## Add an app benchmark variant

1. Add a small Rust package under `examples/<app>/benchmarks/` (or another
   app-local benchmark directory) and list it in the root Cargo workspace.
2. Put the app's deterministic, synthetic fixture and workload construction in
   that package. Seed/profile/topology metadata belongs to the app variant,
   not to this directory.
3. Name the package `jazz-example-<app>-benchmark`. Depend on
   `divan = { workspace = true }` and
   `jazz-benchmark-guard = { path = "../../../crates/benchmark-guard" }`, and
   add one `[[bench]]` target named `walltime` with `harness = false`. Start
   `benches/walltime.rs` with the guard's allocator and contamination check:

   ```rust
   #[global_allocator]
   static ALLOCATOR: jazz_benchmark_guard::Allocator = jazz_benchmark_guard::Allocator;

   fn main() {
       jazz_benchmark_guard::refuse_contaminated_measurement();
       divan::main();
   }
   ```

   Use `divan::black_box` or a returned result so the measured work is not
   optimized out, and keep setup outside the measured closure (use Divan's
   `Bencher` when a workload needs fresh per-iteration input).

4. Prefix every benchmark function with the app, e.g.
   `band_chat_timeline_second_page`. The docs examples page matches CodSpeed
   results by exact name across the whole repository.
5. Add `examples/<app>/benchmarks/metadata.ts` (see
   [dev/benchmarks/metadata](metadata/README.md)) and
   register it in `dev/benchmarks/metadata/index.ts` with one import and one
   spread line. Check it with `pnpm --filter docs test:perf-timeline`.
6. Run the focused local receipt:

   ```sh
   cargo bench -p jazz-example-<app>-benchmark --bench walltime
   ```

7. Add `"<app>": nativeExample("<app>")` to `workloadSpecs` in
   `dev/benchmarks/codspeed-artifact.mjs`. That table feeds both native matrix
   jobs in `.github/workflows/codspeed.yml`:
   an ARM64 Blacksmith runner compiles the suite with mimalloc, and CodSpeed's
   macro runner measures it in wall-clock mode without compiling.

CodSpeed's compatibility crate is intentionally named `divan` at the workspace
level. This keeps the Rust benchmark source identical for local `cargo bench`
and hosted CodSpeed instrumentation.

## Native performance workloads

- [StagePlan](../../examples/stage-plan/benchmarks/README.md): task authoring,
  bulk completion and reopen, the show board, activity pages, the crew
  dashboard and row-dependent policies.
- [BandChat](../../examples/band-chat/benchmarks/README.md): opening and
  sending in a members-only room, timeline pages, unread rooms, caught-up
  resume and live-room fan-out.
- [BandBook](../../examples/band-book/benchmarks/README.md): policy-scoped
  workspace and personal page reads over 100,000 pages, one-shot and live.
- [WorldTour](../../examples/world-tour/benchmarks/README.md): member and
  public three-week calendar windows.
- Wequencer, PosterShop, RecordPlayer, EpicDrop, Jamazon Warehouse,
  MusicAgent and BigLabel: see each app's `benchmarks/` directory.
- [Permissioned resources](../../examples/permissioned-resources/benchmarks/README.md):
  anonymized adopter workload, 27,518-row first sync with deep permissions.

These packages completely replace their former crate-local benchmark targets.
Their profiling binaries and CodSpeed suites use the same workload code.
