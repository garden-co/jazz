# Example benchmarks

Every catalogue app owns a **self-contained benchmark variant** beside its
public schema, deterministic fixture generator, and scenario driver. The
benchmark variant intentionally duplicates the app's schema and workload shape
instead of importing a shared app-runtime helper: it must stay understandable
when opened in isolation and keep its measurement inputs explicit.

The `smoke` crate proves the common Rust/CodSpeed plumbing only. It is not an
app workload and must not become a place for shared application fixtures.

## Add an app benchmark variant

1. Add a small Rust package under `examples/<app>/benchmarks/` (or another
   app-local benchmark directory) and list it in the root Cargo workspace.
2. Put the app's deterministic, synthetic fixture and workload construction in
   that package. Seed/profile/topology metadata belongs to the app variant,
   not to this directory.
3. Depend on `divan = { workspace = true }`, add one `[[bench]]` target with
   `harness = false`, and use `divan::black_box` or a returned result so the
   measured work is not optimized out.
4. Run the focused local receipt:

   ```sh
   cargo bench -p <app-benchmark-package> --bench <suite>
   ```

5. Once the app benchmark is ready for hosted measurement, add it to the
   `cargo codspeed build` and `cargo codspeed run` selection in
   `.github/workflows/codspeed.yml`. Keep setup outside the measured closure;
   use Divan's `Bencher` when a workload needs fresh per-iteration input.

CodSpeed's compatibility crate is intentionally named `divan` at the workspace
level. This keeps the Rust benchmark source identical for local `cargo bench`
and hosted CodSpeed instrumentation.
