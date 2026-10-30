# Contributing

## Prerequisites

### sccache (strongly recommended)

[sccache](https://github.com/mozilla/sccache) caches compiler invocations across feature sets, profiles, and branches. Without it, crates with C dependencies (the bundled SQLite) recompile on every build.

```sh
cargo install sccache   # or: brew install sccache
```

Then add to your shell profile (`~/.zshrc`, `~/.bashrc`, etc.):

```sh
export RUSTC_WRAPPER=sccache
```

### Native build requirements

The bundled SQLite is compiled from source on the first build (cached by `sccache` afterwards).
This requires a working C compiler:

- macOS: `xcode-select --install` is enough.
- Linux: install `build-essential` (Debian/Ubuntu) or `gcc` (Fedora).

### WASM builds on macOS

Install Homebrew LLVM with `brew install llvm`. The WASM build script selects
`llvm-ar` from `PATH` or Homebrew's LLVM installation automatically; no shell
`PATH` changes are needed. Apple's archive tools cannot bundle SQLite's WASM
object files. This selection applies only to WASM builds, leaving native builds
unchanged. Explicit `AR_wasm32-unknown-unknown`, `AR_wasm32_unknown_unknown`,
`TARGET_AR`, and `AR` environment overrides take precedence.

## API documentation

Keep API documentation attached to an authoritative, maintained source rather
than committing a point-in-time export catalogue. The public application guide
is in `docs/content/docs/`; its runnable examples are the adjacent
`examples/docs/` projects. The Rust contract is
`crates/jazz/SPEC/13_db_api.md`, with the other engine contracts in
`crates/jazz/SPEC/`.

For a package's exact TypeScript surface, use its `package.json` export map and
the emitted declaration files for the release being consumed. For Rust, use the
crate's public documentation and its specification. Do not add a manually
maintained or generated-in-place API snapshot: it has no executable source of
truth and will drift as the API evolves.

## Testing

### Pre-commit hooks in restricted shells

The staged Rust hook receives its file list directly from Lefthook and invokes
Cargo through `dev/scripts/clippy-staged.sh`. It asks Cargo for authoritative
workspace metadata once, then invokes Cargo directly; this avoids Node
child-process invocations of `git` and `cargo`, which can be denied by
sandboxed shells while ordinary Git commands still work. Run
`pnpm test:tooling` to exercise workspace-member, standalone, excluded, and
failure paths. Nonmember/auxiliary crates deliberately fall back to the root
workspace guard; maintained standalone crates should have their
own explicit gates. Run `pnpm test:tooling:real` for the slower real-Cargo
probe of that fallback. Direct invocations of
`node dev/scripts/clippy-staged.mjs` remain available for local debugging.

### CLI integration tests

CLI integration scenarios are partitioned by behavior across focused test files;
keep every scenario in exactly one suite. Shared helpers live in the private
`tests/cli/fixtures.ts` factory, which creates isolated temporary roots per file
and cleans up temporary files and stubbed globals after each test.

### Running tests

```sh
pnpm test          # everything (via turbo)
cargo test -p jazz --no-default-features --features testing,transport-compression-zstd   # rust core only
```

### Better Auth adapter test fixtures

Reuse the generated fixture schema and permissions when testing adapter
behaviour. Each case still owns a fresh server, client context and rows, and
publishes its catalogue through the real deployment interface. Keep filesystem
schema-loading coverage in the CLI and loader suites rather than repeating it
in adapter setup.

### TypeScript correctness partition

```sh
node dev/gates/local-ci-equivalent.mjs --ci-partition typescript
pnpm --filter jazz-tools typecheck:react-native
```

The partition prepares and verifies sealed native artifacts before building
and testing their consumers. Package tests run with three Turbo tasks; Jazz
Tools retains its eight-worker Node cap and four-worker Chromium cap.

`build:test` always validates the DSL fixture, runs the test compiler and checks
public interface types. Its test compiler reuses
`node_modules/.cache/tsc/tests.tsbuildinfo`; CI restores compatible state and
TypeScript invalidates changed source, compiler options and native declarations.
Keep cache options on the command, not in `tsconfig.tests.json`: generated
migration fixtures extend that configuration and must not share package state.

The `test` target owns the React Native/Expo compile-only contract, so
`build:test` does not repeat it. The standalone typecheck remains available.
Compare the complete partition as well as focused phases before claiming a
performance improvement.

### Focus one Rust test safely

`dev/t` first asks Cargo for the test inventory using the same target and
features it will run. A filter must resolve to exactly one inventory entry; the
wrapper then invokes that canonical name with Cargo's `--exact`. It refuses an
empty or ambiguous selection before a test runs, prints the selected binary,
feature and cache context, and separately reports inventory/compile and test-run
elapsed time. A distinctive test-name suffix is enough to discover and run the
canonical name. A filter that starts with `db::` or `binding_codec::` selects
`jazz-db` without `-p`.

```sh
# Default: Jazz library tests, substring matching.
dev/t subscription::tests::reopens

# Fully-qualified exact library test.
dev/t --exact db::tests::round_trips

# A layer-crate test: resolve its canonical path from the unique suffix.
dev/t -p jazz-types row_author_rejects_noncanonical_system_origin

# An integration target is explicit, so Cargo cannot search an unintended bin.
dev/t --test integration include_live::renaming_a_sender -- --nocapture
```

Run `pnpm test:focused-rust` to test the wrapper with a mocked Cargo inventory.

### Bounded Rust runs and receipts

Use the repository launcher when a test could hang or when comparing local and
CI run time. It writes a machine-readable JSON receipt containing the exact
command, exact dirty-tree fingerprint (without source contents), toolchain,
cache configuration, shard, timing, and direct exit status.

```sh
# Recommended: per-test slow timeout, named hung-test output, deterministic shard.
cargo install cargo-nextest --locked
node dev/gates/run-rust-tests.mjs --shard-index 1 --shard-count 2 -- \
  --workspace --lib --bins --tests \
  --features jazz/testing,jazz/transport-compression-zstd,jazz-server/test,jazz-cli/test

# No Devbox or Nextest required: preserves Cargo selection and adds an overall
# timeout, but cannot attribute a hang to an individual test.
node dev/gates/run-rust-tests.mjs --timeout-seconds 900 -- -p jazz
```

The Nextest `jazz` profile reports a test slow after 60 seconds and terminates
it one minute later. Hash partitions are deterministic and do not overlap for a
fixed test inventory; keep the shard count identical across all CI shards.

### Snapshot testing with insta in rust

Sync integration tests use [insta](https://insta.rs) for inline snapshot assertions. Snapshots live directly in the test source as `@"..."` strings — no separate `.snap` files.

```rust
insta::assert_snapshot!(tracer.tally(), @"
alice    -> server  : ObjectUpdated (1)
server   -> alice   : PersistenceAck (2)
");
```

When a snapshot doesn't match, the test fails and insta records the new value. To review and update:

```sh
# Install the insta CLI (once)
cargo install cargo-insta

# Run the failing tests
cargo test -p jazz --no-default-features --features testing,transport-compression-zstd

# Review each pending change interactively — shows a diff, asks accept/reject
cargo insta review

# Or accept all pending snapshots at once (when you trust the new output)
cargo insta accept
```

`cargo insta review` rewrites the `@"..."` string in the source file directly.
No git-tracked `.snap` files to manage.
