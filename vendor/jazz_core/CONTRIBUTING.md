# Contributing

Thanks for contributing to jazz_core. This guide covers the local workflow and
verification gates for changes to this repository.

## Prerequisites

- A recent stable Rust toolchain
- `cargo`

## Build

Run builds from the repository root:

```sh
cargo build
```

## Verification Gates

Before a change is considered done, run the following gates from the repository
root:

```sh
cargo test -p jazz
cargo test -p groove
cargo test -p jazz-server
cargo test --doc -p jazz
cargo test --doc -p groove
cargo test -p jazz-sim --test scenario_smoke
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
```

For the TypeScript WASM bindings, use the repo-level helper:

```sh
scripts/test_wasm_bindings.sh
```

It requires `wasm-pack`, Node package dependencies in all three TS WASM example
packages, and Playwright Chromium for the browser smoke test. On a fresh
checkout, run
`scripts/test_wasm_bindings.sh --install` to install package dependencies with
`npm ci` before running the gates.

See [jazz/SPEC/D_testing_gates.md](jazz/SPEC/D_testing_gates.md) (appendix D) for
the full tier descriptions.

## Testing Discipline

Prefer tests through the public surface: the jazz `Db` facade and the groove
`Database`. Use internal hooks only when the behavior cannot be observed
through those surfaces or when a narrowly scoped lower-level unit test is the
clearest way to pin down an invariant.

Changes should be behavior-preserving unless the change intentionally updates
behavior. When behavior changes intentionally, update or add tests so the new
expected behavior is covered.

## Formatting

Run `cargo fmt` before committing. The required formatting gate is:

```sh
cargo fmt --all --check
```
