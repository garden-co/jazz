# Jazz

Distributed, local-first relational database. Rust core, TypeScript client
layers, WASM + NAPI + React Native bindings.

## Rules

- **Public repo.** Never commit customer names, schemas, domains, dumps or
  PII. Real customer data lives only in `jazz-private`; fixtures here are
  anonymized.
- **Durable encodings.** Any change to storage or wire bytes names and
  versions the encoding, pins it with byte-level fixtures, and needs a
  compatibility write-up before landing. A serializer's default layout is not
  a contract.
- **No sync bypasses.** Don't add fast paths that skip sync or permissions;
  make the normal path fast.
- **Tests.** Read `crates/jazz/TESTING_GUIDELINES.md` before writing a Rust
  test.
- **Existing tests encode decisions.** If one fails because the implementation
  diverged, ask before changing the test.
- **Layer crates.** Test-only hooks in a `crates/jazz/layers/*` crate must be
  gated `#[cfg(any(test, feature = "testing"))]`: `cfg(test)` is false in a
  lower crate while Jazz's own tests run, so the hook silently never fires.
- **CI-equivalent.** Only
  `node dev/gates/local-ci-equivalent.mjs --ci-equivalent` may be described as
  CI-equivalent.
- **Backlog.** Follow-ups go in GitHub Issues, not in specs or docs.

## Performance work

- Before a perf trial, read `dev/benchmarks/rejected-experiments.md` and
  search PRs for the same mechanism.
- Don't claim an end-to-end win from a local phase or fewer allocations.
  Compare same-base CodSpeed runs (`benchmark` label).

## Pull requests

Behavior-changing PRs describe before/after behavior, the invariants that
govern it, non-goals, and worked examples for the normal path and the
meaningful edge or failure cases.
