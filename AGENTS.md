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
- **Unsupported over unscalable.** When a shape can't be handled at a cost
  proportional to what it touches, fail with a typed error naming the exact
  pattern ("Reading X is not supported in Y yet"). Never fall back to
  whole-table loads, full scans or other work that grows with unrelated data.
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
- **Examples and snippets.** Take `db` from the query hook that is already in
  the component (`const { data: todos = [], db } = useAll(...)`; `todos.db` in
  Solid and on a Svelte `QuerySubscription`). Use `useDb()` / `getDb()` only
  where a component has no query.
- **Docs sidebar.** A sidebar entry is either a page link or a folder
  expander, never both: docs folders get no `index.mdx` and no `"index"` in
  `meta.json`. Put an overview in its own page or in the pages that need it.
- **Schema changes.** Never delete or move a store (an example's, a dev
  backend's, a test fixture you didn't create) to get past a schema error: it
  means a migration is missing. Run `npx jazz-tools migrations create` before
  the first change to a schema a store already holds, and ship the app's
  `jazz.app.json` with `defineApp(schema, { artifact })`
  (`import artifact from "./jazz.app.json" with { type: "json" }`), and give
  backends its `jazz.permissions.json` as `createJazz({ app, permissions })`.
  Regenerate both with `npx jazz-tools validate` (also after editing
  `permissions.ts`) and commit them; never hand-edit or format them.
- **Backlog.** Follow-ups go in GitHub Issues, not in specs or docs.
- **Changesets.** Name only published packages. Rust-facing notes go under
  `jazz-tools` (the crates ship at its version); private `@jazz/rust` makes
  `changeset version` fail.

## Performance work

- Before a perf trial, read `dev/benchmarks/rejected-experiments.md` and
  search PRs for the same mechanism.
- Don't claim an end-to-end win from a local phase or fewer allocations.
  Compare same-base CodSpeed runs (`benchmark` label).

## Pull requests

Behavior-changing PRs describe before/after behavior, the invariants that
govern it, non-goals, and worked examples for the normal path and the
meaningful edge or failure cases.

CI runs by PR state. Draft PRs run nothing. A ready PR runs only the lint
partition (format, lint, typecheck, workspace check) on its latest push; run
`node dev/gates/local-ci-equivalent.mjs --ci-partition lint` before pushing.
Marking a PR ready puts it in the review and merge queue. A PR is approved by a
GitHub review or by its author's own approval comment once the agent review
passes. The queue merges approved heads together on a `merge-group/<n>` branch,
whose push runs the full suite once (its PR into main runs only lint); on green
it merges each PR in order with its own merge commit. A main push whose tree
equals a commit on a green group's first-parent chain skips the suite, so those
merges don't rerun it. Only the group's tip was tested, so if the queue stops
before merging every PR of a group, it dispatches CI on main. Group pushes run
trusted with repository secrets and write the shared compile cache, so only
admins may push `merge-group/**`, and only approved PRs go into a group. A green full run posts a `full-suite` commit status on
its head and, for a merge group, on each PR head the group merged cleanly;
branch protection requires it on main, so every PR into main (Dependabot,
docs-only and hotfixes included) goes through a merge group or a dispatched
full run, and pushing to a covered PR voids its coverage. Like `test-rust`,
the status trusts repository writers: the workflow that posts it comes from
the ref under test.
PRs into `release` still run the full suite. To get a full run earlier,
dispatch the CI workflow on your branch.
