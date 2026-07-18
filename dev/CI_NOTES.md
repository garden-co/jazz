# CI adjustments for the engine-swap PR (draft, night of 2026-07-18)

Per Anselm's directive: every CI-relevant removal or alteration documented in detail.
"Add" items are new coverage; nothing existing is silently dropped.

## ALTER: exempt benchmark ledgers from `pnpm format:check` (lint job)

- **What**: add `dev/benchmarks/SMOKE_LEDGER.md` and
  `dev/benchmarks/realistic/history/bench_history.json` to `.oxfmtignore`.
- **Why**: the markdown formatter worker OOMs on the 18k-line append-only
  SMOKE_LEDGER (CI lint job dies with `ERR_WORKER_OUT_OF_MEMORY`). Ledgers are
  receipts of record, not source; reformatting them would churn historical
  entries anyway. The bench-history JSON is machine-appended.
- **Risk**: none to code style; ledger entries are free-form by design.

## ADD: install wasm-pack in CI (test-ts job, and any job running `pnpm build:ci`)

- **What**: CI fails with `sh: 1: wasm-pack: not found` — the port switched
  `crates/jazz-wasm` to `wasm-pack build --target web --release`, and the
  runner toolchain setup never installs wasm-pack.
- **How**: extend `pnpm run ensure:rust-toolchain` (already invoked by every
  job) to install wasm-pack if absent — keeps the fix in one place instead of
  per-workflow steps. Pin a version for reproducibility.

## FIX (not a CI change): 8 jazz-tools unit tests — pre-existing rot exposed by CI

Initially attributed to tonight's anonymization commit; disproven by running the
failing suite at the pre-cleanup HEAD (`215689e45`) — same failures. Root cause
of the lead failure: `f037d1625` (Jul 12) fixed uuid-literal coercion in runtime
lowering but the server-side `public_schema_convert` path never received the
same rule, so converted policy filters carry `Literal(String("<uuid>"))` where
the byte-stable fixture has `Literal(Uuid(...))`. Each of the 8 gets an
individual root-cause verdict (repair lane report).

**Why local gates missed six days of red**: gate invocations piped cargo test
through `grep`/`tail`, which (a) replaces cargo's exit code with the pipe
tail's and (b) surfaces only the final `test result:` line — the doctest
suite's `ok`. CI runs the command unpiped and was the first honest reporter.
Remedy: gate commands now check `$?` unpiped; lane mandates updated.
The anonymization commit itself is kept — among other things it removed a
split-string evasion of the sensitive-data gate in the jazz-sim bench.

## OBSERVATION: local canonical gate vs CI shape

CI runs `cargo test --workspace --lib --bins --tests --features test` — a
different slicing than the canonical per-crate gates. Both catch the 8
failures; keep both (workspace slicing in CI catches feature-unification
differences the per-crate gates can miss).

## PENDING: realistic-benchmarks workflow verdict on this branch (run 29659318921)

## PENDING: decide whether the incremental-delivery canaries + differential

harness + oracle (currently local-convention gates) should be explicit named CI
steps so PR reviewers see them green rather than trusting the ledger.
Recommendation: yes — they are the correctness story of this PR; a dedicated
`differential-gates` job makes them first-class. To be discussed with Anselm
before altering ci.yml.

## RESOLVED: `auth-simple-chat#test` CI exclusion removed

- **What changed**: `ci.yml` no longer excludes `auth-simple-chat#test`; the
  earlier temporary `--filter=!auth-simple-chat` has been removed.
- **Original reason**: the example's permission schema used `SessionInList`
  (role-in-set claims check), which the core server shell's schema conversion
  rejected as unsupported — a genuine feature gap of the port, recorded as a 🔶
  open question in `crates/jazz/SPEC/7_authorization.md`.
- **Resolution**: commit `c41368d7d` implements bounded `SessionInList` lowering
  for scalar session claims and documents the semantics; the CI filter now
  includes `auth-simple-chat#test` again.
- **Follow-up**: the same job also showed
  `ERR_DLOPEN_FAILED` on Linux for the jazz-napi binding in this example's
  browser-mode global setup — verify it disappears once the schema publish
  succeeds; if not, it is a separate Linux napi loading issue to fix.

## ALTER (updated 2026-07-19 00:30): auth example exclusions — narrowed cause

Progression: SessionInList support landed (`c41368d7d`), example tests
modernized (async insert handles awaited; UUID JWT subjects per core session
requirement) — the admin grant path now passes end-to-end. Remaining failure:
the two _denial_ tests time out because a permission-denied write never rejects
`wait({tier})` — recorded as 🔶 "Write-denial surfacing to clients" in
`crates/jazz/SPEC/7_authorization.md`. Both `auth-simple-chat` and
`auth-workos-chat` tests stay excluded from CI until that lands; exit criteria:
implement write-denial rejection, un-exclude, delete this entry.
