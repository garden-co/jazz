# Harness preparation validation

These are **baseline mechanics receipts**, not alpha56 final-candidate acceptance.
The installed preview was source `1a5add74c7da1509049ffedaf7f663b2cbd36c6e`,
workflow run `35476001415`, package manifests `2.0.0-alpha.55`.

- Local CLI harness exits 0 with `check: PASS` for the documented local scope.
  The CLI is the binary from that preview's jazz-tools tarball; its digest is
  checked, but no independent CLI producer source manifest was available.
- Separate process phases pass seed, offline pending-store reopen, and server
  restart/reconnect. No NAPI in-process server substitutes for the CLI.
- A copied scenario with its delete call omitted exits 1: the deleted-row oracle
  sees the updated row instead of null.
- A copied scenario reopening a different persistent client path exits 1:
  the offline pending row is absent. Mutations were outside the checkout and
  never committed; historical acceptance artifacts were preserved.
- Appending a newline to the installed package.json (version unchanged) is
  rejected before server startup by byte comparison against the pinned tarball.
  The exact original file bytes were restored afterward.
- Final-preview mode without an authoritative CLI producer manifest is rejected
  before server startup.
- Node syntax checks and focused oxlint pass. No runtime source changed.

Scaffold executable preparation was syntax-checked only: the exact create-jazz
preview package was not in the locally available baseline artifact set. Cloud,
external JWT, denied writes, deterministic cancellation, wire-v2 and browser/RN
device gates remain explicitly NOT_RUN. The runner prints these boundaries.

Process friction: the prior baseline notes reported CLI artifacts unavailable,
but the installed preview package contained them under bin/native. Inspect the
packed package before scheduling another build. The repository uses oxfmt,
not prettier. Final CLI source provenance must come from the producer; do not
construct a manifest afterward merely to satisfy acceptance.

Independent review follow-up adds reproducible synthetic contract tests for
unchecked root/deep nested Jazz dependency overrides, exact scaffold locators
(including a wrong-revision URL spoofed with an expected-SHA fragment), and
SIGINT/SIGTERM process-group cleanup for the runner and npm descendants. Both
TERM-resistant process cases require escalation. The local historical package
baseline still passes with resolution verification and owned process groups.

Mutation sensitivity for these boundaries: disabling dependency-resolution checks
fails both nested-override cases; replacing exact-locator checks with tautologies
fails both SHA-fragment cases; retaining signal exit codes but bypassing signal
cleanup fails child-liveness assertions. These mutations run only in temporary
copies, and the test fixtures kill owned synthetic groups even on failure.
