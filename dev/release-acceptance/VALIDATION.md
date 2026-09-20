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
- The initial manifest-based final gate was replaced by authenticated GitHub
  run/artifact metadata, original ZIP digest and executable/packed-byte equality.
  Missing final artifact evidence fails before server startup. Synthetic API
  fixtures exercise the existing publisher adapter; no live final artifact has
  been accepted by this preparation lane.
- Node syntax checks and focused oxlint pass. No runtime source changed.

Scaffold executable preparation was syntax-checked only: the exact create-jazz
preview package was not in the locally available baseline artifact set. Cloud,
external JWT, denied writes, deterministic cancellation, wire-v2 and browser/RN
device gates remain explicitly NOT_RUN. The runner prints these boundaries.

Process friction: the prior baseline notes reported CLI artifacts unavailable,
but the installed preview package contained them under bin/native. Inspect the
packed package before scheduling another build. The repository uses oxfmt,
not prettier. The publisher has no CLI producer manifest. Preserve the original GitHub
artifact ZIP for the API digest adapter; do not invent a post-download manifest
or recompress extracted files and claim the original artifact digest.

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

The API adapter's synthetic fixtures cover all three approved workflow callers,
failed/incomplete/fork/wrong-head runs, incorrect artifact identity/platform,
missing/wrong ZIP digest, duplicate/extra/path-traversal entries, and executable
versus packed-byte mismatches. Removing the run-source check, ZIP-digest check,
or packed-byte comparison in separate temporary copies each makes its focused
negative test fail with `Missing expected rejection`. These prove test sensitivity
without downloading or claiming a live final candidate artifact.
