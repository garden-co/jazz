# Adopter gate

Scripted adopter checks for a release candidate, run in parallel by `.github/workflows/adopter-gate.yml`, plus a post-publish smoke from the real registries (`.github/workflows/registry-smoke.yml`). They replace the manual gate #adopter-evals ran per release. A person or agent only looks at failures and at items marked REVIEW.

Every check prints `PASS`, `FAIL`, `REVIEW` or `SKIP` lines into `.results/<job>.txt`. `verdict.mjs` turns those into the step summary and `verdict.json`:

- `FAIL`: a regression.
- `REVIEW`: a judgement call, such as a size over its threshold or a known finding.
- `SKIP`: a check that couldn't run, such as t4 without secrets.

The verdict is FAIL, NEEDS_REVIEW or PASS.

## Checks

| Job                                         | Script                   | What it proves                                                                                                                                                                                                                                               | Needs                                                                                                                                                                                         |
| ------------------------------------------- | ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bed-t1`, `bed-t3`, `bed-t5`, `bed-t6`      | `bed.sh <app>`           | Apps on the previous release (`bed/`) move to the RC: lockfile only RC, `generate`, `generate --check`, unedited smoke. t1: migrations, enums, `renameFrom`. t3: rich permissions. t5: Expo web export. t6: modules, aliases, slices                         | RC tarballs                                                                                                                                                                                   |
| `fixtures`                                  | `fixtures.sh`            | t1 stores written by older releases (`bed/t1-migrations/fixture-data*`) open on the RC. Their identity (`store_id`, `app_id`) is in `jazz.sqlite` and equals the old files, which stay unchanged (jazz#5570)                                                 | RC tarballs, sqlite3                                                                                                                                                                          |
| `t10`                                       | `t10.sh`                 | `revokeUser` on one backend instance ends a user's sessions on another, both on one core (jazz#5634). Db lifecycle: a failed offline drain leaves the Db usable, and use after teardown fails explicitly (jazz#5592)                                         | RC tarballs, RC `jazz-tools-linux-x64`                                                                                                                                                        |
| `t9-ts`                                     | `t9.sh ts`               | A stock TS `createJazz` StagePlan backend on the RC. Backend clients: trusted, `forUser`, scoped, unlisted workload refused. End-user privacy                                                                                                                | RC tarballs                                                                                                                                                                                   |
| `t9-rust`, `t9-go`, `t9-kotlin`, `t9-swift` | `t9.sh <lang>`           | Each language client against that backend. B can't read or write A's private show. Scoped clients can't either                                                                                                                                               | Rust: cargo. Go: `libjazz_ffi-linux-x86_64` (embedded, local GOPROXY, `go get`, no cgo). Kotlin: the RC jar or a source build. Swift: the `swift` image plus `libjazz_ffi` through pkg-config |
| `sizes`                                     | `sizes.sh [prev]`        | Browser wasm and jazz-tools JS gzip size vs the previous release: over `AG_SIZE_REVIEW_PCT` (3%) is REVIEW, a missing or empty file FAIL. Cloudflare Worker bundles (`durable-object/`, `backend/`): FAIL over Cloudflare's 64 MiB uncompressed Worker limit | npm                                                                                                                                                                                           |
| `t4`                                        | `t4.sh` (`t4/README.md`) | Next.js + Better Auth on `vercel()` in the long-lived eval project: missing-region error, deploy, same core and data, smoke, wrong-region probe. No retries: a failed step fails (jazz#5553 shows as `invalid_link_challenge`). Serialised                   | `VERCEL_TOKEN` and the t4 vars. Without them: SKIP                                                                                                                                            |

Post-publish: `registry-smoke.sh <version> npm|create-jazz|crates|go|swift|kotlin` installs from npm, crates.io, the Go proxy, the jazz-swift mirror and Maven Central into clean projects, and runs the t9 checks. `AG_WAIT_MIN` polls for registry lag.

## Running locally

```sh
dev/adopter-gate/fetch-rc.sh v2.0.0-alpha.68-rc.1 [attach-language-artifacts run id]
# or: AG_EXPECT_SHA=<full commit sha> dev/adopter-gate/fetch-rc.sh --run <release-artifacts run> --npm-run <RC run>   # waits up to AG_NPM_WAIT_S (300 s) for rc-npm while the RC run is still going
dev/adopter-gate/bed.sh t1 && dev/adopter-gate/fixtures.sh && dev/adopter-gate/t9.sh go   # etc.
node dev/adopter-gate/verdict.mjs dev/adopter-gate/.results
```

Needs Node, pnpm 11, sqlite3 and `ss`, plus cargo, Go, JDK 17 with Gradle, or Swift 6 for the matching t9 job. Ports: bed 8601–8606, fixtures 8611, t10 8630–8632, t9 8780/8790/8791.

## Per release

1. **Before the cut**, an agent reads the release's upgrade notes and adds checks for what's new: a new job or lines in an existing one, as t10 was for .68. It dry-runs them on main and on the previous release. A check meant to catch a fix must FAIL on the previous release, or it proves nothing.
2. **On the RC**, `adopter-gate.yml` runs. The `rc_tag` or `artifacts_run_id` input says where the RC's files come from.
3. **After publish**, `rebase-bed.sh <version>` moves the bed to the new release, so the next RC is checked as an upgrade from it. Commit the result, including `bed/t1-migrations/fixture-data-<minor>`.

## Harness rules

- Provenance (#supply-chain's rule): files from another run (`artifacts_run_id`, `rc_npm_run_id`, `language_run_id`) are used only if that run built the commit being gated (`jazz_sha`, or the RC tag's commit) on a `push` or `workflow_dispatch` (never `pull_request*`) in this repository (not a fork), from a producer workflow: `release-artifacts.yml` or `release-candidate.yml` for `artifacts_run_id`, `release-candidate.yml` for `rc_npm_run_id`, and those or `attach-language-artifacts.yml` for `language_run_id`. `provenance.test.sh` tests it against a stubbed `gh` (CI runs it in the `inputs` job). `fetch-rc.sh` fails closed without it (`AG_EXPECT_SHA`) or on a mismatch, and the verdict job requires the artifacts run's `release-manifest` to name the same commit (the `provenance` row).
- No pass-on-retry: a check that fails once is FAIL (flakes are fixed, not retried). Only network fetches (downloads, installs) retry, and only fetches. t9 kotlin fetches its Gradle dependencies in a step of its own (retried on network errors such as Maven Central's 429, 30 s then 60 s backoff), then builds `--offline` once.

- Kill processes by port or PID, never with `pkill -f`. A pattern can match the shell running it.
- The TS client derives its app id from `serverUrl`. The t9 backend serves the id derived from `http://127.0.0.1:8780/jazz`.
- User tokens and `backendClients` workload tokens come from different issuers (8791 and 8790). A user token from a workload issuer never reaches `authenticate`, so that user is silently signed out.
- Synthetic data only. No keys or tokens in this directory.
