# Shared compiler-cache pilot

Status: proposed; no remote cache is configured yet.

## Decision

Do **not** share `target/`, Cargo registry directories, pnpm stores, or a
network filesystem between a developer machine and CI. Those trees contain
mutable build state, have different locking/lifetime assumptions, and make a
corrupt or malicious cache much harder to isolate.

The one useful cross-machine unit is an `sccache` compilation result. Run a
small, opt-in pilot with S3-compatible object storage (R2 is suitable if
Infrastructure already operates it), layered behind each machine's local disk
cache:

```
developer local disk (read/write) <- read-only -> object store <- read/write - protected CI push
CI-runner local disk (read/write)  <----------------------------^
all pull-request jobs local disk only
```

This lets a devbox reuse results produced after a trusted integration-branch
build without letting development machines or PR code poison the cache that CI
will execute. It deliberately does not attempt bidirectional sharing.

## What we verified on 2026-08-12

- `.github/actions/setup-build` enables `RUSTC_WRAPPER=sccache`. Blacksmith
  jobs use its sticky disk; the persistent `jazz-ci` runner instead uses
  `~/.cache/sccache`.
- The dedicated runner is one Linux/x64 host (`latitude-ci-1`) and its
  self-hosted test job completed setup in 35 seconds, built correctness
  artifacts in 55 seconds, and spent 39 seconds in the concurrent TS/browser
  suites on CI run `31569587196`.
- The devbox's `sccache --show-stats` had 822 Rust hits and 264 Rust misses
  (75.69% Rust hit rate; 88.63% overall). That proves local reuse is already
  meaningful, but it says nothing about cross-host reuse.
- The executable was `sccache 0.17.0`, while its running daemon reported
  client version `0.15.0` and an old cache directory. Configuration is read
  by the daemon: every pilot setup/change must run `sccache --stop-server`
  before collecting baseline or enabling the new config.

## Pilot configuration

Pin the same recent sccache release on both endpoints (at least 0.16 for
read-only backend support), retain the existing local-disk cache, and give
each endpoint a short explicit shell helper rather than a global Cargo config.
Both helpers set `SCCACHE_BASEDIRS` to their respective checkout root so source
paths normalize across `/var/lib/github-actions/...` and a devbox checkout.

The protected integration-branch push job may have credentials and use:

```sh
SCCACHE_MULTILEVEL_CHAIN=disk,s3
SCCACHE_MULTILEVEL_WRITE_ERROR_POLICY=l0
SCCACHE_DIR="$HOME/.cache/sccache"
SCCACHE_BUCKET=<private-bucket>
SCCACHE_ENDPOINT=https://<account>.r2.cloudflarestorage.com
SCCACHE_REGION=auto
SCCACHE_S3_KEY_PREFIX=jazz/sccache/v1/linux-x86_64
```

The devbox uses the same settings plus `SCCACHE_S3_RW_MODE=READ_ONLY`; its
object-store credential has only `GetObject` and prefix-scoped `ListBucket`.
The CI role may `GetObject`, `PutObject`, and the minimum multipart/list
operations under that one prefix. Use TLS, bucket-private access, lifecycle
expiry (start with 14 days), and a hard storage-budget alert. The cache stores
compiled object files, so it contains no tokens or customer material and must
still be treated as executable, untrusted input.

Credentials must be injected only into a protected `push` job after checkout,
never into `pull_request` work. External forks continue to use Blacksmith and
receive neither object-store credentials nor a writable remote cache. Internal
PRs follow the same rule: only a successful push to the protected integration
branch warms the shared cache. This follows the same cache-poisoning boundary
GitHub documents for Actions caches.

## Telemetry and acceptance criteria

Add an `always()` CI step that emits `sccache --show-stats` to the job summary
and a compact receipt (backend, Rust hit/miss count, read/write errors, bytes,
and elapsed time). Do not report cache "wins" without both a cold baseline and
two warmed repetitions on the same commit.

Run this experiment only for the Linux Rust/artifact build path first; NAPI
macOS/Windows release builds and Turbo remain independent caches. The pilot
passes only if all of the following hold:

1. A devbox build of an unchanged integration commit shows remote Rust hits,
   with no cache read/write errors and no artifact-provenance failure.
2. The median devbox clean-ish `cargo check --workspace --all-targets` or
   `pnpm build:test-artifacts` improves by at least 20%, while a normal
   edit/rebuild loop does not regress (local incremental remains the default).
3. The `jazz-ci` runner's median artifact-build phase does not regress by more
   than 5%; a remote outage falls back to compilation rather than failing a
   correctness job.
4. A fork PR has no storage credential in its environment and cannot write the
   prefix (verify with a deliberately denied upload outside the workflow).
5. Storage, requests, and egress stay under the agreed monthly alert/budget for
   two weeks. If remote reads are slower than compilation for common misses,
   remove the helper and retain the runner-local cache.

## Why not the other backends?

Redis/memcached are appropriate only when a low-latency managed instance
already exists; otherwise they add an always-on service and eviction tuning for
little benefit at Jazz's current one-runner scale. WebDAV adds an
authentication/availability surface with no advantage over S3-compatible
object storage. The GitHub Actions cache backend cannot serve a devbox and is
subject to GitHub cache scope/retention, so it is not a cross-host solution.

References: [sccache configuration](https://github.com/mozilla/sccache/blob/main/docs/Configuration.md),
[sccache releases](https://github.com/mozilla/sccache/releases), and [GitHub
Actions cache security](https://docs.github.com/en/actions/concepts/workflows-and-actions/dependency-caching).
