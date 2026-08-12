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
protected CI local disk (read/write) <-------------------------^
PR local disk/daemon (separate OS user or ephemeral container)
```

This lets a devbox reuse results produced after a trusted integration-branch
build without letting development machines or PR code poison the cache that CI
will execute. It deliberately does not attempt bidirectional sharing.

### Non-negotiable trust precondition

Untrusted PR code must not share an OS user, `HOME`, sccache daemon/socket, or
local cache directory with a protected remote-cache writer. A local cache hit
is executable input too: a PR sharing the protected runner's user can modify
the local sccache cache or talk to its daemon, then leave an artifact for the
next protected push to consume.

Today `test-ts` runs internal PRs on the persistent `jazz-ci` runner. Therefore
the remote-cache pilot stays **off** that job until one of these is explicit:

1. Jazz declares every internal PR author and the code they can run on that
   runner fully trusted; or
2. PR work runs hosted/ephemeral, or in a separate unprivileged OS user or
   container, with no access to the protected user's home, cache directory,
   config, credential files, or sccache socket.

The second option is the recommended long-term boundary. A workflow condition
that withholds remote credentials is necessary but not sufficient on a
persistent shared-user runner.

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
  client version `0.15.0` and an old cache directory. That is direct evidence
  that configuration belongs to the daemon, rather than the invoking shell.

## Pilot configuration

Pin the same recent sccache release on both endpoints (at least 0.16 for
read-only backend support), retain the existing local-disk cache, and give
each endpoint a short explicit shell helper rather than a global Cargo config.
Both helpers set `SCCACHE_BASEDIRS` to their respective checkout root so source
paths normalize across `/var/lib/github-actions/...` and a devbox checkout.

The protected helper has a lifecycle contract: stop a pre-existing daemon,
create a mode-`0700` cache/config/socket directory owned only by the protected
user, start sccache with the intended `SCCACHE_CONF`, `SCCACHE_DIR`, and a
private `SCCACHE_SERVER_UDS`, then emit the effective version/config identity
and stats. PR containers/users get different values for all four and must not
be able to traverse the protected directory. Configuration changes repeat that
stop/start sequence; the observed 0.17 CLI/0.15 daemon mismatch is the reason
this is a contract rather than advice.

The protected integration-branch push job may have credentials and use:

```sh
SCCACHE_MULTILEVEL_CHAIN=disk,s3
SCCACHE_MULTILEVEL_WRITE_ERROR_POLICY=l0
SCCACHE_DIR="$HOME/.cache/sccache"
SCCACHE_BUCKET=<private-bucket>
SCCACHE_ENDPOINT=https://<account>.r2.cloudflarestorage.com
SCCACHE_REGION=auto
SCCACHE_S3_USE_SSL=true
SCCACHE_S3_KEY_PREFIX=jazz/sccache/v1/linux-x86_64/rust-1.93.1
```

The devbox uses the same settings plus `SCCACHE_S3_RW_MODE=READ_ONLY`; its
object-store credential has only `GetObject` and prefix-scoped `ListBucket`.
The CI role may `GetObject`, `PutObject`, and the minimum multipart/list
operations under that one prefix. Use TLS, bucket-private access, lifecycle
expiry (start with 14 days), encryption at rest, audit logs, and a hard
storage-budget alert. The cache is both executable input and potentially
source/environment-sensitive data: compiled objects can carry source-derived
content, absolute paths, debug information, or build-script output. Do not put
credentials or customer material there, but also do not assume it contains no
sensitive data; never grant anonymous read/list access.

Credentials must be injected only into a protected `push` job after checkout,
never into `pull_request` work. External forks continue to use Blacksmith and
receive neither object-store credentials nor a writable remote cache. Internal
PRs follow the isolation/trust precondition above; only a successful push to
the protected integration branch warms the shared cache. This follows the same
cache-poisoning boundary GitHub documents for Actions caches.

## Telemetry and acceptance criteria

Add an `always()` CI step that emits `sccache --show-stats` to the job summary
and a compact receipt. The sccache part records its executable/server versions,
safe configuration identity (never credentials), local `SCCACHE_DIR`, local
cache size, Rust hit/miss counts, cache read/write errors, and elapsed time.
Those are compiler/local-disk measurements; neither the displayed cache size
nor sccache's stats establish R2 requests, bytes transferred, egress, or cost.

The corresponding storage receipt comes from the bucket provider's per-prefix
metrics and billing/audit exports: stored bytes, object reads/writes/lists,
download bytes, egress bytes/cost, request cost, and denied/error operations.
Link its timestamped dashboard/export identifier in the CI receipt rather than
copying a hand-maintained number. Do not report cache "wins" without both a
cold baseline and two warmed repetitions on the same commit.

Run this experiment only for the Linux/x64 Rust/artifact build path using the
pinned Rust 1.93.1 toolchain first. Other toolchains and macOS/Windows release
builds get separate prefixes after their own measurement; Turbo remains an
independent cache. The pilot passes only if all of the following hold:

1. A devbox build of an unchanged integration commit shows remote Rust hits,
   with no cache read/write errors and no artifact-provenance failure.
2. The median devbox clean-ish `cargo check --workspace --all-targets` or
   `pnpm build:test-artifacts` improves by at least 20%, while a normal
   edit/rebuild loop does not regress (local incremental remains the default).
3. The `jazz-ci` runner's median artifact-build phase does not regress by more
   than 5%; a remote outage falls back to compilation rather than failing a
   correctness job.
4. A fork PR has no storage credential in its environment and cannot write the
   prefix; an isolated PR job cannot read or connect to the protected user's
   local cache/socket (verify both with deliberately denied probes outside the
   normal workflow).
5. Before enabling writes, Infrastructure records a numeric monthly pilot
   budget in the provider receipt. Stop remote writes immediately if the
   provider's month-to-date cost plus its documented remaining-month forecast
   exceeds that budget, or if per-prefix request/transfer/egress metrics are
   unavailable. Storage, requests, and egress remain below budget for two
   weeks. If remote reads are slower than compilation for common misses, remove
   the helper and retain the runner-local cache.

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
