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
cache. The point is deliberately bidirectional for the _trusted engineering_
path: a compilation a developer has already done can satisfy a later trusted
CI build, and an integration build can satisfy a later developer build.

```
trusted devbox L0 (read/write) <----> trusted-engineering R2 bucket <----> trusted CI L0 (read/write)
                                      (one shared sccache namespace)

release checkout + target + L0 + daemon + release-only R2 bucket  (separate; never reads shared state)
untrusted PR checkout + target + L0 + daemon, no R2 credentials   (separate; no connection to trusted state)
```

This is a trust decision, not a sandbox: anyone issued a trusted-devbox
credential can put executable compiler-cache entries that trusted integration
CI consumes. Initially issue those credentials only to explicitly trusted
engineers and to protected CI; do not make the bucket reachable from arbitrary
developer machines. If that is too broad a trust domain, use read-only devboxes
instead and accept that they cannot warm CI. There is no safe magic setting
that gives an untrusted devbox write access while allowing CI to consume its
objects.

`sccache` has one remote backend/key prefix per daemon. Therefore this pilot
uses one shared trusted namespace, rather than the impossible arrangement of
one daemon reading a developer prefix while writing a CI prefix. A release
build is not a promotion mechanism: it starts a distinct daemon with its own
empty L0 cache, distinct checkout/`CARGO_TARGET_DIR`, and a distinct release
bucket. It may warm/reuse that release bucket only on protected release work.
It must never read the trusted-engineering bucket or any shared Cargo target
tree, so neither multilevel local backfill nor a materialized Cargo artifact
can carry a developer-produced object into a release build. A future signed,
verified object-promotion service could be designed separately; it is
explicitly out of scope for this pilot.

### Non-negotiable trust precondition

Untrusted PR code must not share an OS user, `HOME`, checkout, `CARGO_TARGET_DIR`,
sccache daemon/socket, or local cache directory with a protected remote-cache
writer. A local cache hit or materialized Cargo artifact is executable input:
a PR sharing the protected runner's user can modify the local sccache cache,
talk to its daemon, or alter `target/`, then leave an artifact for the next
protected push to consume.

Today both `test-ts` **and `build-integration`** run internal PRs on the
persistent `jazz-ci` runner. Therefore the remote-cache pilot stays **off**
both jobs until one of these is explicit:

1. Jazz declares every internal PR author and the code they can run on that
   runner fully trusted; or
2. PR work runs hosted/ephemeral, or in a separate unprivileged OS user or
   container, with no access to the protected user's home, cache directory,
   config, credential files, or sccache socket.

The second option is the recommended long-term boundary. A workflow condition
that withholds remote credentials is necessary but not sufficient on a
persistent shared-user runner. The rollout inventory must include every future
job routed to `jazz-ci`, not merely `test-ts`.

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

Each helper has a lifecycle contract: stop its pre-existing daemon, create a
mode-`0700` cache/config/socket directory owned only by that trust class, start
sccache with the intended `SCCACHE_CONF`, `SCCACHE_DIR`, and private
`SCCACHE_SERVER_UDS`, `CARGO_TARGET_DIR`, and checkout, then emit the effective
version/config identity and stats. The release helper has different values for
all five _and an empty, release-only L0_; PR containers/users have another set
and must not be able to traverse either protected directory or checkout/target
tree. Configuration changes repeat that
stop/start sequence; the observed 0.17 CLI/0.15 daemon mismatch is the reason
this is a contract rather than advice.

Trusted devboxes and protected integration-branch push jobs use the same
backend and prefix (with different credentials) and may use:

```sh
SCCACHE_MULTILEVEL_CHAIN=disk,s3
SCCACHE_MULTILEVEL_WRITE_ERROR_POLICY=l0
SCCACHE_DIR=<trust-class-private-l0>
SCCACHE_BUCKET=jazz-sccache-trusted-engineering
SCCACHE_ENDPOINT=https://<account>.r2.cloudflarestorage.com
SCCACHE_REGION=auto
SCCACHE_S3_USE_SSL=true
SCCACHE_S3_KEY_PREFIX=jazz/sccache/v1/linux-x86_64/rust-1.93.1
```

The devbox does **not** set `SCCACHE_S3_RW_MODE=READ_ONLY`: it needs
read/write access to make its completed compilations reusable by trusted CI.
Use a separate long-lived, bucket-scoped R2 API token for each devbox and
protected CI identity, or a short-lived derived S3 credential where the
credential broker restricts that credential's allowed prefix/actions. The
long-lived R2 token is bucket-scoped, not prefix-scoped; temporary S3
credentials may add a prefix restriction, but the design must not depend on
that feature being available to every issuer/client. Dedicate the entire
`jazz-sccache-trusted-engineering` bucket to this trust class; the prefix above
is organisation and cache-versioning, with the dedicated bucket remaining the
authorization boundary and defense in depth. Give the release helper a
different, release-only bucket and token. Do not use an account-wide admin
token in sccache.

Use TLS, bucket-private access, Standard storage (not Infrequent Access), a
14-day lifecycle expiry to start, encryption at rest, and a hard storage-budget
alert. Cloudflare audit logs cover configuration/control-plane events, not
individual S3 `GetObject`/`PutObject` data-plane cache accesses; use the
bucket-operation metrics and timestamped receipts below for access telemetry.
The cache is both executable input and potentially
source/environment-sensitive data: compiled objects can carry source-derived
content, absolute paths, debug information, or build-script output. Do not put
credentials or customer material there, but also do not assume it contains no
sensitive data; never grant anonymous read/list access.

Credentials must be injected only into a protected `push` job after checkout,
never into `pull_request` work. External forks continue to use Blacksmith and
receive neither object-store credentials nor a writable remote cache. Internal
PRs follow the isolation/trust precondition above; until then they receive no
remote credential and run with a PR-only L0/daemon. Protected integration
pushes and explicitly trusted devboxes may warm the shared cache. This follows
the same cache-poisoning boundary GitHub documents for Actions caches.

## Telemetry and acceptance criteria

Add an `always()` CI step that emits `sccache --show-stats` to the job summary
and writes a compact JSON receipt. It records: receipt schema version; UTC
start/end; repository, workflow run/job IDs, event, ref, and commit SHA;
runner trust class; `sccache` client and server versions; toolchain/target;
backend kind; bucket alias and a non-secret cache-prefix/config digest; L0
cache/daemon identity; whether L0 was deliberately empty at start; Rust
hit/miss counts; cache read/write errors; and phase elapsed time. Do not put
credentials, an account ID, absolute home path, or source paths in the receipt.

`sccache` stats alone cannot attribute an individual hit to R2. The remote-hit
experiment therefore starts with an intentionally empty L0 and pairs its
receipt with a provider export for the same timestamp window and dedicated
pilot bucket. Record the export/dashboard identifier, stored bytes/object
count, Class A (write/list/multipart) requests, Class B (read/head) requests,
retrieval bytes/fees if a non-Standard class was ever used, denied/error
operations, and calculated monthly cost. R2 Standard currently bills storage,
Class A, and Class B operations; direct R2 egress is free, so report egress
bytes separately but do not invent an egress charge. The dedicated bucket is
what makes this bucket-level telemetry attributable; R2 should not be assumed
to provide sufficient prefix-level billing. Do not report cache "wins" without
a cold baseline and two warmed repetitions on the same commit.

Run this experiment only for the Linux/x64 Rust/artifact build path using the
pinned Rust 1.93.1 toolchain first. Other toolchains and macOS/Windows release
builds get separate prefixes after their own measurement; Turbo remains an
independent cache. The pilot passes only if all of the following hold:

1. A trusted devbox build of an unchanged integration commit and a protected
   CI build after a devbox warm both show cache hits with an intentionally empty
   L0, no cache read/write errors, and matching provenance receipts. This is
   evidence of cross-host reuse, not a claim that sccache can label each hit
   "remote".
2. The median devbox clean-ish `cargo check --workspace --all-targets` or
   `pnpm build:test-artifacts` improves by at least 20%, while a normal
   edit/rebuild loop does not regress (local incremental remains the default).
3. The `jazz-ci` runner's median artifact-build phase does not regress by more
   than 5%; a remote outage falls back to compilation rather than failing a
   correctness job.
4. A fork PR has no storage credential in its environment and cannot write the
   bucket; both `test-ts` and `build-integration` internal PR executions are
   isolated before either gets a remote credential; an isolated PR job cannot
   read or connect to either protected user's local cache/socket, checkout, or
   `CARGO_TARGET_DIR` (verify all four with deliberately denied probes outside
   the normal workflow).
5. Before enabling writes, Infrastructure records a numeric monthly pilot
   budget in the provider receipt. Stop remote writes immediately if the
   provider's month-to-date cost plus its documented remaining-month forecast
   exceeds that budget, or if dedicated-bucket storage/request telemetry is
   unavailable. Storage and Class A/B request costs remain below budget for two
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
