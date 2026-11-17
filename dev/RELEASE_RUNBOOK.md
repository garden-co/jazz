# Jazz release runbook

Owner: release coordinator. Keep a release-specific checklist with exact commit,
workflow run, package, deployment and acceptance links. Never put credentials in
this document or public receipts.

## 1. Freeze the candidate

- Triage the release milestone. Identify blockers, deferred issues and pending
  approvals; verify each included PR has independent review and passing gates.
- Merge approved changes. Cut the reviewed candidate from `main` into the protected, long-lived `release`
  branch. Confirm Changesets refreshes `changeset-release/release` targeting
  `release`; ordinary development continues on `main`.
- Audit changesets against the previous release: include important user-visible
  fixes, migrations and compatibility changes, with detailed commit/PR references.
  Keep announcement prose separate from the detailed package changelog.
- Verify the intended version, alpha prerelease state, fixed package group and
  exact internal dependency versions. Do not infer the next version from a PR title.
- Explicitly state wire/storage compatibility, required client/server coordination,
  and treatment of existing rows, pending local writes and caches.
- Classify the release path before scheduling it:
  - a normal release is wire- and storage-compatible and can use a rolling Cloud
    deployment;
  - an intentional alpha wire break may use the coordinated sequence below only
    when old clients fail loudly and safely and users are explicitly told to update;
  - a storage break or a transition that could create corrupt or unrecoverable
    tenants requires a separate, release-specific migration plan.
- Record the candidate SHA. New code invalidates relevant candidate receipts.

## 2. Build and test before the release window

- Run the release PR preview workflow (`preview-jazz-tools-alpha-release.yml`).
  It invokes the actual publisher in dry-run mode. Inspect failed jobs immediately.
- Verify all platform builds and packed-package consumers, including NAPI platforms,
  optimized WASM, CLI binaries, RN Android and iOS payloads and wrapper, source/ABI
  receipts, exact dependency versions and tarball size budgets.
- Ensure RN payload packages are part of the fixed release group and published
  before the wrapper. Verify the exact tarballs that will be uploaded.
- Check that `npm create jazz` from the preview prints the live agent brief
  (https://jazz.tools/quickstart.md). Smoke-test published preview packages as an
  adopter: CLI validation/deployment, local CLI server, authentication, write/read/delete of a
  specific row while another remains, reconnect and persistent reopen.
- Include browser/worker and Android/iOS acceptance appropriate to changed code.
  For storage changes, run the published-version compatibility fixtures. Reuse a
  previous manual device receipt only when the relevant code/artifacts are unchanged.
- Give infra the exact publicly fetchable CLI package/artifact and source SHA.
  Have infra prebuild the server image and report its digest. Do not assume a
  configuration revision proves an image version.
- Have infra install that exact image as the sole available `preview` entry in
  the Jazz Cloud version catalog. This does not change the stable default or any
  existing tenant. Preview creation must be explicit: use the dashboard's hidden
  `?preview=true` flow or send `{"preview": true}` to the unclaimed-app generation
  API. Tenant Manager resolves that request against its own catalog and persists
  the exact preview version, image and `preview` lifecycle on the new app.
- Test fresh Cloud preview tenants end to end with the matching image. Confirm the
  created app reports the expected version and image digest; publish schema and
  permissions; configure authentication if relevant; check authenticated
  CRUD/reconnect/persistent reopen, sleep and wake; and exercise both local and
  cross-region paths. Preview tenants remain permanently pinned and are not
  migrated to stable, so create a new disposable tenant for every new candidate
  and delete it after acceptance. Keep tenant credentials in private files.
- Build docs and Inspector previews. Record exact successful deployment IDs and
  verify routes/assets before planning promotion. Coordinate dashboard separately
  with infra.

## 3. Prepare release notes and rollback

- Create a **draft GitHub Release**, titled `Jazz <version>`, using tag
  `v<version>`. The body contains a concise prose overview and upgrade instructions,
  followed by the detailed per-commit package changelog from the release PR.
- Alpha versions are normal GitHub Releases: `prerelease=false`. Draft status is
  separate; keep the release draft until the publication checks below pass.
- Refresh the draft after final changesets. Do not create a source tag against an
  unfinished candidate. The publisher owns the final immutable source tag.
- Prepare announcement text, infra rollout changes and rollback or roll-forward
  actions in advance. Creation maintenance is not part of a normal release.
- Record what can be rolled back (deployment/default image/docs/dist-tags) and what
  cannot (published immutable npm versions). Coordinate wire compatibility before
  any rollback; never overwrite a published version or silently retarget its tag.

## 4. Coordinated publication

- Obtain the release go-ahead. Confirm infra, package publisher and docs owner are
  ready. Record whether the release is a compatible rolling release or an
  intentional alpha wire break. Do not assume changing the Cloud catalog upgrades
  existing tenants.
- For a normal compatible release, require mixed-version acceptance: the current
  client against the candidate server, the candidate client against the current
  server, and the candidate pair. The candidate must tolerate the mixed server
  versions present during a rolling deployment.
- For an intentional alpha wire break, verify that an old client fails clearly
  without corrupting data, accepting partial writes or entering a destructive
  retry loop. Publish the client and docs immediately before Cloud activation so
  an update is available before existing tenants move. Clearly tell alpha users
  that they must update their clients.
- Merge the approved release PR only when ready: the publisher can trigger from
  that merge. Avoid dispatching a duplicate publish run.
- If manual dispatch is needed, use `publish-jazz-tools-alpha.yml` on the exact
  `release` branch with `mode=publish`, `expected-sha=<exact merged version PR SHA>`
  and `version=<expected source version>`. The workflow rejects a moved branch,
  a different version, any other branch, or a commit that is not the merge of
  `changeset-release/release` into `release`. Dry-runs may use feature branches.
  Check workflow inputs before running it. Never publish from a mutable working tree.
- Watch individual jobs and inspect failure logs. Warm caches can shorten the run,
  but do not replace validation or guarantee identical artifacts.
- On partial npm failure, inventory versions already published. Fix credentials or
  packaging, then retry through the publisher's existing-version handling. Never
  republish different bytes under an existing version.
- Verify every expected npm package/version, native payload, dependency and dist-tag
  using registry reads and installation. Since alpha.60 the publisher moves both
  `alpha` and `latest` to the new version for every v2 package, so a plain
  `npm install jazz-tools` gets v2 and Classic is `jazz-tools@0`. To roll `latest`
  back, run `npm dist-tag add <pkg>@<previous> latest` for each package. Confirm the source tag matches the exact
  released source. Build artifacts alone are not proof of npm publication.
- Promote the verified docs deployment. The publisher stages and promotes the
  Inspector itself; if its Inspector jobs fail, re-run the failed jobs on that
  publisher run first, then follow the manual recovery in
  `packages/inspector/README.md` ("Staging a release on Vercel").
- Treat Cloud activation as two explicit operations: selecting the new catalog
  default for newly created apps, and upgrading existing eligible tenants through
  a controlled fleet rollout. Have infra confirm the exact version and image digest
  in the status API and both regions. Never include legacy or preview tenants in a
  stable fleet rollout.
- The release go-ahead names each activation it covers: the npm publish, the
  catalog default for new apps, the fleet rollout of existing tenants and the
  GitHub Release. If the owner defers one, record that in the release checklist
  with who will do it next.
- Right after npm verification, the release coordinator opens the catalog PR that
  makes the release the default for new apps and asks infra to start the fleet
  rollout. The coordinator owns both until infra reports the tenant counts per
  version; the release is not finished while eligible stable tenants remain on an
  older version without a recorded reason.
- With the fleet rollout, hand infra the smoke probe update: bump `jazz-tools`
  and `jazz-napi` in `kubernetes/tests/v2-sync` (garden-co/infra) to the new
  release, port any API changes the release requires, and redeploy the probe.
  Probes left on an old client stop testing what adopters run.
- Create a new ordinary tenant with the real npm release; validate/deploy schema
  and permissions and run authenticated write/read/update/delete plus
  reconnect/reopen. Delete test tenants. Check existing sync probes and rollout
  health between fleet cohorts with infra.
- Use app-creation maintenance only as an exceptional, explicitly reviewed safety
  measure when an intermediate state could create corrupt, incorrectly configured
  or unrecoverable tenants. A brief, intentional alpha wire mismatch in which old
  clients fail loudly is not by itself a reason for creation maintenance.

## 5. Publish the GitHub Release and announcement

- Verify package publication and post-release acceptance are complete, with no
  unresolved compatibility mismatch. Confirm the draft targets the publisher's
  actual `v<version>` tag, not an earlier preview SHA.
- Update the draft with final prose and the final detailed Changesets changelog.
  Publish it with `prerelease=false`; do not use generated commit lists as a
  substitute for the audited changelog. Set latest deliberately for the new normal
  release rather than leaving the benchmark-fixtures release as latest.
- Merge `release` back into `main` with a merge commit (a PR from a branch cut
  from `main`, never squashed), as described under "Release branch lifecycle"
  below. Without it `main` keeps stale package versions, changelogs and
  `.changeset/pre.json`.
- Publish the announcement only with explicit authorization, linking to release
  notes, upgrade instructions and relevant measured benchmarks.
- Close the completed milestone after moving unresolved work to its agreed target.
  Record final package/tag/image/deployment/test links, known limitations and any
  actionable process failures as issues. Monitor early adopter reports.

## Cloud rollback semantics

- A catalog-default rollback affects only tenants created afterward; it does not
  roll back existing tenants.
- Record whether the candidate is binary-downgrade-safe, not merely storage-
  compatible. If the previous server cannot safely read data written by the new
  server, recover by rolling forward to a fixed image.
- Roll existing stable tenants only through the same bounded rollout mechanism used
  for upgrades. Do not change legacy or preview tenants.
- During an intentional alpha wire break, elevated failures from old clients are
  expected until adopters update, but corruption signals, partial writes and
  failures from the candidate client are rollback or roll-forward triggers.

## Current automation boundaries

`changesets-release-pr.yml` creates versions/changelogs and the release PR; it does
not publish. `publish-jazz-tools-alpha.yml` publishes packages and creates the
source tag, then dispatches `publish-jazz-crates-alpha.yml`, which publishes the Rust crates (see "Rust crates
on crates.io"). GitHub Release creation/publication is currently manual. Do not assume
a green Changesets action or an existing tag means the packages or GitHub Release
have been published.

## Rust crates on crates.io

Jazz's Rust crates are versioned in lockstep with the jazz-tools npm package.
`pnpm release:version` runs `dev/scripts/sync-crate-versions.mjs` after
`changeset version`, so the Version Packages PR bumps them together.
`dev/scripts/check-lockstep-versions.mjs` checks npm, the crates and the Go
module's `packages/jazz-go/version.go` (once it exists) in CI's lint partition.
Run it on the Version Packages PR head before merging.

The published crates are exactly the dependency closure of `jazz-app-support`,
`jazz-tools`, `jazz-codegen` and `jazz-ffi` (`dev/scripts/rust-crates.mjs`).
`node dev/scripts/publish-crates.mjs --list` prints them in publish order:

```
jazz-alpha59-alpha60-dump  jazz-ruzstd  jazz-compression  jazz-index  jazz-text
jazz-types  jazz-model  jazz-protocol  jazz-db  jazz-core  jazz-native-transport
jazz-app-support  jazz-codegen  jazz-ffi  jazz-otel  jazz-server  jazz-tools
jazz-wasm-tracing
```

`jazz-core` is the engine (`crates/jazz`, the package formerly named `jazz`;
`jazz` on crates.io is an unrelated project), and its library is still named `jazz`.
`jazz-tools` is the CLI (`crates/jazz-cli`, formerly package `jazz-cli`), so `cargo install jazz-tools`
installs the `jazz-tools` binary. `jazz-ruzstd` is the patched ruzstd fork in
`vendor/ruzstd` and keeps its upstream version (0.8.3). Every other workspace
crate has `publish = false`, except `jazz-wasm-tracing` (the wasm-tracing
fork jazz-wasm uses), which is published last, at its own version. Published crates resolve
upstream `sqlite-wasm-rs`: the root `[patch.crates-io]` only applies inside
this workspace (and only to wasm builds).

`publish-jazz-crates-alpha.yml` is dispatched (`mode: publish`,
`tag: v<version>`) by the `dispatch-crates` job of "Publish jazz-tools alpha",
once its npm publish succeeds. crates.io trusted publishing refuses
`workflow_run` triggers, so the workflow is only ever dispatched; it waits for
the `publish` environment's approval like the npm job. It publishes only after npm has jazz-tools at that version and
`v<version>` points at the release commit. It verifies every package
(`cargo package`). Then, before publishing anything, it checks that every
new crate's bootstrap token is set and that the OIDC exchange works, so a
missing credential stops the run before it is half published. (The exchange
succeeds if any existing crate trusts this workflow, so still do step 1
below for jazz-tools.) It then publishes each crate in order, fetching a
fresh OIDC token right before the first crate that needs one. A version already on
crates.io is skipped, so a failed run can be re-run. For each crate it asks
crates.io whether the crate exists:

- **existing crates** are published with trusted publishing (OIDC);
- **crates that don't exist yet** are published with the `publish`
  environment's `CRATES_IO_BOOTSTRAP_TOKEN` secret.

To check a candidate without publishing, dispatch `publish-jazz-crates-alpha.yml` on the
rc branch with `mode: dry-run`. It runs `cargo publish --dry-run` for every
crate in dependency order, needs no credential, and runs outside the `publish`
environment. A real manual run is `mode: publish` with `tag: v<version>`.

### One-time bootstrap (first publish of new crates)

A crate can only get a trusted publisher once it exists on crates.io, so new
crates are published with a short-lived token the first time:

1. **Before the run:** on crates.io, check that `jazz-tools` (it already
   exists) has a GitHub trusted publisher (Settings → Trusted Publishing) for
   repository `garden-co/jazz`, workflow `publish-jazz-crates-alpha.yml`,
   environment `publish`. One may exist since alpha.62; add it if not.
2. **Create the token:** crates.io → Account Settings → API Tokens → New
   Token. Use the scope `publish-new`, an expiry of 1 day, and crate scopes
   for exactly the new crates. Store it as the `CRATES_IO_BOOTSTRAP_TOKEN`
   secret of the `publish` environment (Settings → Environments → publish).
   Don't paste it anywhere else. The new crates are: jazz-alpha59-alpha60-dump,
   jazz-ruzstd, jazz-compression, jazz-text, jazz-types, jazz-model,
   jazz-protocol, jazz-db, jazz-core, jazz-native-transport, jazz-app-support,
   jazz-codegen, jazz-ffi, jazz-otel, jazz-server.
3. **Publish:** "Publish jazz-tools alpha" dispatches `publish-jazz-crates-alpha.yml`
   after the npm publish; to run it by hand, dispatch it with `mode: publish`, `tag: v<version>`. The summary at the
   end of its log shows which auth each crate used.
4. **Afterwards, for every new crate:** add the same trusted publisher
   (`garden-co/jazz`, `publish-jazz-crates-alpha.yml`, environment `publish`). Then
   delete the `CRATES_IO_BOOTSTRAP_TOKEN` secret and revoke the token on
   crates.io. Also add the `garden-co` owners team to each crate (`cargo
owner --add github:garden-co:<team> <crate>`), so they aren't owned by one
   person only.

**`jazz-index`** (the specialised index kinds, SPEC 20) joins the published
crates with the release that ships specialised indexes: `jazz-db` depends on
it, so it publishes before `jazz-text` and everything after. It is new on
crates.io then, so that release needs the bootstrap above for it alone:
before the run, a `CRATES_IO_BOOTSTRAP_TOKEN` scoped `publish-new` to the
crate `jazz-index` (1-day expiry); afterwards, its trusted publisher
(`garden-co/jazz`, `publish-jazz-crates-alpha.yml`, environment `publish`),
the `garden-co` owners team, and the token deleted and revoked. The run's
preflight stops before publishing anything if the token is missing. 5. From then on every crate publishes through OIDC. A crate added to the
closure later needs steps 2 to 4 for that crate alone. Until then, the
workflow fails for it with a message naming the missing token.

## Kotlin on Maven Central

`jazz-kotlin` (the JVM jar, with every platform's `libjazz_ffi` inside) and
`jazz-kotlin-android` (the AAR) publish to Maven Central as
`tools.jazz:jazz-kotlin` and `tools.jazz:jazz-kotlin-android`, at jazz-tools'
version (the Gradle builds read it from `packages/jazz-tools/package.json`).
`.github/workflows/publish-jazz-kotlin.yml` runs `dev/scripts/publish-kotlin.sh`.

1. **After "Attach language artifacts" and the GitHub Release are published,**
   dispatch "Publish jazz-kotlin" with `mode=dry-run`, `tag=v<version>`. It
   builds `refs/tags/v<version>`'s commit (never a branch of that name) with
   the pinned JDK and Gradle 8.14.3: the jar from the release's
   `libjazz_ffi-<platform>.tar.gz` (checked against `SHA256SUMS-languages`,
   their build-provenance attestations, and `libjazz_ffi-source.txt` naming
   that commit) and the AAR. It signs both with a throwaway key and checks
   every file, POM and signature. Its artifact is what Central would get.
2. **Publish:** dispatch it with `mode=publish`, `tag=v<version>`. The `publish`
   environment's reviewer approves; it reads `MAVEN_CENTRAL_USERNAME` and
   `MAVEN_CENTRAL_PASSWORD` (a Central Portal user token) and
   `MAVEN_SIGNING_KEY` / `MAVEN_SIGNING_KEY_PASSWORD`. One Gradle run builds,
   signs and uploads; the same checks then run on those files. The two
   deployments wait, validated, on central.sonatype.com → Publish →
   Deployments: once the run is green, check them and press Publish.
3. A published version can't be replaced or deleted on Central. A failed run
   publishes nothing; drop any deployment it left in the portal.

The artifacts are signed by the release key, RSA 4096, `Jazz Releases
<releases@jazz.tools>`, fingerprint
`A23F 5FC8 350C E279 A2DF  0C99 3682 3E73 6D53 8486`, which expires 2028-11-15.
Its public key is on keyserver.ubuntu.com and keys.openpgp.org; the private
key and passphrase are backed up in AWS Secrets Manager (shared-services,
us-east-2, `jazz/release/maven-signing-key`). Extend the expiry before then and
re-upload the public key; the fingerprint stays.

## Release branch lifecycle and alpha55 migration

`main` remains the development branch and `.changeset/config.json` keeps
`baseBranch: main` for ordinary development comparisons. Only pushes to `release`
update the Changesets version PR. Manual Changesets dispatch must also select
`release`; preview lookup uses the actual invocation ref, never the repository
default branch. Protect `release` against direct updates, deletion and force
pushes; require reviewed PRs and the applicable CI checks.

For the alpha55 transition:

1. Record the candidate SHA, existing version PR #2748 head SHA, expected version
   (verify `alpha.55` in package manifests), and draft release ID/body before
   changing automation. Preserve the old PR/branch until the replacement is verified.
2. Create `release` at the agreed candidate. Land this setup on `main`, then merge
   the setup into `release` through a normal setup PR. Do not merge #2748 or any
   version PR as part of setup. Setup does not change prerelease bookkeeping and
   cannot pass the publisher's exact merged-version-PR gate.
3. Dispatch Changesets on `release` to create `changeset-release/release` targeting
   `release`. Compare its version, consumed changesets and changelog with #2748;
   carry any intentionally edited release prose into the replacement and preserve
   the draft GitHub Release. Close the superseded PR only after this comparison.
4. Build new preview receipts at the replacement's exact head SHA. An old preview
   is reusable only when the publisher proves complete Git tree and package-version
   equality; a workflow-only change still invalidates reuse. Record expected SHA
   and version before approving publication. No production deployment, package
   publication, source tag or release publication belongs to this setup operation.
5. Configure docs staging to build the `release` candidate, and keep automatic
   production-domain assignment disabled for docs and Inspector. The publisher
   stages the Inspector from the tested prebuilt artifact, not from Vercel's Git
   integration. These Vercel settings are
   external to the build-only `docs.yml`. Record deployment IDs and source SHAs.
   Inspector promotion defaults to `release` and requires an explicit exact SHA;
   it must find that SHA's staged deployment, never the newest `main` deployment.

After publishing, merge `release` back to `main` with a merge commit, preserving
ancestry. Include versioned manifests, changelogs, lockfile changes and
`.changeset/pre.json`; keep any new main-only changeset files and do not add their
IDs to the consumed prerelease list. Resolve conflicts by preserving both the
released bookkeeping and unconsumed development work, then rerun the focused
release-cycle test. Do not reset prerelease state, cherry-pick only package
versions, or regenerate versions on main during the backmerge. For the next cut,
merge the selected main revision into `release` through a reviewed PR; repeat
versioning there. The fixture test in `dev/artifacts/release-branch-policy.test.mjs`
executes the installed pinned Changesets CLI across this full cycle.

The Changesets workflow explicitly dispatches CI and preview because PRs created
with `GITHUB_TOKEN` do not automatically trigger PR workflows. It checks the PR
and branch head before and after dispatch. If either moves, rerun Changesets on
`release`; required checks attach to the actual executed commit and cannot satisfy
a different head. Release pushes run the trusted suite with credentialed caches
disabled, retaining ordinary GitHub caches until external branch OIDC trust is
explicitly enabled. Main pushes retain shared-cache writes; trusted same-repository
PRs retain shared-cache reads. This setup requires no external IAM change.
