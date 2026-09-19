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
- Smoke-test published preview packages as an adopter: `npm create jazz`, CLI
  validation/deployment, local CLI server, authentication, write/read/delete of a
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
  using registry reads and installation. Confirm the source tag matches the exact
  released source. Build artifacts alone are not proof of npm publication.
- Promote the verified docs and Inspector deployments.
- Treat Cloud activation as two explicit operations: selecting the new catalog
  default for newly created apps, and upgrading existing eligible tenants through
  a controlled fleet rollout. Have infra confirm the exact version and image digest
  in the status API and both regions. Never include legacy or preview tenants in a
  stable fleet rollout.
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
source tag. GitHub Release creation/publication is currently manual. Do not assume
a green Changesets action or an existing tag means the packages or GitHub Release
have been published.

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
5. Configure docs and Inspector staging to build the `release` candidate, with
   automatic production-domain assignment disabled. These Vercel settings are
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
