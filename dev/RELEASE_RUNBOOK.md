# Jazz release runbook

Owner: release coordinator. Keep a release-specific checklist with exact commit,
workflow run, package, deployment and acceptance links. Never put credentials in
this document or public receipts.

## 1. Freeze the candidate

- Triage the release milestone. Identify blockers, deferred issues and pending
  approvals; verify each included PR has independent review and passing gates.
- Merge approved changes. Confirm Changesets refreshes the release PR from main.
- Audit changesets against the previous release: include important user-visible
  fixes, migrations and compatibility changes, with detailed commit/PR references.
  Keep announcement prose separate from the detailed package changelog.
- Verify the intended version, alpha prerelease state, fixed package group and
  exact internal dependency versions. Do not infer the next version from a PR title.
- Explicitly state wire/storage compatibility, required client/server coordination,
  and treatment of existing rows, pending local writes and caches.
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
- Test fresh Cloud preview tenants with the matching image: publish schema and
  permissions first, configure auth, check authenticated CRUD/reconnect/reopen,
  then delete disposable tenants. Preview tenants remain version-pinned; use new
  tenants for a new candidate. Keep tenant credentials in private files.
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
- Prepare announcement text and infra activation/maintenance changes in advance.
- Record what can be rolled back (deployment/default image/docs/dist-tags) and what
  cannot (published immutable npm versions). Coordinate wire compatibility before
  any rollback; never overwrite a published version or silently retarget its tag.

## 4. Coordinated publication

- Obtain the release go-ahead. Confirm infra, package publisher and docs owner are
  ready. For a coordinated Cloud transition, enable app-creation maintenance and
  verify it before publication. Do not assume existing tenants are upgraded.
- Merge the approved release PR only when ready: the publisher can trigger from
  that merge. Avoid dispatching a duplicate publish run.
- If manual dispatch is needed, use `publish-jazz-tools-alpha.yml` on the exact
  committed release revision with `mode=publish` and the expected source version.
  Check workflow inputs before running it. Never publish from a mutable working tree.
- Watch individual jobs and inspect failure logs. Warm caches can shorten the run,
  but do not replace validation or guarantee identical artifacts.
- On partial npm failure, inventory versions already published. Fix credentials or
  packaging, then retry through the publisher's existing-version handling. Never
  republish different bytes under an existing version.
- Verify every expected npm package/version, native payload, dependency and dist-tag
  using registry reads and installation. Confirm the source tag matches the exact
  released source. Build artifacts alone are not proof of npm publication.
- Promote the verified docs and Inspector deployments. Have infra activate the
  staged stable image/catalogue and matching dashboard, confirming exact version
  and digest in the status API and both regions.
- Disable creation maintenance when infra confirms readiness. Create a new ordinary
  tenant with the real npm release; validate/deploy schema and permissions and run
  authenticated write/read/update/delete plus reconnect/reopen. Delete test tenants.
  Check existing sync probes and rollout health with infra.

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

## Current automation boundaries

`changesets-release-pr.yml` creates versions/changelogs and the release PR; it does
not publish. `publish-jazz-tools-alpha.yml` publishes packages and creates the
source tag. GitHub Release creation/publication is currently manual. Do not assume
a green Changesets action or an existing tag means the packages or GitHub Release
have been published.
