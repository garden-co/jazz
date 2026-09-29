# Jazz performance timeline data

Read-only CodSpeed wallclock history for public `garden-co/jazz`, snapshotted
daily and served at `/api/timeline` and shown on the docs site's examples & benchmarks page
(`/examples`, `components/showcase/`). No Jazz native build, database, or
CodSpeed token is needed. `/perf-timeline`, the former standalone explorer,
redirects to `/examples`.

Descriptions and throughput counts come from the benchmark-owned metadata
catalogue at [../../../dev/benchmarks/metadata](../../../dev/benchmarks/metadata/README.md), not from
page-specific prose or numbers parsed from names. Unknown names still show
timings but receive no guessed description or throughput.

The page shows the requested **5× estimate** (estimated seconds = measured
seconds / 5), marked `*` with a visible footnote; each history popover also
shows the measured runner time. Rates are work count / median duration, not
an independent mean-throughput or sustained-concurrency measurement.

## Classification

- Released: main commits proven to be included in a semantic-version tag, exact
  tag matches, and audited historical backfills. Main: main runs without proven
  release inclusion. Open PR: currently open PRs. Historical closed/merged PR
  trials and unregistered branches are excluded from the API dataset.
- Releases without an exact measured commit are identified but never assigned
  estimated values. Missing and simulation-only results are excluded; reruns
  remain separate receipts, not averaged.
- A metric card's number is the newest released measurement
  (`lib/showcase/summary.ts`); open-PR experiments never feed a card.
- A benchmark renamed without changing its numbers (declared `stitch` in
  `dev/benchmarks/metadata/former-names.ts`, backed by same-base CodSpeed
  runs) continues its former name's history, so its card keeps its releases.
  Other renames start a new history.

## Sources and caching

The site never calls CodSpeed or GitHub's API. `/api/timeline` only reads a
prebuilt snapshot, `timeline.json` on the `perf-timeline-data` pre-release
(`PERF_TIMELINE_SNAPSHOT_URL` overrides the URL), and is CDN-cached for an
hour and served stale for up to a week while it revalidates. A snapshot more
than two days old shows a visible "last refreshed" warning. When the snapshot
cannot be read, a server instance that already read one keeps serving it; a
cold instance returns HTTP 502 with a retry UI.

`.github/workflows/perf-timeline-snapshot.yml` builds the snapshot once a day,
when a GitHub release is published, when the alpha publish workflow finishes,
and on manual dispatch. It runs `scripts/perf-timeline-snapshot.ts`
(`lib/perf-timeline/snapshot.ts`) on a full-history checkout and uploads the
result to that release with the Actions token; no other secret is needed. A
failed build leaves the previous snapshot in place. The release is marked
pre-release and never latest, and its tag is not a version tag. It lives on a
release, not a branch, so Vercel's git integration never tries to deploy it.

CodSpeed is read through ordinary unauthenticated GraphQL at
`https://gql.codspeed.io/`. The build first lists `repository.runs` with
`commit.branch.pullRequest` metadata but no results, then fetches walltime
distributions via `repository.run(id:)`, one run per request, only for runs the
timeline can admit (main, open PRs, exact tags and registered backfills).
Asking for every run's results at once exceeded CodSpeed's gateway timeout at
~400 runs. `runs` takes no pagination arguments; the UI reports the actual
returned count, not a claim of exhaustive retention. Results of runs older than
a week are reused from the previous snapshot's `codspeed-results.json`; newer
runs are re-read, because results can still be processing right after a run
and re-running a CI job can replace results inside an existing run. A daily
build therefore asks CodSpeed for the run list plus the last week's runs. This
public web API is not a pinned SDK contract: API errors fail the build rather
than publishing demo data. See also `../../../dev/benchmarks/CODSPEED_GQL.md`
for profile access.

Version tags and release ancestry come from the checkout's own git history:
a main commit belongs to the oldest version tag that contains it
(`git tag --contains`). Neither dates nor a shared merge base prove inclusion.
A measured commit missing from the checkout stays unreleased with a visible
warning. No PR trial is reclassified through ancestry, and missing evidence
never fabricates a release measurement.

## Deployment

Deploy through the existing docs project; no separate app, Vercel project or
Vercel setting is required. `tests/examples.browser.mjs` checks the page
against a fixture after `pnpm --filter docs build`. To refresh the data by
hand, dispatch the "Benchmark timeline snapshot" workflow.

## Audited historical backfills

`backfills.ts` maps exact measured harness SHAs and explicit CodSpeed run/result
IDs to a released engine SHA and npm publication date. Empty receipt lists do
not display points. Add IDs only after checking actual job output and uploaded
engine-tree provenance; never whitelist skipped/carried-forward results. GitHub
workflow IDs are not CodSpeed run IDs. The release tag must resolve to the
recorded engine SHA or the mapping is rejected.

Audited backfills appear as Released points and connect to subsequent main points in the same trace. Their `date` is the explicit effective
release date; `measuredAt` remains the original CodSpeed timestamp and `sha`
remains the actual harness commit. `release`/`includedInRelease` are not assigned
from the historical mapping. The receipt retains explicit historical-harness provenance and exposes both dates, the engine commit,
the harness commit and the workflow provenance artifact. This changes timeline
placement only, never CodSpeed metadata or Git commit dates.

The alpha.54 harness lives permanently on `bench/alpha54-codspeed-backfill`;
it must not be merged into main. It retains every released engine crate tree,
released dependency versions and profiles, and imports the five later CodSpeed
workloads with harness-only API adaptations. The standalone native Groove
`record_validation` receipt is outside the CodSpeed workload inventory.

One release may have several registry entries, one per harness commit; each
entry allowlists only its own run/result IDs, and all entries for a release must
agree on engine SHA and effective date (`backfills.test.ts` audits the registry).
The alpha.55 policy harness lives on `chore/alpha55-codspeed-new-bench-backfill`.
The W1 `subscription_fanout_memory` cases added after alpha.56 are backfilled
from `bench/alpha5{4,5,6}-fanout-backfill`; each branch documents its single
diagnostic-only adaptation in `dev/benchmarks/ALPHA5x_FANOUT_BACKFILL.md`.
`first_sync_local_relay_27518_rocksdb` has no release points: its harness needs
the `Node::accept_scope_isolated_relay_subscriber_for_test` engine hook added
after alpha.56, and adding it would change pinned engine bytes.
