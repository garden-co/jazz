# Jazz performance timeline

Small read-only Next.js dashboard for public `garden-co/jazz` CodSpeed wallclock
history. No Jazz native build, database, or CodSpeed token is needed.

Descriptions and throughput counts come from the benchmark-owned metadata
catalogue at [../../../dev/benchmarks/metadata](../../../dev/benchmarks/metadata/README.md), not from
dashboard-specific prose or numbers parsed from names. It currently covers all
47 known current/retired wallclock series. Unknown names still show timings but
receive no guessed description or throughput.

Estimated time is the default. The measured mode is labeled “Deterministic runner”
(CodSpeed wallclock samples still vary). Estimates use the requested **5× assumption**:
estimated seconds = measured seconds / 5; estimated rate = measured rate × 5.
Every estimated number carries `*`, with a visible footnote explaining that this
is illustrative, not a measured machine prediction. The raw receipt table and
upstream/API data remain unmodified. Rates are work count / median duration, not
an independent mean-throughput or sustained-concurrency measurement.

Summary cards run first → latest, with workload rates directly below timings.
Log scale is the default; uncheck “Log scale” to use a linear zero-based axis.
Both sides of the chart show the same Y ticks: rounded 1/2/5 linear steps, or
1/2/5 decade values in log mode (powers of ten for wide ranges). Rounding happens
in displayed units. The full graph and sparklines share the resulting domain.

```sh
pnpm install --filter docs... --ignore-scripts
pnpm --filter docs dev
pnpm --filter docs test:perf-timeline
pnpm --filter docs build
```

## Reading the graph

- Y: wallclock **median in seconds**, automatically formatted as s/ms/µs. The
  optional min–max whisker is observed sample range, not a confidence interval.
- X: measured release/PR/commit checkpoints, ordered by **run timestamp** (reviewed historical backfills use release publication time), with
  equal spacing. Labels show the run's **UTC calendar day**, PR/release and
  commit hash. This is not a Git ancestry diagram or elapsed-time scale.
- Thick amber: main commits proven to be included in a semantic-version tag,
  plus exact tag matches. Solid green: main runs without proven release inclusion.
  Dashed purple: currently open PRs. Historical closed/merged PR trials and
  unregistered other branches are completely excluded from the API dataset, navigation,
  receipts and plots. A PR having merged does not make its earlier experimental
  commits measurements of main. Separate branches/PRs are never connected.
- Releases without an exact measured commit are identified but never assigned
  estimated values. Version tags are used, not inferred npm publication dates.
- An ancestor keeps its measured SHA on the axis; the receipt identifies a tag
  containing it. It is not presented as a measurement of that release's tree.
- Sparklines use the full available history and the same geometry as the large
  plot, including zero/log scale and min–max domain. The active preview follows
  every filter; other previews follow window/status, as they do when selected
  (selecting another benchmark resets the branch filter).
- Missing and simulation-only results are excluded. Completed measurements from
  a run with other failed/pending jobs remain visible with the run status.
- Benchmark IDs separate series even if display names coincide. Reruns remain
  separate receipts, not averaged. Harness, fixtures, machine and configuration
  changes can invalidate comparisons; inspect linked runs and source commits.

The selected benchmark is shareable through `?benchmark=<id-or-name>`. Every
point has a result ID, run link, commit link, and PR link where available. The
receipt table exposes exact seconds for keyboard and assistive-technology users.

## Sources and caching

`lib/source.ts` uses ordinary unauthenticated GraphQL at
`https://gql.codspeed.io/`. It first lists `repository.runs` with
`commit.branch.pullRequest` metadata but no results, then fetches walltime
distributions via `repository.run(id:)` only for runs the timeline can admit
(main, open PRs, exact tags and registered backfills), 15 runs per request (the
API's alias limit). Asking for every run's results at once exceeded CodSpeed's
gateway timeout at ~400 runs. `runs` takes no pagination arguments; the UI
reports the actual returned count, not a claim of exhaustive retention. This public web API is not
a pinned SDK contract: API errors fail visibly rather than returning demo data.
See also `../../../dev/benchmarks/CODSPEED_GQL.md` for profile access.

GitHub's public tags endpoint supplies exact version-tag SHA mappings. Optional
server-only `PERF_GITHUB_TOKEN` can raise its rate limit; never prefix it with
`NEXT_PUBLIC_`. An unavailable GitHub source produces a visible warning and
leaves release classification unknown, while CodSpeed history still works.

Main-run ancestry is checked with GitHub's `compare/<measured-SHA>...<tag-SHA>`:
only `ahead` or `identical` proves inclusion. Neither dates nor a shared merge
base prove inclusion. Deduplicated SHA comparisons cache for one day, with three
concurrent requests and an overall 50-second source deadline. The per-refresh
comparison budget is 40 (200 with the optional token); errors or exhausted
budgets visibly warn that remaining main release statuses are unverified. No PR
trial is reclassified through ancestry, and missing evidence never fabricates a
release measurement.

Upstream CodSpeed fetch and CDN responses are cached for five minutes; the CDN
can serve stale responses for another ten minutes while revalidating. GitHub
tags cache for one hour. Refresh reads that cache; it does not bypass rate
protection. No credentials, callgraph presigned URLs, or private data reach the
browser. A failed CodSpeed fetch returns HTTP 502 with a retry UI.

## Docs route and deployment

The dashboard lives at `/perf-timeline` in the docs app and is intentionally absent
from site navigation and search content. It uses the shared Fumadocs home layout,
fonts, and theme. Its stylesheet is scoped to `.perf-timeline`.

Deploy through the existing docs project; no separate app or Vercel project is
required. The public API remains `/api/timeline`. Set the optional server-only
`PERF_GITHUB_TOKEN` on that project only if higher public-source limits are needed.

After building, run `pnpm --filter docs test:perf-timeline:e2e` for browser checks.

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
