# Jazz performance timeline

Small read-only Next.js dashboard for public `garden-co/jazz` CodSpeed wallclock
history. No Jazz native build, database, or CodSpeed token is needed.

```sh
pnpm install --filter jazz-perf-timeline... --ignore-scripts
pnpm --filter jazz-perf-timeline dev
pnpm --filter jazz-perf-timeline test
pnpm --filter jazz-perf-timeline build
```

## Reading the graph

- Y: wallclock **median in seconds**, automatically formatted as s/ms/µs. The
  optional min–max whisker is observed sample range, not a confidence interval.
- X: measured release/PR/commit checkpoints, ordered by **run timestamp**, with
  equal spacing. Labels show the run's **UTC calendar day**, PR/release and
  commit hash. This is not a Git ancestry diagram or elapsed-time scale.
- Thick amber: exact semantic-version Git-tag commit. Solid green: main runs.
  Dashed purple: currently open PRs. Historical closed/merged PR trials and
  other branches are completely excluded from the API dataset, navigation,
  receipts and plots. A PR having merged does not make its earlier experimental
  commits measurements of main. Separate branches/PRs are never connected.
- Releases without an exact measured commit are identified but never assigned
  estimated values. Version tags are used, not inferred npm publication dates.
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
`https://gql.codspeed.io/`, fetching `repository.runs` with walltime distributions
and `commit.branch.pullRequest` metadata. The API currently returns available
repository history without pagination arguments; the UI reports the actual
returned count, not a claim of exhaustive retention. This public web API is not
a pinned SDK contract: API errors fail visibly rather than returning demo data.
See also `../benchmarks/CODSPEED_GQL.md` for profile access.

GitHub's public tags endpoint supplies exact version-tag SHA mappings. Optional
server-only `PERF_GITHUB_TOKEN` can raise its rate limit; never prefix it with
`NEXT_PUBLIC_`. An unavailable GitHub source produces a visible warning and
leaves release classification unknown, while CodSpeed history still works.

Upstream CodSpeed fetch and CDN responses are cached for five minutes; the CDN
can serve stale responses for another ten minutes while revalidating. GitHub
tags cache for one hour. Refresh reads that cache; it does not bypass rate
protection. No credentials, callgraph presigned URLs, or private data reach the
browser. A failed CodSpeed fetch returns HTTP 502 with a retry UI.

## Vercel

Use the **garden-co** team and a dedicated project, with repository root directory
`dev/perf-timeline`. `vercel.json` limits installation/build to this app and skips
workspace lifecycle scripts. This app must not use the native/WASM artifact
pipeline or the docs site's Vercel install command.

```sh
pnpm dlx vercel --scope garden-co
pnpm dlx vercel --prod --scope garden-co
```

Run CLI commands from the repository root after linking the project with the
correct root directory. `.vercel` metadata and local environment files remain
ignored. Production uses live, cached public sources—not a bundled snapshot.
