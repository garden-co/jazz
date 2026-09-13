# CodSpeed callgraph API receipt

CodSpeed's MCP is the first stop for run discovery, comparisons, benchmark
distributions, and summarized flamegraphs. The web application's GraphQL API
is useful when analysis needs the complete wall-time graph and its raw perf
counters (`cpu_cycles`, `instructions`, L1/L2 hits, and cache misses).

## Authentication

The public `garden-co/jazz` queries below work without authentication
(verified 2026-09-13). Try that first. Private repositories may require a
short-lived token from an authenticated `https://app.codspeed.io` request;
keep it outside the repository and add the authorization header only when
needed. Never commit or paste tokens or presigned URLs into receipts.
The examples below assume `jq`, `curl`, and `gzip`.

## Resolve the newest result IDs

The unauthenticated GraphQL schema has introspection disabled, but this normal
query is stable. It resolves CodSpeed run IDs to benchmark result IDs. Replace
the owner, repository, commit prefixes, and benchmark name as needed.

```sh
curl -sS https://gql.codspeed.io/ \
  -H 'Content-Type: application/json' \
  --data-binary '{"query":"query { repository(owner: \"garden-co\", name: \"jazz\") { runs { id commit { hash } results { id benchmark { id name } } } } }"}' \
  > /tmp/codspeed-runs.json

jq -c '.data.repository.runs[]
  | select(.commit.hash | startswith("HEAD_COMMIT_PREFIX") or startswith("BASE_COMMIT_PREFIX"))
  | {runId: .id, commit: .commit.hash,
     result: (.results[] | select(.benchmark.name == "ingest_walltime_100k"))}' \
  /tmp/codspeed-runs.json
```

The selected `result.id` values become `HEAD_RESULT_ID` and `BASE_RESULT_ID`.
Select the exact run and actual commit hash, not the first array entry: run
ordering is not an authority for recency, and MCP discovery can initially show
a synthetic merge hash before the run resolves to its source commit. Results
may remain empty while processing; absence is not a zero measurement.

## Request and download the complete graph

Do not depend on the web app's persisted `FindBenchmarkCallGraph` hash: it can
rotate. Once the result IDs are known, this ordinary query resolves each
short-lived `callGraphPresignedUrl` directly. Download the artifact immediately.

```sh
for side in base head; do
  case "$side" in
    base) result_id="$BASE_RESULT_ID" ;;
    head) result_id="$HEAD_RESULT_ID" ;;
  esac
  jq -nc --arg id "$result_id" '{
    query: ("query { repository(owner: \"garden-co\", name: \"jazz\") { benchmarkResultById(id: \"" + $id + "\") { id callGraphPresignedUrl } } }")
  }' > "/tmp/codspeed-${side}-callgraph-query.json"

  curl -sS https://gql.codspeed.io/ \
    -H 'Content-Type: application/json' \
    --data-binary @"/tmp/codspeed-${side}-callgraph-query.json" \
    | jq -r '.data.repository.benchmarkResultById.callGraphPresignedUrl' \
    | xargs curl -sS -o "/tmp/codspeed-${side}.json.gz"
  gzip -dc "/tmp/codspeed-${side}.json.gz" \
    > "/tmp/codspeed-${side}.json"
done
```

The artifact has `nodes`, `edges`, `roots`, `processes`, and `threads`. Timing
and perf events live on each edge's `timeDistribution`. The root edge gives the
whole benchmark totals:

```sh
jq -c '.edges[] | select(.source == 0)
  | {cpuTotal: .timeDistribution[0][1].cpuTotal,
     events: .timeDistribution[0][1].execEvents}' \
  /tmp/codspeed-head.json | head -1
```

For performance theses, inspect the full wall-time graph first and use MCP for
run discovery and benchmark distributions. Check the root actually returned by
an MCP rooted query: a partial function filter can return the whole graph.

For function attribution, map node indexes through `.nodes`. Node
`timeDistribution` is self time; incoming edges carry inclusive subtree time.
Sum all relevant thread/distribution entries rather than assuming entry zero
is the entire process. Count the union of overlapping subtrees once; never add
a parent scope to its children. Flamegraph node occurrences are stack-context
occurrences, not invocation counts. Use explicit opt-in counters if a thesis
depends on calls or processed rows. Profile sampling totals and benchmark
wall-time minima/medians are separate receipts, not interchangeable clocks.

Validate symbol filters and ancestry against the code before assigning a
removable budget. For example, `for jazz::protocol::SupportingRow` matches both
Serialize and Deserialize implementations; use the exact trait/function when
separating encode from decode. A derived `Clone` frame containing whole query
execution beneath it is not evidence that cloning itself runs that query.
Optimized inline ancestry can be misleading. Cross-check such scopes with a
phase-bounded native profile using `perf script --no-inline` and the actual
call sites before proposing a large win. Keep instrumented phase timings
separate from clean benchmark timings: per-phase clock reads can materially
perturb a densely instrumented workload. The evidence and examples are in
[the performance outcome log](https://github.com/garden-co/jazz/issues/2913).
