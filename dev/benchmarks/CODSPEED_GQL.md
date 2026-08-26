# CodSpeed callgraph API receipt

CodSpeed's MCP is the first stop for run discovery, comparisons, benchmark
distributions, and summarized flamegraphs. The web application's GraphQL API
is useful when analysis needs the complete wall-time graph and its raw perf
counters (`cpu_cycles`, `instructions`, L1/L2 hits, and cache misses).

## Authentication

Copy the short-lived bearer token from an authenticated
`https://app.codspeed.io` request and keep it outside the repository:

```sh
export CODSPEED_AUTH_TOKEN='...'
```

Never commit or paste the token into a receipt. The examples below assume
`jq`, `curl`, and `gzip`.

## Resolve the newest result IDs

The unauthenticated GraphQL schema has introspection disabled, but this normal
query is stable. It resolves CodSpeed run IDs to benchmark result IDs. Replace
the owner, repository, commit prefixes, and benchmark name as needed.

```sh
curl -sS https://gql.codspeed.io/ \
  -H 'Content-Type: application/json' \
  -H "Authorization: $CODSPEED_AUTH_TOKEN" \
  --data-binary '{"query":"query { repository(owner: \"garden-co\", name: \"jazz\") { runs { id commit { hash } results { id benchmark { id name } } } } }"}' \
  > /tmp/codspeed-runs.json

jq -c '.data.repository.runs[]
  | select(.commit.hash | startswith("HEAD_COMMIT_PREFIX") or startswith("BASE_COMMIT_PREFIX"))
  | {runId: .id, commit: .commit.hash,
     result: (.results[] | select(.benchmark.name == "ingest_walltime_100k"))}' \
  /tmp/codspeed-runs.json
```

The selected `result.id` values become `HEAD_RESULT_ID` and `BASE_RESULT_ID`.

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
    -H "Authorization: $CODSPEED_AUTH_TOKEN" \
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

For function attribution, map node indexes through `.nodes`, select matching
edges by their `source`/`target`, and sum the relevant `timeDistribution`
entries. Prefer the MCP's rooted flamegraph query for ordinary timing analysis;
use the raw graph when exact instruction or memory-event accounting matters.
