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

The selected `result.id` values are `headResultId` and `baseResultId`; the
selected `benchmark.id` is `benchmarkId`.

## Request and download the complete graph

The web app uses the persisted `FindBenchmarkCallGraph` operation. Its response
contains short-lived `callGraphPresignedUrl` values, so download both artifacts
immediately after this request.

```sh
jq -nc \
  --arg benchmarkId "$BENCHMARK_ID" \
  --arg baseResultId "$BASE_RESULT_ID" \
  --arg headResultId "$HEAD_RESULT_ID" \
  '[{
    operationName: "FindBenchmarkCallGraph",
    variables: {
      owner: "garden-co", repository: "jazz", provider: "GITHUB",
      benchmarkId: $benchmarkId,
      baseResultId: $baseResultId,
      headResultId: $headResultId
    },
    extensions: {persistedQuery: {
      version: 1,
      sha256Hash: "5b886421b74801a5a0bb50f4603fe6bfc9bf8dc1c38af83af960874f24825642"
    }}
  }]' > /tmp/codspeed-callgraph-query.json

curl -sS https://gql.codspeed.io/ \
  -H 'Content-Type: application/json' \
  -H "Authorization: $CODSPEED_AUTH_TOKEN" \
  --data-binary @/tmp/codspeed-callgraph-query.json \
  > /tmp/codspeed-callgraph-response.json

for side in base head; do
  jq -r ".[0].data.repository.${side}Result.callGraphPresignedUrl" \
    /tmp/codspeed-callgraph-response.json \
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
