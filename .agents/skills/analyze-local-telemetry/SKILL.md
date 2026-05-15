---
name: analyze-local-telemetry
description: Use when the user wants to analyze OTel traces/logs/metrics captured locally — inspect spans, find slow operations, correlate logs with traces, sanity-check metrics. Talks SQL to `dev/local-telemetry` over HTTP via curl. Trigger phrases include "look at the traces", "what spans did X emit", "why was this slow", "any errors in the logs", "check the metrics".
---

# Analyze local telemetry

`dev/local-telemetry` ingests OTLP/HTTP, writes JSONL to `dev/local-telemetry/data/`, and exposes a DuckDB SQL endpoint at `http://127.0.0.1:4319/sql`. Query it with `curl` to investigate behavior. The same port also serves a sync-flow viewer at `/` (handy to glance at — point a browser there); for programmatic analysis stick to `/sql`.

## Endpoint contract

```sh
curl -s -X POST http://127.0.0.1:4319/sql \
  -H 'Content-Type: application/json' \
  --data @- <<'EOF'
{"query":"SELECT ..."}
EOF
```

Use a heredoc (`--data @- <<'EOF' ... EOF`) — SQL needs single quotes (`'$.field'`) and heredocs avoid shell escaping pain.

Success → `{"columns": [...], "rows": [[...], ...]}`. Error → `{"error": "<DuckDB message>"}` with HTTP 400.

Check it's running first: `curl -s http://127.0.0.1:4319/health` returns `ok`. If it doesn't, tell the user to start it: `cd dev/local-telemetry && go run .` (the README has flags).

## Views

Created lazily — only exist once data has landed for the matching signal. If a view is missing, ask the user to exercise the code path that emits that signal.

| View            | Grain                                                     |
| --------------- | --------------------------------------------------------- |
| `spans`         | one row per span (traces)                                 |
| `logs`          | one row per log record                                    |
| `number_points` | one row per gauge/sum metric data point                   |
| `raw_traces`    | one row per OTLP ExportTraceServiceRequest, `doc` is JSON |
| `raw_logs`      | same shape, for logs                                      |
| `raw_metrics`   | same shape, for metrics                                   |

### `spans` columns

`trace_id`, `span_id`, `parent_span_id`, `name`, `kind` (1=INTERNAL, 2=SERVER, 3=CLIENT, 4=PRODUCER, 5=CONSUMER), `start_time_unix_nano`, `end_time_unix_nano`, `duration_ns`, `service_name`, `scope_name`, `status_code` (0=UNSET, 1=OK, 2=ERROR), `attributes` (JSON array), `events` (JSON array), `raw_span` (JSON).

Attributes are stored OTLP-shape — extract with `json_extract_string(attributes, '$[0].value.stringValue')` or filter:

```sql
SELECT name, (
  SELECT json_extract_string(a, '$.value.stringValue')
  FROM UNNEST(attributes::JSON[]) AS u(a)
  WHERE json_extract_string(a, '$.key') = 'http.route'
  LIMIT 1
) AS route
FROM spans WHERE service_name = 'my-service';
```

### `logs` columns

`time_unix_nano`, `observed_time_unix_nano`, `severity_number` (1–24, 17+ is ERROR), `severity_text`, `body` (JSON — typically `{"stringValue":"..."}`), `trace_id`, `span_id`, `service_name`, `scope_name`, `attributes` (JSON), `raw_record` (JSON).

Body is typed: `json_extract_string(body, '$.stringValue')` for strings.

### `number_points` columns

`name`, `description`, `unit`, `service_name`, `scope_name`, `kind` (`'gauge'` or `'sum'`), `time_unix_nano`, `value` (DOUBLE), `attributes` (JSON).

Histograms aren't pre-flattened — go through `raw_metrics`.

## Recipes

### Top slowest spans

```sql
SELECT name, service_name, duration_ns / 1e6 AS ms
FROM spans
WHERE service_name = 'jazz-server'
ORDER BY duration_ns DESC
LIMIT 20;
```

### Error spans

```sql
SELECT service_name, name, trace_id
FROM spans
WHERE status_code = 2
ORDER BY start_time_unix_nano DESC
LIMIT 50;
```

### Full trace tree

```sql
WITH t AS (SELECT * FROM spans WHERE trace_id = '<trace_id>')
SELECT span_id, parent_span_id, name, duration_ns / 1e6 AS ms
FROM t ORDER BY start_time_unix_nano;
```

### Recent error logs with their span

```sql
SELECT
  time_unix_nano,
  severity_text,
  json_extract_string(body, '$.stringValue') AS msg,
  trace_id, span_id, service_name
FROM logs
WHERE severity_number >= 17
ORDER BY time_unix_nano DESC
LIMIT 50;
```

### Metric over time

```sql
SELECT
  to_timestamp(time_unix_nano / 1e9) AS ts,
  value,
  json_extract_string(attributes, '$[0].value.stringValue') AS first_attr
FROM number_points
WHERE name = 'jazz.sync.batch.size'
ORDER BY ts DESC
LIMIT 100;
```

### Find which service.name values are present

```sql
SELECT DISTINCT service_name FROM spans;
```

## Workflow tips

- Start narrow (`LIMIT 20`, filter by `service_name`) — the dataset can hold ~2 days of telemetry.
- Trace IDs are 32 hex chars, span IDs 16. Filter as strings.
- Times are unix nanoseconds. Convert with `to_timestamp(n / 1e9)`.
- If a JSON extract path doesn't exist, you get SQL NULL — `WHERE field IS NOT NULL` is the right filter.
- The response is JSON — pipe through `python3 -m json.tool` for readability or `jq -r '.rows[][]'` to extract values.
- When investigating a regression, get the user to run the failing case **after** starting the collector, then query.

## When to use the raw\_\* views

When the flattened views drop fields you need (custom attribute shapes, histogram buckets, span events with their attributes). Pattern:

```sql
WITH unfurled AS (
  SELECT
    UNNEST(json_extract(doc, '$.resourceSpans')::JSON[]) AS rs
  FROM raw_traces
)
SELECT json_extract(rs, '$.scopeSpans[0].spans[0].events') FROM unfurled;
```

Cast JSON arrays as `::JSON[]` before `UNNEST`. After casting, schema-unified array elements may have JSON-null entries for missing fields — check with `json_type(x, '$.field') = 'OBJECT'` rather than `IS NOT NULL`.
