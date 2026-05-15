# local-telemetry

Single Go binary that does three things on localhost:

- accepts OTLP/HTTP from your app on **:4318**,
- writes per-signal JSONL to `data/` with 2-day rotation,
- serves a **viewer** UI and a **DuckDB SQL** endpoint on **:4319**.

```
your app  ──OTLP/HTTP:4318──▶  local-telemetry  ──fileexporter──▶  data/{traces,logs,metrics}.jsonl
                                      │                                         ▲
                                      ▼                                         │
                                 http :4319 ◀────── DuckDB views ───────────────┘
                                 GET  /            → viewer (Sync Flow UI)
                                 POST /sql         → {query} → {columns, rows}
                                 GET  /health      → "ok"
```

No yaml config, no Node toolchain, no Docker — `go build` and run.

## Start it

```sh
cd dev/local-telemetry
go build -o local-telemetry .
./local-telemetry
```

Or just `go run .` if you don't want a checked-out binary. On first start the
binary bundles the TSX viewer in-process via esbuild (~100 ms); React and
ReactDOM are loaded by the browser from esm.sh via an import-map, so there's
no `node_modules` anywhere.

Flags:

| Flag               | Default     | Notes                         |
| ------------------ | ----------- | ----------------------------- |
| `--data-dir`       | `./data`    | Where JSONL files live        |
| `--otlp-host`      | `127.0.0.1` | OTLP/HTTP bind host           |
| `--otlp-port`      | `4318`      | OTLP/HTTP receiver port       |
| `--http-host`      | `127.0.0.1` | Viewer + SQL bind host        |
| `--http-port`      | `4319`      | Viewer + SQL HTTP port        |
| `--retention-days` | `2`         | Days of rotated files to keep |

CORS is `*` on the HTTP port so browser tooling can hit it from any origin.

## Use it

### 1. Point an app at the collector

Anything that speaks OTLP/HTTP works. For the React stress test:

```sh
# dev/stress-tests/todo-react/.env
VITE_JAZZ_TELEMETRY_COLLECTOR_URL=http://127.0.0.1:4318
```

Then `pnpm dev` from `dev/stress-tests/todo-react/` and load it in a browser.
The `[jazz] telemetry collector: …` log line confirms the URL was picked up.

For a raw OTel SDK, set the standard env vars:

```sh
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 \
OTEL_EXPORTER_OTLP_PROTOCOL=http/json \
  <your app>
```

### 2. Open the viewer

<http://127.0.0.1:4319/> — a table of `sync.send`/`sync.recv` spans with
window, payload, and layer filters. Rows are clickable to expand the
`payload_json` blob. The page polls the SQL endpoint every 3 s (react-query),
no websocket.

### 3. Query directly

```sh
curl -s -X POST http://127.0.0.1:4319/sql \
  -H 'Content-Type: application/json' \
  -d '{"query":"SELECT name, service_name, duration_ns FROM spans ORDER BY start_time_unix_nano DESC LIMIT 10"}'
```

Response shape:

```json
{
  "columns": ["name", "service_name", "duration_ns"],
  "rows": [["do-thing", "alice-service", 1000000000]]
}
```

Errors: `{"error": "..."}` with HTTP 400.

For interactive analysis from Claude Code, invoke the
`analyze-local-telemetry` skill — it ships with the recipe library and view
schemas.

## Views

Created on demand whenever JSONL exists for the matching signal:

| View            | One row per                                 |
| --------------- | ------------------------------------------- |
| `raw_traces`    | OTLP `ExportTraceServiceRequest` document   |
| `raw_logs`      | OTLP `ExportLogsServiceRequest` document    |
| `raw_metrics`   | OTLP `ExportMetricsServiceRequest` document |
| `spans`         | flattened span                              |
| `logs`          | flattened log record                        |
| `number_points` | flattened gauge/sum metric data point       |

Common columns on `spans`: `trace_id`, `span_id`, `parent_span_id`, `name`,
`kind`, `start_time_unix_nano`, `end_time_unix_nano`, `duration_ns`,
`service_name`, `scope_name`, `status_code`, `attributes` (JSON), `events`
(JSON), `raw_span` (JSON).

Common columns on `logs`: `time_unix_nano`, `observed_time_unix_nano`,
`severity_number`, `severity_text`, `body` (JSON), `trace_id`, `span_id`,
`service_name`, `scope_name`, `attributes` (JSON), `raw_record` (JSON).

Common columns on `number_points`: `name`, `description`, `unit`,
`service_name`, `scope_name`, `kind` (`'gauge'` | `'sum'`), `time_unix_nano`,
`value` (DOUBLE), `attributes` (JSON).

Histograms and exponential histograms aren't pre-flattened — query them from
`raw_metrics` directly.

## Layout

```
dev/local-telemetry/
├── main.go          flags, signals, errgroup
├── collector.go     embedded OTel collector + in-memory confmap
├── sql.go           DuckDB views + /sql + /health handlers
├── ui.go            esbuild bundle + /, /main.js handlers
├── web/             TSX sources (//go:embed-ed)
│   ├── index.html   import-map + #root
│   ├── main.tsx, App.tsx, Flow.tsx, flowRows.ts, api.ts
└── data/            JSONL output (gitignored)
```

## Retention

`fileexporter` rotation handles it natively — `max_days` matches
`--retention-days`. No sweeper goroutine.

## Why this and not `dev/observability/`

`dev/observability/` runs the upstream collector + Grafana/Tempo/Loki/Prom in
Docker — use that when you want Grafana dashboards. `local-telemetry` is for
fast inspection over `curl`/SQL/UI without Docker.
