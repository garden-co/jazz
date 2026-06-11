# local-telemetry

Rust developer telemetry collector for localhost:

- accepts browser-safe OTLP/HTTP from your app on **:4318** and forwards it to
  **Rotel**,
- writes per-signal Rotel JSON files under `data/`,
- serves a **Yew** viewer UI and a **DuckDB SQL** endpoint on **:4319**.

```
your app  --OTLP/HTTP:4318-->  CORS proxy  -->  Rotel file exporter
                                                       |
                                                       v
                                      data/{spans,logs,metrics}/*.json
                                                       |
                                                       v
                                                  http :4319
                                                  GET  /        -> viewer (Sync Flow UI)
                                                  POST /sql     -> {query} -> {columns, rows}
                                                  GET  /health  -> "ok"
```

## Start it

Build the Yew UI once:

```sh
cd dev/local-telemetry/web
trunk build --release
```

Then run the collector:

```sh
cd dev/local-telemetry
cargo run --features collector --
```

Build a release binary with:

```sh
cd dev/local-telemetry
cargo build --release --features collector
```

Rotel's Rust build requires `protoc` on `PATH`. On macOS, install it with:

```sh
brew install protobuf
```

Flags:

| Flag               | Default     | Notes                                                |
| ------------------ | ----------- | ---------------------------------------------------- |
| `--data-dir`       | `./data`    | Rotel JSON output directory                          |
| `--otlp-host`      | `127.0.0.1` | OTLP/HTTP bind host                                  |
| `--otlp-port`      | `4318`      | Browser-safe OTLP/HTTP proxy port                    |
| `--http-host`      | `127.0.0.1` | Viewer + SQL bind host                               |
| `--http-port`      | `4319`      | Viewer + SQL HTTP port                               |
| `--retention-days` | `2`         | Accepted for compatibility; ignored by Rotel JSON IO |

CORS is open on the OTLP proxy and viewer ports so browser tooling can call
them from any origin. Rotel still owns OTLP decoding and file export; the proxy
only handles browser preflight and forwards request bytes to Rotel.

## Use it

### 1. Point an app at the collector

Anything that speaks OTLP/HTTP works. For the React stress test:

```sh
# dev/stress-tests/todo-react/.env
VITE_JAZZ_TELEMETRY_COLLECTOR_URL=http://127.0.0.1:4318
```

For a raw OTel SDK:

```sh
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 \
OTEL_EXPORTER_OTLP_PROTOCOL=http/json \
  <your app>
```

### 2. Open the viewer

<http://127.0.0.1:4319/> shows `sync.send` and `sync.recv` spans with window,
payload, and layer filters. Rows expand to show `payload` and `payload_json`.
The Yew app polls `/sql` every 3 seconds.

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

Errors use `{"error": "..."}` with HTTP 400.

## Views

Views are refreshed on each `/sql` request when matching Rotel JSON files exist:

| View            | One row per                           |
| --------------- | ------------------------------------- |
| `raw_traces`    | Rotel `ResourceSpans` JSON document   |
| `raw_logs`      | Rotel `ResourceLogs` JSON document    |
| `raw_metrics`   | Rotel `ResourceMetrics` JSON document |
| `spans`         | flattened span                        |
| `logs`          | flattened log record                  |
| `number_points` | flattened gauge/sum metric point      |

Rotel writes JSON arrays under:

- `data/spans/traces_*.json`
- `data/logs/logs_*.json`
- `data/metrics/metrics_*.json`

Common columns on `spans`: `trace_id`, `span_id`, `parent_span_id`, `name`,
`kind`, `start_time_unix_nano`, `end_time_unix_nano`, `duration_ns`,
`service_name`, `scope_name`, `status_code`, `attributes` (JSON), `events`
(JSON), `raw_span` (JSON).

Common columns on `logs`: `time_unix_nano`, `observed_time_unix_nano`,
`severity_number`, `severity_text`, `body` (JSON), `trace_id`, `span_id`,
`service_name`, `scope_name`, `attributes` (JSON), `raw_record` (JSON).

Common columns on `number_points`: `name`, `description`, `unit`,
`service_name`, `scope_name`, `kind` (`'gauge'` or `'sum'`),
`time_unix_nano`, `value` (DOUBLE), `attributes` (JSON).

Histograms and exponential histograms are available through `raw_metrics`.

## Layout

```
dev/local-telemetry/
├── Cargo.toml
├── src/
│   ├── main.rs          flags, signals, task orchestration
│   ├── collector.rs     Rotel Agent wiring
│   ├── http.rs          Axum routes + static assets
│   ├── sql.rs           /sql request execution
│   ├── views.rs         DuckDB views for Rotel JSON files
│   └── ui.rs            static asset lookup
├── tests/               black-box HTTP/SQL tests
├── web/                 Yew viewer built by Trunk
│   ├── Cargo.toml
│   ├── Trunk.toml
│   ├── index.html
│   └── src/main.rs
└── data/                Rotel JSON output (gitignored)
```

## Why this and not `dev/observability/`

`dev/observability/` runs the upstream collector + Grafana/Tempo/Loki/Prom in
Docker. `local-telemetry` is for fast inspection over `curl`, SQL, and a small
local UI without Docker.
