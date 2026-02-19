# Local Observability Stack

OTel Collector + Grafana/Tempo/Prometheus/Loki for viewing traces locally.

```
jazz-server ──OTLP gRPC:4317──→ OTel Collector ──OTLP:4317──→ grafana/otel-lgtm
                                                                 ├── Tempo (traces)
                                                                 ├── Prometheus (metrics)
                                                                 ├── Loki (logs)
                                                                 └── Grafana UI (:3000)
```

## Prerequisites

- Docker

## Start

```sh
cd dev/observability
docker compose up -d
```

## Build the server with OTel

```sh
cargo build -p jazz-tools --features otel
```

## Run an instrumented server

```sh
JAZZ_OTEL=1 \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
RUST_LOG=jazz_tools=debug,groove=debug \
cargo run -p jazz-tools --features otel -- server <APP_ID>
```

## View traces

Open http://localhost:3000 → Explore → Tempo → Search.

## Stop

```sh
docker compose down      # stop containers
docker compose down -v   # stop + wipe data
```

## Environment variables

| Variable                      | Purpose                                       | Default                 |
| ----------------------------- | --------------------------------------------- | ----------------------- |
| `JAZZ_OTEL`                   | Enable the OTel tracing layer (`1` to enable) | off                     |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Collector endpoint                            | `http://localhost:4317` |
| `OTEL_SERVICE_NAME`           | Service name in traces                        | `jazz-server`           |
| `RUST_LOG`                    | Log filter for `tracing` subscriber           | —                       |
