# Environment variables

| Variable | CLI flag | Default | Description |
|----------|----------|---------|-------------|
| `JAZZ_BACKEND_SECRET` | `--backend-secret` | — | Secret for backend session impersonation |
| `JAZZ_ADMIN_SECRET` | `--admin-secret` | — | Secret for admin operations (schema/policy sync) |
| `JAZZ_SLOW_QUERY_MS` | — | `50` | Server only: log query shapes whose runs take at least this long, naming the app, with their plan and an index hint. The first slow shape is logged at once, later ones at most every 10 s. `0` turns the log off. Clients set the same threshold with `slowQueryThresholdMs` in their config |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | — | When set, enables OpenTelemetry trace + log export via OTLP/HTTP. Stdout keeps emitting human-readable logs. |
