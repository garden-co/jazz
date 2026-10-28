# Environment variables

| Variable | CLI flag | Default | Description |
|----------|----------|---------|-------------|
| `JAZZ_JWKS_URL` | `--jwks-url` | — | JWKS endpoint for JWT validation |
| `JAZZ_JWT_PUBLIC_KEY` | `--jwt-public-key` | — | Single JWK JSON object or PEM public key for JWT validation |
| `JAZZ_BACKEND_SECRET` | `--backend-secret` | — | Secret for backend session impersonation |
| `JAZZ_ADMIN_SECRET` | `--admin-secret` | — | Secret for admin operations (schema/policy sync) |
| `JAZZ_ALLOW_LOCAL_FIRST_AUTH` | `--allow-local-first-auth` | see NODE_ENV | Allow local-first bearer auth |
| `JAZZ_JWKS_CACHE_TTL_SECS` | — | `300` | JWKS cache TTL in seconds |
| `JAZZ_JWKS_MAX_STALE_SECS` | — | `300` | Max time (past TTL) to serve stale JWKS on fetch failure |
| `JAZZ_SLOW_QUERY_MS` | — | `50` | Server only: log query shapes whose runs take at least this long, naming the app, with their plan and an index hint. The first slow shape is logged at once, later ones at most every 10 s. `0` turns the log off. Clients set the same threshold with `slowQueryThresholdMs` in their config |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | — | When set, enables OpenTelemetry trace + log export via OTLP/HTTP. Stdout keeps emitting human-readable logs. |
