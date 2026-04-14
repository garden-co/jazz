# Environment variables

| Variable                   | CLI flag            | Default | Description                                              |
| -------------------------- | ------------------- | ------- | -------------------------------------------------------- |
| `JAZZ_JWKS_URL`            | `--jwks-url`        | —       | JWKS endpoint for JWT validation                         |
| `JAZZ_BACKEND_SECRET`      | `--backend-secret`  | —       | Secret for backend session impersonation                 |
| `JAZZ_ADMIN_SECRET`        | `--admin-secret`    | —       | Secret for admin operations (schema/policy sync)         |
| `JAZZ_ALLOW_ANONYMOUS`     | `--allow-anonymous` | `false` | Allow anonymous local auth mode                          |
| `JAZZ_ALLOW_DEMO`          | `--allow-demo`      | `false` | Allow demo local auth mode                               |
| `JAZZ_JWKS_CACHE_TTL_SECS` | —                   | `300`   | JWKS cache TTL in seconds                                |
| `JAZZ_JWKS_MAX_STALE_SECS` | —                   | `300`   | Max time (past TTL) to serve stale JWKS on fetch failure |
| `JAZZ_OTEL`                | —                   | `0`     | Set to `1` to enable OpenTelemetry tracing               |
