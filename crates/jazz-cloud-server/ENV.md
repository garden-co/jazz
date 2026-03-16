# Environment variables

| Variable | CLI flag | Default | Description |
|----------|----------|---------|-------------|
| `JAZZ_INTERNAL_API_SECRET` | `--internal-api-secret` | — | Secret for internal API (app management) |
| `JAZZ_SECRET_HASH_KEY` | `--secret-hash-key` | — | Key for hashing backend/admin secrets |
| `JAZZ_JWKS_CACHE_TTL_SECS` | — | `300` | JWKS cache TTL in seconds |
| `JAZZ_JWKS_MAX_STALE_SECS` | — | `300` | Max time (past TTL) to serve stale JWKS on fetch failure |
