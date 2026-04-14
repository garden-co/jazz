# Environment variables

| Variable                   | CLI flag                | Default | Description                              |
| -------------------------- | ----------------------- | ------- | ---------------------------------------- |
| `JAZZ_INTERNAL_API_SECRET` | `--internal-api-secret` | —       | Secret for internal API (app management) |
| `JAZZ_SECRET_HASH_KEY`     | `--secret-hash-key`     | —       | Key for hashing backend/admin secrets    |

JWKS cache TTL and max-stale are configured per app via the management/internal app APIs next to `jwks_endpoint`, not as global server environment variables.
