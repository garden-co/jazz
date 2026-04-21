---
"jazz-tools": minor
---

Add static external JWT verification alongside JWKS-based verification in `jazz-tools`.

The Rust server CLI now accepts `--jwt-public-key` / `JAZZ_JWT_PUBLIC_KEY`, and the TypeScript backend `createJazzContext(...)` path now accepts `jwtPublicKey`. Both server entrypoints reject configs that set both `jwksUrl` and the new static-key option at the same time.
