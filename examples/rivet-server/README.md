# Jazz server on Rivet

Jazz client → Rivet TS actor → Rivet Rust core actor → actor-local SQLite.

1. Run `crates/jazz-rivet` in runner pool `jazz` (Rivet 3.1.1+).
2. Create a `jazzServer` actor in pool `jazz-server` using Rivet's dashboard/API.
3. Configure this runner's `.env.local`:

```dotenv
RIVET_ENDPOINT=https://<namespace>:<server-token>@<rivet-host>
RIVET_POOL=jazz-server
JAZZ_CORE_SECRET=<same-secret-as-core-at-least-32-characters>
JAZZ_PUBLIC_URL=https://<rivet-host>/gateway/<actor-id>@<public-token>/request/jazz
JAZZ_ALLOWED_ORIGINS=https://<client-app-host>
```

Run `pnpm --filter rivet-server dev`. Ordinary Jazz clients use `JAZZ_PUBLIC_URL`
as `serverUrl`; no Rivet client SDK required. Hibernation is disabled.

Verify a deployed server with `pnpm --filter rivet-server test:integration`.
