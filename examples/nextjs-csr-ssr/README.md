# Jazz + Next.js example

The canonical Next.js server-side rendering / RSC story for Jazz: a Server Component reading the database via `jazz-tools/backend` alongside a Client Component using the standard `jazz-tools/react` hooks. The `next-*` starters under `starters/` are pure client-side; this example is where SSR + Server Actions + `BACKEND_SECRET` live.

- `cp .env.example .env` and fill in values (`NEXT_PUBLIC_JAZZ_APP_ID`, `NEXT_PUBLIC_JAZZ_SERVER_URL`, `BACKEND_SECRET`)
- `pnpm run dev`

The browser e2e (`pnpm test:e2e`) runs the app against the sync server named in
`NEXT_PUBLIC_JAZZ_SERVER_URL` — the hosted server by default — so it needs a real
app id and `BACKEND_SECRET`.

## Hot points

- `next.config.ts` uses `withJazz(...)` from `jazz-tools/dev/next`
- Public Jazz connection vars are `NEXT_PUBLIC_JAZZ_APP_ID` and `NEXT_PUBLIC_JAZZ_SERVER_URL`
- The SSR example still keeps `BACKEND_SECRET` explicit because backend access is server-only
