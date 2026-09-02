# Jazz + Next.js example

The canonical Next.js server-side rendering / RSC story for Jazz: a Server Component reading the database via `jazz-tools/backend` alongside a Client Component using the standard `jazz-tools/react` hooks. The `next-*` starters under `starters/` are pure client-side; this example is where SSR + Server Actions + `BACKEND_SECRET` live.

- `pnpm dev`

`withJazz` starts a local Jazz server for development and keeps its generated backend credential in the Next server process. No tracked `.env` file or manually configured backend credential is needed. Set `JAZZ_E2E_IN_MEMORY=1` only for isolated browser tests.

## Hot points

- `next.config.ts` uses `withJazz(...)` from `jazz-tools/dev/next`
- Public Jazz connection vars are `NEXT_PUBLIC_JAZZ_APP_ID` and `NEXT_PUBLIC_JAZZ_SERVER_URL`
- Server-side database access uses `getBackendDb()` from `lib/jazz-server.ts`
