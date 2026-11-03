# Jazz + Next.js example

The canonical Next.js server-side rendering / RSC story for Jazz: a Server Component reading the database via `jazz-tools/backend` alongside a Client Component using the standard `jazz-tools/react` hooks. This example is where SSR + Server Actions + `BACKEND_SECRET` live.

- `pnpm dev`

`pnpm dev` starts a local Jazz core (`dev/examples/with-local-core.mjs`), deploys this schema to it, and passes its URL, app id and a generated backend secret to `next dev`. No tracked `.env` file or manually configured backend credential is needed. Set `JAZZ_IN_MEMORY=1` only for isolated browser tests.

## Hot points

- `next.config.ts` only marks `jazz-napi` as a server external package
- Public Jazz connection vars are `NEXT_PUBLIC_JAZZ_APP_ID` and `NEXT_PUBLIC_JAZZ_SERVER_URL`
- Server-side database access uses `getBackendDb()` from `lib/jazz-server.ts`
