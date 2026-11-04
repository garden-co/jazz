# Jazz + Next.js example

The canonical Next.js server-side rendering / RSC story for Jazz: a Server Component reading the database via `jazz-tools/backend` alongside a Client Component using the standard `jazz-tools/react` hooks. Jazz runs inside the Next app itself (`createJazz`), so server components, server actions and the browser all use the same Jazz.

- `pnpm dev`

`pnpm dev` runs `next dev`; Jazz keeps its data in `.jazz/`. No `.env` file or credential is needed. Set `JAZZ_IN_MEMORY=1` only for isolated browser tests.

## Hot points

- `next.config.ts` only marks `jazz-napi` as a server external package
- `lib/jazz-server.ts` creates the app's Jazz; `app/jazz/[...path]/route.ts` serves browser sync on the app's own origin
- The browser needs no Jazz env vars: it syncs with `/jazz`
- Server-side database access uses `getBackendDb()` (the backend's own `db`)
