# BandChat (Vite React + Hono + Better Auth)

The same BandChat as [`../nextjs-betterauth`](../nextjs-betterauth), as a
single-page React app with Jazz running inside a Hono server. Read that app's
README for what BandChat demonstrates; this one only differs in how it is
wired.

- `server/jazz.ts` calls `createJazz`, and `server/index.ts` mounts it under
  `/jazz` (plain requests through `jazz.fetch`, sync sockets through
  `jazz.attach(server)`), next to Better Auth at `/api/auth/*`. The browser
  talks only to this app's own origin.
- Better Auth's tables live in BandChat's own Jazz (`jazz.db`), and
  `authenticate` reads the Better Auth session cookie. No JWTs.
- In production the Hono server also serves the built SPA from `dist/`. In
  development Vite serves the app on http://127.0.0.1:3000 and proxies `/api`
  and `/jazz` to the Hono server on port 3001.

## Run it

```sh
pnpm dev          # http://127.0.0.1:3000, data in .jazz/, local secrets generated
pnpm build        # checks the deployment config, then builds dist/
pnpm start        # production server on APP_ORIGIN's port (or PORT)
pnpm test:e2e     # Playwright smoke against `pnpm dev`
```

Deployments set `APP_ORIGIN`, `VITE_JAZZ_APP_ID` and `BETTER_AUTH_SECRET`, and
optionally `JAZZ_SERVER_URL` with the app's secrets (see `.env.example`).
