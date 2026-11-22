# BandChat, end-to-end encrypted (Vite React + Hono + Better Auth)

[`../react-hono-betterauth`](../react-hono-betterauth) with message text,
attachment names and read markers end-to-end encrypted: each room is an
encrypted space, and only its members hold its key. The server stores and
relays those columns as ciphertext.

- `server/jazz.ts` calls `createJazz`, and `server/index.ts` mounts it under
  `/jazz` (plain requests through `jazz.handle`, sync sockets through
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
pnpm test         # permissions, server and deploy tests
pnpm test:e2e     # Playwright smoke against `pnpm dev`
```

Deployments set `APP_ORIGIN`, `JAZZ_APP_ID` and `BETTER_AUTH_SECRET`, and
optionally `JAZZ_SERVER_URL` with the app's secrets (see `.env.example`).
