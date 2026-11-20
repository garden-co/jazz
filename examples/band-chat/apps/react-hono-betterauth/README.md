# BandChat (Vite React + Hono + Better Auth)

The same BandChat as [`../nextjs-betterauth`](../nextjs-betterauth), as a
single-page React app with Jazz running inside a Hono server. Read that app's
README for what BandChat demonstrates; this one only differs in how it is
wired.

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
pnpm test:e2e     # Playwright smoke against `pnpm dev`
```

Deployments set `APP_ORIGIN`, `JAZZ_APP_ID` and `BETTER_AUTH_SECRET`, and
optionally `JAZZ_SERVER_URL` with the app's secrets (see `.env.example`).

## Deploy to Cloudflare

The same app runs entirely on Cloudflare, in one Worker (`server/worker.ts`,
`wrangler.jsonc`). There is no Node server and no Vercel:

- `JazzCore`, a SQLite-backed Durable Object, is the app's Jazz core and holds
  its data. Attachments and other large values go to an R2 bucket, bound as
  `JAZZ_BLOBS`; only their index stays in the object's SQLite.
- `JazzBackend`, a second Durable Object, runs `createJazz` (on jazz-wasm),
  Better Auth and the Hono routes. A Jazz keeps its socket to the core open
  across requests, which only a Durable Object can.
- The Worker's `fetch` serves the built SPA from static assets and forwards
  `/api`, `/jazz` and `/health` to `JazzBackend`.

```sh
pnpm cf:dev                                 # local, with secrets from .dev.vars
npx wrangler r2 bucket create band-chat-jazz-blobs  # once: the app's large values
npx wrangler secret put BETTER_AUTH_SECRET  # a random secret
npx wrangler secret put JAZZ_CORE_SECRET    # 32+ random characters; keep it across deployments
pnpm cf:deploy                              # set APP_ORIGIN in wrangler.jsonc to the deployed URL first
```

For `pnpm cf:dev`, put both secrets in `.dev.vars` (`BETTER_AUTH_SECRET=…`,
`JAZZ_CORE_SECRET=…`). The Worker, with its two wasm modules (core and
backend), is about 20 MB, under Workers' 64 MiB limit on every plan.
