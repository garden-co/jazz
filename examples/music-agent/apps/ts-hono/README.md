# MusicAgent (vanilla TypeScript + Hono)

The same MusicAgent as [`../nextjs`](../nextjs), with no UI framework: the
browser app is plain TypeScript and DOM, and Jazz, Better Auth and the agent
run inside a Hono server. Read the [family README](../../README.md) for what
MusicAgent demonstrates; this app only differs in how it is wired.

- `server/index.ts` mounts the app's Jazz (`createJazz` in
  `src/lib/backend-jazz-client.ts`) under `/jazz` (plain requests through
  `jazz.fetch`, sync sockets through `jazz.attach(server)`), Better Auth under
  `/api/auth`, and the agent routes (`src/server/routes.ts`). Replies run in
  the background after the response; the browser watches the turn's row.
- `src/client/main.ts` follows Better Auth's session with
  `createJazzApp({ serverUrl: "/jazz", auth: betterAuth(authClient) })`, and
  `src/client/chat.ts` renders conversations from `db.subscribe` results.
- In production the Hono server also serves the built app from `dist/`. In
  development Vite serves it on http://127.0.0.1:3000 and proxies `/api` and
  `/jazz` to the Hono server on port 3001.

## Run it

```sh
pnpm dev          # http://127.0.0.1:3000, scripted agent, data in .jazz/
pnpm build        # builds dist/ (needs VITE_JAZZ_APP_ID)
pnpm start        # production server on APP_ORIGIN's port (or PORT)
pnpm test:e2e     # Playwright smoke against `pnpm dev`
```

Production sets `APP_ORIGIN`, `VITE_JAZZ_APP_ID` and `BETTER_AUTH_SECRET`.
`MUSIC_AGENT_PROVIDER=anthropic` with `ANTHROPIC_API_KEY` answers with Claude
instead of the scripted agent.
