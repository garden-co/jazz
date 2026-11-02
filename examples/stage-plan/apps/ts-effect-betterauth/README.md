# StagePlan (TypeScript + Effect + Hono + Better Auth)

StagePlan's show board written with Effect on both sides of the wire, with no
UI framework. Read the [family README](../../README.md) for the data model and
permissions; this app shows the Effect bindings.

- `src/model.ts` holds StagePlan's writes as Effects over the `Jazz` service
  (`jazz-tools/effect`): creating a show with its chief and invite, adding and
  moving tasks, and adding the standard pre-show checklist, each one
  transaction.
- The browser (`src/main.ts`, `src/planner.ts`) follows Better Auth's session
  with `createJazzApp({ serverUrl: "/jazz", auth: betterAuth(authClient) })`,
  wraps the client's database with `Jazz.fromDb`, and draws each live query
  from `jazz.stream(...)` with plain DOM.
- The Hono server (`server/index.ts`) mounts the app's Jazz (`createJazz` in
  `server/jazz.ts`) under `/jazz` and Better Auth under `/api/auth`.
  `POST /api/shows/:id/checklist` (`server/checklist.ts`) runs the same
  `addChecklist` Effect as the requesting user (`Jazz.fromDb(await
jazz.forRequest(request))`), so permissions.ts decides, exactly as for the
  browser.

## Run it

```sh
pnpm dev          # http://127.0.0.1:3000, data in .jazz/
pnpm build        # builds dist/ (needs VITE_JAZZ_APP_ID)
pnpm start        # production server on APP_ORIGIN's port (or PORT)
pnpm test:e2e     # Playwright smoke against `pnpm dev`
```

Production sets `APP_ORIGIN`, `VITE_JAZZ_APP_ID` and `BETTER_AUTH_SECRET`, and
optionally `JAZZ_SERVER_URL` with `JAZZ_ADMIN_SECRET` and `BACKEND_SECRET`.
