# StagePlan on Convex: notes

## Versions

- `convex` 1.50.0, local backend binary `precompiled-2026-10-22-1447815` (downloaded by the CLI)
- `@convex-dev/auth` 0.0.98 with `@auth/core` 0.37.4, **Anonymous** provider
- React 19.2.4, Vite 8.0.1, same Astryx / garden-co design packages as the Jazz copy

Why Convex Auth 0.0.98 and not 2.0.0: 2.0.0 came out on 2026-11-12 with a new
component-based API. Its anonymous method is marked `@internal` ("not public until
the anonymous sign-in method is ready") and isn't in the package exports. 0.0.98 is
still maintained (2026-11-09) and is the line whose docs show the `Anonymous` and
`Password` providers. BandChat and Wequencer use the same line so all three ports match.

## Backend

A local anonymous deployment: `CONVEX_AGENT_MODE=anonymous npx convex dev`, with no
Convex account. It writes `.env.local` (`VITE_CONVEX_URL=http://127.0.0.1:3210`) and
keeps its data in `.convex/local/default/` (SQLite). `scripts/setup-auth.mjs` follows
Convex Auth's manual setup: it generates a JWT key pair and sets `JWT_PRIVATE_KEY`,
`JWKS` and `SITE_URL` on the deployment.

- `pnpm backend`: runs the local backend and pushes functions (`convex dev`)
- `pnpm seed`: `npx convex run seed:run`. This internal action wipes every table,
  then writes `fixtures/stage-plan.mjs` as the seed chief, one internal mutation per
  show. The fixture is imported straight into the Convex bundle. Like the Jazz seed,
  it writes no activity rows, and comments are by the chief.
- `harness.mjs`: `seed()` starts `convex dev` if it isn't running, sets up auth and
  seeds. `start()` runs `vite build`, then serves `dist/` with `vite preview` on
  :4402. `rttUrl` is the backend's `/version`. `stop()` stops both.

The backend always uses ports 3210/3211, so run one Convex hero at a time.

## How the port maps

- **Permissions** (README / Jazz `permissions.ts`, "Who can do what") are checks inside
  every query and mutation, in `convex/lib/access.ts`. Outsiders get `null` for a show,
  the same answer as for a missing show, and `permission_denied` for everything else.
  I checked them with a script against the running backend: outsider reads, a bad
  invite code, crew reading or rotating invites, crew deleting seeded or others' tasks,
  crew editing the show or removing others, assigning to non-crew, and checklist privacy.
- **Joining with an invite**: the `crew.join` mutation checks the code against the
  private `showInvites` row and inserts the membership. The Jazz app's workaround
  (store `inviteCode` on the membership, clear it once accepted) isn't needed, so
  `showCrew` has no `inviteCode` column.
- **Multi-row writes** (create show + chief + invite; task + activity row; remove crew
  + unassign their tasks; new account + demo show) are each one mutation, and so one
  transaction.
- **Ids and timestamps**: components use `_id` instead of `id`, and `_creationTime` /
  an `updatedAt` field instead of Jazz's `$createdAt` / `$updatedAt`. That's a
  mechanical rename in the copied components. JSX, labels, routes and `data-*`
  attributes are unchanged.
- **Rejected writes**: Jazz's `db.onMutationError` toast becomes `useWrite`
  (`src/model/write.ts`), which catches a failed mutation and shows the same toast text.

## Optimistic updates

**None in StagePlan.** Convex's docs present optimistic updates as an opt-in for
specific interactions (their example is sending a chat message). They're not the
default for CRUD like adding or moving a card. So every board write here waits for the
server's mutation result plus the query update.

## Approximations

- **Task notes**: Jazz merges concurrent text edits (`merge("text")`). Here the last
  save wins. Convex has no text-merge primitive in its standard docs (ProseMirror sync
  is a separate component).
- **Sync switch** (top nav): Jazz pauses and resumes syncing to show offline edits. The
  Convex client has no public pause/resume API and doesn't queue writes offline. So the
  switch shows the websocket connection state and is disabled. The harness doesn't
  touch it.
- **Demo-show repair**: the Jazz app has a "The demo show wasn't fully saved / Save the
  rest" banner, plus `ensureChiefSetup`, for writes the server rejects after they were
  applied locally. With Convex the demo show is one transactional mutation, so there's
  nothing partial to repair. That banner and the repair code are dropped. This, the
  sync switch and the `canDelete` effect are why `ui` counts about 120 lines fewer than
  the Jazz copy.
- **Anonymous accounts**: a Convex Auth anonymous session lives in `localStorage`, like
  Jazz's device account. A fresh browser context is a new guest, and a reload keeps the
  session.

## LOC categories (`loc.json`)

- `schema`: `convex/schema.ts`
- `permissions`: `convex/lib/access.ts`, the helpers every function calls
- `server`: the Convex functions, auth config and HTTP routes
- `data`: client glue (`src/App.tsx`, `src/account.ts`, `src/main.tsx`, `src/model/`)

Excluded: `convex/_generated/`, the seed and the harness plumbing.
