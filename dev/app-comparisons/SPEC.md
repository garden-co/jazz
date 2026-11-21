# Jazz hero apps on other sync stacks: first-signal comparison

Goal (2026-11-14): a first signal on **relative performance** and **lines of code** of
Jazz versus alternative tech, using **our hero example apps**, several of them to cover different
load patterns, with the apps as similar and the comparison as fair as we can make it. This is a
first signal, not a benchmark suite: every number says what it measured and where.

## The apps

Each hero app is copied from the jazz monorepo (`examples/<hero>/apps/<variant>`, at
`de1cc17c9a`) into `<hero>/jazz/`, on published `jazz-tools@2.0.0-alpha.62`, then ported to each
other stack as `<hero>/<stack>/`.

| hero | Jazz variant copied | load pattern | auth |
|---|---|---|---|
| `stage-plan` | `react-localfirst` (React + Vite) | permissioned task board: CRUD, filtered live columns, inherited row permissions, invite links | anonymous guests (device accounts) |
| `band-chat` | `react-hono-betterauth` (React + Vite + Hono) | chat: appends, newest-page reads over a large history, scroll-back, private rooms by membership | email + password |
| `wequencer` | `next-betterauth` (Next.js) | many small subscriptions (1,024-pad grid), rapid toggles, concurrent edits to the same rows | email + password |
| `big-label` | `examples/big-label` (Next.js, at jazz main `5496e88`) | multi-tenant SaaS: membership-scoped reads, roles, indexed tenant lists with search and paging, relations, serializable writes | email + password |

Stacks, first round: `instantdb` (hosted: an ephemeral/unclaimed app), `convex` (local anonymous
deployment, `npx convex dev`, no account), `zero` (Rocicorp Zero: local `zero-cache` + local
Postgres). Jazz runs its in-app backend locally.

## Porting rules (the fairness contract)

- **Same UI.** Copy the Jazz app's components, styles, routes and copy text; change only what
  reads and writes data (hooks, actions, providers, schema, permissions, server). The rendered
  DOM, labels and URLs stay the same, so `harness/` drives every port with the same script.
- **Same behaviour**, including the permission rules in the hero's README ("Who can do what"),
  **enforced server-side** by the stack's own mechanism (Instant perms, Convex function checks,
  Zero permissions / mutators). If a behaviour can't be done, approximate it and write it down.
- **Idiomatic for the stack.** Follow its official docs and quickstart for structure, hooks and
  auth; use its optimistic updates / local caching only as its docs present them as standard. No
  stack-specific perf tricks that the Jazz app doesn't get too.
- **Auth.** StagePlan: the stack's anonymous/guest user. BandChat, Wequencer and BigLabel: email + password
  where the stack has it; otherwise the stack's documented custom-auth route (e.g. a server that
  mints tokens). Sign-in is untimed.
- Features out of a first round's reach (attachments, sketches, audio, presence) may be left out
  **in every stack alike**, including the Jazz copy's measured paths: the harness doesn't touch them.
- Record versions, approximations and anything that didn't work in `<hero>/<stack>/NOTES.md`.

## Seed

`fixtures/<hero>.mjs` generates stack-agnostic records deterministically (fixture-local keys;
each stack maps them to its own ids). Every app has `pnpm seed`, writing them through the stack's
admin/server API (untimed), as the fixture's seed user, so harness users can't delete seeded rows.

## Harness

`harness/` (Playwright, Chromium headless, same machine) runs each scenario against the app's
**production build** served locally, 1 warm-up then **median of 7**, after a 5 s settle before reloads and before the echo phases (echo scenarios: 20 samples).
Each app provides `harness.mjs`: `start()` (serve the built app + its backend, return the URL),
`signIn(page, user)` and `reset()`/`seed()` hooks. Common metrics for every hero: `bundle_gzip`
(JS shipped) and `rtt` (median HTTP round trip to the stack's server, so network distance isn't
read as engine speed).

**StagePlan** (a fresh guest joins "The Late Lanterns" through its invite link; 3,000 tasks over
30 shows)
- `sp_join_board`: open the join link in a fresh context → all 100 task cards on the board.
- `sp_reload_board`: reload → all 100 cards (whatever the stack caches is warm).
- `sp_add_task`: add a task → its card is on the board.
- `sp_move_done`: move a card to Done → it shows in the Done column.
- `sp_cross_client`: guest A adds a task → guest B's board shows it.

**BandChat** (users a and b are members of all 256 rooms; 10,000 messages, 2,000 in "Busy room")
- `bc_open_room`: signed in, fresh context, open `/?room=<busy>` → the newest 200 messages (the UI's page) are shown.
- `bc_send`: send a message → it is shown in the room.
- `bc_switch_room`: switch between "Busy room" and another room through its room link → that room's messages are shown. (The UI keeps a fixed 200-message window and has no scroll-back paging, so `bc_scroll_back` was dropped.)
- `bc_cross_client`: a sends → b, in the same room, sees it.
- `bc_resume`: reload a caught-up client → newest message visible.

**Wequencer** (users a and b are editors of "Bench groove": 16 tracks × 64 steps, ~25% on)
- `wq_open_pattern`: open the session in a fresh signed-in context → all 1,024 pads rendered, the right number on.
- `wq_reload`: reload → the same.
- `wq_toggle_pad`: click a pad → it shows the new state.
- `wq_cross_client`: a toggles a pad → b sees it.
- `wq_concurrent`: a and b each toggle 32 pads (16 the same) at once → both grids equal and stable.

**BigLabel** (8 labels × 512 releases = 4,096, 64 artists, 6 members, 3 teams and 2 catalogues each; user a is
admin of label 0, "Northwind Records", and editor of label 1; b is editor of label 0; the Releases page shows 20
rows, newest first)
- `bl_open_releases`: signed in, fresh context, label 0 chosen, open `/#/releases` → its first 20 releases, in order.
- `bl_reload`: reload → the same.
- `bl_search`: enter a title word in the search box → the first page of matching releases (a different word each
  sample; clearing it is untimed).
- `bl_next_page`: "Go to next page" → the next 20 releases (page after page, each shown for the first time).
- `bl_switch_label`: pick label 1 in the side nav's label menu → its Overview with its name and counts (64 artists,
  512 releases, 7 members, 3 teams). Switching back is untimed. Samples after the warm-up switch to a label
  shown before, so whatever the stack keeps of it is warm (like `bl_reload`).
- `bl_update_status`: on a release's page, Edit → Status → Save (timed from the Save click) → the new status shows.
- `bl_cross_client`: a renames a release the same way → b, on label 0's Releases page, sees the new title.

## LOC

`harness/loc.mjs` counts non-blank, non-comment lines per app, by category from the app's
`loc.json` (globs): `schema`, `permissions`, `server` (backend, auth, seed excluded), `data`
(hooks, actions, providers, glue), `ui` (the shared components, for reference: nearly equal
by construction). Generated code (e.g. Jazz `migrations/index.ts`, Convex `_generated/`),
lockfiles, config and tests are excluded. Two totals: `data_layer` (`schema + permissions + server
+ data`) and `total` (everything, `ui` included). Jazz's components call its hooks inline, so a
port rewrites lines inside `ui` too: `total` is the honest whole-app number, and the `ui` diff
against the Jazz copy shows how much of the UI the port had to touch.

## Layout and branches

Originally `<hero>/<stack>/` per app, `fixtures/`, `harness/` and `results/<date>/` in a separate repository,
with each stack's port on branch `port/<stack>`. Since the move into the jazz repo, the ports are
`examples/<hero>/apps/<base>-<stack>`, and the Jazz side is the upstream hero app itself: see
[`README.md`](README.md).
