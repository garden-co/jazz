# Harness

```sh
cd dev/app-comparisons/harness && pnpm install --ignore-workspace
node run.mjs stage-plan/jazz [--variant thin] [--runs 7] [--samples 20]   # writes ../results/<date>/<hero>.<stack>.<variant>.json
node loc.mjs                                                              # every target with a loc.json
```

A target `<hero>/<stack>` resolves to a directory through `apps.mjs` (see `../README.md`): the ports are
`examples/<hero>/apps/<base>-<stack>`, and `<hero>/jazz` is the upstream hero app, whose hooks sit in its
`comparison/` folder.

## What each app provides: `harness.mjs`

```js
export const distDir = "dist";            // built JS, for bundle size (relative to the app)
export async function seed({ variant }) {} // reset the backend and write fixtures/<hero>.mjs; return ctx (below)
export async function start({ variant }) {} // build the production app, start it and its backend:
                                           // return { url, stop, where, rttUrl? } (where: "local ..." / "hosted, <region>")
export async function signIn(page, userKey, ctx) {} // band-chat, wequencer, big-label only: sign in as fixture user "a" or "b"
```

`ctx` per hero, as the scenarios use it:
- `stage-plan`: `{ showId, code }` of fixture show `show-0` ("The Late Lanterns"); the join link is `#/join/<showId>/<code>`.
- `band-chat`: `{ busyRoomId, quietRoomId, users }` (fixture rooms `room-0`, `room-1`; `users` = the fixture's USERS, for signIn). Users a and b must be members of every room, admitted the app's own way where the stack allows it. Selectors: `[data-message-id]` bubbles, the "Message input" textbox, the "Send" button, room links `a[href*="room=<id>"]`.
- `wequencer`: `{ sessionId, onPads, users }` (fixture session `session-0`, its number of enabled pads). Users a and b are editors. The session page is `/dashboard/<sessionId>`; pads are `button.pad` with `aria-label="<track>, step <n>"` and `aria-pressed`.

- `big-label`: `{ labelId, secondLabelId, renameReleaseId, statusReleaseId, users }`: the stack's ids of fixture labels
  `label-0` ("Northwind Records") and `label-1` ("Low Tide Records"), of label 0's newest release (renamed by
  `bl_cross_client`) and second-newest (`bl_update_status`); `users` = fixture users `a` and `b` (email + password).
  User a is admin of label 0 and editor of label 1, b editor of label 0, all admitted the app's own way where the
  stack allows it (see `examples/big-label/comparison/seed.ts`). Every fixture member is a signed-up user and has run the app's
  bootstrap (their personal label). Expected rows, pages and counts come from `fixtures/big-label.mjs`, so no other ids
  are needed. Selectors (from the copied Astryx UI):
  - label 0 is preselected with `localStorage["big-label-organization"] = labelId` (an init script; the app remembers
    the switcher's choice there), then `/#/releases` opens the Releases page;
  - release rows: `tr` whose first `td` links `a[href="#/releases/<id>"]` (the title); the third `td` is the catalogue
    number (columns Title, Artist, Cat. no., Release date, Status; 20 rows a page, newest first);
  - search: `input[placeholder="Search title or catalogue number"]`; pager: `button[aria-label="Go to next page"]`;
  - label switcher: `button[aria-label="Open menu"]` in the side nav, then the `[role=menuitem]` whose first text is
    the label's name; the Overview's `h1` is the label's name, its stats are an `h2` right after the label text
    ("Artists", "Releases", "Members", "Teams");
  - release page `/#/releases/<id>`: `dt`/`dd` metadata ("Status" → the status badge, e.g. "Scheduled"); the "Edit"
    button opens `dialog[open]` "Edit release"; its fields are found through their `<label for>` (text starting
    "Title", "Status"): the Title input, the Status selector button (`role=option`s "Planning", "Scheduled",
    "Released"), and the "Save" button.

Also export `lastSeed()` (the ctx of the last seed, for `--no-seed`).

`harness/adapters/vite-app.mjs` has `sh()` and `serve()` helpers. Selectors come from the copied UI
(`data-task-id`, `data-status`, the "Add a task" placeholder, the board's arrow-key moves), so a
port that keeps the UI unchanged needs no harness changes.
