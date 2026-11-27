# BigLabel on Jazz: why writes, paging and cross-client trail (2026-11-14)

Scope: `big-label/jazz` on published jazz-tools 2.0.0-alpha.62. Three scenarios where report v3 has Jazz behind
every other stack:

| scenario (report v3, median ms) | Jazz thin+upgrade | Jazz now | Jazz thin | best other |
|---|---|---|---|---|
| `bl_update_status` | 126 | 126 | 128 | Zero 7, Neon 13 |
| `bl_next_page` | 146 | 137 | 32 | Postgres 29, Zero 48 |
| `bl_cross_client` | 144 | 141 | 114 | Zero 50, Postgres 62, Convex 63 |

All numbers below come from `harness/biglabel-writes/probe.mjs`, on the same 2-vCPU sandbox class as the report.
The probe times from the in-page click to a MutationObserver/rAF match, so it reads 20–40 ms lower than `run.mjs`
for the same build. Compare rows within this README, not with the report. Noise between runs of the same build is
about ±10 ms (update, next) and ±20 ms (cross). Every claim below held in at least two runs.

## Answers

**1. `bl_update_status`: the guess is half right.**
- **Local-first does echo locally.** The status badge changes when the serializable transaction commits locally, and
  the commit frame leaves *after* the DOM change.
- **But the local commit waits for two sequential server reads** of the catalogue number's holders:
  - **The dialog's pre-check.** `submit` in `src/components/forms.tsx` calls `db.all(...)`. A browser read with no
    tier is local-first with `firstLoadRemoteWaitMs: 5000` (`resolveEffectiveQueryExecutionOptions`,
    `runtime/client.ts`), so a one-shot query always asks the server (`all_serialized_query`, `sync.rs:14662`).
  - **`saveRelease`'s `tx.all`.** A serializable read always fetches from the authority unless the tier is the
    internal `local-only` (`read_admitted`, `crates/jazz/layers/db/src/db.rs:10602`). Passing
    `firstLoadRemoteWaitMs: 0` does nothing there.
  - Each read is one round trip (~14 ms) plus ~20 ms of client hops between them (page → worker → page → React
    → transaction).
- **Thin has no local echo.** A save is five sequential POSTs: pre-check query, begin, `tx.all`, write, commit.
  The page shows the change only when the commit's subscription update comes back (~95 ms here).

**2. `bl_next_page`: it is the offset prefix, not sort, SQLite indexes or rendering.**
- **The server resends the whole prefix on every page.** The client rewrites `limit 21 offset n` to `limit n+21
  offset 0` before subscribing, so it can apply the offset itself (`remote_query`, `sync.rs:3209-3216`).
- **Rows the client already holds are not skipped.** Skipping needs a resume of the *same* query
  (`held_as_of`, `sync.rs:11731-11864`), and every page is a new query.
- **So each page costs more than the last:** +13.6 KB and about +4 ms per page.

  | page | 3 | 6 | 9 | 12 | 14 |
  |---|---|---|---|---|---|
  | KB received | 49 | 87 | 127 | 168 | 196 |
  | ms (probe median) | 70 | 85 | 93 | 106 | 119 |

  The harness pages up to page 22 (~300 KB), which is why its median is 137–146.
- **CPU per page:** the worker spends ~45 ms, mostly WASM/SQLite ingesting the rows plus 6 ms of `postMessage`.
  The page spends ~80 ms, ~30 of it the table render, the same as thin (`raw/default.profile/*.cpuprofile`, via
  `profile-top.mjs`).
- **Before the request leaves,** the worker waits ~15 ms for its 16 ms server-pump debounce.
- **The old pages stay subscribed.** The subscription cache keeps unmounted queries for 30 s
  (`cleanupDelayMs = 30_000`, `subscriptions-orchestrator.js`). See fix #4 for what that costs the writes after
  paging.

**3. `bl_cross_client`: mostly the rename's reads and a's outbound queue, not the HTTP stream.** One sample of
local-first, upstream, 179 ms on b:

| ms | what happens |
|---|---|
| 0–24 | pre-check server read |
| 24–60 | client hop, then the serializable read from the server |
| 81 | a's own page shows the new title (local commit) |
| 115 | a's commit frame leaves, ~34 ms after a's render (`LOW-HANGING-FRUIT.md` #2) |
| 129 | the commit POST gets its 204 |
| 155 | b's stream delivers the 1 KB update |
| 179 | b's DOM shows it (~24 ms of worker → page → render) |

- **The HTTP stream itself costs little.** b's chunk arrives within 0–25 ms of the commit's 204.
- **Every POST pays for auth.** The server's sync-POST handler takes a median 10.6 ms (p90 32), and 7.6 ms of it
  is the app's `authenticate` (Better Auth `getSession`, a database read through Jazz). `ownStream`
  (`backend/create-jazz.js`) runs it on every POST on an open stream.
- **In the harness order, the stale page subscriptions add another ~55 ms** (fix #4).

## Ranked fixes

Gain per effort. Effort: S = hours, M = a day or two, L = architectural. "lf" = local-first (default/now).

| # | Fix | Where | Measured / estimated gain | Effort |
|---|---|---|---|---|
| 1 | **Drop the release dialog's separate pre-check**: submit runs `saveRelease` at once, awaits its local commit (the callback already reads the holders and throws `CatalogNumberTakenError`), and shows that error on the field. Same check, one read fewer. | app: `src/components/forms.tsx` `ReleaseDialog` `submit`/`save` (upstream `examples/big-label`) | lf update **91 → 53 ms**, cross 163 → 98 (measured, `a`). Thin: one POST fewer, within noise (`t1/t2`) | S (prototyped, knob `no-precheck`) |
| 2 | **Let a serializable read use the local replica.** Honour `firstLoadRemoteWaitMs: 0` (or a tier option) in `tx.all`/`tx.one` of a serializable transaction. The authority re-runs every read at commit and rejects a changed one (`serialization_conflict`, `sync.rs:14064-14155`). Per the module doc, a stale replica "can only cause a conflict, never a non-serializable commit" (`sync.rs:38-44`). | jazz: `Transaction.readAll` in `runtime/db.ts` → `local-only`; core `read_admitted` (`db.rs:10602`); then `saveRelease` passes the option | with #1: lf update **53 → 12.5 ms** (Zero 7, Neon 13), a's own echo 12 ms, cross 98 → 77 (measured, `b`) | S to prototype (2 lines, done: `tx-read-local`), M as public API + docs |
| 3 | **Stop calling `authenticate` on every stream POST.** Cache the answer per stream session, keyed by the request's credential headers, until its `expiresAt` (`revokeUser` clears it). App-side today: Better Auth's documented `session.cookieCache`. | jazz: `ownStream` in `backend/create-jazz.ts`; app: `src/lib/auth.ts` | sync POST handler 10.6 → 2.9 ms, auth 7.6 → 1.4 ms. Thin: update **97 → 80**, cross 97 → 79, next 30 → 24. lf, writes only: update 90 → 76, cross 141 → 130 (measured, `cookiecache*`) | S (app knob prototyped: `BL_AUTH_COOKIE_CACHE=1`) |
| 4 | **Don't keep unmounted windowed queries live for 30 s**, or don't refresh them on the server for every write. After paging, each status change pulls 25–33 KB of page windows to the writer (0.4 KB without), and the server recomputes ~20 offset windows per write. | jazz: `cleanupDelayMs` in `subscriptions-orchestrator.ts` (shorter for offset/limit queries, or on navigation); server fanout for offset windows (`fanout.rs:975`) | with #1–#3: update **23 → 13 ms**, cross **127 → 74 ms** (measured, `gap0` vs `gap31000`). That is the harness order: `bl_next_page` runs just before both write scenarios | S (tune) / M (fanout) |
| 5 | **Don't resend the prefix for an offset page.** Either dedupe per session (ship a row version once per sync session, whichever subscription asks), or send the prefix as ids/versions only. App-side alternative: keyset (cursor) paging, as the Zero port does. | jazz: `remote_query` (`sync.rs:3191-3227`), opening pass `held_as_of`/`watch.hold` (`sync.rs:11731-11864`) | lf next page flat at ~55–60 ms for every page (extrapolated from the per-page fit: the page-3 cost minus 35 KB × 0.3 ms/KB). Harness median 137–146 → ~60 (**−80 ms**). Session dedupe also shrinks #4's write fanout | M (keyset in the app) / L (session dedupe) |
| 6 | **Post the first outbound frame synchronously** (`LOW-HANGING-FRUIT.md` #2, re-applied here) | jazz: `BrowserWorkerTransportPump.scheduleOutboundDrain` | on top of #1+#2: cross 77 → 66 (`b`); 131 → 128 in `d`, within noise | S (prototyped) |
| 7 | **Pipeline the thin transaction**: send begin + read in one POST and write + commit in one, instead of four sequential round trips | jazz: thin runtime transaction path | est. thin update and cross −25 to −35 ms (two of four ~15 ms round trips; −10 to −15 after #3) | M (not prototyped) |

With **#1–#4 together**, local-first measures update **12–13 ms** and cross-client **63–74 ms** (Zero 50, Convex
63, Postgres 62) (`cookiecache-w*`, `gap31000`). #5 is the one that closes `bl_next_page`.

**Not a win: `SERVER_PUMP_DEBOUNCE_MS = 0`** (worker and page, apply.py `debounce0`). With it, cross was 30–35 ms
*slower* (176 vs 141) and the fixed update 23–31 instead of 12.5 (`c1`–`c3` vs `b`). More, smaller server pumps
compete for the 2 cores. Don't ship it without a better heuristic (for example, flush the first frame at once and
debounce the rest).

## Fairness notes

- #1 and #4's "navigate away" are app changes to the Jazz copy only. #1 belongs upstream in
  `examples/big-label`, so it's a Jazz-team call, not a port tweak. The other ports keep their own pre-check
  behaviour; check that before re-running the matrix with it.
- #3's app variant is a Better Auth setting. Every port that uses Better Auth could turn it on too. The Jazz-side
  fix needs no app change.
- The probe doesn't prove that a stale local read in #2 always ends in a conflict rejection. That rests on the
  `sync.rs` code and doc cited above. Jazz should add a test for it before shipping.

## Files

- `harness/biglabel-writes/serve.mjs`: seed and serve a variant. `restart.sh <variant>`: rebuild and serve (env:
  `BL_ROUTE_TIMING`, `BL_AUTH_COOKIE_CACHE`).
- `harness/biglabel-writes/probe.mjs`: drives `bl_next_page`, `bl_update_status` and `bl_cross_client` like
  `scenarios/big-label.mjs`. It records CDP Network for the page **and** the SharedWorker (POST and stream chunk
  times, and with `--bodies` the frame contents), DOM mutation times, long tasks and optional CPU profiles
  (`--profile`). Flags: `--fix <knobs>` sets `localStorage["bl-fix"]`, `--only`, `--gap`.
- `harness/biglabel-writes/apply.py <pristine-dist> <patches>`: dist patches `tx-read-local`,
  `sync-outbound-first` (both gated by the knob) and `debounce0`.
- `harness/biglabel-writes/profile-top.mjs`: self time per function and bucket of a `.cpuprofile`.
- `sweep-update.sh`: one probe run per knob set.
- App knobs (inactive unless set): `bl-fix` = `no-precheck`, `precheck-local`, `tx-local` in
  `src/components/forms.tsx` and `src/lib/mutations.ts`. `BL_ROUTE_TIMING=1` (route + auth timing to stderr) in
  `app/jazz/[...path]/route.ts` and `src/lib/jazz.server.ts`. `BL_AUTH_COOKIE_CACHE=1` in `src/lib/auth.ts`.
- `raw/`: per-run JSON (`next.json`, `update.json`, `cross.json` with every sample's network timeline;
  `summary.json`), CPU profiles in `raw/default.profile/`. `raw/BUILDS.md` maps run tags to builds. `logs/`:
  sweep outputs.

Reproduce (Ubuntu 26.04: `PLAYWRIGHT_HOST_PLATFORM_OVERRIDE=ubuntu24.04-x64` for `playwright install`, plus the apt
libraries from `stage-plan/zero/NOTES.md` and `libxfixes3`):

```sh
cd harness/biglabel-writes
node serve.mjs --variant default            # seeds once; later: ./restart.sh default
node probe.mjs --variant default --samples 10                              # upstream
python3 apply.py <copy of pristine jazz-tools/dist> sync-outbound-first,tx-read-local && ./restart.sh default
node probe.mjs --variant default --samples 10 --only update,cross --fix no-precheck,tx-local,tx-read-local,sync-outbound-first
```

Caveats: the dist patches are prototypes against alpha.62, not PR-ready code. The probe's numbers are lower than
`run.mjs`'s, as said above. Under `next start`, Jazz uses the HTTP stream (the WebSocket upgrade gets a 426), so
these numbers don't say how a WebSocket would do.
