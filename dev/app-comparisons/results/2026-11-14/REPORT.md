# Jazz hero apps vs Zero, Convex, InstantDB, plain Postgres and Neon Realtime (2026-11-14, v3)

> **Paths have moved.** This report predates the move into the jazz repo. `<hero>/<stack>` is now
> `examples/<hero>/apps/<base>-<stack>`, `<hero>/jazz` is the upstream hero app, and `harness/`,
> `fixtures/` and `results/` are under `dev/app-comparisons/` (see [`../../README.md`](../../README.md)).
> The numbers are unchanged; they were measured on the comparison's own Jazz copies.

Four Jazz hero apps, each on five other stacks.

**The heroes:**
- **StagePlan:** a permissioned task board.
- **BandChat:** chat over 10,000 messages.
- **Wequencer:** a 1,024-pad grid with concurrent edits.
- **BigLabel:** a multi-tenant SaaS with 8 labels, 4,096 releases, roles and admin-only membership.

**How it was measured:**
- **Same app everywhere.** The UI is copied verbatim; only the data layer differs. Every stack seeds the same fixture and enforces permissions on the server.
- **One session.** All 33 runs ran back to back on one machine with one harness: production builds, headless Chromium.
- **Samples.** Each figure is the median of 7 page loads or 20 echo samples (Wequencer concurrent: 5), after 1 warm-up. The harness waits 5 s for things to settle before reloads and echo phases.
- **Result:** 0 lost samples. InstantDB BandChat stalled once (see caveats) and passed on its retry.

This replaces v2; v1 and v2 are in git history.

**Stacks and where they ran:**

| stack | version | backend |
|---|---|---|
| Jazz | jazz-tools 2.0.0-alpha.62 | in the app server, local |
| Zero | @rocicorp/zero 1.12.0 | Postgres 18 + zero-cache, local |
| Convex | convex 1.50.0, @convex-dev/auth 0.0.98 | local backend |
| InstantDB | @instantdb 1.0.67 | **hosted** temporary app, us-east, about 6 ms rtt |
| Postgres + custom sync | Postgres 18, Hono/`pg`/`ws`, TanStack Query | hand-written, local |
| Neon Realtime | `@neon/realtime` 0.2.x, public beta | **hosted** in aws-us-east-1, 3–6 ms rtt; app server local |

**Jazz variants:**
- *as shipped*: starts thin, upgrades to local-first in the background.
- *now*: `localFirst: "now"`, local-first from the first load.
- *thin*: no local store.

BigLabel ships thin upstream, so its labels say so.

## The signal

1. **Opening and reloading is where Jazz loses everywhere.**

   | reload, ms | StagePlan | Wequencer | BigLabel | BandChat (resume) |
   |---|---|---|---|---|
   | Jazz, thin | 285 | 403 | 260 | 1,234 |
   | Jazz, as shipped | 527 | 874 | 450 | 2,471 |
   | Jazz, now | 514 | 690 | 540 | 1,658 |
   | fastest other stack | InstantDB 120 | InstantDB 264 | InstantDB 175 | Convex 894 |

   - Local-first Jazz pays for WASM, worker and store startup on every load (3.1 MB gzip of WASM, which no other stack ships). Its warm local store never beats a server round trip.
   - The fixes are in `LOW-HANGING-FRUIT.md`. The two cheapest: the self-signed token mint (−49 ms thin, −105 ms local-first) and an early worker warm-up.
2. **Plain Postgres with hand-written sync is fast, and costs the most code.**
   - Speed: fastest StagePlan join (282 ms) and Wequencer open (445 ms), second-fastest cross-client in StagePlan (31 ms) and BandChat (356 ms).
   - Code: its data layer is the largest or second largest in every hero. Against Jazz it is 1.6–1.8× in StagePlan, BandChat and Wequencer, and 1.3× in BigLabel.
   - What it doesn't have: offline use, local queries, or merging. Its sync is refetch-on-notify from a single server process, with optimistic patches written by hand.
3. **Concurrent editing is Jazz's clearest win.** In Wequencer, two users toggle 48 pads at once, and both grids converge in:
   - Jazz: 202–381 ms
   - Postgres: 583
   - Zero: 786
   - Neon: 1,071
   - Convex optimistic: 1,197
   - InstantDB: 1,491
   - Convex: 1,830
4. **Delivery to another user: thin Jazz is fastest, except in BigLabel.**

   | cross-client, ms | Jazz thin | Jazz local-first | fastest other stack |
   |---|---|---|---|
   | StagePlan | 26 | 65–67 | Postgres 31 |
   | Wequencer | 16 | 43–44 | Convex optimistic 25 |
   | BandChat | 259 | 692–751 | Postgres 356 |
   | BigLabel | 114 | 141–144 | Zero 50 |

   - Local-first's extra cost is mostly the sending tab queuing the outbound frame behind React's render. A ~15-line fix brought BandChat's 592 ms to 333 in the profiling prototype (`LOW-HANGING-FRUIT.md` #2).
5. **New in v3: BigLabel exposes two Jazz write and paging costs worth profiling.**
   - `bl_update_status` takes Jazz 126–128 ms vs Zero 7, Neon 13, InstantDB 29, Convex 52, Postgres 72. The release form goes through `saveRelease`, a serializable transaction that waits for the server to accept it. A local echo of the change would close most of this.
   - `bl_next_page` takes local-first Jazz 137–146 ms vs thin 32 and 29–83 elsewhere. That is a `limit/offset` page over the local store.
6. **Local writes are fast wherever optimistic updates exist.**
   - Wequencer pad toggle: Convex optimistic 2 ms, Jazz local-first 4–6, Zero 4, Postgres 5, Neon 8.
   - Without them: thin Jazz 19, InstantDB 25, Convex 30.
7. **Jazz's data layer is mid-pack among the sync engines, and well under the DIY stacks.** Lines of schema + permissions + server + data:

   | | InstantDB | Convex | Jazz | Zero | Neon | Postgres |
   |---|---|---|---|---|---|---|
   | StagePlan | 528 | 686 | 591 | 794 | 938 | 1,024 |
   | BandChat | 619 | 787 | 642 | 747 | 1,001 | 1,009 |
   | Wequencer | 511 | 516 | 545 | 722 | 1,013 | 997 |
   | BigLabel | 1,026 | 1,096 | 1,115 | 1,282 | 1,567 | 1,459 |

   Where permissions live:
   - Jazz and InstantDB declare them.
   - Convex and Postgres check them in server code.
   - Zero checks them in its queries and mutators.
   - Neon checks them in its sealed SQL queries and write handlers, because it has no RLS.
8. **Neon Realtime (beta):**
   - **Speed:** consistently mid-to-slow on loads (StagePlan reload 510 ms, Wequencer open 1,095 ms). Every live query first makes a round trip to seal (authorise) itself before subscribing.
   - **Code:** the largest data layers, partly because the beta leaves the client hook and optimistic overlay to the app.
   - **Writes:** fast locally (12–13 ms in StagePlan) with its optimistic overlay. Next Server Actions run one at a time, which slows Wequencer's concurrent case.

## Caveats

- **Different unread-count queries.** Convex and Postgres BandChat use one aggregated unread-count query. Jazz, Zero, InstantDB and Neon run one per room (~255), as the Jazz app does. With one query per room, Convex's local backend hit its 1 s function limit. So their BandChat open and resume times aren't strictly comparable.
- **InstantDB BandChat stalls on temporary apps.** Sometimes all ~260 startup queries return `operation-timed-out`, and the client doesn't re-subscribe. That happened on the first v3 attempt, and the retry passed.
- **Approximated in every port:**
  - StagePlan's notes are last-write-wins, not text-merged.
  - The offline "Sync" switch only shows status; no other stack takes writes while paused.
- **Approximated per port** (each port's `NOTES.md` lists them):
  - **Zero:** no binary attachments, and BigLabel pages with a cursor rather than `offset`.
  - **InstantDB:** one slow profile rule dropped.
  - **Neon:** no server-rendered live data.
- **BigLabel `bl_switch_label`** returns to a label already shown, so it measures a warm switch.
- **Jazz under `next start`** (Wequencer, BigLabel) falls back from WebSocket to the HTTP stream.
- **Wequencer's `rtt` for Jazz** is measured against the server-rendered page, so it isn't comparable.
- **Page-level "JS loaded"** misses what a SharedWorker downloads. The tables list WASM and worker JS from the build separately. Convex BandChat's page-level figure didn't record ("n/a"); its build is 251 KB.
- **The machine** is a 2-vCPU, 4 GB cloud sandbox. Treat differences under about 10 ms as noise.

## Tables

### stage-plan

| metric (median, ms) | Jazz (as shipped) | Jazz (now) | Jazz (thin) | Zero | Convex | InstantDB | Postgres + custom sync | Neon Realtime |
|---|---|---|---|---|---|---|---|---|
| `sp_join_board` | 521 | 1175 | 464 | 509 | 397 | 495 | 282 | 595 |
| `sp_reload_board` | 527 | 514 | 285 | 266 | 133 | 120 | 211 | 510 |
| `sp_add_task` | 15 | 14 | 19 | 13 | 41 | 16 | 24 | 12 |
| `sp_move_done` | 21 | 21 | 25 | 17 | 44 | 19 | 21 (p95 42) | 13 |
| `sp_cross_client` | 67 | 65 | 26 | 54 | 61 | 173 | 31 | 67 |
| JS loaded by the page (gzip KB) | 436 | 436 | 374 | 354 | 247 | 273 | 236 | 252 |
| WASM in build (gzip KB) | 3110 | 3110 | 3110 | 0 | 0 | 0 | 0 | 0 |
| worker JS in build (gzip KB) | 286 | 286 | 286 | 0 | 0 | 0 | 0 | 0 |
| rtt to backend (ms) | 1.4 | 1.2 | 1.3 | 0.9 | 0.8 | 6.1 | 0.9 | 3.1 |
| backend | local (Jazz backend in the Vite preview server) | local (Jazz backend in the Vite preview server) | local (Jazz backend in the Vite preview server) | local (Postgres 18 + zero-cache + Hono app server, same machine) | local (anonymous Convex local backend via `npx convex dev`, Vite preview for the app) | hosted (Instant Cloud temporary app, api.instantdb.com via CloudFront IAD / us-east) | local (Postgres 18 + Hono/pg/ws app server, same machine) | hosted (Neon Postgres + Realtime, aws-us-east-1); Hono app server local |

| LOC | jazz | zero | convex | instantdb | postgres | neon |
|---|---|---|---|---|---|---|
| schema | 118 | 204 | 76 | 106 | 108 | 107 |
| permissions | 86 | 0 | 63 | 93 | 25 | 0 |
| server | 22 | 66 | 470 | 0 | 435 | 392 |
| data | 365 | 524 | 77 | 329 | 456 | 439 |
| data_layer | 591 | 794 | 686 | 528 | 1024 | 938 |
| ui | 1498 | 1429 | 1377 | 1447 | 1430 | 1435 |
| total | 2089 | 2223 | 2063 | 1975 | 2454 | 2373 |

### band-chat

| metric (median, ms) | Jazz (as shipped) | Jazz (now) | Jazz (thin) | Zero | Convex | InstantDB | Postgres + custom sync | Neon Realtime |
|---|---|---|---|---|---|---|---|---|
| `bc_open_room` | 1404 | 2308 | 1261 | 1121 | 1121 | 1946 | 953 | 1406 |
| `bc_resume` | 2471 | 1658 | 1234 | 1074 | 894 | 1925 | 919 | 1466 |
| `bc_send` | 247 | 239 | 261 | 245 | 262 | 332 | 287 | 539 |
| `bc_switch_room` | 172 (p95 519) | 158 (p95 457) | 158 (p95 408) | 177 (p95 409) | 187 (p95 628) | 309 (p95 1005) | 167 (p95 516) | 203 (p95 511) |
| `bc_cross_client` | 751 | 692 | 259 (p95 561) | 918 | 584 | 760 | 356 | 1306 |
| JS loaded by the page (gzip KB) | 438 | 438 | 376 | 351 | n/a | 264 | 235 | 305 |
| WASM in build (gzip KB) | 3110 | 3110 | 3110 | 0 | 0 | 0 | 0 | 0 |
| worker JS in build (gzip KB) | 287 | 287 | 287 | 0 | 0 | 0 | 0 | 0 |
| rtt to backend (ms) | 1.1 | 0.8 | 0.9 | 1 | 0.9 | 6.6 | 0.9 | 3 |
| backend | local (Jazz in the app's Hono server) | local (Jazz in the app's Hono server) | local (Jazz in the app's Hono server) | local (Postgres 18 + zero-cache + Hono/Better Auth server, same machine) | local (anonymous Convex local backend via `npx convex dev`, Vite preview for the app) | hosted (Instant Cloud temporary app, api.instantdb.com via CloudFront IAD / us-east); auth server local | local (Postgres 18 + Hono/Better Auth/pg/ws app server, same machine) | hosted (Neon Postgres + Realtime + Managed Better Auth, aws-us-east-1); Hono app server local |

| LOC | jazz | zero | convex | instantdb | postgres | neon |
|---|---|---|---|---|---|---|
| schema | 107 | 195 | 64 | 152 | 91 | 81 |
| permissions | 122 | 0 | 56 | 108 | 22 | 0 |
| server | 129 | 70 | 440 | 73 | 402 | 359 |
| data | 284 | 482 | 227 | 286 | 494 | 561 |
| data_layer | 642 | 747 | 787 | 619 | 1009 | 1001 |
| ui | 2086 | 1842 | 1984 | 2068 | 1840 | 1845 |
| total | 2728 | 2589 | 2771 | 2687 | 2849 | 2846 |

### wequencer

| metric (median, ms) | Jazz (as shipped) | Jazz (now) | Jazz (thin) | Zero | Convex | Convex (optimistic) | InstantDB | Postgres + custom sync | Neon Realtime |
|---|---|---|---|---|---|---|---|---|---|
| `wq_open_pattern` | 856 | 1867 | 674 | 685 | 569 | 643 | 510 | 445 | 1095 |
| `wq_reload` | 874 | 690 | 403 | 289 | 303 | 310 | 264 | 278 | 762 |
| `wq_toggle_pad` | 4 (p95 8) | 6 | 19 (p95 51) | 4 | 30 | 2 (p95 7) | 25 | 5 | 8 |
| `wq_cross_client` | 43 | 44 | 16 | 26 | 30 | 25 | 65 (p95 209) | 30 | 72 |
| `wq_concurrent` | 351 | 381 | 202 (p95 405) | 786 | 1830 | 1197 | 1491 | 583 | 1071 |
| JS loaded by the page (gzip KB) | 538 | 538 | 512 | 497 | 327 | 327 | 382 | 327 | 463 |
| WASM in build (gzip KB) | 3110 | 3110 | 3110 | 0 | 0 | 0 | 0 | 0 | 0 |
| worker JS in build (gzip KB) | 176 | 176 | 176 | 0 | 0 | 0 | 0 | 0 | 0 |
| rtt to backend (ms) | 6.9 | 7.2 | 7.1 | 0.8 | 0.7 | 0.8 | 6 | 1.5 | 4.5 |
| backend | local (Jazz inside the Next server) | local (Jazz inside the Next server) | local (Jazz inside the Next server) | local (Postgres 18 + zero-cache + Next/Better Auth server, same machine) | local (anonymous Convex local backend via `npx convex dev`, `next start` for the app) | local (anonymous Convex local backend via `npx convex dev`, `next start` for the app) | hosted (Instant Cloud temporary app, api.instantdb.com via CloudFront IAD / us-east); Next server local | local (Postgres 18 + Next/Better Auth/Hono API routes + a ws server fed by LISTEN/NOTIFY, same machine) | hosted (Neon Postgres + Realtime + Managed Better Auth, aws-us-east-1); Next server local |

| LOC | jazz | zero | convex | instantdb | postgres | neon |
|---|---|---|---|---|---|---|
| schema | 115 | 181 | 72 | 143 | 109 | 108 |
| permissions | 89 | 0 | 44 | 74 | 24 | 0 |
| server | 152 | 112 | 370 | 81 | 402 | 415 |
| data | 189 | 429 | 30 | 213 | 462 | 490 |
| data_layer | 545 | 722 | 516 | 511 | 997 | 1013 |
| ui | 1846 | 1829 | 1827 | 1898 | 1831 | 1829 |
| total | 2391 | 2551 | 2343 | 2409 | 2828 | 2842 |

### big-label

| metric (median, ms) | Jazz (thin + upgrade) | Jazz (now) | Jazz (thin, as shipped) | Zero | Convex | InstantDB | Postgres + custom sync | Neon Realtime |
|---|---|---|---|---|---|---|---|---|
| `bl_open_releases` | 663 | 1744 | 542 | 643 | 463 | 308 | 391 | 799 |
| `bl_reload` | 450 | 540 | 260 | 224 | 206 | 175 | 199 | 538 |
| `bl_search` | 12 (p95 111) | 9 (p95 71) | 8 (p95 47) | 9 (p95 86) | 17 (p95 46) | 51 (p95 195) | 8 (p95 46) | 89 |
| `bl_next_page` | 146 | 137 | 32 | 48 | 53 | 50 | 29 | 83 (p95 234) |
| `bl_switch_label` | 8 | 8 | 8 | 11 | 11 (p95 31) | 218 | 11 | 75 (p95 165) |
| `bl_update_status` | 126 | 126 | 128 | 7 | 52 (p95 110) | 29 (p95 155) | 72 | 13 |
| `bl_cross_client` | 144 | 141 | 114 | 50 (p95 99) | 63 (p95 132) | 109 | 62 | 77 |
| JS loaded by the page (gzip KB) | 558 | 558 | 533 | 518 | 352 | 373 | 347 | 483 |
| WASM in build (gzip KB) | 3110 | 3110 | 3110 | 0 | 0 | 0 | 0 | 0 |
| worker JS in build (gzip KB) | 176 | 176 | 176 | 0 | 0 | 0 | 0 | 0 |
| rtt to backend (ms) | 2.2 | 2 | 2.4 | 1 | 0.6 | 6.1 | 1.9 | 5.8 |
| backend | local (Jazz inside the Next server) | local (Jazz inside the Next server) | local (Jazz inside the Next server) | local (Postgres 18 + zero-cache + Next/Better Auth server, same machine) | local (anonymous Convex local backend via `npx convex dev`, `next start` for the app) | hosted (Instant Cloud temporary app, api.instantdb.com via CloudFront IAD / us-east); Next server local | local (Postgres 18 + Next/Better Auth/Hono API routes + a ws server fed by LISTEN/NOTIFY, same machine) | hosted (Neon Postgres + Realtime + Managed Better Auth, aws-us-east-1); Next server local |

| LOC | jazz | zero | convex | instantdb | postgres | neon |
|---|---|---|---|---|---|---|
| schema | 129 | 221 | 76 | 236 | 172 | 123 |
| permissions | 195 | 38 | 88 | 141 | 67 | 38 |
| server | 647 | 512 | 902 | 503 | 797 | 983 |
| data | 144 | 511 | 30 | 146 | 423 | 423 |
| data_layer | 1115 | 1282 | 1096 | 1026 | 1459 | 1567 |
| ui | 2428 | 2399 | 2366 | 2429 | 2422 | 2301 |
| total | 3543 | 3681 | 3462 | 3455 | 3881 | 3868 |
