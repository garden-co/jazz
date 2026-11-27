# Report v4 (2026-11-15): what alpha.64 changed for Jazz

> **Paths have moved.** This report predates the move into the jazz repo. `<hero>/<stack>` is now
> `examples/<hero>/apps/<base>-<stack>`, `<hero>/jazz` is the upstream hero app, and `harness/`,
> `fixtures/` and `results/` are under `dev/app-comparisons/` (see [`../../README.md`](../../README.md)).
> The numbers are unchanged; they were measured on the comparison's own Jazz copies.

v4 asks one question: what did the fixes from the profiling threads do in published jazz-tools 2.0.0-alpha.64?
- **Most of the fixes are in alpha.64.** They are listed in `../2026-11-14/LOW-HANGING-FRUIT.md` and the BigLabel profile ([#11](https://github.com/garden-co/jazz-app-comparisons/pull/11)).
- **Two are not in yet:**
  - the thin handshake pipelining ([jazz#5508](https://github.com/garden-co/jazz/pull/5508), waiting for #dev's okay on its SPEC text);
  - local-first early warm-up ([jazz#5533](https://github.com/garden-co/jazz/pull/5533)).

**Setup:**
- The same machine and harness as v3, with the Jazz app copies upgraded from alpha.62 to alpha.64 (`730973e`).
- BandChat no longer needs its patch, because [jazz#5473](https://github.com/garden-co/jazz/pull/5473) shipped.
- The BigLabel copy has the example fix [jazz#5526](https://github.com/garden-co/jazz/pull/5526) (no duplicate pre-check).
- 16 runs: three Jazz variants × 4 heroes, plus Convex as a drift check. 0 lost samples.
- Each figure is the median of 7 page loads or 20 echo samples (5 for Wequencer concurrent). The harness waits 5 s before reloads and echo phases.
- v3's numbers for the other stacks are still current. Convex, re-run as a drift check, moved between −15% and +25% in the first pass and came back within about 5% on re-run. So treat single-run differences under about 20% as noise, and read the re-checked rows below.

## What got better

| | alpha.62 (v3) | alpha.64 (v4) | other stacks (v3) |
|---|---|---|---|
| BigLabel: status update, as shipped / now / thin | 126 / 126 / 128 ms | **46 / 43 / 57** | Convex 42–52, InstantDB 29, Zero 7 |
| BigLabel: cross-client, thin | 114 | **50** | Zero 50, Convex 57–63, Postgres 62 |
| BigLabel: cross-client, local-first | 141–144 | **102–113** | |
| BigLabel: next page, local-first | 137–146 | **105–109** | 29–83 (see the open item below) |
| BigLabel: open releases, `now` | 1,744 | **1,515** | |
| StagePlan: reload, thin | 285 | **232** | Convex 133–150, InstantDB 120, Zero 266 |
| StagePlan: reload, as shipped | 527 | **456–459** | |
| BandChat: cross-client, local-first | 692–751 | **611–644** | Convex 537–584, InstantDB 760, Zero 918 |
| Wequencer: reload, `now` | 690 | **611** | |
| Wequencer: cross-client, thin / now | 16 / 44 | **8–11 / 35** | Zero 26, Convex 23–30 |
| Worker JS in the build (gzip) | 176–287 KB | **111–115 KB** | none |
| WASM (gzip, debug symbols kept) | 3,110 KB | **2,986 KB** | none |

- **The biggest change is BigLabel's writes.** A status change is now level with Convex, and thin cross-client delivery is level with Zero. Behind that:
  - the example no longer checks the catalogue number twice;
  - unused page subscriptions are parked;
  - HTTP stream sends reuse the last `authenticate` answer.
- **Startup improved, but Jazz still loses on reload.** Thin StagePlan reload is down 19%, from the token-mint and socket fixes, but Convex and InstantDB still reload in 120–150 ms. The two fixes that would help most here are the ones not in alpha.64 yet: #5508 (−44 ms at 40 ms RTT) and #5533, the local-first warm-up (prototype: `now` reload 581 → 459 ms). Beyond those, local-first still loads its WASM twice, once in the page and once in the worker.

## Re-checked: no regressions

Three numbers looked worse in the first pass, so I ran them again:

| | v3 | v4, run 1 | v4, re-runs |
|---|---|---|---|
| StagePlan: join, as shipped | 521 | 717 | **494** |
| BandChat: resume, thin | 1,234 | 1,612 | **1,259** |
| Wequencer: concurrent edits, thin (5 samples per run) | 202 | 567 | **337, 271, 267** (fastest sample 173–199 vs v3's 176) |
| Convex StagePlan join (drift reference) | 397 | 499 | **372** |

- The first two were a noisy stretch on the machine; Convex moved the same way at the same time.
- Wequencer's thin concurrent case varies a lot between runs: v2 measured 316, v3 202. Its fastest samples are unchanged, so there's no evidence of a regression. More samples per run would settle it.

## Still open

- **BigLabel next page, local-first: 105–109 ms on alpha.64 is the real number.** The earlier "26 ms" branch measurement was the thin path: that build's WASM fingerprint didn't match, so local-first silently fell back to thin. alpha.64's server dedupe works, and late pages now ship only their ~20 new rows instead of the whole prefix. What's left is local-first's own per-page cost: about 22 KB of row images, about 38 ms from data arrival to DOM, and the 16 ms pump debounce. #fix-query-paging is measuring #5525's real gain and correcting its PR bodies.
- **What's left in local-first paging** is written up as [jazz#5650](https://github.com/garden-co/jazz/issues/5650): ~70 ms per page. Every evaluation in the worker, the tab and the local answer walks the whole `limit n+21` prefix and nests relations for every row in it. The ranked fixes start with nesting relations only for the window's rows (−7–8 ms, and the cost stops growing with depth). #1–#4 together are estimated at ~70 → ~50 ms per page.
- **[jazz#5508](https://github.com/garden-co/jazz/pull/5508), pipelining the thin handshake:** waits for #dev's okay on the SPEC text.
- **[jazz#5533](https://github.com/garden-co/jazz/pull/5533), local-first early warm-up:** in review.
- **Open decisions:**
  - **Serializable local reads**, which would take BigLabel's update to ~12 ms, the same as Zero. They need a decision, after #correctness fixes the two holes it found.
  - **A `schema` option** on `createJazzClient`, to speed up local-first first visits by an estimated 100+ ms. It would be public API.

## Tables: Jazz, alpha.62 → alpha.64 (first pass)


### stage-plan

| median ms, alpha.62 → alpha.64 | jazz default | jazz now | jazz thin | convex default |
|---|---|---|---|---|
| sp_join_board | 521 → **717** | 1175 → **1235** | 464 → **425** | 397 → **499** |
| sp_reload_board | 527 → **456** | 514 → **492** | 285 → **232** | 133 → **150** |
| sp_add_task | 15 → **18** | 14 → **18** | 19 → **27** | 41 → **43** |
| sp_move_done | 21 → **24** | 21 → **24** | 25 → **30** | 44 → **58** |
| sp_cross_client | 67 → **70** | 65 → **72** | 26 → **26** | 61 → **59** |
| JS loaded (KB) | 436 → 428 | 436 → 428 | 374 → 362 | 247 → 247 |
| WASM (KB gzip) | 3110 → 2986 | 3110 → 2986 | 3110 → 2986 | 0 → 0 |
| worker JS (KB gzip) | 286 → 111 | 286 → 111 | 286 → 111 | 0 → 0 |

### wequencer

| median ms, alpha.62 → alpha.64 | jazz default | jazz now | jazz thin | convex default |
|---|---|---|---|---|
| wq_open_pattern | 856 → **887** | 1867 → **1686** | 674 → **704** | 569 → **600** |
| wq_reload | 874 → **901** | 690 → **611** | 403 → **448** | 303 → **253** |
| wq_toggle_pad | 4 → **4** | 6 → **5** | 19 → **14** | 30 → **25** |
| wq_cross_client | 43 → **39** | 44 → **35** | 16 → **11** | 30 → **23** |
| wq_concurrent | 351 → **321** | 381 → **434** | 202 → **567** | 1830 → **1391** |
| JS loaded (KB) | 538 → 539 | 538 → 539 | 512 → 496 | 327 → 327 |
| WASM (KB gzip) | 3110 → 2986 | 3110 → 2986 | 3110 → 2986 | 0 → 0 |
| worker JS (KB gzip) | 176 → 115 | 176 → 115 | 176 → 115 | 0 → 0 |

### big-label

| median ms, alpha.62 → alpha.64 | jazz default | jazz now | jazz thin | convex default |
|---|---|---|---|---|
| bl_open_releases | 663 → **632** | 1744 → **1515** | 542 → **523** | 463 → **405** |
| bl_reload | 450 → **440** | 540 → **436** | 260 → **249** | 206 → **182** |
| bl_search | 12 → **7** | 9 → **7** | 8 → **10** | 17 → **18** |
| bl_next_page | 146 → **109** | 137 → **105** | 32 → **26** | 53 → **55** |
| bl_switch_label | 8 → **7** | 8 → **7** | 8 → **9** | 11 → **12** |
| bl_update_status | 126 → **46** | 126 → **43** | 128 → **57** | 52 → **42** |
| bl_cross_client | 144 → **102** | 141 → **113** | 114 → **50** | 63 → **57** |
| JS loaded (KB) | 558 → 561 | 558 → 561 | 533 → 518 | 352 → 352 |
| WASM (KB gzip) | 3110 → 2986 | 3110 → 2986 | 3110 → 2986 | 0 → 0 |
| worker JS (KB gzip) | 176 → 115 | 176 → 115 | 176 → 115 | 0 → 0 |

### band-chat

| median ms, alpha.62 → alpha.64 | jazz default | jazz now | jazz thin | convex default |
|---|---|---|---|---|
| bc_open_room | 1404 → **1282** | 2308 → **2290** | 1261 → **1129** | 1121 → **969** |
| bc_resume | 2471 → **2108** | 1658 → **1508** | 1234 → **1612** | 894 → **817** |
| bc_send | 247 → **243** | 239 → **223** | 261 → **230** | 262 → **228** |
| bc_switch_room | 172 → **146** | 158 → **148** | 158 → **152** | 187 → **163** |
| bc_cross_client | 751 → **644** | 692 → **611** | 259 → **266** | 584 → **537** |
| JS loaded (KB) | 438 → 439 | 438 → 439 | 376 → 373 | n/a → 239 |
| WASM (KB gzip) | 3110 → 2986 | 3110 → 2986 | 3110 → 2986 | 0 → 0 |
| worker JS (KB gzip) | 287 → 111 | 287 → 111 | 287 → 111 | 0 → 0 |
