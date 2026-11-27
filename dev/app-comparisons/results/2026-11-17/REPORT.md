# Regression check (2026-11-17): Jazz on alpha.66 vs alpha.64

> **Paths have moved.** This report predates the move into the jazz repo. `<hero>/<stack>` is now
> `examples/<hero>/apps/<base>-<stack>`, `<hero>/jazz` is the upstream hero app, and `harness/`,
> `fixtures/` and `results/` are under `dev/app-comparisons/` (see [`../../README.md`](../../README.md)).
> The numbers are unchanged; they were measured on the comparison's own Jazz copies.

**Setup.** The four Jazz app copies were upgraded with `npx jazz-tools@2.0.0-alpha.66 upgrade` (`c33b387`). The schema hashes are unchanged. The machine and harness are the same as in v4: 7 runs and 20 samples per variant, with a 5 s settle and 0 lost samples. The two open perf fixes, the thin handshake pipelining ([jazz#5508](https://github.com/garden-co/jazz/pull/5508)) and the local-first warm-up ([jazz#5533](https://github.com/garden-co/jazz/pull/5533)), aren't in .66, so this run looks for regressions, not gains.

**Result: no regressions beyond noise, with one small watch item.**
- **Within run-to-run noise (±15% here):**
  - **Load and reload:** every hero and variant.
  - **Writes:** every hero and variant.
  - **Paging:** every hero and variant.
  - **Wequencer, StagePlan and BigLabel cross-client:** within noise as well.
- **JS loaded by the page:** 8–17 KB less on every hero, in both thin and local-first.
- **WASM:** +12 KB gzip (2,986 → 2,998 KB).
- **Watch item: BandChat thin cross-client.** It came in at 303 and 307 ms, against 266 ms on alpha.64 and 259 on alpha.62, in two runs. That's about +40 ms on the thin path. It is at the edge of noise on this 2-core box, so it's not yet a confirmed regression. If it persists, look at the thin stream path (BandChat syncs over WebSocket under Hono). BandChat thin open and send came back within noise on the re-run (1,170 and 254 ms).

**Upgrade notes** (the `upgrade` path on four real apps):
- **StagePlan, Wequencer and BigLabel:** `upgrade` ran clean.
- **BandChat's first try** was refused with an actionable message. Its history had no migration from the older versions to the composite-index version, because alpha.64's `validate` records a composite index as a new version but writes no migration. Two changes fixed it:
  - A step-less migration (`migrations/20261114T191500-room-messages-index-…`).
  - The issuer migration's generated-form filename, as [jazz#5491](https://github.com/garden-co/jazz/pull/5491) did upstream.
- **`permissions.ts`:** `upgrade` kept it where policies import generated Better Auth permissions (BandChat, BigLabel). Elsewhere it moved the policies into `schema.ts`.

## Tables (alpha.64 first pass → alpha.66)


### stage-plan

| median ms, alpha.64 → alpha.66 | jazz default | jazz now | jazz thin |
|---|---|---|---|
| sp_join_board | 717 → **768** | 1235 → **1194** | 425 → **409** |
| sp_reload_board | 456 → **472** | 492 → **421** | 232 → **243** |
| sp_add_task | 18 → **19** | 18 → **16** | 27 → **19** |
| sp_move_done | 24 → **25** | 24 → **22** | 30 → **26** |
| sp_cross_client | 70 → **77** | 72 → **62** | 26 → **26** |
| JS loaded (KB) | 428 → 420 | 428 → 420 | 362 → 354 |
| WASM (KB gzip) | 2986 → 2998 | 2986 → 2998 | 2986 → 2998 |
| worker JS (KB gzip) | 111 → 111 | 111 → 111 | 111 → 111 |

### wequencer

| median ms, alpha.64 → alpha.66 | jazz default | jazz now | jazz thin |
|---|---|---|---|
| wq_open_pattern | 887 → **786** | 1686 → **1643** | 704 → **615** |
| wq_reload | 901 → **857** | 611 → **659** | 448 → **354** |
| wq_toggle_pad | 4 → **4** | 5 → **6** | 14 → **10** |
| wq_cross_client | 39 → **36** | 35 → **37** | 11 → **9** |
| wq_concurrent | 321 → **328** | 434 → **397** | 567 → **291** |
| JS loaded (KB) | 539 → 523 | 539 → 523 | 496 → 480 |
| WASM (KB gzip) | 2986 → 2998 | 2986 → 2998 | 2986 → 2998 |
| worker JS (KB gzip) | 115 → 115 | 115 → 115 | 115 → 115 |

### big-label

| median ms, alpha.64 → alpha.66 | jazz default | jazz now | jazz thin |
|---|---|---|---|
| bl_open_releases | 632 → **562** | 1515 → **1511** | 523 → **542** |
| bl_reload | 440 → **428** | 436 → **494** | 249 → **239** |
| bl_search | 7 → **8** | 7 → **9** | 10 → **7** |
| bl_next_page | 109 → **99** | 105 → **103** | 26 → **21** |
| bl_switch_label | 7 → **7** | 7 → **6** | 9 → **8** |
| bl_update_status | 46 → **43** | 43 → **43** | 57 → **69** |
| bl_cross_client | 102 → **98** | 113 → **91** | 50 → **49** |
| JS loaded (KB) | 561 → 545 | 561 → 545 | 518 → 502 |
| WASM (KB gzip) | 2986 → 2998 | 2986 → 2998 | 2986 → 2998 |
| worker JS (KB gzip) | 115 → 115 | 115 → 115 | 115 → 115 |

### band-chat

| median ms, alpha.64 → alpha.66 | jazz default | jazz now | jazz thin |
|---|---|---|---|
| bc_open_room | 1282 → **1246** | 2290 → **2193** | 1129 → **1321** |
| bc_resume | 2108 → **2078** | 1508 → **1511** | 1612 → **1350** |
| bc_send | 243 → **247** | 223 → **218** | 230 → **273** |
| bc_switch_room | 146 → **159** | 148 → **154** | 152 → **151** |
| bc_cross_client | 644 → **655** | 611 → **627** | 266 → **303** |
| JS loaded (KB) | 439 → 422 | 439 → 422 | 373 → 356 |
| WASM (KB gzip) | 2986 → 2998 | 2986 → 2998 | 2986 → 2998 |
| worker JS (KB gzip) | 111 → 111 | 111 → 111 | 111 → 111 |
