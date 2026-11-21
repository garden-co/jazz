# Jazz local-first startup profile (2026-11-14)

This profile covers what local-first adds on top of the thin client when StagePlan reloads: the local-first chunk, page and worker WASM, the SharedWorker, the store open and the first local reads. It is measurement only. Jazz is the published `jazz-tools@2.0.0-alpha.62`, patched locally with `performance.mark`s and, where stated, prototype fixes. No jazz PRs.

The thin client's own startup is in [#6](https://github.com/garden-co/jazz-app-comparisons/pull/6) (`results/2026-11-14/thin-startup/`, branch `profile/thin-startup`). Its fix 1 (`fix-mint`) turned out to be the largest local-first fix too. It is re-measured here on the local-first path and copied verbatim as `patch/thin-fix-mint.mjs`.

**Machine.** A 2-core sandbox, so CPU contention between the page and the worker is stronger than on a laptop. Absolute times differ from REPORT.md; compare the columns with each other.

| case (100 cards in the DOM, median ms from navigation start) | fresh device (join) | reload | 6th reload in the same context |
|---|---:|---:|---:|
| thin | 490 | 277 | 236 |
| `now` (local-first from the first load) | 1533 | 581 | 487 |
| as shipped (thin first, then upgrades in the background; reloads open local-first at once) | 875 | 637 | 487 |

## TL;DR

1. **On reload, the data comes from the local store and the server is never waited on.** Debug logs: the worker's store delivers shows, crew and tasks at ~512–517 ms, and the server's `Welcome` reaches the worker at ~599 ms. Local-first is slower anyway, because before the store can answer its first read it does ~270 ms of startup that thin doesn't: the local-first chunk, the 9 MB WASM loaded and compiled **twice** (page and worker), the worker realm start, the lease and IndexedDB epoch, and `WasmDb.openBrowser`. Thin, meanwhile, spends ~60 ms on socket plus round trip. A warm store can only win once its startup is shorter than a server round trip.
2. **Every local-first step starts only after `createAccountManagerWithMinter` resolves the account.** That's 80 ms on reload, mostly one ~70 ms long task: noble's Ed25519 base-point precompute (found by #jazz-thin-startup). The long task also blocks the SharedWorker from *starting at all*: a worker constructed before it gets its `timeOrigin` only when the task ends.
3. **Fixes measured (reload, 100 cards):**
   - thin's `fix-mint`: 581 → **476 ms** (−105)
   - plus an *early warm-up* prototype (spawn the remembered worker and start the page's WASM fetch before the account resolves): **459 ms**, −122 in all. On its own, without `fix-mint`, the warm-up saves 17 ms on reload and 83 ms on a fresh device.
   - fresh-device join: 1533 → **1391 ms**
4. **The floor after those fixes is two concurrent loads of the same 9 MB WASM**, page and worker, ~120–160 ms each on 2 cores. Reading the body from the HTTP cache alone takes 50–100 ms per realm, and the compile is cheap (37 ms alone, thanks to lazy compilation). Removing one of the two loads is the next big lever (fix 4).

## How

- **Marks** (`patch/marks.mjs <app>`; `--restore` undoes it) are literal search-and-replace edits on the installed dist, each of which must match exactly once. All marks are named `lf:*`:
  - Page side:
    - `session/create-jazz-session.js`: account manager start/ready, `openClient`, the chunk import
    - `accounts/context.js`: `createDb` start, auth resolved, end
    - `runtime/db.js`: `runtimeSource.load` (page WASM), `Db.createWithBrowserWorker`
    - `runtime/wasm-loader.js`: glue import, fetch, compile, instantiate
    - `connection-manager/browser-connection-manager.js`: lease await/got, client created (opens the worker connection), worker connection ready
    - `runtime/default-runtime-source.js`: the page's `NativeRuntimeAdapter` construction
  - Worker side (`worker/jazz-broker-worker.js`, the bundled SharedWorker):
    - the WASM loader
    - `acquireBrowserPhysicalDatabaseEpoch`, `IndexedDbPageStore.open`, the epoch claim
    - `initialize()`: owner, WASM, `WasmDb.openBrowser` start/end
    - `connectTab`: init awaited, `runtime-ready`
- **Runner:** `harness/breakdown.mjs`, the PR #4 probe, plus three new options:
  - `--reloads N` reloads N times in one browser context. The summary gets `reload` (the 1st) and `reloadLast` (the Nth, with warm V8 JS code caches and a warm HTTP cache).
  - `--tag` and `--out lf-startup` name and place the case directory.
  - Every case here is `--runs 5 --reloads 6`: 5 contexts, each one join (fresh device, with an empty store) followed by 6 reloads.
- **Noise:** medians of 5. Individual samples spread ±30–50 ms, so differences under ~20 ms are not meaningful.
- **Tables:** `node table.mjs label=<case>/summary.json:<join|reload|reloadLast> ...`.
- **`BENCH_IMMUTABLE_ASSETS=1`** (in `stage-plan/jazz/vite.config.ts`) serves `/assets/*` with `Cache-Control: public, max-age=31536000, immutable`, as a CDN deployment would. Vite preview's default is `no-cache`, so every reload revalidates every asset.

Reproduce:
```sh
node results/2026-11-14/lf-startup/patch/marks.mjs stage-plan/jazz
cd harness && node breakdown.mjs stage-plan --variant now --runs 5 --reloads 6 --tag base --out lf-startup
```

## 1. Timeline

`now` and as-shipped (`default`), marks only. Median ms from navigation start.

| milestone | now, fresh device (join) | now, reload | now, 6th reload | as shipped, fresh device (join) | as shipped, reload | as shipped, 6th reload |
|---|---:|---:|---:|---:|---:|---:|
| app JS running (main.tsx) | 198 | 95 | 38 | 185 | 94 | 37 |
| session: account manager starts | 230 | 102 | 47 | 214 | 101 | 46 |
| session: account manager ready | 232 | 182 | 121 | 217 | 173 | 126 |
| session: local-first chunk import starts | 306 | 182 | 121 |  | 174 | 126 |
| local-first chunk fetched | 322 | 187 | 125 | 364 | 177 | 129 |
| createDb starts (account resolved) | 331 | 192 | 131 | 291 | 183 | 136 |
| SharedWorker created | 332 | 192 | 131 | 372 | 183 | 136 |
| page: jazz-wasm glue imported | 352 | 204 | 143 | 397 | 203 | 144 |
| page: WASM compile starts | 359 | 210 | 147 | 417 | 218 | 150 |
| worker: script running (connect) | 400 | 234 | 188 | 475 | 238 | 211 |
| worker: lease store open (IDB) done | 441 | 296 | 205 | 550 | 259 | 250 |
| page: lease granted | 470 | 301 | 219 | 618 | 267 | 257 |
| worker: WASM compile starts (prefetch) | 457 | 264 | 241 | 528 | 279 | 236 |
| page: WASM bytes downloaded | 480 | 263 | 205 | 596 | 295 | 218 |
| page: WASM instantiated | 512 | 298 | 239 | 638 | 332 | 250 |
| createDb returns | 514 | 305 | 263 | 297 | 333 | 259 |
| client ready (JazzProvider children) | 517 | 306 | 265 | 300 | 334 | 260 |
| page runtime created (first query) | 654 | 358 | 289 | 759 | 402 | 288 |
| page → worker: connect-runtime | 661 | 359 | 291 | 765 | 404 | 290 |
| worker ← connect-runtime | 767 | 385 | 312 | 890 | 422 | 306 |
| worker: WASM instantiated | 534 | 397 | 307 | 648 | 381 | 338 |
| worker: WasmDb.openBrowser starts | 768 | 398 | 312 | 891 | 423 | 339 |
| worker: WasmDb.openBrowser done | 922 | 449 | 357 | 1030 | 462 | 363 |
| page ← runtime-ready | 927 | 452 | 360 | 1034 | 464 | 366 |
| page ← first frames from worker | 997 | 464 | 368 | 1082 | 478 | 376 |
| worker WebSocket open | 1004 | 475 | 372 | 1085 | 494 | 377 |
| first render with 100 tasks | 1429 | 526 | 441 | 838 | 532 | 443 |
| board committed (layout effect) | 1507 | 565 | 474 | 874 | 617 | 475 |
| 100 cards in the DOM | 1533 | 581 | 487 | 875 | 637 | 487 |

(As-shipped join: the board comes from the thin client at 875 ms, while the local-first rows above boot alongside it for the background upgrade. Its `createDb`/`client ready` rows at 297/300 belong to the thin client.)

What each stage is, on a `now` reload:

- **Local-first chunk** (`local-first-*.js`, 7 KB, plus `metro-setup-error-*.js` 256 KB and `delete-local-first-store-*.js` 44 KB). It is imported only in `openClient`, after the account manager (182 → 187). The fetch itself is ~5 ms; the cost is *when* it starts.
- **Page WASM** (`jazz_wasm_bg-*.wasm`, 9.0 MB: 7.9 MB code, 490 KB name section, 361 KB data; 3.2 MB gzipped). The glue is imported at 204 ms, the response headers arrive at 210, the body is read by 263 and the module instantiated at 298. `wasm-loader.ts` already uses streaming compile (wasm-bindgen's `instantiateStreaming`).
- **SharedWorker** (`jazz-broker-worker-*.js`, 436 KB). It is created by `startBrowserWorkerLease` in `accounts/context.ts`, already *in parallel with* the page WASM, at 192 ms. It runs its script at 234 ms, after a lease probe, the IndexedDB epoch and `IndexedDbPageStore.open`. Its WASM prefetch (`prefetchWorkerWasmModule`) starts at 264 ms, *the same URL as the page's*, and the worker re-reads all 9 MB (body done at 360) and instantiates it at 397 ms.
  - **So the worker starts ~18 ms after the page's WASM** (192 vs the glue import at 204 / headers at 210), and the two compiles run at the same time.
- **Store.** It is IndexedDB, not OPFS: `IndexedDbPageStore` holds the SQLite VFS pages, plus a Web Lock / epoch record per physical root. It is opened in two places:
  - at lease time (IDB open + epoch claim, ~10–60 ms, off the critical path);
  - in `initialize()` → `WasmDb.openBrowser(pageStore, schema, …)`: **~50 ms warm and ~155 ms on a fresh device.** This is the "store open".
- **`connect-runtime`** carries the schema, so it can only be sent once the page has created its runtime. That happens lazily, on the first `useAll` (`onClientCreated` → `openBrowserWorkerConnection`, 358 ms). On a reload it arrives at the worker (385) about when the worker's WASM finishes (397), so it isn't yet the binding constraint. On a fresh device the page runtime construction takes ~140 ms (517 → 654).
- **Schema/catalogue.** No separate schema fetch exists on this path: the schema travels inside `connect-runtime` to `WasmDb.openBrowser`, and the worker's fingerprint check (`compareRuntimeAdmission`) is in-memory. The schema is encoded once per realm per load (`encodeSchema`), which is cheap next to the rest.
- **First local query** (debug-log run, 3rd reload: `stage-plan.now.r3.log-debug` in scratch, not committed). The board needs **two** read waves, because `useMe`'s `crew` read must deliver before ShowPage mounts and sends `shows`, `showCrew` and `tasks`. Each wave is page → worker → store → worker → page:
  - wave 1: sent at runtime-ready +7 ms, delivered at +26 ms;
  - wave 2: sent at +43 ms, the store answers 108 rows (54 KB) at +72, delivered on the page at +96.
  - The page's WASM spends ~15–20 ms decoding and materialising the 54 KB before the subscription delivers.
- **Render:** ~40 ms from data to the board's layout effect, and another ~15 ms until 100 cards are in the DOM.

## 2. What overlaps already, and what could

Jazz already overlaps more than PR #4 suggested. The worker is spawned before the page's WASM, it prefetches its own WASM on the lease probe, and the lease's IndexedDB open runs alongside the page's WASM load. What is still serial or duplicated:

| | today | could |
|---|---|---|
| account manager → everything | Every local-first step waits for `createAccountManagerWithMinter` (80 ms, one ~70 ms long task). | Make the mint cheap (thin fix 1), and start the account-independent work before it (prototype A). |
| main-thread long tasks → worker start | A SharedWorker constructed during the mint task starts its script only after the task ends (observed: worker `timeOrigin` = the end of the long task). | Don't run long tasks before the worker has started; yield first (prototype A waits for the probe answer). |
| page WASM ∥ worker WASM | The same 9 MB is read and compiled in both realms, at the same time, contending for the cores. | Load it in one realm only (fix 4). A `WebAssembly.Module` can't be posted to a SharedWorker (it's a different agent cluster), so it can't be shared. |
| page runtime → `connect-runtime` → `openBrowser` | The worker's store open waits for the page's first query, because the schema comes with it. | Open from the schema the worker stored last time, or pass the app schema in the provider config, so `openBrowser` starts as soon as the worker's WASM is ready (fix 3). |
| `useMe` read → ShowPage reads | Two read waves through the worker. | Issue them in parallel (app change). The same waterfall costs thin a server round trip. |
| WASM code cache | Each load's compile still takes ~80–110 ms after 6 reloads, and immutable caching doesn't change that. V8's lazy compilation makes the compile itself cheap; the cost is reading 9 MB and instantiating. | A smaller binary helps only a little: `wasm-opt -Oz` gives 9.0 → 7.7 MB, and Chrome's compile time goes 37 → 34 ms alone, 80 → 72 ms page and worker together (`patch/wasm-compile-bench.mjs`). |

## 3. Why a warm local store doesn't beat a server round trip

Reload on this machine, thin vs `now` (the thin numbers come from the `thin.base` case):

- **Thin.**
  - Account resolved at 168 ms, client ready at 172.
  - Socket open at 198, first server frame at 206, 100 tasks at 234.
  - From client ready, network plus answer take **~62 ms**.
- **Local-first.**
  - Account resolved at 182.
  - Then serially: chunk → page WASM (298) → `createDb`/lease (305) → lazy page runtime (358) → `connect-runtime` → worker WASM (397) → `openBrowser` (449) → runtime-ready (452).
  - That's **~270 ms before the first read can even be sent**, plus ~74 ms for the two read waves through the worker.

In thin, one server round trip on localhost costs ~10 ms; hosted, about 40–100 ms. A local read can only win if the local path, from the account being known to rows in the page, takes less than that. What it would take, roughly in order:

1. Nothing blocking the main thread before the local-first work starts (the mint; fix 1).
2. The worker realm and both WASM loads started from the first script, with no dependency on the account (prototype A, fix 2).
3. The store opened as soon as the worker has WASM, without waiting for the page's first query (fix 3).
4. Only one 9 MB WASM load per page load, or none on the page for reads (fix 4). This is the big one: with 1–3 in place, the two concurrent loads are the critical path (`m+e+immut`, 6th reload: page WASM 52 → 171, worker WASM 120 → 281, store open → 343).
5. One read wave instead of two (app; fix 5).

With 1–3 done, the remaining gap to thin is mostly 4. That is an architectural choice: today the page runs a full in-memory `WasmDb` peer (`NativeRuntimeAdapter`) so that writes and queries stay synchronous.

## 4. Ranked fixes

Measured on this box (reload = 1st reload, median of 5; join = fresh device). "Est." means not prototyped.

| # | fix | saves (reload / fresh device) | effort | where |
|---|---|---|---|---|
| 1 | **Cheap token mint** (thin's fix 1: a 4-bit Ed25519 window instead of noble's 8-bit precompute at import, plus reusing the minted token within and across loads) | **−105 / −106 ms** (581 → 476, 1533 → 1427), measured | S | `thin/self-signed-token.ts` `mintSelfSignedToken`, reached through `mintLocalFirstTokenInJs` in `openBrowserSessionCore`'s `createAccountManagerWithMinter`. Credit: #jazz-thin-startup. |
| 2 | **Early local-first warm-up** before the account resolves: start the chunk and the page's `loadWasmModule()`, spawn the SharedWorker named by the last lease (remembered in localStorage), and send it a lease *probe* with `wasmPrefetch` (no admission, no store). Then yield until the worker answers and the WASM request is out, before the mint. | on top of 1: **−17 / −36 ms** (476 → 459, 1427 → 1391); alone: −17 / −83, because the mint's long task blocks it; measured | M | `session/create-jazz-session.ts` `openBrowserSessionCore`; `runtime/native-runtime/browser-shared-worker-connection.ts` (remember the name in `SharedBrowserForegroundNodeLease.acquire`, new `warmBrowserWorkerFromHint`); `local-first/index.ts` (export `warmUp`). Prototype: `patch/proto-early.mjs`. |
| 3 | **Open the worker's store without waiting for the page's first query**: send `connect-runtime` (or a "preopen") as soon as the account is known, using the app schema from the provider config or the schema the worker stored last time. Also create the page runtime eagerly at `createDb` instead of on the first `useAll`. | est. 20–50 ms reload (with 1 + 2: worker WASM at 250, but `openBrowser` waits until 273, and the lazy page runtime costs 30 more); est. 100+ ms fresh device (page runtime 517 → 654) | M–L | `connection-manager/browser-connection-manager.ts` `onClientCreated` / `openBrowserWorkerConnection`; `worker/jazz-broker-worker-core.ts` `connectTab` / `initialize`; `default-runtime-source.ts` `createClient` |
| 4 | **One WASM load per page load.** Either the page doesn't instantiate WASM until it needs to write (the worker serves reads as decoded rows), or the page runs without its own `WasmDb` peer. | est. 60–120 ms reload on 2 cores: one 9 MB read (50–100 ms) plus the compile contention; also ~25–140 ms of page runtime construction | L (architecture) | `runtime/default-runtime-source.ts` `nativeSchemaView` / `load`; `NativeRuntimeAdapter`; worker protocol |
| 5 | **App: no `useMe` → ShowPage read waterfall** (mount the show's reads in parallel with `useMe`) | est. ~40 ms local-first reload; one RTT on thin | S | `stage-plan/jazz/src` (`model/me.ts`, `App.tsx`/router) |
| 6 | **Immutable caching of hashed assets** (deployment config, not Jazz) | −72 ms reload (581 → 509), almost all of it the main bundle's revalidation and JS code cache (`main.tsx` 95 → 36); nothing measurable on top of 1 + 2 or on later reloads, and the WASM doesn't get faster; measured | S | hosting config; `BENCH_IMMUTABLE_ASSETS` here. Applies to every stack in the comparison, so it's for deployment guidance, not the comparison. |
| 7 | **Smaller WASM** (`wasm-opt -Oz`, strip the 490 KB name section) | est. ~10–20 ms (−14% bytes, −10% compile) | S | jazz-wasm build |
| 8 | `WasmDb.openBrowser` itself: ~50 ms warm, ~155 ms on a fresh device | unknown; needs a WASM-level profile of the worker | ? | `crates/jazz-wasm` `openBrowser` |

Combined, measured: 1 + 2 = **581 → 459 ms reload, 1533 → 1391 ms fresh device**; adding 6 doesn't change it further (458).

| milestone (now, reload) | base | early warm-up | fix-mint | fix-mint + early | immutable assets | all three |
| app JS running (main.tsx) | 95 | 95 | 43 | 83 | 36 | 32 |
| warm-up starts (prototype) |  | 102 |  | 91 |  | 71 |
| warm-up: worker spawned (prototype) |  | 116 |  | 97 |  | 73 |
| warm-up: worker alive (prototype) |  | 184 |  | 149 |  | 112 |
| warm-up: hands over to session (prototype) |  | 163 |  | 149 |  | 112 |
| session: account manager starts | 102 | 163 | 81 | 149 | 74 | 112 |
| session: account manager ready | 182 | 279 | 85 | 166 | 144 | 139 |
| createDb starts (account resolved) | 192 | 284 | 93 | 183 | 148 | 147 |
| SharedWorker created | 192 | 116 | 93 | 97 | 148 | 72 |
| page: WASM compile starts | 210 | 134 | 105 | 112 | 161 | 85 |
| worker: script running (connect) | 234 | 171 | 136 | 147 | 183 | 110 |
| page: lease granted | 301 | 340 | 187 | 224 | 241 | 200 |
| page: WASM instantiated | 298 | 278 | 212 | 203 | 261 | 181 |
| client ready (JazzProvider children) | 306 | 341 | 223 | 225 | 266 | 202 |
| page → worker: connect-runtime | 359 | 376 | 259 | 256 | 301 | 232 |
| worker: WASM instantiated | 397 | 338 | 308 | 250 | 360 | 262 |
| worker: WasmDb.openBrowser done | 449 | 432 | 336 | 323 | 387 | 303 |
| page ← runtime-ready | 452 | 434 | 338 | 325 | 389 | 306 |
| first render with 100 tasks | 526 | 507 | 416 | 392 | 460 | 396 |
| 100 cards in the DOM | 581 | 564 | 476 | 459 | 509 | 458 |

| case (100 cards) | join (fresh device) | reload | 6th reload | reload minus main.tsx |
|---|---:|---:|---:|---:|
| now, base | 1533 | 581 | 487 | 486 |
| now, early warm-up (v2) | 1450 | 564 | 480 | 469 |
| now, fix-mint | 1427 | 476 | 485 | 433 |
| now, fix-mint + early | 1391 | 459 | 464 | 376 |
| now, immutable assets | 1530 | 509 | 490 | 473 |
| now, all three | 1492 | 458 | 491 | 426 |
| as shipped, base | 875 | 637 | 487 | 543 |
| thin, base | 490 | 277 | 236 | 191 |

The 6th reload barely moves with any fix (~465–490). It is already CPU-bound on the two concurrent WASM loads, which is fix 4.

**About prototype A (early warm-up).** The first version (`proto-early-v1`) called `warmUp()` without waiting and saved nothing: its continuations ran only after the mint's long task. Version 2 waits up to 60 ms for the worker's probe answer and the page's WASM request before letting the mint run, and that delays the account manager. So on its own it moves work earlier but makes the account later (on 2 cores the mint and the compiles compete). It pays off once the mint is cheap (fix 1).

Safety of the prototype:
- The probe is the message `SharedBrowserForegroundNodeLease` already sends first. The worker answers it with `worker-alive` and prefetches its WASM. It performs no admission and opens no store, and the bootstrap port holds the realm alive for up to 30 s.
- A remembered name that turns out wrong (another account, a new deploy's asset scope) only spawns an idle realm, which the browser reclaims.
- Not handled in the prototype: in-memory `wasmModule`/`wasmSource` runtime sources (skipped, as `workerWasmPrefetch` skips them), and worker generations (it reads the current one like `acquire` does).

## Files

- `patch/marks.mjs`: the `lf:*` marks (page and worker).
- `patch/proto-early.mjs`: prototype A (fix 2). Apply it after the marks.
- `patch/with-thin-fix-mint.mjs` + `patch/thin-fix-mint.mjs`: #jazz-thin-startup's fix 1, applied in place (copied from `profile/thin-startup`).
- `patch/wasm-compile-bench.mjs`: Chrome compile time for WASM binaries, alone and page + worker together.
- `table.mjs`: the tables above, from the summaries.
- `stage-plan.<variant>.<tag>.marks/`: `summary.json` (medians of join, reload and 6th reload) and `run-N.json`. Each run holds raw marks, port messages, frames and resources for the page and the worker; `reloadLast` is the 6th reload.
  - `now.base`, `default.base`, `thin.base`: baselines.
  - `now.proto-early-v1`, `now.proto-early2`: prototype A without and with the yield.
  - `now.mint`, `now.mint-early`: fix 1, and fix 1 + prototype A.
  - `now.immutable`, `now.mint-early-immutable`: with `BENCH_IMMUTABLE_ASSETS=1`.
- Harness changes: `harness/breakdown.mjs` (`--reloads`, `--tag`, `--out`) and `stage-plan/jazz/vite.config.ts` (`BENCH_IMMUTABLE_ASSETS`, off by default, so the default build is unchanged).
