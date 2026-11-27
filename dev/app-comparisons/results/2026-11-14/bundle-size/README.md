# Where Jazz's shipped JavaScript comes from, and what to cut first (2026-11-14)

Profile of jazz-tools **2.0.0-alpha.62** / jazz-wasm 2.0.0-alpha.62 in the StagePlan hero app (`stage-plan/jazz`, Vite 8 / rolldown).
Wequencer (`wequencer/jazz`, Next 16 / Turbopack) is used as a cross-check.
jazz source references are to garden-co/jazz `main` at `de1cc17`.

Every number is from a production build with sourcemaps, loaded in headless Chromium.
The scripts are in `scripts/` (see [How to reproduce](#how-to-reproduce)). Sizes are gzip -9 / brotli q11 of the bytes the page fetched.

## TL;DR

- **The shared UI (Astryx + React + formatjs) is ~215–220 KB of every port. Jazz's own share of the first page load is 142 KB gzip thin and 200 KB local-first.** Convex's StagePlan ships 23 KB of data layer (`convex` + `@convex-dev/auth`), measured the same way. The fixes below close part of that gap. Most of it is structural: the thin client is built on the full `Db` / client / subscription stack.
- **`metro-setup-error` is not React Native code.**
  - It's rolldown's name for the chunk shared by the page entry and the lazy `jazz-tools/local-first` chunk. Rolldown names a shared chunk after its first module, `dist/runtime/metro-setup-error.js`, which holds 0.5 KB of error strings.
  - Its contents are the Db / client core (`runtime/db.js`, `client.js`, `subscription-manager.js`, `query-adapter.js`, the connection manager) plus @noble crypto.
  - The page entry imports it statically, so it's modulepreloaded on every load. In the thin build the same code sits in `index`.
- **The harness's "JS loaded" leaves out the two biggest Jazz downloads:**
  - the SharedWorker: 111 KB gzip in Vite, 178 KB in Next;
  - the WASM: **8.9 MB raw, 3.1 MB gzip, 2.15 MB brotli**, fetched by both the page and the worker.
  - On the local-first variants, these dwarf the page JS.
- No dev-only inspector, Node-only or duplicated packages of any size reach the browser: the inspector bits total under 1 KB, and the Node lease is correctly swapped for its `.browser.js` stub.
  - What is dead weight is a second, unminified copy of the worker that Vite emits but never loads (174 KB gzip in `dist/`, finding 1b).
  - That copy is most of the gap between the "751 KB full build" and what loads.

## Ranked fixes

"Measured" means the patch was applied to a scratch copy of jazz-tools in StagePlan, rebuilt, loaded in Chromium, and the app still rendered its data with no console errors (local-first opened its IndexedDB store).

| # | Fix | Cause (exact place) | Saves (gzip) | Effort | Status |
|---|---|---|---|---|---|
| 1a | **Minify and code-split the published broker worker** | `packages/jazz-tools/scripts/bundle-broker-worker.mjs:55-69`: esbuild runs with no `minify` and no `splitting`. So the 912 KB worker ships unminified, and the lazy `import("@opentelemetry/…")` in `src/runtime/sync-telemetry.ts:372-376` is inlined (OTel is 22% of the worker). | **Next/Turbopack: −62 KB** of worker (178 → 116), since Turbopack serves `dist/worker` verbatim. **Vite: −21.7 KB** (110.9 → 89.2), but only with `worker: { format: "es" }`. Vite's default `iife` worker format inlines dynamic imports again. | S | Vite: measured. Next: size measured, not exercised (the app has a sign-in wall). |
| 1b | **Stop Vite emitting a dead copy of the raw worker** | `src/runtime/browser-worker-assets.ts:12` (`bundledBrowserWorkerUrl`, `new URL("../worker/jazz-broker-worker.js", import.meta.url)`). Vite copies the unminified 912 KB file as an asset, while the real worker is built from the `new SharedWorker(new URL(…))` in `browser-shared-worker-connection.ts:67`. | 0 KB loaded; **−174 KB of `dist/`** (StagePlan "all JS" 749 → ~575) | S | Measured by inspection: the asset is in `dist/` and never requested. |
| 2 | **Make `schema` an ES namespace** (`export * as schema from …`) instead of `Object.assign({}, …)` | `src/schema-namespace.ts:40`: the object literal pins every builder, including `definePermissions` (`:48`). That keeps the whole permissions DSL (`dist/permissions/index.js`) and its @noble sha256 in every client bundle, though permissions only run on the server. | **−9.4 KB** thin and local-first (Vite). Turbopack: ≈0. | S | Measured |
| 3 | **Declare `"sideEffects"`** in jazz-tools' package.json (e.g. `["**/*.css", …audited files]`) | `packages/jazz-tools/package.json` has no `sideEffects` field, so bundlers keep modules for their top-level code. | **−4.9 KB** thin (on top of #2). Local-first moves 18 KB from the first-load window into the deferred upgrade. Turbopack: ≈−1 KB with #2. | S, plus an audit of intentional side effects | Measured |
| 4 | **Drop the WASM `name` section from release builds** (ship the names as a separate debug artifact) | `crates/jazz-wasm/Cargo.toml:72`: `wasm-opt = ["-O", "-g", "--low-memory-unused"]`. `-g` keeps a 490 KB `name` section. | **WASM −80 KB gzip / −61 KB br** (3,107 → 3,027 gzip) | S (profiler names move to a debug build) | Measured (post-build strip, app works) |
| 5 | **Build SQLite without FTS5 and R-Tree** | `sqlite-wasm-rs` 0.5.5 `build.rs` `FULL_FEATURED` hard-codes `-DSQLITE_ENABLE_FTS5` and `-DSQLITE_ENABLE_RTREE` (plus `SESSION`, `DBSTAT`, `DBPAGE`…). No Jazz crate uses FTS5 or R-Tree (grep). They're 240 KB of WASM code. | **WASM ≈ −80 KB gzip** (est. at the binary's 0.35 gzip ratio) | M (patch or fork the crate, or upstream a feature flag) | Estimated |
| 6 | **Load the JSON-schema validator only when the schema has a JSON-schema column** | `src/runtime/wasm-loader.ts:94` `installJsonSchemaValidator(wasmModule)` runs on every WASM load. It pulls `json-schema-{evaluate,regex,formats,metaschemas,json}.js`. StagePlan has no JSON columns. | **−13 KB** local-first page, ~−15 KB worker (est.), −3 KB thin | M (the Rust callback is sync; it needs a "has JSON schemas" flag at registration) | Estimated from attribution |
| 7 | **Sign device-account tokens with WebCrypto Ed25519** (lazy @noble fallback) | `src/thin/self-signed-token.ts:8-10` and `src/local-first-auth/identity.ts:1-3` import @noble ed25519 + sha512 + sha1 into the page. | **≈ −10 to −14 KB** thin and local-first | M–L (these APIs are sync today; WebCrypto is async) | Estimated (13.9 KB of @noble on the thin page) |
| 8 | **Error codes instead of prose in production** | ~470 prose strings in jazz-tools on the thin page (e.g. `METRO_*` in `runtime/metro-setup-error.ts`, `[JAZZ_CLASSIC_API_REMOVED]`, the broker-load hints) | ≈ −5 to −8 KB (the strings are 10 KB gzipped on their own) | L | Estimated (`scripts/strings.mjs`) |
| 9 | **A thin client that doesn't load the local-first engine shell** | The thin path imports `runtime/db.js` (21.7 KB gzip alone), `client.js`, `subscription-manager.js`, `query-adapter.js`, the connection manager, session/accounts and the native-runtime adapter. Chain: `react/index.js → react-core/use-one.js → runtime/db.js`, and `session/create-jazz-session.js → thin/index.js → thin-runtime.js → native-runtime-adapter.js` (`scripts/why.mjs`). | Up to ~80 of the 142 KB | XL (architecture) | n/a |

**Measured together (#2 + #3 + #1a with `worker.format: "es"` + #4), StagePlan:**

| | baseline | patched | Δ |
|---|---:|---:|---:|
| thin: page JS | 373.1 | 359.0 | **−14.1** |
| local-first: page JS (all loaded) | 434.9 | 424.7 | −10.2 |
| local-first: page JS loaded in the first 1.5 s | 410.9 | 383.0 | −27.9 |
| local-first: SharedWorker | 110.9 | 89.2 | **−21.7** |
| local-first: WASM gzip / brotli | 3,107 / 2,152 | 3,027 / 2,090 | −80 / −62 |

With the WASM at 3 MB, the cheapest big lever for local-first is #4 + #5 (≈ −160 KB gzip), not the JS. For the "JS loaded" metric it is #2 + #3 (+ #6 / #7 next).

## Attribution

### StagePlan initial load, gzip KB (from `data/attribution-*.md`)

Each chunk's real gzip size is split across its modules in proportion to rollup-plugin-visualizer's per-module gzip.

| | thin | local-first (as shipped) | Convex port |
|---|---:|---:|---:|
| Astryx + React + formatjs + design (same in every port) | 218.3 | 221.5 | 214.0 |
| app code | 13.1 | 13.5 | 10.1 |
| Db / client core (`runtime/db.js`, `client.js`, `subscription-manager.js`, `query-adapter.js`, `row-transformer`, …) | 32.8 | 34.2 | |
| session / accounts | 18.5 | 18.9 | |
| other jazz-tools (≈150 small modules) | 17.9 | 24.7 | |
| @noble crypto | 13.9 | 14.6 | |
| transport / worker plumbing | 12.3 | 20.6 | |
| native-runtime adapter / codecs | 11.6 | 26.2 | |
| schema DSL / IR / migrations | 11.6 | 11.9 | |
| thin client | 8.0 | 8.2 | |
| permissions DSL (fix #2) | 7.5 | 7.7 | |
| JSON-schema validator (fix #6) | 3.0 | 13.3 | |
| local-first store (IndexedDB) | 2.3 | 6.6 | |
| wasm-bindgen glue | | 9.2 | |
| react bindings | 2.2 | 2.2 | |
| **all Jazz** | **141.8** | **199.8** | **22.8** (convex + auth) |
| **total page JS** | **373.1** | **434.9** | **246.9** |

The totals reproduce the report's "JS loaded" (374 / 436 / 247).

Treemaps (open in a browser): `treemaps/stage-plan-thin.html`, `treemaps/stage-plan-local-first.html`.

### Not counted as "JS loaded" (local-first only)

| asset | raw | gzip | brotli | notes |
|---|---:|---:|---:|---|
| `jazz-broker-worker-*.js` (SharedWorker, Vite-rebundled) | 435 KB | 110.9 KB | | 30% native-runtime adapter, 22% OpenTelemetry (fix 1a), 13% JSON-schema (fix 6), 11% wasm glue |
| `jazz_wasm_bg.wasm` | 8,965 KB | 3,107 KB | 2,152 KB | fetched twice, by the page and by the worker |
| dead copy of `dist/worker/jazz-broker-worker.js` | 913 KB | 174 KB | | emitted, never loaded (fix 1b) |
| Next/Turbopack: `media/jazz-broker-worker.*.js` | 913 KB | 178 KB | | the unminified published file, served as-is |

### Inside the WASM (`data/wasm-by-crate.txt`, `data/wasm-sections.txt`)

| | KB raw |
|---|---:|
| code section | 7,878 |
| … `jazz_db` | 2,041 |
| … `core` (mostly generic instantiations) | 1,247 |
| … `jazz_model` | 1,030 |
| … SQLite (C) | ~625 (FTS5 211, R-Tree 30: fix 5) |
| … `alloc`, serde, serde_json, postcard | ~1,240 |
| `name` section (fix 4) | 490 |
| data | 361 |

The largest functions are sync-engine entry points (`server_tick` 149 KB, `serve_as` 110 KB, `sync_tick` 79 KB) after fat LTO inlining. This isn't dead server code: every `Db` plays both roles, and the browser worker relays for its tabs (`sync_tick` → `server_tick`). Making the WASM substantially smaller beyond fixes 4–5 means reducing monomorphisation and inlining in `jazz_db` / `jazz_model`, which is a Rust project of its own.

## Why the big pieces are there

- **`metro-setup-error` chunk (StagePlan local-first, 76 KB gzip, eager):** see TL;DR. Importer chain to its main resident: `index.html → main.tsx → App.tsx → jazz-tools/react → react-core/use-one.js → runtime/db.js`. Renaming won't shrink it; fix 9 would.
- **Permissions DSL in the client:** `schema.ts → jazz-tools (index.js) → schema-namespace.js → permissions/index.js`. The app only needs `s.table`, `s.string`, … in the browser.
- **JSON-schema validator in the page:** `jazz-tools/local-first → runtime/default-create-db.js → default-runtime-source.js → wasm-loader.js → json-schema-validator.js`, installed unconditionally.
- **The page loads the 9 MB WASM too (local-first):** `default-runtime-source.ts` creates a main-thread peer runtime (`mainThreadPeerRuntime`) next to the worker's. That's by design (optimistic local writes in the page), but it means a second compile of the binary on every page load. It's worth weighing alongside the report's ~300 ms local-first startup.
- **OTel in the worker:** it's lazy in the page (separate `esm-*` chunks, never fetched). In the worker, esbuild (no `splitting`) and then Vite (`iife` workers) both inline it.
- **Wequencer (Next/Turbopack) sign-in page:** 400 KB loaded, ≈154 KB of it jazz-tools + @noble, the same shape as StagePlan thin. Fixes #2 / #3 only saved ~1 KB there: Turbopack doesn't shake namespace members the way rolldown does. For Next apps, fix 1a (minify the published worker) is the big one.

## Caveats

- One app (StagePlan) is profiled in depth. Wequencer only checks the Next/Turbopack differences, on its sign-in page (the app needs an account to get further).
- Brotli is computed by the scripts. The harness's `vite preview` serves everything uncompressed with `Cache-Control: no-cache`, so on localhost the WASM goes over the wire at 8.9 MB.
- The first-load split ("first 1.5 s") depends on timing. "All loaded" is the stable number and matches the harness.
- The worker prototype (1a) was rebuilt from the published `dist` with esbuild, not from source. It put the wasm-bindgen glue in its own chunk, which costs the worker one extra request. A real fix should keep the glue in the entry chunk.

## How to reproduce

```sh
cd stage-plan/jazz && pnpm install && pnpm add -D rollup-plugin-visualizer
# A Chromium for Playwright (see harness/); the scripts load the harness's playwright.
results/2026-11-14/bundle-size/scripts/build-and-measure.sh $PWD baseline   # writes /tmp/bundle-size/baseline/*
node results/2026-11-14/bundle-size/scripts/attribute.mjs /tmp/bundle-size/baseline/thin.json /tmp/bundle-size/baseline/dist-thin/assets <chunk files loaded>
node results/2026-11-14/bundle-size/scripts/why.mjs /tmp/bundle-size/baseline/thin.graph.json runtime/db.js --static
python3 results/2026-11-14/bundle-size/scripts/wasm-attr.py node_modules/jazz-wasm/pkg/jazz_wasm_bg.wasm 30 'fts5|Fts5'
```

| script | what it does |
|---|---|
| `build-and-measure.sh` | Builds thin and local-first with `viz.config.ts`, serves each with `vite preview` (in-memory Jazz backend) behind `logproxy.mjs`, and runs `measure-load.mjs`. Prints the page JS, the SharedWorker JS (from the proxy log; Playwright doesn't see worker fetches) and the WASM, plus a smoke check (console errors, IndexedDB stores, page text). |
| `viz.config.ts` | The app's vite config plus rollup-plugin-visualizer (treemap + raw data) and a dump of the module graph (static vs dynamic imports). |
| `attribute.mjs` | Splits each chunk's real gzip / brotli across packages and Jazz module groups. |
| `why.mjs` | Shortest import chain from the entry to a module (`--static`: only what lands in the first load). |
| `wasm-sections.py`, `wasm-attr.py` | WASM section sizes, and code bytes per Rust crate / function from the name section. |
| `strings.mjs` | Prose string bytes per package in a minified chunk (acorn + sourcemap). |
| `sme-dir.js` | source-map-explorer totals over a directory of chunks (used for Next). |
| `prototype-rebundle-worker.mjs` | Fix 1a prototype: re-bundles `dist/worker` with splitting (and optional minify). |

`data/load-<patch>-<variant>.json` are the raw page loads for baseline, `p2-sideeffects` (#2 + #3), `p3c-worker-split-es` (+ #1a) and `p4-wasm-strip` (+ #4). Each patch is cumulative.
