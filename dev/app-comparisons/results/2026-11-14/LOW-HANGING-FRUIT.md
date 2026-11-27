# Jazz's weak spots in the comparison: lowest-hanging fruit (2026-11-14)

This summarises four profiling threads. Each one has a PR in this repo with its scripts, raw data and prototypes (none merged):
- thin-client startup, [#6](https://github.com/garden-co/jazz-app-comparisons/pull/6)
- JS and WASM size, [#8](https://github.com/garden-co/jazz-app-comparisons/pull/8)
- local-first startup, [#9](https://github.com/garden-co/jazz-app-comparisons/pull/9)
- local-first delivery, [#10](https://github.com/garden-co/jazz-app-comparisons/pull/10)

Timings come from marks-only runs on 2-core sandboxes. Effort: S = hours, M = a day or two, L = architectural.

## Ranked by gain per effort

| # | Fix | Where | Measured gain | Effort |
|---|---|---|---|---|
| 1 | **Stop recomputing the Ed25519 base-point table and minting the self-signed token twice per load.** A 4-bit window, one mint per load, and reuse within the token's 1 h TTL. | `thin/self-signed-token.ts` `mintSelfSignedToken`, callers in `accounts/` | thin reload −49 ms (−24 ms at 40 ms RTT); local-first `now` reload −105 ms. Its long task currently blocks the SharedWorker from starting. | S (prototyped) |
| 2 | **Post the first outbound frame per task synchronously** instead of queueing it behind React's render. | `BrowserWorkerTransportPump.scheduleOutboundDrain` | local-first sender frame leaves at 7 ms instead of 219 ms. BandChat cross-client 592 → 333 ms on the receiver's DOM (thin: 280). | S, ~15 lines (prototyped) |
| 3 | **Warm up early:** spawn the remembered SharedWorker and start the page's WASM before the account resolves. | local-first session start | `now` reload 581 → 459 ms with #1; fresh device 1,533 → 1,391 ms | M (prototyped) |
| 4 | **Pipeline the thin handshake:** send the schema bind and subscribes behind Hello, without waiting for Welcome and the schema answer. Needs an in-order guarantee for frames sent before Welcome. | `thin/thin-runtime.ts`, `WebSocketCarrier` | −15 ms locally, −44 ms at 40 ms RTT (first result 87 ms sooner) | M (prototyped) |
| 5 | **Dial at `createDb`**, not at React's first subscription. | `direct-connection-manager.ts` | about −9 ms locally, −40 to −60 ms at 40 ms RTT | M |
| 6 | **Minify and code-split the published worker bundle** (esbuild currently runs with neither; OpenTelemetry is inlined and is 22% of the worker). Drop the dead unminified copy Vite emits. | `scripts/bundle-broker-worker.mjs:55-69`, `runtime/browser-worker-assets.ts:12` | Next worker 178 → 116 KB; Vite −21.7 KB (with `worker.format: "es"`) | S |
| 7 | **Build the WASM without `-g`**, and drop the FTS5/R-Tree that sqlite-wasm-rs hard-codes. | `crates/jazz-wasm/Cargo.toml:72`; sqlite-wasm-rs `build.rs` | −80 KB gzip (measured) plus ~−80 KB (estimated) of the 3.1 MB gzip, which is fetched by page and worker | S / M |
| 8 | **`schema` as an ES namespace**, so clients don't keep the permissions DSL. | `src/schema-namespace.ts:40` | −9.4 KB gzip on every client | S |
| 9 | **`"sideEffects"` in jazz-tools' package.json** (after an audit) | `packages/jazz-tools/package.json` | −4.9 KB thin; 18 KB leaves local-first's first load | S |
| 10 | **Open the worker store without waiting for the page's first query.** | worker start | est. −20 to −50 ms on local-first reload | M |
| 11 | **One WASM load per page load instead of two** (page and worker both load the 9 MB module). | local-first architecture | est. −60 to −120 ms local-first startup, plus half the WASM transfer on a cold load | L |

Together, #1, #4 and the socket-without-IndexedDB tweak take a thin reload from 227 to 170 ms locally (−25%), or from 518 to 419 ms at 40 ms RTT. #1 and #3 take a local-first `now` reload from 581 to 459 ms.

## Structural, not quick

- **The thin client is built on the full `Db`/client/subscription stack.** That's most of Jazz's 119 KB (gzip) JS gap to Convex. `runtime/db.js` alone is 21.7 KB.
- **The 3.1 MB gzip of WASM is the size that matters most for local-first.** The page-level "JS loaded" figure never showed it, because the worker fetches it.

## App-side (not Jazz), and a fairness note

- `React.memo` on BandChat's message groups and room rows takes cross-client from 592 to 149 ms with unpatched Jazz. It works because Jazz keeps unchanged rows identical. All ports share the UI, so the comparison keeps it unmemoised.
- StagePlan's `useMe` → ShowPage read waterfall costs about one RTT (~40 ms).
- Astryx's per-row cost (`Timestamp`, `Button`/`useTooltip`) dominates BandChat in every stack.

## Caveats

- The prototypes are patched local copies of jazz-tools alpha.62, not PR-ready code.
- #2 measured the sender's own echo ~35 ms slower, likely CPU contention on 2 cores, unproven. StagePlan reload came out 15–45 ms slower, within noise. Both need a recheck before shipping.
- #4 changes protocol ordering assumptions.
