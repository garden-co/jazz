# Jazz local-first: the sender's delivery delay (BandChat, 2026-11-14)

This follows up on [#4](https://github.com/garden-co/jazz-app-comparisons/pull/4): in local-first, the sending tab posts a new message's frame to its SharedWorker only about 330 ms after the click. Why, what it scales with, and what would remove it.

Everything is measured on published `jazz-tools@2.0.0-alpha.62`, except where a case says it is patched. The patches are prototypes in this folder; there are no Jazz PRs.

## Summary

1. **It isn't a re-query. It's a microtask race that a synchronous React re-render wins.**
   - At 4.6–5.3 ms after the click, the write commits natively.
   - At ~7.5 ms, Jazz delivers the new rows to `useAll`'s `useSyncExternalStore` listeners. React queues its synchronous render as a microtask.
   - At ~8.0 ms, the core tick (`Binding::tick`) produces the frame for the worker. `QueueTransport::send_frame` calls the pump's outbound scheduler, which queues the drain as another microtask, behind React's.
   - React then renders for ~210 ms. The drain posts the frame only after that (219 ms median).
   - Jazz itself (JS + WASM) is ~10 ms of the window. The rest is the app's re-render:
     - Astryx UI components: ~110 ms
     - the React reconciler: ~70 ms
     - GC/native: ~37 ms
2. **It scales with how much the sender re-renders, nothing else.** The frame leaves at:
   - 219 ms with 200 bubbles rendered (the app);
   - 80 ms with 50;
   - 54 ms with 20.

   These change nothing:
   - room size: 200 / 2,000 / 6,000 messages give 220 / 219 / 240 ms;
   - query window: 500 rows, still rendering 200, gives 229 ms;
   - the ~255 unread subscriptions: Jazz JS is ≤ 7 ms in every trace.
3. **Posting synchronously when the transport announces the frame removes the delay.**
   - Patch `sync-outbound-first`: ~15 lines in `browser-worker-transport.js`. It posts the wire frames synchronously, at most once per task; the rest of a task's frames are batched as before.
   - The frame leaves at **7 ms instead of 219**.
   - The receiver's DOM gets the message at **333 ms instead of 592**, close to thin (280).
   - The unchanged `run.mjs`: BandChat `bc_cross_client` 913 → **665 ms**, StagePlan `sp_cross_client` ~60 → 55 ms.
   - The simpler `sync-outbound` (every frame synchronously) does the same for delivery, but makes StagePlan's join 110–150 ms slower in 3 of 3 repeats. Batching matters during startup.
4. **The bigger lever is the app's own render, on both ends.**
   - Jazz keeps unchanged row objects identical across updates. So `React.memo` on the message groups and room rows (flag `VITE_BENCH_MEMO=1`) makes a send re-render only what changed.
   - Sender DOM: 221 → 29 ms. Receiver DOM: 592 → **149 ms** with unpatched Jazz, 150 with the patch. Thin with memo: 43 ms.
   - This is app code shared by every port, so whether to apply it is a fairness decision for the comparison (below).

## How it was measured

- **Harness:** `harness/lf-delivery.mjs`, built on `breakdown.mjs` and its probe.
  - It opens users a and b in the busy room and waits 30 s for the background local-first upgrade to settle.
  - Then it sends 10 messages from a, plus one warm-up.
  - Per send, it records these times from the click: `send:committed`, `data:newest` (a's first React render with the message), the probe's first port message carrying the text ("frame leaves a"), and the bubble in a's and b's DOM (MutationObserver).
  - Medians are over the 9 untraced samples. Noise is about ±10 ms on the sender and ±40 ms on b.
- **Traces:** sample 3 of each case is traced: a Chrome trace of a's renderer (timeline + V8 CPU samples at ~0.1 ms). That sample is left out of the medians.
  - `harness/lf-delivery-analyze.mjs` splits a trace through the build's source maps. Buckets:
    - `react` (react-dom/scheduler)
    - `ui` (@astryxdesign/core, @stylexjs)
    - `app` (src/)
    - `jazz-js` (jazz-tools, by file)
    - `wasm`
    - `native` (GC, builtins, style/layout)
  - The analyzer also gives the call sequence, the stack that posted the frame, and timeline events.
  - `components-unminified*` are the same with `BENCH_NO_MINIFY=1`, so the stacks have names.
- **App knobs** (flag-gated, off by default; the shipped build is unchanged):
  - `VITE_BENCH_HISTORY` (the window, default 200)
  - `VITE_BENCH_RENDER_LIMIT` (render only the newest N bubbles; the query is unchanged)
  - `VITE_BENCH_NO_UNREAD=1` (no per-room unread queries)
  - `VITE_BENCH_MEMO=1`
  - `BENCH_BUSY_MESSAGES` (messages in the busy room, fixture default 2,000 of 10,000)
- **Patches:** `harness/lf-patches/apply.py <pristine dist> <names>` edits the app's installed `jazz-tools/dist` in place, or restores it with `none`. `APP=stage-plan/jazz` targets StagePlan. The resulting diffs are in `patches/`.
  - `sync-outbound`: in `BrowserWorkerTransportPump.scheduleOutboundDrain`, call `drainOutboundFrames()` synchronously, with the microtask as fallback if that throws. It never threw: 0 errors in ~550 synchronous drains per run.
  - `sync-outbound-first` (recommended): the same, but at most once per task (a flag reset by `setTimeout(0)`), and only `recvWireFrames()`. Later frames in the same task, and auxiliary frames, go through the existing microtask. 14 synchronous drains per run, 0 errors.
  - `defer-delivery`: commit paths deliver to local subscriptions one task later. It had no effect (see below).
  - `no-server-debounce`: `SERVER_PUMP_DEBOUNCE_MS` 16 → 0.
  - `marks`: instrumentation only. It adds `performance.mark`s at the native commit, `pumpSubscriptions`, the outbound scheduler, the pump and each non-empty drain.
- **Machine:** this sandbox has 2 vCPUs (Xeon 2.9 GHz). Both browser contexts, both SharedWorkers and the Jazz server share them. Absolute numbers are lower than the REPORT machine (PR #4 measured the frame at ~337 ms; here it's 219), but the shape is the same.
- **Other runs:**
  - `worker-hops/` is `breakdown.mjs band-chat --settle 30000` (with probes in the workers) per patch. It got a new `--out` option so it doesn't overwrite PR #4's data.
  - `run-mjs/` holds unchanged `harness/run.mjs` results (20 samples, the as-shipped default variant), pristine vs patched. The originals in `results/2026-11-14/*.json` were restored.
- **Reproduce:**
  - `node harness/lf-delivery.mjs --tag baseline`
  - `python3 harness/lf-patches/apply.py <copy of pristine dist> sync-outbound && node harness/lf-delivery.mjs --tag patch-sync-outbound`
  - Tables: `node harness/lf-delivery-report.mjs`.

## 1. The sender's main thread, click → frame posted

#### Traced sample, self time by bucket (ms)

| case | window | react | ui | app | jazz-js | wasm | native | other-js |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| baseline | click → frame 238 | 71 | 110 | 4 | 5 | 4 | 37 | 7 |
| baseline, unminified | click → frame 233 | 70 | 109 | 4 | 5 | 1 | 38 | 6 |

Everything happens in one task: the click's microtask checkpoint, a single 486–560 ms `RunMicrotasks`. Layout (7 ms) comes at the end.

The heaviest functions (inclusive, unminified, ms):
- Astryx `Button`: 49
- `useTooltip`: 39. Both are the per-bubble `MessageActions`.
- `Timestamp`: 35, of which `formatInstant` 32. This is per bubble and per room row.
- `useLayerImplementation`: 19
- `SideNavItem`: 10. This is the 256 room rows, which re-render because the send also updates `rooms.lastActivityAt`.
- GC: ~20

Jazz's own frames (`typed-app` row building, `use-all`, `native-codec`, `subscription-manager`) add up to ≤ 5 ms. WASM (commit, tick, subscription pull) adds ≤ 5 ms.

#### The sequence (unminified trace, `marks` patch, ms after the click)

| t | what |
|---:|---|
| 2.8 | `send:start` (the app's `send()`) |
| 4.1–4.4 | React sync render and commit for `setSending(true)` |
| 4.6–5.3 | `db.transaction` → `commitTransaction` → WASM `Db::commit_ops`; then `pumpSubscriptions` (nothing ready to deliver yet) |
| 5.6–6.9 | the probe's DOM check (measurement overhead, ~1.5 ms) |
| 7.1–7.8 | a WASM subscription-stream future resolves → `readSubscription`'s `consume` → `applySubscriptionChunk` → `SubscriptionManager.handleDelta` → the orchestrator → the `useSyncExternalStore` listener. **React queues its render microtask.** |
| 7.3 | the follower pump runs (`pump`: drain empty, then `progressPeerTransport`) |
| 8.0 | `Binding::tick` → `QueueTransport::send_frame` → `outbound_ready` → `scheduleOutboundDrain` **queues the drain microtask, behind React's** |
| 8.1 | `send:committed` |
| 8.3–226 | `processRootScheduleInMicrotask` → `performSyncWorkOnRoot`: React renders the room |
| 228.5 | the drain: `drainOutboundFrames` → `postMessage` (the probe sees the frame here) |
| 230.8 | the bubble in a's DOM |

**Answer to "is the commit flushed only after a synchronous local re-query plus re-render?":**
- Only after a synchronous re-render. The local re-query is not the problem: it costs a few ms and happens before the render.
- The frame for the worker exists ~0.5 ms after React's render is queued. It just waits for that render, because both are microtasks in the same checkpoint and React's is first.

**Why `defer-delivery` didn't help:** with delivery from the commit-time `pumpSubscriptions` moved to a later task, the frame still left at 229 ms. React is woken by the async `readSubscription` stream reader (above), not by that call.

## 2. Where it scales

All on unpatched Jazz. Median ms after the click; 9 samples each.

| case | knob | committed | render starts | frame leaves a | a dom | b dom |
|---|---|---:|---:|---:|---:|---:|
| baseline | 200-message window, 2,000 in the room, 200 bubbles | 7 | 23 | **219** | 221 | 592 |
| room size 200 | `BENCH_BUSY_MESSAGES=200` | 7 | 24 | 220 | 222 | 632 |
| room size 6,000 | `BENCH_BUSY_MESSAGES=6000` | 7 | 24 | 240 | 242 | 609 |
| window 500, render 200 | `VITE_BENCH_HISTORY=500`, `VITE_BENCH_RENDER_LIMIT=200` | 8 | 25 | 229 | 232 | 657 |
| window 50 | `VITE_BENCH_HISTORY=50` | 5 | 24 | **80** | 81 | 297 |
| render 20 (window 200) | `VITE_BENCH_RENDER_LIMIT=20` | 5 | 23 | **54** | 54 | 283 |
| no unread queries | `VITE_BENCH_NO_UNREAD=1` | 7 | 75 | 288 | 291 | 677 |
| memoized groups and rows | `VITE_BENCH_MEMO=1` | 7 | 9 | **27** | 29 | **149** |

What it shows:
- **Render cost is the whole story.** The delay follows the number of bubbles rendered: 200 → 219 ms, 50 → 80, 20 → 54.
  - The query window (500 vs 200 with the same render) doesn't matter.
  - Neither does the room's size (200 / 2,000 / 6,000).
  - The traces agree:
    - window 50: react 30 + ui 36 ms;
    - render 20: react 21 + ui 17 ms;
    - jazz-js 4–5 ms in each.
- **The subscriptions don't cost the sender.** Turning off the ~255 unread queries made it *slower* (288 ms). Without counts, every room row renders an Astryx `<Timestamp>` instead of a `<Badge>`, and `formatInstant` is the most expensive UI function. The trace: ui 171 ms, jazz-js 2 ms.
- **What stays at 20 bubbles (~54 ms)** is roughly the 256 room rows re-rendering, plus React and the composer.
- **Memoization works because Jazz preserves row identity.** `applyBulkSubscriptionDelta` (`runtime/subscription-manager.js`) keeps unchanged items. With `React.memo` on `MessageGroup` (comparing rows by identity, and their reactions) and on `RoomNavItem` (comparing the room object and summary fields), a send renders one bubble group and one room row. The sender's render drops from ~210 ms to ~20 ms, with no long tasks. The receiver gains the same way: 592 → 149 ms.

## 3. Letting the worker hop start before the re-render (prototype)

| case | jazz-tools | committed | render starts | frame leaves a | a dom | b dom |
|---|---|---:|---:|---:|---:|---:|
| baseline | pristine | 7 | 23 | 219 | 221 | 592 |
| patch-defer-delivery | `defer-delivery` | 8 | 25 | 229 | 231 | 621 |
| patch-sync-outbound | `sync-outbound` | 7 | 28 | 7 | 257 | 360 |
| **patch-sync-outbound-first** | `sync-outbound-first` | 7 | 27 | **7** | 266 | **333** |
| patch-sync-outbound, render 20 | `sync-outbound` | 5 | 27 | 4 | 62 | 164 |
| app-memo | pristine | 7 | 9 | 27 | 29 | 149 |
| app-memo, sync-outbound | `sync-outbound` | 8 | 10 | 7 | 34 | 150 |
| thin | (thin, own WebSocket) | 5 | 47 | 4 (ws) | 318 | 280 |
| thin-memo | (thin) | 5 | 34 | 4 (ws) | 61 | 43 |

#### Unchanged `harness/run.mjs` (20 samples, median ms; `run-mjs/`; repeats separated by /)

| | pristine | sync-outbound | sync-outbound-first |
|---|---:|---:|---:|
| BandChat `bc_cross_client` | 913 | 666 | **665** |
| BandChat `bc_send` | 246 | 255 | 279 |
| StagePlan `sp_cross_client` | 61 / 62 / 59 | 51 / 50 / 50 | 55 / 55 |
| StagePlan `sp_join_board` | 597 / 595 / 592 | 748 / 735 / 704 | **551 / 554** |
| StagePlan `sp_reload_board` | 525 / 523 / 534 | 586 / 548 / 530 | 549 / 579 |

#### Hop by hop (`worker-hops/`, `breakdown.mjs` probes in the workers, settled 30 s, median ms after the click)

| hop | pristine | sync-outbound | sync-outbound + no-server-debounce |
|---|---:|---:|---:|
| a page → a worker (port) | 229 | 6 | 6 |
| a worker → server (ws) | 242 | 20 | 17 |
| server → b worker (ws) | 277 | 72 | 75 |
| b worker → b page (port) | 307 | 92 | 97 |
| b: React render with the message | 335 | 132 | 134 |
| b: message in the DOM | 584 | 413 | 392 |

What it shows:
- **The frame can leave before the re-render, and then nothing else on the path waits for the sender.**
  - After the fix: sender → worker 7 ms, worker → server ~13, server → b's worker ~55, b's worker ~20, b's page receives and starts rendering ~40, and b renders ~250–280.
  - **b's own render is now most of the remaining time**, the same as thin's. The memo flag removes it.
- **The cost on the sender's own echo:** a's DOM 221 → 257 ms (9 samples each; it was 230 → 275 in an earlier run). `run.mjs`'s `bc_send` barely moves: 246 → 255.
  - The traced render is the same components, each 15–25% slower (unminified: 233 → 277 ms).
  - The likely cause is CPU contention on this 2-vCPU box: b's ~250–280 ms render and both workers now run *during* a's render instead of after it. Real senders and receivers are on different machines.
  - I haven't proven this. It could be checked by pinning the tabs to separate machines or cores.
- **The 16 ms server-pump debounce in the sending worker costs ~3 ms** (worker in → ws out: 13.3 → 10.3 ms). The rest of that hop is the worker's own tick. Not worth it on its own.
- **Startup needs the batching.** With `sync-outbound`, StagePlan's join is 110–150 ms slower in all three repeats. Every frame is posted on its own, where the microtask batched them, and a join sends many.
  - `sync-outbound-first` posts synchronously only once per task. That keeps join at pristine levels or better (551/554 vs 592–597) and keeps the delivery gain.
  - Reload is 549/579 vs 523–534 pristine. That is 15–45 ms, within this box's spread for reloads (plain `sync-outbound` gave 530–586), but it is worth a check on a quieter machine.
- `bc_send` (the sender's own echo in `run.mjs`) is 246 / 255 / 279 ms. The direction fits the contention explanation above. It is unproven.

## 4. Ranked fixes

| # | fix | where | saves (measured here) | effort |
|---|---|---|---|---|
| 1 | **Post page→worker frames synchronously when the native transport announces them** (the first time per task; batch the rest as now), instead of from a microtask that queues behind React's `useSyncExternalStore` render | `packages/jazz-tools/src/runtime/native-runtime/browser-worker-transport.ts`, `BrowserWorkerTransportPump.scheduleOutboundDrain` (line ~150; it is called from the Rust `QueueTransport::send_frame` → `outbound_ready` in `crates/jazz/layers/db/src/binding_host.rs` through `WasmTransport.setOutboundScheduler`, `crates/jazz-wasm/src/lib.rs`) | BandChat: the frame leaves at 7 ms instead of 219 (−212). Cross-client: −259 ms here (592 → 333), −248 ms in `run.mjs` (913 → 665), so local-first comes close to thin. StagePlan cross-client −5 to −10 ms. The saving equals the sender's local re-render, so it grows with the app. | S: about 15 lines (`patches/sync-outbound-first.diff`). Tests: page→worker ordering, an aux-frame interleave, a re-entrant drain during a tick, and startup batching (StagePlan join/reload) |
| 2 | **Memoize BandChat's message groups and room rows** (`React.memo` with identity comparators). This is app code, not Jazz. It relies on Jazz keeping unchanged row objects identical, which it does. | `band-chat/*/src/components/RoomView.tsx` `MessageGroup`, `RoomNav.tsx` `RoomNavItem` (the flag-gated version is here) | Sender re-render ~210 → ~20 ms. The frame leaves at 27 ms even without fix 1. Cross-client 592 → 149 ms (−443). The same render dominates `bc_send` (214–239 ms in every stack in REPORT.md). | S for Jazz BandChat. A fairness call for the comparison: either every port gets it (whether it works depends on whether each data layer keeps row identity) or none does. |
| 3 | Reduce Astryx's per-row cost: `Timestamp`/`formatInstant` (~32 ms per send), and `Button` + `useTooltip` in `MessageActions` (~88 ms inclusive per send for ~200 bubbles) | `@astryxdesign/core` (upstream), or in the app: render `MessageActions` lazily on hover, and cache formatted times | ~40–120 ms per render on both ends (estimated from inclusive times; not prototyped). Mostly superseded by fix 2. | M; upstream |
| 4 | Drop the worker's 16 ms server-pump debounce, or skip it for user writes | `native-runtime-adapter.ts:153` `SERVER_PUMP_DEBOUNCE_MS`, `scheduleServerPump` | ~3 ms | XS, but loses batching |
| – | Not worth it: fewer subscriptions, a smaller query window or room, or deferring the commit-time `pumpSubscriptions` | — | ≤ 5 ms (Jazz JS + WASM is ≤ 12 ms in every trace); `defer-delivery` measured 0 | — |

**Notes on fix 1:**
- `sync-outbound-first` is the form to start from:
  - it reads only `recvWireFrames()` synchronously;
  - the auxiliary queue and its trace stay on the existing microtask;
  - after the first synchronous post, a task's later frames are batched as before, so a startup burst isn't one `postMessage` per frame.
- The once-per-task flag is reset with `setTimeout(0)`. A real version might reset it from the pump's own turn bookkeeping instead.
- If an earlier frame in the same task takes the synchronous slot, the user's write waits for the microtask again. That didn't happen in BandChat. StagePlan's smaller gain (55 vs 50 ms with plain `sync-outbound`) may be this case.
- `send_frame` releases its outbound `RefCell` borrow before it calls `outbound_ready`, which is why `take_outbound` from inside the callback works.
- An alternative that doesn't touch the callback: notify React listeners one task later (in the subscriptions orchestrator). That would delay every local echo by a task, and it races the same way if anything else queues first. The synchronous drain is the more direct fix.

**StagePlan** (55 vs 23 ms in REPORT.md): it has the same mechanism. The board's local re-render is small (tens of ms), so fix 1 saves only ~10 ms there.

## Caveats

- The sandbox has 2 vCPUs, with both tabs, both workers and the server on one machine. That likely inflates the patched sender's own render (section 3).
- The analyzer attributes minified frames through source maps by function start. File buckets are reliable. In minified traces, function names are only approximate. The unminified traces carry the real names.
- The probe's DOM check (MutationObserver + `querySelectorAll` over the bubbles) costs ~1.5–3.5 ms per mutation on the sender. It is included in every case alike.
- `VITE_BENCH_MEMO`'s room-row comparator ignores `onSelect`, which BandChat recreates on each render but which always selects by room id. A real change would stabilise it with `useCallback`.

## Files

- `README.md`: this file. `report.json`: medians and CPU splits for every case (`node harness/lf-delivery-report.mjs`).
- `<case>/`:
  - `sample-NN.json`: raw marks and port/WebSocket logs for a and b, per send. `warmup.json`.
  - `summary.json`
  - `trace-03.json.gz`: the raw Chrome trace (loads in DevTools Performance).
  - `trace-03.to-port.json` / `.to-dom.json`: its split, click → frame posted / → a's DOM.
  - `components-unminified*/` traces samples 2–4.
- `patches/*.diff`: the prototypes against `jazz-tools@2.0.0-alpha.62/dist`.
- `worker-hops/<patch>/band-chat.default.settle30s.marks/`: `breakdown.mjs` output, workers included.
- `run-mjs/*.json`: `harness/run.mjs` records, pristine vs `sync-outbound`.
- Harness:
  - `harness/lf-delivery.mjs`, `lf-delivery-analyze.mjs`, `lf-delivery-report.mjs`, `lf-patches/apply.py`
  - `breakdown.mjs --out`; the probe now also marks `probe:port-out-needle`
  - app knobs in `band-chat/jazz/src/bench.ts`, `vite.config.ts` (`BENCH_SOURCEMAP`, `BENCH_NO_MINIFY`), and `fixtures/band-chat.mjs` (`BENCH_BUSY_MESSAGES`)
