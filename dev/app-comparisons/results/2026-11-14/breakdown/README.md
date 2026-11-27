# Jazz startup and cross-client breakdown (2026-11-14)

This is measurement only, for the questions in #local-first-first-callback. Jazz itself is unpatched (published `jazz-tools@2.0.0-alpha.62`). The app changes are flag-gated marks, and the default builds are unchanged.

## How

- **App marks** (`VITE_BENCH_MARKS=1`), in `src/bench.ts` of each app:
  - `app:main`: main.tsx is running.
  - `jazz:client-ready`: the first render of the JazzProvider's children.
  - StagePlan: `join:accepted`; `data:show` and `data:tasks-100` (first render with the data); `render:board-100` (the board's layout effect).
  - BandChat: `send:start` and `send:committed`; `data:newest` (each render with a new newest message).
- **Debug logs**: `VITE_JAZZ_LOG=debug` sets the client config's `logLevel` (`DbConfig.logLevel`, accepted by `JazzClientConfig`). That level reaches both the page's WASM and the worker's WASM.
- **First-load wait**: `VITE_FIRST_LOAD_WAIT_MS=0` replaces the 5,000 ms `firstLoadRemoteWaitMs` in `ShowPage.tsx` and `actions.ts`.
- **Probe** (`harness/breakdown-probe.js`): observes from outside Jazz, with no patching.
  - Where: every page (`addInitScript`) and every SharedWorker. For the worker, a raw CDP connection runs `Target.setAutoAttach` with `waitForDebuggerOnStart` and a `shared_worker` filter. The probe is injected before the worker's first line runs.
  - WebAssembly: wraps `instantiateStreaming`/`instantiate`/`compile*`. This gives the "WASM instantiated" marks for both page and worker.
  - WebSocket frames and MessagePort messages: timestamps, the worker protocol's message `type`, and whether a message carries the test message's text.
  - Also: console output (worker console via CDP `Runtime.consoleAPICalled` timestamps), long tasks, and DOM conditions (`dom:cards-100`, `dom:needle`).
- **Times**: all times are epoch-based (`performance.timeOrigin + now`). Each is shown relative to the navigation start of its page, or to the click on Send.
- **Runs**:
  - StagePlan: 1 warm-up + 5 runs per case, each run = join in a fresh context, then reload.
  - BandChat: 1 warm-up + 15 sends per case, with a and b in the busy room.
  - The BandChat columns differ only in how long the harness waits after b opens before sending: "settled 30 s" waits 30 s instead of `run.mjs`'s 1.5 s.
- **Noise**: medians of 5 runs, so ±30–50 ms.
- **Reproduce**: `node harness/breakdown.mjs stage-plan --variant now [--log debug] [--wait0]`. Tables: `node harness/breakdown-report.mjs 2026-11-14`.

Each case directory holds `warmup.json`, `run-N.json`/`sample-NN.json` (raw marks, frames, port messages, console, worker data) and `summary.json`. The `*.profile.marks` cases add CPU profiles of both tabs for one send.

What couldn't be measured without patching Jazz: inside `createDb`, i.e. between `app:main` and `jazz:client-ready`; the worker's IndexedDB/store open (seen only as the gap between the `connect-runtime` and `runtime-ready` messages); and the subscription callback itself. `data:*` marks are the first React render with the data, which runs synchronously from the store notification. With `debug`, `subscription delivered table = tasks` in the log is the engine-side callback.

## StagePlan

#### StagePlan reload, marks only (median ms from navigation start)

| milestone | thin | default | now | default, wait 0 | now, wait 0 |
|---|---:|---:|---:|---:|---:|
| HTML received | 3 | 4 | 3 | 6 | 4 |
| app JS running (main.tsx) | 99 | 105 | 99 | 116 | 108 |
| local-first chunk fetched |  | 198 | 188 | 204 | 212 |
| SharedWorker created |  | 205 | 192 | 209 | 218 |
| worker started |  | 211 | 200 | 218 | 224 |
| page WASM instantiated |  | 325 | 315 | 348 | 347 |
| client ready (JazzProvider children) | 206 | 361 | 319 | 350 | 373 |
| worker WASM instantiated |  | 387 | 380 | 468 | 416 |
| page asks worker for runtime |  | 412 | 377 | 415 | 421 |
| worker runtime ready (store open) |  | 471 | 465 | 531 | 509 |
| page WebSocket open (thin) | 236 |  |  |  |  |
| page: first server frame | 244 |  |  |  |  |
| worker WebSocket open |  | 558 | 498 | 557 | 533 |
| first render with the show | 283 | 577 | 546 | 608 | 604 |
| first render with 100 tasks | 305 | 577 | 546 | 608 | 604 |
| board committed (layout effect) | 343 | 656 | 595 | 652 | 661 |
| 100 cards in the DOM | 346 | 679 | 610 | 664 | 675 |

#### StagePlan join, marks only (median ms from navigation start)

| milestone | thin | default | now | default, wait 0 | now, wait 0 |
|---|---:|---:|---:|---:|---:|
| HTML received | 3 | 4 | 4 | 3 | 3 |
| app JS running (main.tsx) | 192 | 208 | 194 | 216 | 195 |
| local-first chunk fetched |  | 424 | 315 | 412 | 323 |
| SharedWorker created |  | 438 | 336 | 429 | 337 |
| worker started |  | 458 | 347 | 442 | 344 |
| page WASM instantiated |  | 744 | 519 | 727 | 525 |
| client ready (JazzProvider children) | 323 | 340 | 525 | 339 | 532 |
| worker WASM instantiated |  | 751 | 560 | 769 | 556 |
| page asks worker for runtime |  | 874 | 688 | 899 | 669 |
| worker runtime ready (store open) |  | 1171 | 965 | 1154 | 931 |
| page WebSocket open (thin) | 381 | 408 |  | 394 |  |
| page: first server frame | 392 | 424 |  | 409 |  |
| worker WebSocket open |  | 1213 | 1036 | 1214 | 1030 |
| worker: first server frame |  | 1220 | 1048 | 1224 | 1043 |
| join accepted (navigate to board) | 442 | 563 | 1290 | 497 | 1265 |
| first render with the show | 468 | 695 | 1480 | 525 | 1519 |
| first render with 100 tasks | 468 | 811 | 1481 | 626 | 1519 |
| board committed (layout effect) | 515 | 851 | 1560 | 654 | 1637 |
| 100 cards in the DOM | 535 | 851 | 1604 | 655 | 1658 |

#### StagePlan reload, logLevel "debug" (median ms from navigation start)

| milestone | thin | default | now | default, wait 0 | now, wait 0 |
|---|---:|---:|---:|---:|---:|
| HTML received | 3 | 3 | 3 | 3 | 5 |
| app JS running (main.tsx) | 90 | 103 | 100 | 103 | 109 |
| local-first chunk fetched |  | 196 | 203 | 196 | 204 |
| SharedWorker created |  | 200 | 206 | 202 | 229 |
| worker started |  | 206 | 214 | 207 | 236 |
| page WASM instantiated |  | 326 | 350 | 367 | 381 |
| client ready (JazzProvider children) | 181 | 328 | 352 | 369 | 383 |
| worker WASM instantiated |  | 410 | 464 | 427 | 423 |
| page asks worker for runtime |  | 362 | 421 | 421 | 521 |
| worker runtime ready (store open) |  | 474 | 542 | 511 | 723 |
| page WebSocket open (thin) | 209 |  |  |  |  |
| page: first server frame | 217 |  |  |  |  |
| worker WebSocket open |  | 527 | 562 | 550 | 818 |
| first render with the show | 243 | 588 | 710 | 610 | 935 |
| first render with 100 tasks | 269 | 588 | 710 | 610 | 1038 |
| board committed (layout effect) | 291 | 674 | 754 | 652 | 1066 |
| 100 cards in the DOM | 292 | 705 | 769 | 672 | 1066 |
| page → upstream: Join |  | 484 | 548 | 519 | 757 |
| page ← upstream: Welcome |  | 496 | 561 | 527 | 808 |
| worker: first tab read hits store |  | 489 | 555 | 523 | 770 |
| worker store → tab: Rows(tasks) |  | 551 | 603 | 581 | 892 |
| worker → server: Join |  | 571 | 586 | 630 | 840 |
| worker ← server: Welcome |  | 627 | 623 | 674 | 901 |
| page ← upstream: Rows(tasks) |  | 558 | 613 | 584 | 899 |
| page: tasks subscription delivered |  | 580 | 557 | 586 | 925 |
| page: tasks settled (Global) |  | 717 | 700 | 686 | 1068 |

#### StagePlan join, logLevel "debug" (median ms from navigation start)

| milestone | thin | default | now | default, wait 0 | now, wait 0 |
|---|---:|---:|---:|---:|---:|
| HTML received | 4 | 3 | 3 | 3 | 3 |
| app JS running (main.tsx) | 195 | 193 | 191 | 194 | 195 |
| local-first chunk fetched |  | 386 | 315 | 395 | 316 |
| SharedWorker created |  | 392 | 329 | 411 | 334 |
| worker started |  | 416 | 337 | 438 | 343 |
| page WASM instantiated |  | 666 | 526 | 669 | 526 |
| client ready (JazzProvider children) | 324 | 314 | 531 | 312 | 532 |
| worker WASM instantiated |  | 725 | 571 | 731 | 559 |
| page asks worker for runtime |  | 807 | 694 | 851 | 686 |
| worker runtime ready (store open) |  | 1098 | 923 | 1095 | 929 |
| page WebSocket open (thin) | 385 | 367 |  | 378 |  |
| page: first server frame | 397 | 384 |  | 392 |  |
| worker WebSocket open |  | 1150 | 1008 | 1165 | 999 |
| worker: first server frame |  | 1156 | 1025 | 1180 | 1023 |
| join accepted (navigate to board) | 459 | 493 | 1298 | 480 | 1329 |
| first render with the show | 476 | 530 | 1502 | 532 | 1631 |
| first render with 100 tasks | 541 | 679 | 1502 | 592 | 1631 |
| board committed (layout effect) | 563 | 724 | 1614 | 626 | 1712 |
| 100 cards in the DOM | 564 | 724 | 1647 | 627 | 1739 |
| page → upstream: Join |  | 1123 | 950 | 1141 | 958 |
| page ← upstream: Welcome |  | 1150 | 1007 | 1170 | 997 |
| worker: first tab read hits store |  | 1186 | 969 | 1213 | 979 |
| worker store → tab: Rows(tasks) |  | 1347 | 1461 | 1357 | 1577 |
| worker → server: Join |  | 1159 | 1032 | 1182 | 1026 |
| worker ← server: Welcome |  | 1179 | 1052 | 1201 | 1050 |
| worker ← server: Rows(tasks) |  | 1220 | 1380 | 1245 | 1443 |
| page ← upstream: Rows(tasks) |  | 1363 | 1470 | 1373 | 1591 |
| page: tasks subscription delivered |  |  | 1487 |  | 1616 |
| page: tasks settled (Global) |  | 1582 |  | 1637 |  |

### What it shows

**Thin reload (346 ms).** No WASM and no worker are involved. The time splits roughly as follows:
- About 100 ms before `main.tsx` runs: the bundle loads and evaluates.
- About 105 ms from there to client-ready. This is the page's Jazz client setup, which can't be broken down further without hooks.
- 40 ms to the first server frame: WebSocket open plus handshake.
- About 60 ms until the first render with all 100 tasks (the server's answer and decoding).
- About 40 ms for React to commit the board.

So against Convex's 119 ms reload, Jazz thin spends about 200 ms on startup (bundle and client setup) and about 100 ms on the server round trip plus render.

**Local-first reload (default 679 ms, now 610 ms).** The data is on the device. The debug logs show the worker's store answering the tab's read (`store→session Rows(tasks)`, ~550 ms) before the server even sends `Welcome` (~625 ms). So the server is not on the path. The time before the data is startup:
- The local-first chunk is fetched about 100 ms after `main.tsx`.
- Page WASM is instantiated about 120 ms later (~325 ms).
- Worker WASM follows (~385 ms).
- `connect-runtime` → `runtime-ready`, i.e. the worker opening its store, takes ~60 ms (412 → 471).
- Another ~100 ms passes until the first render with tasks (577).
- The board render is also slower than in thin (~100 ms vs 40 ms), presumably because the main thread is busier.

**`firstLoadRemoteWaitMs: 0` doesn't change the reload** (default 664 vs 679, now 675 vs 610, within noise). On a reload the store already has the rows, so the wait never applies.

**Join.**
- Thin: 535 ms.
- Default: 851 ms. The thin client gets the board while the local-first chunk, both WASM instances and the worker boot run alongside. The thin part of default is also slower: the show renders at 695 ms vs 468 ms in thin.
- Default with wait 0: 655 ms. **The 5 s first-load wait costs the default join about 200 ms.**
- Now: 1,604 ms, not helped by wait 0. The path:
  - A fresh device's store takes ~280 ms to open (`connect-runtime` 688 → `runtime-ready` 965).
  - The worker's WebSocket opens only after that (1,036).
  - `joinShow`'s reads settle about 250 ms later (`join:accepted` 1,290).
  - The board's queries go to the server through the worker (data at 1,480).

**`logLevel: "debug"`** adds no consistent cost on StagePlan; the numbers are within the 5-run noise. The log-derived rows (bottom of the debug tables) put hop timestamps on the same timeline.

## BandChat cross-client

#### BandChat cross-client, marks only (median ms after the click on Send)

| milestone | thin | default | thin, settled 30 s | default, settled 30 s |
|---|---:|---:|---:|---:|
| a: send handler starts | 3 | 4 | 3 | 4 |
| a: transaction committed locally | 7 | 12 | 7 | 11 |
| a page → server (thin WebSocket) | 5 |  | 5 |  |
| a page → worker (port) |  | 372 |  | 337 |
| a worker ← page |  | 373 |  | 337 |
| a worker → server |  | 387 |  | 356 |
| b page ← server (thin WebSocket) | 42 | 1171 | 40 |  |
| b worker ← server |  | 453 |  | 440 |
| b worker → page |  | 483 |  | 484 |
| b page ← worker |  | 488 |  | 485 |
| b: React render with the message | 74 | 551 | 71 | 522 |
| b: message in the DOM | 339 | 916 | 336 | 809 |

#### BandChat cross-client, logLevel "debug" (median ms after the click on Send)

| milestone | thin | default | thin, settled 30 s | default, settled 30 s |
|---|---:|---:|---:|---:|
| a: send handler starts | 3 | 4 | 3 | 4 |
| a: transaction committed locally | 7 | 12 | 7 | 13 |
| a page → server (thin WebSocket) | 5 | 9 | 5 |  |
| a page → worker (port) |  | 309 |  | 328 |
| a worker ← page |  | 309 |  | 328 |
| a worker → server |  | 325 |  | 361 |
| b page ← server (thin WebSocket) | 35 | 50 | 36 |  |
| b worker ← server |  | 403 |  | 427 |
| b worker → page |  | 766 |  | 725 |
| b page ← worker |  | 769 |  | 731 |
| b: React render with the message | 64 | 877 | 74 | 845 |
| b: message in the DOM | 394 | 1197 | 374 | 1171 |

Main-thread long tasks in the 1.5 s after the click (median sum, settled 30 s):

| | sender a | receiver b |
|---|---:|---:|
| thin | 384 ms | 607 ms |
| default (local-first) | 1,439 ms | 711 ms |

### What it shows

**Receiver render, both variants: about 270–440 ms from the React render with the message to the message in the DOM.** That is thin 74 → 339, local-first 522 → 809. This is main-thread work on b: React re-rendering the room, then native style/layout/GC.
- The CPU profile (`*.profile.marks/profile-sample-5.top.json`) puts it in "(program)" (native), GC, and the app/UI JavaScript.
- The page's own WASM self time is about 20 ms per send.
- So thin's 290 ms in `run.mjs` is mostly this render, not the network: b's page gets the frame from the server 40 ms after the click.

**The local-first extra (~470 ms at the DOM, settled: 809 vs 336) is mostly on the sender, before the message leaves the tab.**
- a commits locally at 11 ms.
- Its page hands the frames to its worker only at ~337 ms, after a main-thread task triggered by the local commit (the re-render). In total, a's main thread spends ~1.4 s in long tasks in the 1.5 s after the click, vs 0.4 s thin.
- After that, the path is quick:
  - a worker → server: +19 ms (356)
  - server → b worker: +84 ms (440)
  - b worker → b page: +45 ms (485)
  - b's React render: +37 ms (522)

**Hop costs.** The worker hops (page → worker, worker → page) cost about 1–5 ms each, and the server plus both workers about 130 ms in all. The ~330 ms wait on the sender and the receiver's ~290 ms render make up most of the 809 ms.

**Without settling (default, 1.5 s after b opens, as `run.mjs` does):**
- The background local-first upgrade is still syncing all rooms' messages (frames of ~250 KB in the debug logs).
- The first samples take 1.5–2.5 s, and a few go through b's thin WebSocket while b is mid-upgrade (`b page ← server` 1,171 ms).
- They settle into the steady-state pattern from about 12 s after b opened (see the `+N ms` in each sample's `sinceOpenMs`).

**`debug` logging inflates the local-first path** (b worker → page 725 vs 484 ms). Read the marks-only tables for timing and the debug logs for what happens.

Context: BandChat's newest-page query currently sorts without an index (TS can't declare `(roomId, $createdAt)` yet; being fixed separately). Here it isn't visible in the page's WASM time; the receiving worker's in → out is ~45 ms. It may matter more for `bc_open_room`, which wasn't part of this breakdown.
