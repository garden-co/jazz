# Jazz thin-client reload: where the time goes (2026-11-14)

Question: what does even the **thin** Jazz client, with no local store and no WASM, pay before its first query result? StagePlan thin reload was 283 ms (report machine) vs. Convex 122 ms and InstantDB 123 ms.

Short answer: about a fifth of it (49 of 227 ms here) is **one pure-JS Ed25519 signature**, minted again on every load. The rest is a serial chain: a lazy client, an IndexedDB lookup before the socket, and three protocol round trips before the first subscribe goes out. Three small jazz-tools patches take the local reload from **227 → 170 ms** (−25%) on this machine. At 40 ms RTT they take it from **518 → 419 ms**.

Measurement only. The patches are prototypes in a local copy of the published `jazz-tools@2.0.0-alpha.62` dist (no jazz PRs). Synthetic data only (the StagePlan seed).

## How

- **`thin-reload.mjs`**: builds `stage-plan/jazz` thin (`VITE_JAZZ_THIN=1`, `VITE_BENCH_MARKS=1`, minified, via `vite.thin-profile.config.ts` into `dist-<label>`), serves it with `vite preview` (the Jazz backend in-process, as `harness.mjs`), and drives Chromium (Playwright headless shell 145) through each run.
  - Each run: a fresh context joins the show, waits for 100 cards plus 1.5 s, then **reloads** and waits for 100 cards again. Same as `harness/breakdown.mjs`.
  - It records performance marks (the app's own plus the `jz:*` marks from the patch), resource timing, and WebSocket frames (`harness/breakdown-probe.js`).
  - Options:
    - `--jazz-tools <copy>` builds against a patched copy.
    - `--trace` records a Chrome trace with CPU profile.
    - `--no-minify` gives readable names in the profile.
    - `--rtt-ms N` puts `latency-proxy.mjs` (a TCP proxy, N/2 each way, plus one RTT for each new connection's handshake) between the browser and the backend. Chrome's own throttling leaves WebSockets alone, so it can't be used here.
- **`patch/apply.mjs <variant>`** copies the installed jazz-tools next to itself in the pnpm store and applies literal search/replace edits, each required to match exactly once.
  - Variants: `marks` (marks only), `fix-w4`, `fix-mint`, `fix-socket`, `fix-pipe` and `fix-all` (each = marks + that fix).
  - `thin-reload.mjs` points the app's `node_modules/jazz-tools` symlink at the copy for the build only, and restores it afterwards.
- **`analyze-trace.mjs runs/<label>/trace.json`** prints main-thread tasks, network, and CPU self/inclusive time per phase between marks.
- **`measure-all.sh`** (local, 11 runs per variant) and **`measure-rtt.sh 40`** (40 ms RTT, 7 runs per variant) reproduce the tables. All timings come from **marks-only runs** (no `logLevel: "debug"`).
- **Machine:** this sandbox is ~1.6× faster than the report machine (thin cards 208–227 ms here vs. 346 ms in `breakdown/`). The proportions carry over, the absolute numbers don't. The noise is about ±10 ms on medians of 11 runs.

## 1. Timeline of a thin reload (unpatched + marks, local, median of 11)

| ms | milestone | what happens |
|---:|---|---|
| 0 | navigation start | |
| 2 | HTML received | |
| 23 | main chunk received | `index-*.js`, 1.2 MB minified: ~455 KB astryx UI, ~470 KB jazz-tools (277 KB `runtime/`), 171 KB react-dom |
| 68 | `app:main` | **~42 ms** background parse of the main chunk (`v8.parseOnBackground`) plus ~7 ms `v8.evaluateModule` plus 8 ms CSS parse. No WASM is fetched or compiled in thin. |
| 77 | `jz:session:start` | `JazzProvider` → `openBrowserSessionCore` |
| 77 → 126 | `jz:mint:start` → `:end` | **49 ms: `mintSelfSignedToken` (via `mintLocalFirstTokenInJs`, from `createAccountManagerWithMinter` → `prepareAccountManager` → `restore`)**. Almost all of it is noble's `getPublicKey` → `getPrecomputes` → `precomputeWindow`: the Ed25519 base-point table. |
| 126 → 136 | `jz:accounts:end` | A second mint of the same token (`retainSecret`/`rootAccount`, ~3 ms now that the table is warm), plus the account store |
| 136 → 141 | `jazz:client-ready` | `createJazzSessionOwner` → `openThin` → `createThinDb` (~3 ms). `firstBackendAnswer` doesn't wait: the device account is signed in. |
| 150 | `jz:client-created` | The runtime client is created **lazily, by the first `useAll`/`useOne` subscription** in React's commit, ~9 ms after client-ready. Only then does `connectTransport` run. |
| 150 → 157 | `jz:thin:open` → `ws:create` | **~7–10 ms: `StreamFallbackSocket` awaits `recallStreamEndpoint(url)`** (open the IndexedDB `jazz-http-stream-endpoints`, then one read) before it creates the native WebSocket |
| 157 → 163 | `ws:open` | TCP + HTTP upgrade (1 RTT, plus TCP on a real network) |
| 163 → 168 | `jz:thin:negotiated` | Prelude (auth JSON, 1079 B) + ClientHello → Welcome: **round trip 1** (the server verifies the token) |
| 168 → 170 | `jz:thin:schema-bound` | Schema request (3874 B) → answer: **round trip 2**. Every live query waits for it (`bindSchema` → `openLive`). |
| 170 → 172 | `jz:thin:subscribe-sent` → first `jz:thin:delta` | The first subscribe (the account's crew row, from `useSetup`/`useMe`) → its answer: **round trip 3** |
| 185 | `data:show` | **App waterfall:** ShowPage mounts only after `useMe`'s answer, then subscribes to show, crew and tasks (**round trip 4**) |
| 208 | `data:tasks-100` | The tasks answer (14–31 KB) plus decode (`decodeSubscriptionDelta` ~3 ms) |
| 227 | `render:board-100`, 100 cards in the DOM | ~20 ms React render/commit of the board |

In parallel, not on the critical path: `GET …/accounts/backend-user` (the session's "who is signed in" check) starts with the socket and doesn't block anything.

**What's not there:** no WASM fetch, compile or instantiate. No worker. No schema/catalogue download: the client sends its schema (3.9 KB) rather than receiving one. Repeating it on every load costs one round trip, not CPU.

### Round trips before the first query is sent (thin reload)

1. Navigation: HTML (plus a TCP connection).
2. Assets: the hashed JS/CSS are revalidated (`304`, parallel). The same holds for every stack under `vite preview`.
3. WebSocket: a new TCP connection plus the HTTP upgrade.
4. Prelude/Hello → Welcome.
5. Schema → schema answer.

After that the **first subscribe goes out**, and its answer is round trip 6. The app's own waterfall (`useMe` → ShowPage) adds round trip 7 before the board's queries are answered. With 40 ms RTT, `ws:open` → first delta alone is **132 ms** (unpatched) and the reload is **518 ms**.

## 2. What's serial that needn't be

| # | serial step | why it's avoidable |
|---|---|---|
| a | Ed25519 key/sign on every load (49 ms) | The token has a 1 h TTL but is minted fresh on each load, and **twice** per load. noble 2.0.1 `abstract/edwards.js` enables `Point.BASE.precompute(8)` at import, which builds a 4096-point table on the first `getPublicKey` (its own comment: "slows down first publicKey computation by 20ms"). |
| b | IndexedDB lookup before every socket (7–10 ms) | It only matters for backends that can't upgrade sockets (rare). A page can answer synchronously from localStorage, or open the socket while the lookup runs. |
| c | Schema bind waits for Welcome; subscribes wait for the schema answer (2 RTT) | The server handles a connection's frames in order. In the prototype, sending schema and subscribes right behind the hello works against the alpha.62 backend. |
| d | Runtime client (and socket) created only at the first React subscription (~9 ms locally; on a network, the whole socket handshake could overlap the React render) | The socket, prelude and hello don't need the schema. Only the schema bind does. `createThinDb` could dial at once. |
| e | App: `useMe` → ShowPage (1 RTT) | App structure, not Jazz. #jazz-lf-startup sees the same hop on local-first (~40 ms page→worker→store). |

## 3. Measured: prototypes (marks-only, ms from navigation start, median)

### Local (no added latency, 11 runs each)

| milestone | unpatched | fix-w4 | fix-mint | fix-socket | fix-pipe | fix-all |
|---|---:|---:|---:|---:|---:|---:|
| main.tsx runs | 68 | 68 | 70 | 66 | 68 | 65 |
| mint start | 77 | 78 | 79 | 75 | 75 | 70 |
| mint end | 126 | 88 | 80 | 119 | 116 | 71 |
| client ready | 141 | 101 | 86 | 130 | 128 | 79 |
| runtime client created | 150 | 111 | 95 | 139 | 136 | 87 |
| WebSocket created | 157 | 120 | 103 | 140 | 144 | 87 |
| WebSocket open | 163 | 125 | 109 | 147 | 148 | 96 |
| first subscribe sent | 170 | 131 | 116 | 158 | 138 | 88 |
| Welcome | 168 | 130 | 115 | 155 | 154 | 104 |
| schema bound | 170 | 131 | 116 | 158 | 154 | 106 |
| first result (delta) | 172 | 134 | 119 | 161 | 155 | 107 |
| first render with the show | 185 | 153 | 136 | 178 | 175 | 122 |
| first render with 100 tasks | 208 | 159 | 155 | 185 | 188 | 142 |
| **100 cards in the DOM** | 227 | 194 | 178 | 214 | 212 | 170 |
| **saved vs. unpatched** |  | **33** | **49** | **13** | **15** | **57** |
| runs | 11 | 11 | 11 | 11 | 11 | 11 |

With `fix-pipe`, "first subscribe sent" is when it is queued (before the socket opens); it goes out right behind the hello.

### 40 ms RTT (latency-proxy, 7 runs each)

| milestone | unpatched | fix-mint | fix-socket | fix-pipe | fix-all |
|---|---:|---:|---:|---:|---:|
| main.tsx runs | 145 | 152 | 145 | 145 | 142 |
| mint start | 153 | 162 | 151 | 151 | 147 |
| mint end | 195 | 163 | 196 | 195 | 148 |
| client ready | 206 | 169 | 208 | 207 | 156 |
| runtime client created | 215 | 178 | 216 | 215 | 162 |
| WebSocket created | 223 | 188 | 217 | 221 | 163 |
| WebSocket open | 285 | 253 | 279 | 284 | 225 |
| first subscribe sent | 373 | 343 | 366 | 216 | 163 |
| Welcome | 332 | 300 | 324 | 329 | 270 |
| schema bound | 373 | 343 | 366 | 330 | 270 |
| first result (delta) | 417 | 385 | 409 | 330 | 271 |
| first render with the show | 474 | 453 | 473 | 430 | 368 |
| first render with 100 tasks | 497 | 458 | 493 | 457 | 401 |
| **100 cards in the DOM** | 518 | 493 | 516 | 474 | 419 |
| **saved vs. unpatched** |  | **24** | **1** | **44** | **99** |
| runs | 7 | 7 | 7 | 7 | 7 |

At 40 ms RTT, every protocol step is one RTT: `ws:create` → `ws:open` (TCP + upgrade) 62 ms, → Welcome 47 ms, → schema bound 41 ms, → first result 44 ms. `fix-pipe` collapses the last three into one.

## 4. Ranked fixes

Savings are measured here (this machine, 100 cards in the DOM), local / at 40 ms RTT. On the report machine the CPU parts (mint, parse) are about 1.6× larger.

| rank | fix | saved here (local / 40 ms RTT) | effort | where (jazz monorepo, `packages/jazz-tools/src/`) |
|---|---|---:|---|---|
| **1** | **Don't pay for Ed25519 on every load** (`fix-mint`, prototyped). (i) A 4-bit base-point window instead of noble's default 8 (`fix-w4` alone: −33 ms; covers first visits too). (ii) Mint once per load: the second mint in `retainSecret` reuses the first. (iii) Reuse the 1 h token across loads while > 10 min of it is left (localStorage, keyed by a hash of issuer + audience + seed). The alternative to (i) is WebCrypto Ed25519 (native, ~1 ms, but async). | **−49 / −24** (client-ready −55 / −37); est. −70…80 ms on the report machine | S | `thin/self-signed-token.ts` `mintSelfSignedToken`; the two callers in `accounts/` (`restore`, `retainSecret` via `prepareAccountManager`); `mintLocalFirstTokenInJs` in `thin/index.ts`. **Also on the local-first critical path.** |
| **2** | **Pipeline the thin handshake** (`fix-pipe`, prototyped): the schema request goes right behind the ClientHello, and live queries right behind the schema, without waiting for Welcome or the schema answer. ws:open → first result goes from 3 RTT to 1. | **−15 / −44** (first result −87 at 40 ms RTT) | M (client small; needs a protocol guarantee: frames before Welcome are queued in order and dropped cleanly on refused auth) | `thin/thin-runtime.ts` `open`, `bindSchema`, `sendFrame`, `executeSubscription`; `runtime/native-runtime/websocket.ts` `WebSocketCarrier` (send before negotiation) |
| **3** | **Dial at `createDb`, not at the first subscription** (not prototyped). The socket, prelude and hello don't need the schema, so `createThinDb` could open the carrier at once and bind the schema when the first query brings it. The handshake (TCP + upgrade, 62 ms at 40 ms RTT) then overlaps React's first render. Best combined with 2 and with 1 (with the token cached, the socket could open right after `restore`). | est. −9 / −40…60 | M | `runtime/connection-manager/direct-connection-manager.ts` `onClientCreated`/`connectClient`; `thin/thin-runtime.ts` (a runtime per schema → a carrier per Db); `session/create-jazz-session.ts` `openThin`. **Also delays local-first `connect-runtime` (~30 ms, #jazz-lf-startup).** |
| 4 | **No IndexedDB before the socket** (`fix-socket`, prototyped): on a page, a localStorage mirror of the stream-endpoint memory, and a native socket at once when it has no entry. A worker keeps IDB. (Or open the socket while the lookup runs.) | **−13 / ~0** (WebSocket created −17 / −6; at 40 ms RTT within noise at cards) | S | `runtime/native-runtime/http-stream-socket.ts` `StreamFallbackSocket` constructor; `runtime/native-runtime/stream-endpoint-memory.ts` |
| 5 | **Smaller thin bundle.** The 1.2 MB main chunk takes ~42 ms to parse before `main.tsx`, and ~470 KB of it is jazz-tools (`runtime/` 277 KB, `permissions/` 32 KB, `thin/` 29 KB, noble 40 KB). Code that thin never runs (e.g. policy/permissions evaluation, codegen helpers, local-first-only paths in `runtime/`) could move behind the `client: () => import("jazz-tools/local-first")` split. | est. −10…15 (not measured) | L | `bundle-size.mjs` (sourcemap byte attribution); `runtime/` imports from `thin/` and `session/` |
| 6 | (App, not Jazz) **StagePlan's `useMe` → ShowPage waterfall**: ShowPage's show/crew/tasks reads start only after the crew row arrives. Start them in parallel, or prefetch at the route. | ~−13 / −40 (1 RTT) | S (app) | `stage-plan/jazz/src/components/StagePlan.tsx` `useSetup`, `ShowPage.tsx` |

**All three prototypes together (`fix-all`):** local 227 → **170 ms (−57, −25%)**; 40 ms RTT 518 → **419 ms (−99, −19%)**. Fix 3 isn't in it. With fix 3, at 40 ms RTT the socket handshake would also leave the critical path.

Caveats of the prototypes:
- `fix-mint` keeps a bearer token (1 h, self-signed, scoped to this audience) in localStorage next to the seed that can mint it. That changes no threat model the seed in the same storage doesn't already have, but say so in a real PR.
- `fix-pipe` was tested only against the alpha.62 `createJazz` backend in `vite preview`, and only on the happy path. Reconnects and refused auth need tests.
- `fix-socket`'s mirror only helps pages. The worker still pays the IDB lookup.

## Shared with local-first

Sent to #local-first-first-callback and #jazz-lf-startup:
- **The mint (a) is on the local-first path too, and it costs more there.** #jazz-lf-startup measured `createAccountManagerWithMinter` on a `now` reload (marks-only, 5 runs, the same kind of 2-core sandbox) at **80 ms** (102 → 182 ms), with a single ~70 ms long task. That task blocks the SharedWorker from starting: a worker spawned before the mint only gets its `timeOrigin` once the task ends. The async continuation of the page's `import("jazz-wasm")` waits for it too. #jazz-lf-startup is measuring `fix-mint` on `now`.
- **The IndexedDB lookup (b)** is in the same `StreamFallbackSocket` that the worker's socket uses.
- **The lazy runtime client (d)** also delays local-first's `connect-runtime` (~30 ms, ~130 ms on a fresh device; #jazz-lf-startup).

## Files

- `thin-reload.mjs`, `vite.thin-profile.config.ts`, `latency-proxy.mjs`, `analyze-trace.mjs`, `bundle-size.mjs`, `make-tables.mjs`, `measure-all.sh`, `measure-rtt.sh`
- `patch/apply.mjs` (marks), `patch/fix-w4.mjs`, `patch/fix-mint.mjs`, `patch/fix-socket.mjs`, `patch/fix-pipe.mjs`
- `runs/base/`: the published jazz-tools without any patch (5 runs, cards 210 ms), a check that the marks add nothing measurable.
- `runs/<variant>/{run-N.json,summary.json}`: raw marks, frames and resources for every run. `runs/<variant>.txt` holds the printed medians.
- `runs/trace-marks-min/trace.json` (minified) and `runs/trace-marks-unmin/trace.json` (readable names): one Chrome trace each with CPU profile, for `chrome://tracing`/Perfetto or `analyze-trace.mjs`. Tracing itself adds ~80 ms (profiler start-up in the old page's `visibilitychange`), so read them for shape and never for timings.
