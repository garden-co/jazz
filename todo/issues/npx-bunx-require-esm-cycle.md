---
name: jazz-tools CLI fails under npx/bunx due to require(esm) cycle when tsx hook is registered
description: `jazz-tools migrations create` crashes with ERR_REQUIRE_CYCLE_MODULE on jazz-wasm because tsx's ESM hook routes jazz-wasm through the CJS→ESM bridge and jazz-wasm has no "type":"module"
type: project
---

# npx/bunx: require(esm) cycle loading jazz-wasm via tsx hook

## What

`npx jazz-tools@2.0.0-alpha.27 migrations create …` (and `bunx …`) crashes:

```
Cannot require() ES Module …/jazz-wasm/pkg/jazz_wasm.js in a cycle.
A cycle involving require(esm) is not allowed …
```

Works under `pnpm exec` / `pnpm dlx`. Reported by Tobias.

## Priority

high

## Root cause

Reproduced on Node 24.15 against published `jazz-tools@2.0.0-alpha.27`.

Minimal repro (no migrations flow needed):

```js
// test.mjs in a dir where `npm i jazz-tools@2.0.0-alpha.27` has run
import { register as registerEsm } from "tsx/esm/api";
registerEsm();
await import("jazz-wasm"); // → ERR_REQUIRE_CYCLE_MODULE
```

Two things conspire:

1. `packages/jazz-tools/src/cli.ts` calls `registerEsm()` at module top level (becomes line 31 of `dist/cli.js`). This installs tsx's ESM loader hook globally for the process.
2. `crates/jazz-wasm/package.json` declares `"main": "pkg/jazz_wasm.js"` with **no `"type": "module"`**, even though `wasm-pack build --target web` emits ESM (`export class …`).

When `dist/runtime/client.js` does `await import("jazz-wasm")`, tsx's hook handles it. Because the package has no `"type": "module"`, Node routes through the CJS loader first. Node then detects ESM syntax in the file and re-enters the ESM loader via `require(esm)` (`loadESMFromCJS` → `importSyncForRequire`). That re-entry is a cycle with the module currently being initialised, which Node refuses in Node 22.12+.

Adding `"type": "module"` to `crates/jazz-wasm/package.json` makes tsx's hook take the straight-through ESM path with no CJS bridge, and `ERR_REQUIRE_CYCLE_MODULE` disappears. Verified against the repro above.

### Why pnpm exec / pnpm dlx works and npx / bunx doesn't

Not fully confirmed, but the difference is almost certainly a consequence of node_modules layout: pnpm's symlinked isolated layout changes which realpath tsx resolves for jazz-wasm, which changes whether Node's cycle tracker considers the tsx-triggered load a cycle. npx/bunx install flat under `~/.npm/_npx/<hash>/node_modules`, which reliably triggers the cycle.

## Fix direction

Smallest fix: add `"type": "module"` to `crates/jazz-wasm/package.json`. The emitted file is already ESM; the package.json just needs to agree.

Worth also considering:

- Does `jazz-tools` CLI actually need tsx registered for the `migrations create` subcommand? `registerEsm()` runs unconditionally at cli.js top level; making it lazy (only for commands that load user TS) would avoid hook-induced side effects on wasm loading.
- Sanity-check other wasm-pack-emitted packages in the workspace for the same oversight.

## Repro pointers

- Entry: `packages/jazz-tools/bin/jazz-tools.js` → `node dist/cli.js migrations …`.
- Hook registration: `packages/jazz-tools/src/cli.ts` (imports `register as registerEsm` from `tsx/esm/api` and calls it at top level).
- jazz-wasm loader: `packages/jazz-tools/src/runtime/client.ts` → `loadWasmModule()` → `await import("jazz-wasm")`.
- Package-level offender: `crates/jazz-wasm/package.json` (`"main": "pkg/jazz_wasm.js"`, no `"type"` field).
- Full command repro (needs a pre-existing snapshot file so the hash-compute path runs before any network):
  ```bash
  mkdir -p migrations/snapshots
  echo '{"tables":[]}' > migrations/snapshots/20260101T000000-aabbccddeeff.json
  npx -y jazz-tools@2.0.0-alpha.27 migrations create \
    --fromHash aabbccddeeff --toHash bbccddeeffaa \
    --server-url=http://127.0.0.1:1 --admin-secret=x
  ```
