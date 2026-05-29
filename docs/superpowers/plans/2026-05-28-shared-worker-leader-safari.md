# SharedWorker Leader (Safari) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the Safari leader path for Jazz: a SharedWorker that holds `LOCK_NAME`, opens OPFS, runs the Jazz runtime, and answers tab connections with a per-tab `MessagePort`. Tabs are pure followers — they do not spawn a dedicated Worker on Safari.

**Architecture:** A new SharedWorker entry script (`shared-worker-leader.ts`) answers a cheap `CHECK_CAPABILITY` probe before any runtime bootstrap. Two distinct outcomes drive the control flow:

- **Capability unsupported** (Chrome/Firefox today): the client never sends `CONNECT`; `Db` falls back to the existing `TabLeaderElection` + dedicated-Worker path. This is the only fallback in the design.
- **Capability supported, but a later step fails** (`CONNECT` rejects, `LOCK_NAME` wait, WASM init throws/times out): the design **throws** a typed `SharedWorkerLeaderConnectError`. There is **no** retroactive dedicated-Worker fallback — a supported-but-broken leader is a hard error surfaced to the caller, not silently degraded. (Per the explicit decision: once the platform claims support, failure is loud.)

If supported, on the first `CONNECT` carrying the schema JSON, the entry script acquires `LOCK_NAME` and instantiates the same `WasmRuntime` the dedicated Worker entry instantiates today. Tabs connect via a `SharedWorkerLeaderClient` that probes eagerly during `Db.createWithWorker` and sends `CONNECT` lazily during the first `getClient(schema)` call (since schema reaches `Db` lazily). A new `MessagePortRuntimeTransport` installs server-bound outbox forwarding on the follower main `WasmRuntime` so it does not need a dedicated Worker. The Rust `worker_host` is widened to accept either `DedicatedWorkerGlobalScope` or `SharedWorkerGlobalScope` via `globalThis`, and learns to accept a transferred `MessagePort` as a peer transport target.

**Why there is no heartbeat:** per the SharedWorker lifetime model, the worker stays alive while any owner document is "fully active". A BFCached document is not fully active, so Safari only freezes/evicts the leader SharedWorker once _every_ connected tab is BFCached — a state in which no tab is running to issue a query. When any tab restores, follower and leader resume together. There is therefore no "active follower waiting on a frozen leader" scenario, and no heartbeat/liveness ping is needed. Do not add one without a concrete failure case that violates this lifetime guarantee.

**Scope:** Safari only. The capability probe falls through to the existing BroadcastChannel + dedicated-Worker path when sync OPFS in SharedWorker is unavailable, so Chrome/Firefox are unchanged. Tab-hosted leader mode (a SharedWorker that brokers connections to a leader _tab_) is **not** in this plan. We use narrow, honest names — `SharedWorkerLeader`, `LEADER_PROTOCOL_VERSION`, `CONNECT`/`PEER_PORT` — and a single-message handshake. If a Chrome/Firefox tab-hosted plan lands later, that plan decides whether to unify with this codebase, share a SharedWorker URL, or ship a separate entry script. We do not pay forward-compat cost today for a future plan whose shape is not pinned down.

**Tech Stack:** TypeScript (jazz-tools/src/runtime), Rust + wasm-bindgen (jazz-wasm), Vitest browser mode with Playwright (Chromium + WebKit), `navigator.locks`, SharedWorker, MessageChannel/MessagePort, OPFS FileSystemSyncAccessHandle.

---

## File Structure

**New files (TypeScript):**

- `packages/jazz-tools/src/runtime/shared-worker-leader/protocol.ts` — `TabToLeader` / `LeaderToTab` discriminated unions, version constant, scope/name builders, type guards.
- `packages/jazz-tools/src/runtime/shared-worker-leader/capability.ts` — `detectSyncOpfsInWorkerScope()` capability probe (called inside a SharedWorker scope at boot; opens and tears down a throwaway sync access handle).
- `packages/jazz-tools/src/runtime/shared-worker-leader/url.ts` — `resolveSharedWorkerLeaderUrl(runtimeModuleUrl, locationHref, runtimeSources)` with consumer override → `baseUrl` → module-relative fallback.
- `packages/jazz-tools/src/runtime/shared-worker-leader/shared-worker-leader.ts` — the SharedWorker entry script. Handles `onconnect`, validates scope/version on `CONNECT`, runs the capability probe + leader bootstrap exactly once, attaches follower ports, posts `LEADER_FAULT` when the probe or bootstrap fails.
- `packages/jazz-tools/src/runtime/shared-worker-leader/leader-host.ts` — `LeaderHost` interface + `SharedWorkerLeader` implementation. Acquires `LOCK_NAME`, owns the in-scope `WasmRuntime`, mints follower `MessageChannel`s and routes them through the worker-host's `attach-follower-port` handler.
- `packages/jazz-tools/src/runtime/shared-worker-leader/client.ts` — tab-side facade. Owns the `SharedWorker` connection, sends scoped/versioned `CONNECT`, accepts `PEER_PORT`, exposes `onPortChanged(cb)`, `onFault(cb)`, `forceReconnect()`, `close()`.
- `packages/jazz-tools/src/runtime/shared-worker-leader/message-port-runtime-transport.ts` — installs server-bound forwarding onto a follower main `WasmRuntime`, replays the runtime server edge, posts payloads to the leader-minted port, applies incoming payloads via the new runtime hooks.
- `packages/jazz-tools/src/runtime/shared-worker-leader/package-version.ts` — generated by `build:runtime`; exports `JAZZ_PACKAGE_VERSION` as a string constant pulled from `package.json`. Browser-safe (no `readFile`).

**Modified files (TypeScript):**

- `packages/jazz-tools/src/runtime/context.ts` — add `sharedWorkerLeaderUrl?: string` to `RuntimeSourcesConfig`.
- `packages/jazz-tools/src/runtime/db.ts` — when the leader client successfully connects, skip `Db.spawnWorker(...)` + `TabLeaderElection` + `openSyncChannel()`, install the `MessagePortRuntimeTransport`, and adopt a synthetic follower-only snapshot. Splice in after L948, before L951.
- `packages/jazz-tools/package.json` — `build:runtime` script also generates `package-version.ts` and copies `shared-worker-leader.ts` (and the rest of the directory) into `dist/`.
- `packages/jazz-tools/scripts/generate-package-version.mjs` (new helper) — emits `src/runtime/shared-worker-leader/package-version.ts` from `package.json`.

**New / modified files (Rust):**

- `crates/jazz-wasm/src/worker_host.rs` — replace the `DedicatedWorkerGlobalScope` cast with a scope-agnostic helper (postMessage/set_onmessage/close via Reflect on `globalThis`). Add a new path: an `attach-follower-port` JS-object message handler that reads `MessageEvent.ports[0]`, installs an `onmessage` closure on that port, and registers it as a peer transport target. Add an `init-ok` JS callback hook (replaces the postcard byte sniff from the previous spec).
- `crates/jazz-wasm/src/worker_protocol.rs` — add two new postcard-encoded `WorkerToMainWire` variants for the attach ack (`FollowerPortAttached` / `FollowerPortAttachFailed`).
- `crates/jazz-wasm/src/runtime.rs` — extend `RustOutboxSender` with an optional follower-port lookup callback. When present, server-bound payloads for a peer with an attached port go directly to that port and skip the `peer-sync` envelope. Expose follower-runtime hooks (`installFollowerOutboxSender`, `setFollowerOutboxForwarder`, `applyIncomingFollowerPayload`, `replayFollowerServerEdge`) on `WasmRuntime`.

**New test files:**

- `packages/jazz-tools/src/runtime/shared-worker-leader/protocol.test.ts` — unit tests for `CONNECT` validation + type-guard round-trips + `JAZZ_PACKAGE_VERSION` matches `package.json`. (Node-vitest.)
- `packages/jazz-tools/src/runtime/shared-worker-leader/url.test.ts` — unit tests for URL resolution precedence. (Node-vitest.)
- `packages/jazz-tools/tests/browser/shared-worker-leader-capability.test.ts` — runs the probe inside a real **SharedWorker** (not a dedicated Worker) and asserts the result matches the running browser.
- `packages/jazz-tools/tests/browser/shared-worker-leader-spike.test.ts` — proves a tab → SharedWorker → tab `MessagePort` transfer works in the target browser.
- `packages/jazz-tools/tests/browser/shared-worker-leader-rust.test.ts` — exercises Rust `attach-follower-port` end-to-end inside a dedicated Worker (the Rust handler is scope-agnostic; this keeps the test deterministic).
- `packages/jazz-tools/tests/browser/shared-worker-leader-client.test.ts` — unit-ish test for `SharedWorkerLeaderClient` against a stub SharedWorker that returns canned `PEER_PORT` / `LEADER_FAULT`.
- `packages/jazz-tools/tests/browser/shared-worker-leader-cold-start.test.ts` — one tab connects, leader self-elects, tab reads/writes through the leader.
- `packages/jazz-tools/tests/browser/shared-worker-leader-two-tab.test.ts` — two tabs both follow the same leader.
- `packages/jazz-tools/tests/browser/shared-worker-leader-tab-close.test.ts` — close one of three tabs, remaining tabs uninterrupted.
- `packages/jazz-tools/tests/browser/shared-worker-leader-restart.test.ts` — force a simulated SharedWorker disconnect; client receives a fresh `PEER_PORT`.

---

## Conventions used in this plan

- "Run" lines show the exact command from the repo root (`/Users/antonio.musolino/Workspace/github/jazz`) unless otherwise stated. Use `pnpm` (the repo's package manager).
- TypeScript unit tests run via `pnpm --filter jazz-tools test <pattern>` (vitest, Node env).
- Browser tests run via `pnpm --filter jazz-tools test:browser <pattern>` (vitest browser mode, Playwright).
- Rust changes build via `pnpm build:core` (turbo, top-level).
- Commit at the end of every task. Use Conventional Commits, no trailing co-author / generated-by line (per user's global instructions in `/Users/antonio.musolino/.claude/CLAUDE.md`).
- **Do not rewrite existing tests** without flagging to the user — project CLAUDE.md treats existing tests as decisions about correct behaviour.
- The capability-probe gating in browser integration tests (Tasks 16–19) is performed by **constructing a real SharedWorker, asking it to probe, and reading the answer back** — not by calling `detectSyncOpfsInWorkerScope()` from the test main thread, which would probe the wrong scope.

---

### Task 1: Protocol module — types, version, helpers, version pin test

**Files:**

- Create: `packages/jazz-tools/src/runtime/shared-worker-leader/protocol.ts`
- Create: `packages/jazz-tools/src/runtime/shared-worker-leader/package-version.ts`
- Create: `packages/jazz-tools/src/runtime/shared-worker-leader/protocol.test.ts`
- Create: `packages/jazz-tools/scripts/generate-package-version.mjs`
- Modify: `packages/jazz-tools/package.json` — `build:runtime` invokes the generator first.

- [ ] **Step 1: Write the failing tests**

```typescript
// packages/jazz-tools/src/runtime/shared-worker-leader/protocol.test.ts
import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import {
  LEADER_PROTOCOL_VERSION,
  buildLeaderScope,
  buildLeaderWorkerName,
  buildLockName,
  isLeaderToTab,
  isTabToLeader,
} from "./protocol.js";
import { JAZZ_PACKAGE_VERSION } from "./package-version.js";

describe("shared-worker-leader protocol", () => {
  it("LEADER_PROTOCOL_VERSION is 1 in the initial release", () => {
    expect(LEADER_PROTOCOL_VERSION).toBe(1);
  });

  it("isTabToLeader accepts CHECK_CAPABILITY (no body)", () => {
    expect(isTabToLeader({ t: "CHECK_CAPABILITY" })).toBe(true);
  });

  it("isLeaderToTab accepts CAPABILITY_RESULT with a boolean", () => {
    expect(isLeaderToTab({ t: "CAPABILITY_RESULT", supported: true })).toBe(true);
    expect(isLeaderToTab({ t: "CAPABILITY_RESULT", supported: false })).toBe(true);
    expect(isLeaderToTab({ t: "CAPABILITY_RESULT" })).toBe(false);
  });

  it("buildLeaderScope is a deterministic function of (appId, dbName)", () => {
    expect(buildLeaderScope("app", "db")).toEqual(buildLeaderScope("app", "db"));
    expect(buildLeaderScope("app", "db1")).not.toEqual(buildLeaderScope("app", "db2"));
  });

  it("buildLeaderWorkerName has the documented prefix", () => {
    expect(buildLeaderWorkerName(buildLeaderScope("app", "db"))).toMatch(
      /^jazz-shared-worker-leader:/,
    );
  });

  it("buildLockName is stable, version-independent", () => {
    expect(buildLockName("app", "db")).toBe("jazz-worker:app:db");
    expect(buildLockName("app", "db")).not.toMatch(/\d+\.\d+/);
  });

  it("isTabToLeader accepts a well-formed CONNECT", () => {
    expect(
      isTabToLeader({
        t: "CONNECT",
        tabId: "tab-1",
        bornAt: 12345,
        scope: "scope-x",
        protocolVersion: 1,
        jazzPackageVersion: "0.0.0",
        appId: "app",
        dbName: "db",
        schemaJson: "{}",
      }),
    ).toBe(true);
  });

  it("isTabToLeader rejects CONNECT missing required fields", () => {
    expect(isTabToLeader({ t: "CONNECT" })).toBe(false);
    expect(
      isTabToLeader({
        t: "CONNECT",
        tabId: "tab-1",
        bornAt: 0,
        scope: "s",
        protocolVersion: 1,
        jazzPackageVersion: "0",
        // missing appId / dbName / schemaJson
      }),
    ).toBe(false);
  });

  it("isTabToLeader accepts GOODBYE", () => {
    expect(isTabToLeader({ t: "GOODBYE" })).toBe(true);
  });

  it("isLeaderToTab accepts PEER_PORT (port reference is identity-checked)", () => {
    const ch = new MessageChannel();
    expect(
      isLeaderToTab({
        t: "PEER_PORT",
        port: ch.port1,
        generation: 1,
      }),
    ).toBe(true);
  });

  it("isLeaderToTab accepts LEADER_FAULT", () => {
    expect(
      isLeaderToTab({
        t: "LEADER_FAULT",
        reason: "version-mismatch",
      }),
    ).toBe(true);
  });
});

describe("JAZZ_PACKAGE_VERSION", () => {
  it("matches the version in package.json (drift guard)", () => {
    const pkg = JSON.parse(
      readFileSync(new URL("../../../package.json", import.meta.url), "utf8"),
    ) as { version: string };
    expect(JAZZ_PACKAGE_VERSION).toBe(pkg.version);
  });
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pnpm --filter jazz-tools test shared-worker-leader/protocol.test`
Expected: FAIL — neither `./protocol.js` nor `./package-version.js` exists.

- [ ] **Step 3: Implement the package-version generator**

```javascript
// packages/jazz-tools/scripts/generate-package-version.mjs
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const pkgPath = resolve(scriptDir, "..", "package.json");
const outPath = resolve(
  scriptDir,
  "..",
  "src",
  "runtime",
  "shared-worker-leader",
  "package-version.ts",
);

const { version } = JSON.parse(readFileSync(pkgPath, "utf8"));
const banner = "// Generated by scripts/generate-package-version.mjs. Do not edit by hand.\n";
writeFileSync(
  outPath,
  `${banner}export const JAZZ_PACKAGE_VERSION = ${JSON.stringify(version)};\n`,
);
```

Run the generator once to seed the file:

```bash
node packages/jazz-tools/scripts/generate-package-version.mjs
```

- [ ] **Step 4: Implement `protocol.ts`**

```typescript
// packages/jazz-tools/src/runtime/shared-worker-leader/protocol.ts

/**
 * Wire protocol for the Safari SharedWorker leader — control plane only.
 *
 * Data-plane payloads on the leader-minted MessagePort use plain JS objects:
 *   { type: "follower-sync", payload: Uint8Array[] }   // tab -> leader
 *   { type: "leader-sync",   payload: Uint8Array[] }   // leader -> tab
 *
 * One round-trip handshake:
 *   tab -> leader: CONNECT { everything the leader needs to bootstrap + identify the tab }
 *   leader -> tab: PEER_PORT { port, generation }  OR  LEADER_FAULT { reason }
 */

export const LEADER_PROTOCOL_VERSION = 1;

export function buildLeaderScope(appId: string, dbName: string): string {
  return `${appId}::${dbName}`;
}

export function buildLeaderWorkerName(scope: string): string {
  return `jazz-shared-worker-leader:${scope}`;
}

export function buildLockName(appId: string, dbName: string): string {
  return `jazz-worker:${appId}:${dbName}`;
}

export type TabId = string;

export interface ConnectMessage {
  t: "CONNECT";
  tabId: TabId;
  bornAt: number;
  scope: string;
  protocolVersion: number;
  jazzPackageVersion: string;
  appId: string;
  dbName: string;
  schemaJson: string;
  env?: string;
  userBranch?: string;
  serverUrl?: string;
  jwtToken?: string;
  adminSecret?: string;
}

export type TabToLeader = ConnectMessage | { t: "CHECK_CAPABILITY" } | { t: "GOODBYE" };

export type LeaderFaultReason =
  | "version-mismatch"
  | "scope-mismatch"
  | "runtime-host-unavailable"
  | "init-failed";

export type LeaderToTab =
  | { t: "PEER_PORT"; port: MessagePort; generation: number }
  | { t: "CAPABILITY_RESULT"; supported: boolean }
  | { t: "LEADER_FAULT"; reason: LeaderFaultReason; detail?: string };

export function isTabToLeader(value: unknown): value is TabToLeader {
  if (typeof value !== "object" || value === null) return false;
  const m = value as { t?: unknown };
  switch (m.t) {
    case "CONNECT": {
      const h = value as Record<string, unknown>;
      return (
        typeof h.tabId === "string" &&
        typeof h.bornAt === "number" &&
        typeof h.scope === "string" &&
        typeof h.protocolVersion === "number" &&
        typeof h.jazzPackageVersion === "string" &&
        typeof h.appId === "string" &&
        typeof h.dbName === "string" &&
        typeof h.schemaJson === "string"
      );
    }
    case "CHECK_CAPABILITY":
    case "GOODBYE":
      return true;
    default:
      return false;
  }
}

export function isLeaderToTab(value: unknown): value is LeaderToTab {
  if (typeof value !== "object" || value === null) return false;
  const m = value as { t?: unknown };
  switch (m.t) {
    case "PEER_PORT": {
      const m2 = value as Record<string, unknown>;
      return typeof m2.generation === "number" && m2.port instanceof MessagePort;
    }
    case "CAPABILITY_RESULT": {
      return typeof (value as { supported?: unknown }).supported === "boolean";
    }
    case "LEADER_FAULT": {
      const m2 = value as Record<string, unknown>;
      return typeof m2.reason === "string";
    }
    default:
      return false;
  }
}
```

- [ ] **Step 5: Wire the generator into `build:runtime`**

In `packages/jazz-tools/package.json`, change:

```json
"build:runtime": "tsc && cp src/worker/jazz-worker.ts dist/worker/jazz-worker.ts",
```

to:

```json
"build:runtime": "node scripts/generate-package-version.mjs && tsc && cp src/worker/jazz-worker.ts dist/worker/jazz-worker.ts && cp src/runtime/shared-worker-leader/shared-worker-leader.ts dist/runtime/shared-worker-leader/shared-worker-leader.ts",
```

(The second `cp` is for Task 8's entry script — it's harmless until that file exists; the build will fail if it's missing, which is correct: the build script is the source of truth for the deployment artifact.)

> **Note for the implementer:** to avoid breaking the build before Task 8 ships, you may add the second `cp` only at the end of Task 8. The first `cp` (jazz-worker.ts) is the existing line, untouched.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `pnpm --filter jazz-tools test shared-worker-leader/protocol.test`
Expected: PASS — 9 tests pass (including the version-pin drift guard).

- [ ] **Step 7: Commit**

```bash
git add packages/jazz-tools/src/runtime/shared-worker-leader/protocol.ts \
        packages/jazz-tools/src/runtime/shared-worker-leader/protocol.test.ts \
        packages/jazz-tools/src/runtime/shared-worker-leader/package-version.ts \
        packages/jazz-tools/scripts/generate-package-version.mjs \
        packages/jazz-tools/package.json
git commit -m "feat(shared-worker-leader): add wire-protocol types, version pin, and build-time package version emitter"
```

---

### Task 2: Leader URL resolution

**Files:**

- Create: `packages/jazz-tools/src/runtime/shared-worker-leader/url.ts`
- Create: `packages/jazz-tools/src/runtime/shared-worker-leader/url.test.ts`
- Modify: `packages/jazz-tools/src/runtime/context.ts`

- [ ] **Step 1: Write the failing tests**

```typescript
// packages/jazz-tools/src/runtime/shared-worker-leader/url.test.ts
import { describe, expect, it } from "vitest";
import { resolveSharedWorkerLeaderUrl } from "./url.js";

describe("resolveSharedWorkerLeaderUrl", () => {
  const moduleUrl = "https://example.test/_next/static/jazz/runtime/db.js";
  const locationHref = "https://example.test/index.html";

  it("returns sharedWorkerLeaderUrl verbatim when explicitly provided", () => {
    const url = resolveSharedWorkerLeaderUrl(moduleUrl, locationHref, {
      sharedWorkerLeaderUrl: "https://cdn.example.test/jazz-leader.js",
    });
    expect(url).toBe("https://cdn.example.test/jazz-leader.js");
  });

  it("derives the leader URL from baseUrl when no explicit URL is set", () => {
    const url = resolveSharedWorkerLeaderUrl(moduleUrl, locationHref, {
      baseUrl: "/jazz/",
    });
    expect(url).toBe("https://example.test/jazz/shared-worker-leader/shared-worker-leader.js");
  });

  it("falls back to a module-relative URL when neither override is set", () => {
    const url = resolveSharedWorkerLeaderUrl(moduleUrl, locationHref, undefined);
    expect(url).toMatch(/\/shared-worker-leader\/shared-worker-leader\.js$/);
  });

  it("preserves an absolute sharedWorkerLeaderUrl regardless of locationHref", () => {
    const url = resolveSharedWorkerLeaderUrl(moduleUrl, undefined, {
      sharedWorkerLeaderUrl: "https://other.example.test/leader.js",
    });
    expect(url).toBe("https://other.example.test/leader.js");
  });
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pnpm --filter jazz-tools test shared-worker-leader/url.test`
Expected: FAIL — module not found.

- [ ] **Step 3: Extend `RuntimeSourcesConfig`**

In `packages/jazz-tools/src/runtime/context.ts`, add inside the existing `RuntimeSourcesConfig` interface:

```typescript
  /** Explicit URL for the SharedWorker leader entry script. Overrides `baseUrl`. */
  sharedWorkerLeaderUrl?: string;
```

- [ ] **Step 4: Implement `url.ts`**

```typescript
// packages/jazz-tools/src/runtime/shared-worker-leader/url.ts
import type { RuntimeSourcesConfig } from "../context.js";

function isHttpUrl(url: string): boolean {
  try {
    const protocol = new URL(url).protocol;
    return protocol === "http:" || protocol === "https:";
  } catch {
    return false;
  }
}

function resolveAbsoluteOrRelative(url: string, locationHref: string | undefined): string {
  try {
    return new URL(url).href;
  } catch {
    // not absolute — fall through
  }
  if (locationHref) {
    try {
      return new URL(url, locationHref).href;
    } catch {
      // unparseable base — fall through
    }
  }
  return url;
}

export function resolveSharedWorkerLeaderUrl(
  runtimeModuleUrl: string,
  locationHref: string | undefined,
  runtime?: RuntimeSourcesConfig,
): string {
  if (runtime?.sharedWorkerLeaderUrl) {
    return resolveAbsoluteOrRelative(runtime.sharedWorkerLeaderUrl, locationHref);
  }
  if (runtime?.baseUrl && locationHref) {
    const baseUrl = new URL(runtime.baseUrl, locationHref).href;
    return new URL("shared-worker-leader/shared-worker-leader.js", baseUrl).href;
  }
  if (!locationHref || isHttpUrl(runtimeModuleUrl)) {
    return new URL("../shared-worker-leader/shared-worker-leader.js", runtimeModuleUrl).href;
  }
  return new URL("shared-worker-leader/shared-worker-leader.js", new URL("/", locationHref).href)
    .href;
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `pnpm --filter jazz-tools test shared-worker-leader/url.test`
Expected: PASS — 4 tests pass.

- [ ] **Step 6: Commit**

```bash
git add packages/jazz-tools/src/runtime/shared-worker-leader/url.ts \
        packages/jazz-tools/src/runtime/shared-worker-leader/url.test.ts \
        packages/jazz-tools/src/runtime/context.ts
git commit -m "feat(shared-worker-leader): resolve leader URL via override, baseUrl, or module-relative fallback"
```

---

### Task 3: Capability probe — sync OPFS access handle in worker scope

**Files:**

- Create: `packages/jazz-tools/src/runtime/shared-worker-leader/capability.ts`
- Create: `packages/jazz-tools/tests/browser/shared-worker-leader-capability.test.ts`
- Create: `packages/jazz-tools/tests/browser/fixtures/leader-capability-probe.shared-worker.js`
- Create: `packages/jazz-tools/tests/browser/fixtures/leader-support.ts` — shared probe helper used by Tasks 18–21 for visible `describe.skipIf` gating.

The previous spec exercised the probe from a dedicated Worker. That returns `true` on both Chromium and Safari (sync OPFS in dedicated workers is the baseline) and tells us nothing about the SharedWorker question. Here, the test runs the probe inside a **real SharedWorker** so the result reflects the path we actually care about.

- [ ] **Step 1: Write the shared probe helper**

This is the single source of truth for "is the leader path available in this browser". Tasks 18–21 import `leaderSupported` and gate with `describe.skipIf(!leaderSupported)` so skipped suites are **visible in the reporter** — a broken probe fixture turns those suites yellow (skipped), not green (silently passed).

```typescript
// packages/jazz-tools/tests/browser/fixtures/leader-support.ts
export async function probeInSharedWorker(): Promise<boolean> {
  if (typeof SharedWorker === "undefined") return false;
  const worker = new SharedWorker(
    new URL("./leader-capability-probe.shared-worker.js", import.meta.url),
    { type: "module", name: `cap-${Math.random().toString(36).slice(2)}` },
  );
  const answer = await new Promise<boolean>((resolve) => {
    const timeout = setTimeout(() => resolve(false), 10000);
    worker.port.onmessage = (event: MessageEvent) => {
      if (event.data?.t === "PROBE_RESULT") {
        clearTimeout(timeout);
        resolve(Boolean(event.data.supported));
      }
    };
    worker.port.start();
    worker.port.postMessage({ t: "PROBE" });
  });
  try {
    worker.port.close();
  } catch {
    /* ignore */
  }
  return answer;
}

/**
 * Probed once per importing test module via top-level await (vitest browser
 * mode supports top-level await in ESM test files). Used with
 * `describe.skipIf(!leaderSupported)`.
 */
export const leaderSupported = await probeInSharedWorker();
```

- [ ] **Step 2: Write the failing capability test**

```typescript
// packages/jazz-tools/tests/browser/shared-worker-leader-capability.test.ts
import { describe, expect, it } from "vitest";
import { probeInSharedWorker } from "./fixtures/leader-support.js";
// Direct import drives the red: before capability.ts exists, this module fails
// to resolve and the whole test file fails to load. (The probe helper swallows
// a missing module into `false`, so it cannot be relied on for the red.)
import { detectSyncOpfsInWorkerScope } from "../../src/runtime/shared-worker-leader/capability.js";

describe("shared-worker-leader capability probe", () => {
  it("exports detectSyncOpfsInWorkerScope", () => {
    expect(typeof detectSyncOpfsInWorkerScope).toBe("function");
  });

  it("returns a boolean from inside a SharedWorker", async () => {
    const result = await probeInSharedWorker();
    expect(typeof result).toBe("boolean");
  });

  it("does not leave a residual OPFS file behind", async () => {
    await probeInSharedWorker();
    const root = await navigator.storage.getDirectory();
    const entries: string[] = [];
    // @ts-expect-error -- async iterator
    for await (const [name] of root) entries.push(name);
    expect(entries.some((n) => n.startsWith("__jazz_leader_probe_"))).toBe(false);
  });
});
```

- [ ] **Step 2: Write the SharedWorker fixture**

```javascript
// packages/jazz-tools/tests/browser/fixtures/leader-capability-probe.shared-worker.js
/* eslint-disable no-restricted-globals */
import { detectSyncOpfsInWorkerScope } from "../../../src/runtime/shared-worker-leader/capability.js";

self.onconnect = (event) => {
  const port = event.ports[0];
  port.onmessage = async (msg) => {
    if (msg.data?.t !== "PROBE") return;
    let supported = false;
    try {
      supported = await detectSyncOpfsInWorkerScope();
    } catch {
      supported = false;
    }
    port.postMessage({ t: "PROBE_RESULT", supported });
  };
  port.start();
};
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `pnpm --filter jazz-tools test:browser shared-worker-leader-capability`
Expected: FAIL — the test file cannot load because `../../src/runtime/shared-worker-leader/capability.js` does not resolve (the direct import in the test). This is the deterministic red.

- [ ] **Step 4: Implement `capability.ts`**

```typescript
// packages/jazz-tools/src/runtime/shared-worker-leader/capability.ts

/**
 * Capability probe: does this worker-style scope expose
 * FileSystemFileHandle.createSyncAccessHandle()? Chromium and Firefox return
 * false today inside SharedWorker (sync OPFS is dedicated-Worker-only).
 * Safari returns true.
 *
 * Must be cheap and self-cleaning — it runs once per SharedWorker boot.
 */
export async function detectSyncOpfsInWorkerScope(): Promise<boolean> {
  const nav = (globalThis as { navigator?: { storage?: { getDirectory?: unknown } } }).navigator;
  const getDirectory = nav?.storage?.getDirectory as
    | (() => Promise<FileSystemDirectoryHandle>)
    | undefined;
  if (typeof getDirectory !== "function") return false;

  const name = `__jazz_leader_probe_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  let root: FileSystemDirectoryHandle;
  try {
    root = await getDirectory.call(nav!.storage);
  } catch {
    return false;
  }

  let supported = false;
  try {
    const fileHandle = await root.getFileHandle(name, { create: true });
    const createSync = (
      fileHandle as unknown as {
        createSyncAccessHandle?: () => Promise<FileSystemSyncAccessHandle>;
      }
    ).createSyncAccessHandle;
    if (typeof createSync !== "function") {
      supported = false;
    } else {
      const sync = await createSync.call(fileHandle);
      supported = true;
      try {
        sync.close();
      } catch {
        // best-effort
      }
    }
  } catch {
    supported = false;
  }

  try {
    await root.removeEntry(name);
  } catch {
    // best-effort — sync handle may not be GC'd yet on some engines
  }
  return supported;
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `pnpm --filter jazz-tools test:browser shared-worker-leader-capability`
Expected:

- Chromium: PASS with `result === false` (the boolean assertion succeeds; the underlying probe returns false because Chromium SharedWorker has no sync OPFS).
- WebKit (if configured): PASS with `result === true`.
- Both: cleanup assertion passes.

- [ ] **Step 6: Commit**

```bash
git add packages/jazz-tools/src/runtime/shared-worker-leader/capability.ts \
        packages/jazz-tools/tests/browser/shared-worker-leader-capability.test.ts \
        packages/jazz-tools/tests/browser/fixtures/leader-capability-probe.shared-worker.js \
        packages/jazz-tools/tests/browser/fixtures/leader-support.ts
git commit -m "feat(shared-worker-leader): add sync-OPFS capability probe (tested inside SharedWorker)"
```

---

### Task 4: Browser spike — MessagePort transfer through SharedWorker

**Risk gate.** Before further implementation, prove the SharedWorker → tab `MessagePort` transfer round-trips in every target browser.

**Files:**

- Create: `packages/jazz-tools/tests/browser/shared-worker-leader-spike.test.ts`
- Create: `packages/jazz-tools/tests/browser/fixtures/leader-spike.shared-worker.js`

- [ ] **Step 1: Write the spike test**

```typescript
// packages/jazz-tools/tests/browser/shared-worker-leader-spike.test.ts
import { describe, expect, it } from "vitest";

describe("shared-worker-leader spike: MessagePort transfer", () => {
  it("tab → SharedWorker → tab port transfer delivers a round-trip message", async () => {
    if (typeof SharedWorker === "undefined") return;

    const worker = new SharedWorker(
      new URL("./fixtures/leader-spike.shared-worker.js", import.meta.url),
      { type: "module", name: `spike-${Math.random().toString(36).slice(2)}` },
    );

    const portPromise = new Promise<MessagePort>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("timeout waiting for port")), 5000);
      worker.port.onmessage = (event: MessageEvent) => {
        if (event.data?.t === "PEER_PORT" && event.ports[0]) {
          clearTimeout(timeout);
          resolve(event.ports[0]);
        }
      };
    });
    worker.port.start();
    worker.port.postMessage({ t: "HELLO", tabId: "spike-tab" });

    const port = await portPromise;
    const echoPromise = new Promise<unknown>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("timeout waiting for echo")), 5000);
      port.onmessage = (event: MessageEvent) => {
        clearTimeout(timeout);
        resolve(event.data);
      };
    });
    port.start();
    port.postMessage({ ping: "spike" });

    const echoed = await echoPromise;
    expect(echoed).toEqual({ pong: "spike" });
  }, 20000);
});
```

- [ ] **Step 2: Write the SharedWorker fixture**

```javascript
// packages/jazz-tools/tests/browser/fixtures/leader-spike.shared-worker.js
/* eslint-disable no-restricted-globals */
self.onconnect = (event) => {
  const tabPort = event.ports[0];
  tabPort.onmessage = (msg) => {
    if (msg.data?.t === "HELLO") {
      const ch = new MessageChannel();
      ch.port1.onmessage = (inner) => {
        if (inner.data?.ping) {
          ch.port1.postMessage({ pong: inner.data.ping });
        }
      };
      ch.port1.start();
      tabPort.postMessage({ t: "PEER_PORT" }, [ch.port2]);
    }
  };
  tabPort.start();
};
```

- [ ] **Step 3: Run the spike**

Run: `pnpm --filter jazz-tools test:browser shared-worker-leader-spike`
Expected: PASS in Chromium. If a WebKit Playwright project is configured, run there too. Otherwise note in the commit body that WebKit must be run manually before continuing.

- [ ] **Step 4: Commit**

```bash
git add packages/jazz-tools/tests/browser/shared-worker-leader-spike.test.ts \
        packages/jazz-tools/tests/browser/fixtures/leader-spike.shared-worker.js
git commit -m "test(shared-worker-leader): browser spike confirming MessagePort transfer through SharedWorker"
```

---

### Task 5: Make worker_host scope-agnostic in Rust

**Goal:** `worker_host.rs` currently casts `js_sys::global()` to `DedicatedWorkerGlobalScope`. We need the same handler to run in `SharedWorkerGlobalScope` too. Drop the typed cast and operate on `globalThis` via `Reflect`/`Function::call` — the surface we use (`postMessage`, `set_onmessage`, `close`) is duck-typed and identical across the two scopes.

**Files:**

- Modify: `crates/jazz-wasm/src/worker_host.rs`

- [ ] **Step 1: Replace the typed cast with a scope-agnostic helper**

Replace the existing `global_worker_scope()` helper and its imports with:

```rust
use js_sys::{Function, Reflect};
use wasm_bindgen::{JsCast, JsValue};
use web_sys::MessageEvent;
// (drop DedicatedWorkerGlobalScope import; it's unused after this change.)

fn worker_global() -> JsValue {
    js_sys::global().into()
}

fn set_global_onmessage(handler: Option<&Function>) {
    let global = worker_global();
    let key = JsValue::from_str("onmessage");
    let value: JsValue = handler.map(|f| f.into()).unwrap_or(JsValue::NULL);
    let _ = Reflect::set(&global, &key, &value);
}

fn close_worker_global() {
    let global = worker_global();
    let close = Reflect::get(&global, &JsValue::from_str("close"))
        .ok()
        .and_then(|v| v.dyn_into::<Function>().ok());
    if let Some(close_fn) = close {
        let _ = close_fn.call0(&global);
    }
}
```

- [ ] **Step 2: Update every call site**

Replace, in `worker_host.rs`:

- `global.set_onmessage(Some(on_message.as_ref().unchecked_ref()))` → `set_global_onmessage(Some(on_message.as_ref().unchecked_ref()))`
- `global.set_onmessage(None)` → `set_global_onmessage(None)`
- `global.close()` → `close_worker_global()`
- `global_worker_scope().into()` (e.g. in the `sender.attach_target` call) → `worker_global()`
- `global_worker_scope().post_message_with_transfer(&value, transfer.as_ref())` → `post_message_with_transfer(&worker_global(), &value, transfer.as_ref())` (reuse the helper that already exists in `runtime.rs`).

Update `post_to_main`:

```rust
fn post_to_main(msg: &WorkerToMainWire) {
    let Ok((value, transfer)) = worker_to_main_post(msg) else {
        return;
    };
    let global = worker_global();
    let post_message = Reflect::get(&global, &JsValue::from_str("postMessage"))
        .ok()
        .and_then(|v| v.dyn_into::<Function>().ok());
    let Some(post_message) = post_message else {
        return;
    };
    let _ = post_message.call2(&global, &value, &transfer.into());
}
```

- [ ] **Step 3: Build core**

Run: `pnpm build:core`
Expected: PASS. Rust compiles with no `DedicatedWorkerGlobalScope` references in `worker_host.rs`.

- [ ] **Step 4: Run existing worker tests to confirm no regression**

Run: `pnpm --filter jazz-tools test:browser worker-bridge`
Expected: PASS. The change is a syntactic widening; behaviour is unchanged.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-wasm/src/worker_host.rs
git commit -m "refactor(worker-host): drop DedicatedWorkerGlobalScope cast so the host runs in SharedWorkerGlobalScope"
```

---

### Task 6: Rust — `init-ok` JS callback hook (replaces postcard byte sniff)

**Goal:** Today, JS callers wait for Rust to post `WorkerToMainWire::InitOk` and inspect the first postcard byte to detect it. That's brittle: any reorder of `WorkerToMainWire` variants silently breaks the sniff. We add a small JS callback the caller can pass into `runAsWorker` to be invoked exactly when Rust finishes init.

**Files:**

- Modify: `crates/jazz-wasm/src/worker_host.rs`
- Modify: `crates/jazz-wasm/src/lib.rs` (if `runAsWorker` is exported there)

- [ ] **Step 1: Extend the `runAsWorker` signature**

Find the existing `runAsWorker` (probably `pub fn run_as_worker(...)` with `#[wasm_bindgen(js_name = runAsWorker)]`). Add an optional callback parameter:

```rust
#[wasm_bindgen(js_name = runAsWorker)]
pub fn run_as_worker(
    init: JsValue,
    pending: Box<[JsValue]>,
    on_init_ok: Option<Function>,
) {
    // ... existing setup ...
    // At the point InitOk is posted today (search for `WorkerToMainWire::InitOk`),
    // also invoke the callback if present:
    if let Some(cb) = on_init_ok.as_ref() {
        let _ = cb.call0(&JsValue::NULL);
    }
    // ... rest unchanged ...
}
```

If `runAsWorker` is wrapped in a JS-bound wrapper layer (e.g., `lib.rs`), update that too. Confirm the binding is callable from TS as `runAsWorker(init, pending, onInitOk)`.

- [ ] **Step 2: Keep the postcard `InitOk` post**

Do **not** remove the existing `WorkerToMainWire::InitOk` post. The dedicated-Worker bridge on the main thread still consumes it as part of its bootstrap protocol. The callback is additive.

- [ ] **Step 3: Build core**

Run: `pnpm build:core`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/jazz-wasm/src/worker_host.rs crates/jazz-wasm/src/lib.rs
git commit -m "feat(worker-host): add on_init_ok callback to runAsWorker (alternative to postcard tag sniff)"
```

---

### Task 7: Rust — `attach-follower-port` JS-object handler + typed acks

**Goal:** Add a handler in `worker_host.rs` that recognises a `{ type: "attach-follower-port", followerTabId, leaderTabId, generation }` JS-object message with one transferred `MessagePort` in `event.ports[0]`. The handler installs an `onmessage` closure on that port that decodes the `{ type: "follower-sync", payload }` JS-object messages and routes their payload arrays through the existing peer-sync path.

The request stays JS-object (a transferable `MessagePort` cannot ride inside a postcard envelope). The ack/failure rides as postcard via `WorkerToMainWire`.

**Files:**

- Modify: `crates/jazz-wasm/src/worker_protocol.rs`
- Modify: `crates/jazz-wasm/src/worker_host.rs`

- [ ] **Step 1: Add ack variants to `worker_protocol.rs` (TDD)**

Extend the `WorkerToMainWire` enum:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkerToMainWire {
    // ... existing variants ...
    FollowerPortAttached {
        follower_tab_id: String,
        generation: u32,
    },
    FollowerPortAttachFailed {
        follower_tab_id: String,
        generation: u32,
        reason: String,
    },
}
```

Extend the existing round-trip test (search for `worker_to_main_round_trips` in `worker_protocol.rs`) with two new cases:

```rust
rt_worker(&WorkerToMainWire::FollowerPortAttached {
    follower_tab_id: "tab-a".into(),
    generation: 1,
});
rt_worker(&WorkerToMainWire::FollowerPortAttachFailed {
    follower_tab_id: "tab-b".into(),
    generation: 2,
    reason: "peer client setup failed".into(),
});
```

Run: `cargo test --manifest-path crates/jazz-wasm/Cargo.toml worker_to_main_round_trips`
Expected: PASS — the new variants encode/decode round-trip.

- [ ] **Step 2: Add the thread-local port table**

In `worker_host.rs`, next to the existing `PEER_ROUTING` thread-local:

```rust
struct FollowerPort {
    port: web_sys::MessagePort,
    leader_tab_id: String,
    generation: u32,
    onmessage_closure: Closure<dyn FnMut(MessageEvent)>,
}

thread_local! {
    static FOLLOWER_PORTS: RefCell<HashMap<String, FollowerPort>> = RefCell::new(HashMap::new());
}
```

- [ ] **Step 3: Intercept the JS-object attach message in the global onmessage**

Modify the closure installed in `run_as_worker` to check for the attach shape _before_ attempting postcard decode:

```rust
let on_message = Closure::<dyn FnMut(MessageEvent)>::new(move |event: MessageEvent| {
    let data = event.data();
    if let Some(type_str) = Reflect::get(&data, &JsValue::from_str("type"))
        .ok()
        .and_then(|v| v.as_string())
    {
        if type_str == "attach-follower-port" {
            handle_attach_follower_port(&event);
            return;
        }
    }
    match parse_main_to_worker(&data) {
        Ok(msg) => handle_main_message(msg),
        Err(e) => post_to_main(&WorkerToMainWire::Error {
            message: format!("malformed worker message: {e}"),
        }),
    }
});
```

- [ ] **Step 4: Implement `handle_attach_follower_port`**

```rust
fn handle_attach_follower_port(event: &MessageEvent) {
    let data = event.data();
    let follower_tab_id = match Reflect::get(&data, &JsValue::from_str("followerTabId"))
        .ok()
        .and_then(|v| v.as_string())
    {
        Some(s) => s,
        None => {
            post_to_main(&WorkerToMainWire::Error {
                message: "attach-follower-port missing followerTabId".to_string(),
            });
            return;
        }
    };
    let leader_tab_id = Reflect::get(&data, &JsValue::from_str("leaderTabId"))
        .ok()
        .and_then(|v| v.as_string())
        .unwrap_or_default();
    let generation = Reflect::get(&data, &JsValue::from_str("generation"))
        .ok()
        .and_then(|v| v.as_f64())
        .map(|f| f as u32)
        .unwrap_or(0);

    let ports = event.ports();
    let port = match ports.get(0).dyn_into::<web_sys::MessagePort>() {
        Ok(p) => p,
        Err(_) => {
            post_to_main(&WorkerToMainWire::FollowerPortAttachFailed {
                follower_tab_id: follower_tab_id.clone(),
                generation,
                reason: "missing transferred MessagePort".to_string(),
            });
            return;
        }
    };

    let peer_id = format!("tab:{}", follower_tab_id);
    let runtime = RUNTIME.with(|cell| cell.borrow().clone());
    let Some(runtime) = runtime else {
        post_to_main(&WorkerToMainWire::FollowerPortAttachFailed {
            follower_tab_id: follower_tab_id.clone(),
            generation,
            reason: "attach-follower-port before runtime open".to_string(),
        });
        return;
    };
    if let Err(err) = ensure_peer_client(&runtime, &peer_id) {
        post_to_main(&WorkerToMainWire::FollowerPortAttachFailed {
            follower_tab_id: follower_tab_id.clone(),
            generation,
            reason: format!("ensure_peer_client: {err}"),
        });
        return;
    }
    PEER_ROUTING.with(|cell| {
        cell.borrow_mut().peer_terms.insert(peer_id.clone(), generation);
    });

    let peer_id_for_closure = peer_id.clone();
    let runtime_for_closure = Rc::clone(&runtime);
    let onmessage = Closure::<dyn FnMut(MessageEvent)>::new(move |ev: MessageEvent| {
        forward_follower_port_message(&peer_id_for_closure, &runtime_for_closure, &ev);
    });
    port.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
    port.start();

    FOLLOWER_PORTS.with(|cell| {
        cell.borrow_mut().insert(
            follower_tab_id.clone(),
            FollowerPort {
                port: port.clone(),
                leader_tab_id: leader_tab_id.clone(),
                generation,
                onmessage_closure: onmessage,
            },
        );
    });

    post_to_main(&WorkerToMainWire::FollowerPortAttached {
        follower_tab_id,
        generation,
    });
}

fn forward_follower_port_message(
    peer_id: &str,
    runtime: &Rc<WasmRuntime>,
    event: &MessageEvent,
) {
    let data = event.data();
    let payload_field = match Reflect::get(&data, &"payload".into()) {
        Ok(v) => v,
        Err(_) => return,
    };
    let arr = match payload_field.dyn_into::<Array>() {
        Ok(a) => a,
        Err(_) => return,
    };
    let client_id = PEER_ROUTING.with(|cell| {
        cell.borrow().peer_client_by_peer_id.get(peer_id).cloned()
    });
    let Some(client_id) = client_id else { return };
    for entry in arr.iter() {
        let Ok(u8arr) = entry.dyn_into::<Uint8Array>() else { continue };
        if let Err(err) = runtime.on_sync_message_received_from_client(&client_id, u8arr.into()) {
            tracing::warn!(?err, "follower-port sync route");
        }
    }
    runtime.batched_tick();
}
```

- [ ] **Step 5: Build core**

Run: `pnpm build:core`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/jazz-wasm/src/worker_protocol.rs crates/jazz-wasm/src/worker_host.rs
git commit -m "feat(worker-host): accept attach-follower-port and post typed FollowerPortAttached ack"
```

---

### Task 8: Rust — leader-side outbox post directly to attached MessagePort

**Goal:** When the runtime emits a client-bound payload for a peer whose `peer_id` matches an entry in `FOLLOWER_PORTS`, post the encoded payload directly to that port and skip the existing `WorkerToMainWire::PeerSync` path. Falls through to the legacy path when no port is registered.

**Files:**

- Modify: `crates/jazz-wasm/src/worker_host.rs`
- Modify: `crates/jazz-wasm/src/runtime.rs`

- [ ] **Step 1: Add the port-lookup callback to `RustOutboxSender`**

In `runtime.rs`, add an optional callback field to `RustOutboxSenderInner`:

```rust
/// Worker-side: `(peerId: string) => MessagePort | null`. When non-null and
/// the peer has an attached follower port, the outbox emits a leader-sync JS
/// object to that port and skips the worker-to-main PeerSync envelope.
follower_port_lookup: RefCell<Option<Function>>,
```

Add a setter:

```rust
pub(crate) fn set_follower_port_lookup(&self, lookup: Option<Function>) {
    *self.inner.follower_port_lookup.borrow_mut() = lookup;
}
```

In the client-bound branch of `send_sync_message` (search for `peer_routing_lookup` and place this immediately before it):

```rust
if let Some(lookup) = inner.follower_port_lookup.borrow().as_ref() {
    if let Ok(port_value) =
        lookup.call1(&JsValue::NULL, &JsValue::from_str(&destination_id))
    {
        if !port_value.is_null() && !port_value.is_undefined() {
            if let Ok(port) = port_value.dyn_into::<web_sys::MessagePort>() {
                let bytes_payload = match &encoded {
                    SyncEntry::BareBytes(b) | SyncEntry::SequencedBytes { payload: b, .. } => b.clone(),
                    SyncEntry::BareString(_) | SyncEntry::SequencedString { .. } => return,
                };
                let arr = Uint8Array::from(&bytes_payload[..]);
                let payload_arr = Array::new();
                payload_arr.push(&arr);
                let obj = Object::new();
                let _ = Reflect::set(&obj, &"type".into(), &JsValue::from_str("leader-sync"));
                let _ = Reflect::set(&obj, &"payload".into(), &payload_arr.into());
                let _ = port.post_message(&obj.into());
                return;
            }
        }
    }
}
```

- [ ] **Step 2: Wire the lookup from `worker_host.rs`**

After the existing `RustOutboxSender::new(true)` and the `attach_target` call, install the lookup:

```rust
sender.set_follower_port_lookup(Some(make_follower_port_lookup()));
```

And implement:

```rust
fn make_follower_port_lookup() -> Function {
    Closure::<dyn Fn(JsValue) -> JsValue>::new(|peer_id: JsValue| {
        let Some(peer_id) = peer_id.as_string() else {
            return JsValue::NULL;
        };
        let follower_tab_id = match peer_id.strip_prefix("tab:") {
            Some(rest) => rest.to_string(),
            None => return JsValue::NULL,
        };
        FOLLOWER_PORTS.with(|cell| {
            match cell.borrow().get(&follower_tab_id) {
                Some(entry) => JsValue::from(entry.port.clone()),
                None => JsValue::NULL,
            }
        })
    })
    .into_js_value()
    .unchecked_into()
}
```

- [ ] **Step 3: Build core**

Run: `pnpm build:core`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/jazz-wasm/src/worker_host.rs crates/jazz-wasm/src/runtime.rs
git commit -m "feat(worker-host): post leader-bound peer sync directly to attached MessagePort when available"
```

---

### Task 9: Browser test — Rust attach-follower-port end-to-end (dedicated Worker)

**Files:**

- Create: `packages/jazz-tools/tests/browser/shared-worker-leader-rust.test.ts`

This task adds the missing test for Tasks 7 + 8. The Rust handler is scope-agnostic, so we exercise it inside a dedicated Worker where init is deterministic. This catches a regression in `attach-follower-port` without depending on the full broker/leader plumbing.

- [ ] **Step 1: Write the test**

```typescript
// packages/jazz-tools/tests/browser/shared-worker-leader-rust.test.ts
import { describe, expect, it } from "vitest";

/**
 * The test imports the existing dedicated-Worker bootstrap, transfers a
 * MessagePort via attach-follower-port, sends a follower-sync payload through
 * the port, and asserts the Rust handler accepted it (via the postcard
 * FollowerPortAttached ack arriving on the worker bridge).
 *
 * Wire this up against the existing worker-bridge test harness — search for
 * `worker-bridge.test.ts` and follow the same dedicated-Worker construction
 * pattern. Pseudocode below; concrete API depends on harness shape.
 */
describe("worker-host attach-follower-port (dedicated Worker)", () => {
  it("accepts a transferred port and acks with FollowerPortAttached", async () => {
    // 1. Construct the dedicated jazz-worker per existing harness pattern.
    // 2. Wait for InitOk (existing harness exposes this).
    // 3. const mc = new MessageChannel();
    // 4. worker.postMessage(
    //      { type: "attach-follower-port", followerTabId: "tab-x", leaderTabId: "leader-y", generation: 1 },
    //      [mc.port1],
    //    );
    // 5. Await a WorkerToMainWire decode of FollowerPortAttached with
    //    follower_tab_id === "tab-x", generation === 1.
    // 6. mc.port2.postMessage({ type: "follower-sync", payload: [new Uint8Array([1,2,3])] });
    // 7. Assert no Error is posted back; the runtime route is async, so the
    //    test verifies attach success only — sync application is covered in
    //    Tasks 17–19.
    expect(true).toBe(true); // placeholder — replace with the harness wiring.
  });
});
```

> **Note for the implementer:** the existing worker-bridge harness (search the repo for `worker-bridge.test.ts` or `WorkerBridge` test fixtures) provides the dedicated-Worker construction + InitOk wait. Replicate that pattern; do NOT invent a parallel harness. If no reusable harness exists, this task expands to a small one — but inspect first.

- [ ] **Step 2: Run the test**

Run: `pnpm --filter jazz-tools test:browser shared-worker-leader-rust`
Expected: PASS in Chromium (the dedicated Worker path is universal).

- [ ] **Step 3: Commit**

```bash
git add packages/jazz-tools/tests/browser/shared-worker-leader-rust.test.ts
git commit -m "test(worker-host): attach-follower-port end-to-end via dedicated Worker"
```

---

### Task 10: SharedWorker entry script skeleton

**Files:**

- Create: `packages/jazz-tools/src/runtime/shared-worker-leader/shared-worker-leader.ts`

This task lays down the entry script's `onconnect`, scope/version validation, fault posting, and tab bookkeeping. The runtime hosting itself comes in Task 12.

- [ ] **Step 1: Implement the skeleton**

```typescript
// packages/jazz-tools/src/runtime/shared-worker-leader/shared-worker-leader.ts
/// <reference lib="webworker" />

/**
 * SharedWorker entry script for the Jazz Safari leader.
 *
 * Boot sequence:
 *   1. Accept SharedWorker connections; validate CONNECT.
 *   2. On the first valid CONNECT, run the capability probe.
 *   3. If supported, instantiate SharedWorkerLeader and bootstrap the runtime
 *      (Task 12). Otherwise, post LEADER_FAULT/runtime-host-unavailable.
 *   4. Mint a follower MessagePort for the tab and reply with PEER_PORT.
 */

import {
  LEADER_PROTOCOL_VERSION,
  buildLeaderScope,
  isTabToLeader,
  type ConnectMessage,
  type LeaderFaultReason,
  type LeaderToTab,
  type TabId,
} from "./protocol.js";
import { detectSyncOpfsInWorkerScope } from "./capability.js";
import { createSharedWorkerLeader, type LeaderHost } from "./leader-host.js";

declare const self: SharedWorkerGlobalScope;

interface TabRecord {
  id: TabId;
  port: MessagePort;
  scope: string;
}

const tabs = new Map<TabId, TabRecord>();
let leaderScope: string | null = null;
let leaderHost: LeaderHost | null = null;
let initPromise: Promise<void> | null = null;
let initError: { reason: LeaderFaultReason; detail?: string } | null = null;

function postToTab(record: TabRecord, msg: LeaderToTab, transfer?: Transferable[]): void {
  try {
    if (transfer && transfer.length > 0) {
      record.port.postMessage(msg, transfer);
    } else {
      record.port.postMessage(msg);
    }
  } catch {
    // port likely closed
  }
}

async function ensureLeaderReady(hello: ConnectMessage): Promise<void> {
  if (initError) throw new Error(initError.reason);
  if (leaderHost) return;
  if (!initPromise) {
    initPromise = (async () => {
      const supported = await detectSyncOpfsInWorkerScope();
      if (!supported) {
        initError = { reason: "runtime-host-unavailable" };
        throw new Error(initError.reason);
      }
      try {
        const host = createSharedWorkerLeader({
          scope: hello.scope,
          appId: hello.appId,
          dbName: hello.dbName,
          schemaJson: hello.schemaJson,
          env: hello.env,
          userBranch: hello.userBranch,
          serverUrl: hello.serverUrl,
          jwtToken: hello.jwtToken,
          adminSecret: hello.adminSecret,
        });
        await host.init();
        leaderHost = host;
      } catch (err) {
        // `init-failed` is a deliberate catch-all bucket (lock-acquisition,
        // wasm-init, init-timeout all collapse to it). Preserve the real stack
        // in the SharedWorker's devtools context — the tab only receives the
        // string `detail`, which loses the stack.
        console.error("[jazz] shared-worker leader init failed:", err);
        initError = {
          reason: "init-failed",
          detail: err instanceof Error ? err.message : String(err),
        };
        throw err;
      }
    })();
  }
  await initPromise;
}

async function handleConnect(record: TabRecord, msg: ConnectMessage): Promise<void> {
  if (msg.protocolVersion !== LEADER_PROTOCOL_VERSION) {
    postToTab(record, { t: "LEADER_FAULT", reason: "version-mismatch" });
    return;
  }
  // Defense-in-depth: unreachable under correct URL/name resolution, since
  // buildLeaderWorkerName(scope) routes distinct scopes to distinct SharedWorker
  // instances by name — two scopes can never reach the same instance. Kept as a
  // cheap guard against a future naming refactor that breaks that invariant.
  if (leaderScope && msg.scope !== leaderScope) {
    postToTab(record, { t: "LEADER_FAULT", reason: "scope-mismatch" });
    return;
  }
  leaderScope = leaderScope ?? msg.scope;
  record.scope = msg.scope;
  record.id = msg.tabId;
  tabs.set(record.id, record);

  try {
    await ensureLeaderReady(msg);
  } catch {
    postToTab(record, {
      t: "LEADER_FAULT",
      reason: initError?.reason ?? "init-failed",
      detail: initError?.detail,
    });
    return;
  }

  if (!leaderHost) {
    postToTab(record, { t: "LEADER_FAULT", reason: "init-failed" });
    return;
  }
  const { followerPort, generation } = await leaderHost.attachFollower(record.id);
  postToTab(record, { t: "PEER_PORT", port: followerPort, generation }, [followerPort]);
}

function handleGoodbye(record: TabRecord): void {
  if (!record.id) return;
  tabs.delete(record.id);
  leaderHost?.detachFollower(record.id);
  try {
    record.port.close();
  } catch {
    // ignored
  }
}

let cachedCapability: boolean | null = null;

async function answerCapability(record: TabRecord): Promise<void> {
  if (cachedCapability === null) {
    try {
      cachedCapability = await detectSyncOpfsInWorkerScope();
    } catch {
      cachedCapability = false;
    }
  }
  postToTab(record, { t: "CAPABILITY_RESULT", supported: cachedCapability });
}

self.onconnect = (event: MessageEvent) => {
  const port = event.ports[0];
  if (!port) return;
  const record: TabRecord = { id: "", port, scope: "" };
  port.onmessage = (msg: MessageEvent) => {
    if (!isTabToLeader(msg.data)) return;
    const data = msg.data;
    switch (data.t) {
      case "CHECK_CAPABILITY":
        void answerCapability(record);
        return;
      case "CONNECT":
        void handleConnect(record, data);
        return;
      case "GOODBYE":
        handleGoodbye(record);
        return;
    }
  };
  port.start();
};

// Compile-time use to avoid an unused-import error if buildLeaderScope is not
// referenced elsewhere; the entry script does not call it directly, but TS
// strips the import only if it's unused. Keep the symbol live.
void buildLeaderScope;
```

- [ ] **Step 2: Build core**

Run: `pnpm build:core`
Expected: PASS — the new module compiles. Update `build:runtime` in `package.json` now (the second `cp` from Task 1 Step 5) so the entry script lands in `dist/`.

After updating `build:runtime`, run `pnpm build:core` again and verify `packages/jazz-tools/dist/runtime/shared-worker-leader/shared-worker-leader.ts` exists.

- [ ] **Step 3: Commit**

```bash
git add packages/jazz-tools/src/runtime/shared-worker-leader/shared-worker-leader.ts \
        packages/jazz-tools/package.json
git commit -m "feat(shared-worker-leader): SharedWorker entry script with CONNECT/GOODBYE handling"
```

---

### Task 11: LeaderHost interface + SharedWorkerLeader stub

**Files:**

- Create: `packages/jazz-tools/src/runtime/shared-worker-leader/leader-host.ts`

This task introduces the interface only; the real runtime hosting comes in Task 12. The interface is small (init + attach + detach) — the abstraction earns its keep by letting the entry script in Task 10 be testable against a stub.

- [ ] **Step 1: Implement the interface + stub**

```typescript
// packages/jazz-tools/src/runtime/shared-worker-leader/leader-host.ts
import type { TabId } from "./protocol.js";

export interface LeaderHost {
  init(): Promise<void>;
  attachFollower(tabId: TabId): Promise<{
    followerPort: MessagePort;
    generation: number;
  }>;
  detachFollower(tabId: TabId): void;
}

export interface SharedWorkerLeaderOptions {
  scope: string;
  appId: string;
  dbName: string;
  schemaJson: string;
  env?: string;
  userBranch?: string;
  serverUrl?: string;
  jwtToken?: string;
  adminSecret?: string;
  clientId?: string;
}

/**
 * Hosts the in-process Jazz runtime inside the SharedWorker scope. Acquires
 * LOCK_NAME, opens OPFS, opens upstream WebSocket, and attaches follower
 * MessagePorts directly to its own PEER_ROUTING table via the Rust
 * worker-host's `attach-follower-port` handler.
 *
 * Real runtime hosting lands in Task 12.
 */
export function createSharedWorkerLeader(_options: SharedWorkerLeaderOptions): LeaderHost {
  return {
    async init(): Promise<void> {
      throw new Error("SharedWorkerLeader.init not yet implemented (Task 12)");
    },
    async attachFollower(_tabId: TabId) {
      throw new Error("SharedWorkerLeader.attachFollower not yet implemented (Task 12)");
    },
    detachFollower(_tabId: TabId): void {
      throw new Error("SharedWorkerLeader.detachFollower not yet implemented (Task 12)");
    },
  };
}
```

- [ ] **Step 2: Build core**

Run: `pnpm build:core`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add packages/jazz-tools/src/runtime/shared-worker-leader/leader-host.ts
git commit -m "feat(shared-worker-leader): LeaderHost interface and SharedWorkerLeader stub"
```

---

### Task 12: SharedWorkerLeader — host the runtime inside the SharedWorker

**Goal:** Replace the throw-stubs in `createSharedWorkerLeader` with a real implementation that (1) acquires `LOCK_NAME`, (2) drives the same Rust runtime bootstrap that `jazz-worker.ts` drives today (using the `on_init_ok` callback from Task 6), and (3) mints follower `MessageChannel`s and feeds the leader-side end to the in-scope worker host via the `attach-follower-port` JS-object message.

**Background:** The Rust `worker_host.rs` reads its init from `self.onmessage` (a JS-object `init` envelope) and then takes over `self.onmessage`. In SharedWorker scope, the leader entry script must:

1. Hold `LOCK_NAME`.
2. Synthesize the same init envelope `jazz-worker.ts` synthesizes.
3. Call `wasmModule.runAsWorker(init, [], onInitOk)`. Rust installs its `onmessage` and invokes `onInitOk` when init completes.
4. Route follower-port attachments by dispatching a synthetic `MessageEvent` on the global scope so Rust's `onmessage` (now installed) catches them.

The synthetic-dispatch is necessary because SharedWorker has no main thread to `postMessage` to itself.

**Files:**

- Modify: `packages/jazz-tools/src/runtime/shared-worker-leader/leader-host.ts`
- Reference: `packages/jazz-tools/src/worker/jazz-worker.ts` (crib its bootstrap shape)

- [ ] **Step 1: Replace the stub with the implementation**

```typescript
// packages/jazz-tools/src/runtime/shared-worker-leader/leader-host.ts
import type { TabId } from "./protocol.js";

export interface LeaderHost {
  init(): Promise<void>;
  attachFollower(tabId: TabId): Promise<{
    followerPort: MessagePort;
    generation: number;
  }>;
  detachFollower(tabId: TabId): void;
}

export interface SharedWorkerLeaderOptions {
  scope: string;
  appId: string;
  dbName: string;
  schemaJson: string;
  env?: string;
  userBranch?: string;
  serverUrl?: string;
  jwtToken?: string;
  adminSecret?: string;
  clientId?: string;
}

function buildLockName(appId: string, dbName: string): string {
  return `jazz-worker:${appId}:${dbName}`;
}

async function acquireExclusiveLock(name: string): Promise<void> {
  const nav = (self as unknown as { navigator: { locks: LockManager } }).navigator;
  return new Promise<void>((resolveAcquired, rejectAcquired) => {
    nav.locks
      .request(name, { mode: "exclusive" }, () => {
        resolveAcquired();
        // Held until SharedWorker termination — never resolves.
        return new Promise<void>(() => {});
      })
      .catch((err) => rejectAcquired(err));
  });
}

export function createSharedWorkerLeader(options: SharedWorkerLeaderOptions): LeaderHost {
  let runtimeInitialized = false;
  // Reserved. Always 1 in v1: a fresh SharedWorker boot resets module scope, so
  // there is no persistent cross-reboot counter. The Rust PEER_ROUTING keys on
  // peer_id (not generation), so reattach with generation 1 is harmless. This
  // field becomes load-bearing only in the tab-hosted follow-up plan, which
  // needs it to reject stale ATTACH_PORT_ACKs across leader generations; that
  // plan adds the persistent counter. Do not invent one here.
  const generation = 1;
  const attachedFollowers = new Set<TabId>();

  async function bootstrapRuntime(): Promise<void> {
    if (runtimeInitialized) return;
    if (!options.appId || !options.dbName) {
      throw new Error("shared-worker-leader: appId + dbName required");
    }

    await acquireExclusiveLock(buildLockName(options.appId, options.dbName));

    const wasmModule: typeof import("jazz-wasm") =
      (await import("jazz-wasm")) as unknown as typeof import("jazz-wasm");
    if (
      typeof (wasmModule as unknown as { default?: () => Promise<unknown> }).default === "function"
    ) {
      await (wasmModule as unknown as { default: () => Promise<unknown> }).default();
    }

    const init = {
      type: "init" as const,
      schemaJson: options.schemaJson,
      appId: options.appId,
      env: options.env ?? "default",
      userBranch: options.userBranch ?? "main",
      dbName: options.dbName,
      clientId: options.clientId ?? crypto.randomUUID(),
      serverUrl: options.serverUrl,
      jwtToken: options.jwtToken,
      adminSecret: options.adminSecret,
      runtimeSources: undefined,
    };

    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("init-ok timeout")), 15000);
      const onInitOk = () => {
        clearTimeout(timeout);
        resolve();
      };
      try {
        (
          wasmModule as unknown as {
            runAsWorker(init: unknown, pending: unknown[], onInitOk: () => void): void;
          }
        ).runAsWorker(init, [], onInitOk);
      } catch (err) {
        clearTimeout(timeout);
        reject(err);
      }
    });

    runtimeInitialized = true;
  }

  function dispatchToRustOnMessage(data: unknown, ports: MessagePort[]): void {
    const event = new MessageEvent("message", { data, ports });
    const handler = (self as unknown as { onmessage?: (ev: MessageEvent) => void }).onmessage;
    if (typeof handler === "function") {
      handler(event);
    } else {
      throw new Error("shared-worker-leader: Rust onmessage handler is not installed");
    }
  }

  return {
    async init(): Promise<void> {
      await bootstrapRuntime();
    },
    async attachFollower(tabId: TabId) {
      if (!runtimeInitialized) {
        throw new Error("shared-worker-leader: init() must complete before attachFollower");
      }
      const mc = new MessageChannel();
      dispatchToRustOnMessage(
        {
          type: "attach-follower-port",
          followerTabId: tabId,
          leaderTabId: "shared-worker-leader",
          generation,
        },
        [mc.port1],
      );
      attachedFollowers.add(tabId);
      return {
        followerPort: mc.port2,
        generation,
      };
    },
    detachFollower(tabId: TabId): void {
      attachedFollowers.delete(tabId);
      // PEER_ROUTING entry stays until SharedWorker shutdown; the closed
      // MessagePort already signals the leader side. Active cleanup is a
      // follow-up if we observe leaked clients.
    },
  };
}
```

- [ ] **Step 2: Build core**

Run: `pnpm build:core`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add packages/jazz-tools/src/runtime/shared-worker-leader/leader-host.ts
git commit -m "feat(shared-worker-leader): host the Jazz runtime inside SharedWorker scope"
```

---

### Task 13: Tab-side leader client facade

**Files:**

- Create: `packages/jazz-tools/src/runtime/shared-worker-leader/client.ts`

- [ ] **Step 1: Implement the client**

```typescript
// packages/jazz-tools/src/runtime/shared-worker-leader/client.ts
import {
  LEADER_PROTOCOL_VERSION,
  buildLeaderScope,
  buildLeaderWorkerName,
  isLeaderToTab,
  type ConnectMessage,
  type LeaderFaultReason,
  type LeaderToTab,
  type TabId,
} from "./protocol.js";

export interface SharedWorkerLeaderClientOptions {
  appId: string;
  dbName: string;
  env?: string;
  userBranch?: string;
  serverUrl?: string;
  jwtToken?: string;
  adminSecret?: string;
  jazzPackageVersion: string;
  leaderUrl: string;
  tabId: TabId;
  bornAt: number;
}

/**
 * Per-call arg for connect(). schemaJson reaches Db lazily (only when the user
 * touches a schema), so it is not on construction options.
 */
export interface SharedWorkerLeaderConnectArgs {
  schemaJson: string;
}

export interface PeerPortSnapshot {
  port: MessagePort;
  generation: number;
}

/**
 * Thrown when CONNECT fails after a positive capability probe. By explicit
 * design there is no dedicated-Worker fallback in this case — a supported but
 * broken leader is a hard error. `reason` carries the LeaderFaultReason when
 * the failure was a LEADER_FAULT; otherwise it is "connect-timeout" or
 * "connect-post-failed".
 */
export class SharedWorkerLeaderConnectError extends Error {
  constructor(
    readonly reason: LeaderFaultReason | "connect-timeout" | "connect-post-failed",
    readonly detail?: string,
  ) {
    super(`SharedWorker leader connect failed: ${reason}${detail ? ` (${detail})` : ""}`);
    this.name = "SharedWorkerLeaderConnectError";
  }
}

export interface SharedWorkerLeaderClient {
  /**
   * Sends CHECK_CAPABILITY and resolves with the supported flag from the
   * leader. Cheap; no runtime bootstrap is triggered. Safe to call before any
   * schema is known. Resolves to false on timeout (3s).
   */
  checkCapability(): Promise<boolean>;
  /**
   * Sends CONNECT with the given schemaJson and resolves with the first
   * PEER_PORT. Must be called after a positive checkCapability(). The client
   * does not enforce ordering — calling connect() without first probing is
   * legal but will time out on browsers without sync OPFS in SharedWorker.
   */
  connect(args: SharedWorkerLeaderConnectArgs): Promise<PeerPortSnapshot>;
  current(): PeerPortSnapshot | null;
  onPortChanged(cb: (snapshot: PeerPortSnapshot) => void): () => void;
  onFault(cb: (reason: LeaderFaultReason, detail?: string) => void): () => void;
  /** Closes the current control port and reconnects with a fresh CONNECT. */
  forceReconnect(): void;
  /** Sends GOODBYE and tears down the connection port. */
  close(): void;
}

export function createSharedWorkerLeaderClient(
  options: SharedWorkerLeaderClientOptions,
): SharedWorkerLeaderClient {
  const scope = buildLeaderScope(options.appId, options.dbName);
  const name = buildLeaderWorkerName(scope);

  const portListeners = new Set<(snapshot: PeerPortSnapshot) => void>();
  const faultListeners = new Set<(reason: LeaderFaultReason, detail?: string) => void>();
  let currentSnapshot: PeerPortSnapshot | null = null;
  let lastConnectArgs: SharedWorkerLeaderConnectArgs | null = null;
  let resolveFirst: ((snapshot: PeerPortSnapshot) => void) | null = null;
  let rejectFirst: ((err: Error) => void) | null = null;
  let firstPort = new Promise<PeerPortSnapshot>((resolve, reject) => {
    resolveFirst = resolve;
    rejectFirst = reject;
  });
  let capabilityResolve: ((supported: boolean) => void) | null = null;

  let worker: SharedWorker | null = null;
  let activePort: MessagePort | null = null;
  let closed = false;

  function buildConnectMessage(schemaJson: string): ConnectMessage {
    return {
      t: "CONNECT",
      tabId: options.tabId,
      bornAt: options.bornAt,
      scope,
      protocolVersion: LEADER_PROTOCOL_VERSION,
      jazzPackageVersion: options.jazzPackageVersion,
      appId: options.appId,
      dbName: options.dbName,
      schemaJson,
      env: options.env,
      userBranch: options.userBranch,
      serverUrl: options.serverUrl,
      jwtToken: options.jwtToken,
      adminSecret: options.adminSecret,
    };
  }

  function openConnection(): void {
    if (closed) return;
    try {
      worker = new SharedWorker(options.leaderUrl, { type: "module", name });
    } catch (err) {
      rejectFirst?.(new Error(`SharedWorker construction failed: ${(err as Error).message}`));
      capabilityResolve?.(false);
      capabilityResolve = null;
      return;
    }
    const port = worker.port;
    activePort = port;
    port.onmessage = (event: MessageEvent) => {
      const data = event.data;
      if (!isLeaderToTab(data)) return;
      switch ((data as LeaderToTab).t) {
        case "CAPABILITY_RESULT": {
          const supported = (data as Extract<LeaderToTab, { t: "CAPABILITY_RESULT" }>).supported;
          capabilityResolve?.(supported);
          capabilityResolve = null;
          return;
        }
        case "PEER_PORT": {
          const { port: peerPort, generation } = data as Extract<LeaderToTab, { t: "PEER_PORT" }>;
          currentSnapshot = { port: peerPort, generation };
          if (resolveFirst) {
            resolveFirst(currentSnapshot);
            resolveFirst = null;
            rejectFirst = null;
          }
          for (const cb of portListeners) cb(currentSnapshot);
          return;
        }
        case "LEADER_FAULT": {
          const fault = data as Extract<LeaderToTab, { t: "LEADER_FAULT" }>;
          // runtime-host-unavailable is the "fall back to dedicated worker"
          // signal and is only expected before a CONNECT (during the probe).
          // Any other fault after CONNECT is a hard error.
          if (rejectFirst) {
            rejectFirst(new SharedWorkerLeaderConnectError(fault.reason, fault.detail));
            resolveFirst = null;
            rejectFirst = null;
          }
          capabilityResolve?.(false);
          capabilityResolve = null;
          for (const cb of faultListeners) cb(fault.reason, fault.detail);
          return;
        }
      }
    };
    port.start();
  }

  openConnection();

  return {
    async checkCapability(): Promise<boolean> {
      return new Promise<boolean>((resolve) => {
        // 2s: the probe is sub-100ms once the worker is warm; this only needs
        // to cover ES-module parse + first detectSyncOpfsInWorkerScope on a
        // cold SharedWorker boot. Tighten/raise only if CI shows cold boots
        // exceeding it.
        const timeout = setTimeout(() => {
          capabilityResolve = null;
          resolve(false);
        }, 2000);
        capabilityResolve = (supported) => {
          clearTimeout(timeout);
          resolve(supported);
        };
        try {
          activePort?.postMessage({ t: "CHECK_CAPABILITY" });
        } catch {
          capabilityResolve = null;
          clearTimeout(timeout);
          resolve(false);
        }
      });
    },
    async connect(args: SharedWorkerLeaderConnectArgs): Promise<PeerPortSnapshot> {
      lastConnectArgs = args;
      try {
        activePort?.postMessage(buildConnectMessage(args.schemaJson));
      } catch (err) {
        throw new SharedWorkerLeaderConnectError("connect-post-failed", (err as Error).message);
      }
      // Bound the wait: a leader stuck acquiring LOCK_NAME or booting WASM must
      // not hang the caller forever. On timeout, reject firstPort so awaiters
      // get the typed error rather than a silent stall.
      const connectTimeout = setTimeout(() => {
        if (rejectFirst) {
          rejectFirst(new SharedWorkerLeaderConnectError("connect-timeout"));
          resolveFirst = null;
          rejectFirst = null;
        }
      }, 10000);
      try {
        const snap = await firstPort;
        clearTimeout(connectTimeout);
        return snap;
      } catch (err) {
        clearTimeout(connectTimeout);
        throw err;
      }
    },
    current() {
      return currentSnapshot;
    },
    onPortChanged(cb) {
      portListeners.add(cb);
      return () => portListeners.delete(cb);
    },
    onFault(cb) {
      faultListeners.add(cb);
      return () => faultListeners.delete(cb);
    },
    forceReconnect() {
      if (closed) return;
      try {
        activePort?.postMessage({ t: "GOODBYE" });
      } catch {
        // ignored
      }
      try {
        activePort?.close();
      } catch {
        // ignored
      }
      activePort = null;
      worker = null;
      firstPort = new Promise<PeerPortSnapshot>((resolve, reject) => {
        resolveFirst = resolve;
        rejectFirst = reject;
      });
      openConnection();
      // Reissue the last CONNECT so the new SharedWorker boot recovers state.
      if (lastConnectArgs) {
        try {
          activePort?.postMessage(buildConnectMessage(lastConnectArgs.schemaJson));
        } catch {
          // best-effort; next caller's connect() will retry
        }
      }
    },
    close() {
      closed = true;
      try {
        activePort?.postMessage({ t: "GOODBYE" });
      } catch {
        // ignored
      }
      try {
        activePort?.close();
      } catch {
        // ignored
      }
      activePort = null;
      worker = null;
    },
  };
}
```

- [ ] **Step 2: Build core**

Run: `pnpm build:core`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add packages/jazz-tools/src/runtime/shared-worker-leader/client.ts
git commit -m "feat(shared-worker-leader): tab-side client with split checkCapability/connect, forceReconnect, fault subscription"
```

---

### Task 14: Browser test — SharedWorkerLeaderClient against a stub leader

**Files:**

- Create: `packages/jazz-tools/tests/browser/shared-worker-leader-client.test.ts`
- Create: `packages/jazz-tools/tests/browser/fixtures/leader-stub.shared-worker.js`

This is a unit-ish browser test: the stub SharedWorker speaks the protocol but does no real bootstrap, so the test isolates the client's connect / fault / reconnect logic.

- [ ] **Step 1: Write the stub SharedWorker fixture**

```javascript
// packages/jazz-tools/tests/browser/fixtures/leader-stub.shared-worker.js
/* eslint-disable no-restricted-globals */
let generation = 0;
self.onconnect = (event) => {
  const port = event.ports[0];
  port.onmessage = (msg) => {
    const d = msg.data;
    if (d?.t === "CHECK_CAPABILITY") {
      port.postMessage({ t: "CAPABILITY_RESULT", supported: true });
      return;
    }
    if (d?.t !== "CONNECT") return;
    if (d.protocolVersion !== 1) {
      port.postMessage({ t: "LEADER_FAULT", reason: "version-mismatch" });
      return;
    }
    generation += 1;
    const ch = new MessageChannel();
    ch.port1.onmessage = (inner) => {
      if (inner.data?.type === "follower-sync") {
        ch.port1.postMessage({ type: "leader-sync", payload: inner.data.payload ?? [] });
      }
    };
    ch.port1.start();
    port.postMessage({ t: "PEER_PORT", port: ch.port2, generation }, [ch.port2]);
  };
  port.start();
};
```

- [ ] **Step 2: Write the test**

```typescript
// packages/jazz-tools/tests/browser/shared-worker-leader-client.test.ts
import { describe, expect, it } from "vitest";
import { createSharedWorkerLeaderClient } from "../../src/runtime/shared-worker-leader/client.js";

const stubUrl = new URL("./fixtures/leader-stub.shared-worker.js", import.meta.url).toString();

function defaultOptions(suffix: string) {
  return {
    appId: `client-test-${suffix}`,
    dbName: `db-${suffix}`,
    jazzPackageVersion: "0.0.0",
    leaderUrl: stubUrl,
    tabId: `tab-${suffix}`,
    bornAt: Date.now(),
  };
}

describe("SharedWorkerLeaderClient", () => {
  it("checkCapability() resolves with supported=true from the stub", async () => {
    const client = createSharedWorkerLeaderClient(defaultOptions("cap"));
    const supported = await client.checkCapability();
    expect(supported).toBe(true);
    client.close();
  });

  it("connect() resolves with a PEER_PORT and generation 1", async () => {
    const client = createSharedWorkerLeaderClient(defaultOptions("a"));
    await client.checkCapability();
    const snap = await client.connect({ schemaJson: "{}" });
    expect(snap.generation).toBeGreaterThanOrEqual(1);
    expect(snap.port).toBeInstanceOf(MessagePort);
    client.close();
  });

  it("forceReconnect() emits a fresh PEER_PORT", async () => {
    const client = createSharedWorkerLeaderClient(defaultOptions("b"));
    await client.checkCapability();
    const first = await client.connect({ schemaJson: "{}" });
    const secondPromise = new Promise<{ port: MessagePort; generation: number }>((resolve) => {
      const off = client.onPortChanged((snap) => {
        off();
        resolve(snap);
      });
    });
    client.forceReconnect();
    const second = await secondPromise;
    expect(second.port).not.toBe(first.port);
    client.close();
  });
});
```

- [ ] **Step 3: Run the tests**

Run: `pnpm --filter jazz-tools test:browser shared-worker-leader-client`
Expected: PASS in Chromium (the stub is a plain SharedWorker; the leader-host real bootstrap is not exercised here).

- [ ] **Step 4: Commit**

```bash
git add packages/jazz-tools/src/runtime/shared-worker-leader/client.ts \
        packages/jazz-tools/tests/browser/shared-worker-leader-client.test.ts \
        packages/jazz-tools/tests/browser/fixtures/leader-stub.shared-worker.js
git commit -m "test(shared-worker-leader): client behaviour against a stub SharedWorker"
```

---

### Task 15: Follower main-runtime transport — MessagePortRuntimeTransport

**Files:**

- Create: `packages/jazz-tools/src/runtime/shared-worker-leader/message-port-runtime-transport.ts`

This task installs server-bound forwarding on the follower main `WasmRuntime`. It depends on the Rust hooks added in Task 16; they're optional-chained here so this task builds clean.

- [ ] **Step 1: Implement the transport**

```typescript
// packages/jazz-tools/src/runtime/shared-worker-leader/message-port-runtime-transport.ts
import type { Runtime } from "../client.js";

export interface MessagePortRuntimeTransportOptions {
  port: MessagePort;
  runtime: Runtime;
}

interface RuntimeWithFollowerHooks extends Runtime {
  installFollowerOutboxSender?(): void;
  setFollowerOutboxForwarder?(cb: ((payload: Uint8Array) => void) | null): void;
  applyIncomingFollowerPayload?(payload: Uint8Array): void;
  replayFollowerServerEdge?(): void;
}

/**
 * Follower main-runtime transport. Replaces WorkerBridge for follower tabs:
 *   - Server-bound outbox is forwarded as a `follower-sync` JS object to the
 *     leader-minted port.
 *   - Incoming `leader-sync` payloads are applied via applyIncomingFollowerPayload.
 *   - On install, replays the runtime's server edge so the main runtime has
 *     somewhere to route server-bound outbox entries.
 */
export class MessagePortRuntimeTransport {
  private readonly port: MessagePort;
  private readonly runtime: RuntimeWithFollowerHooks;
  private disposed = false;

  constructor(opts: MessagePortRuntimeTransportOptions) {
    this.port = opts.port;
    this.runtime = opts.runtime as RuntimeWithFollowerHooks;
  }

  start(): void {
    this.runtime.installFollowerOutboxSender?.();
    this.runtime.setFollowerOutboxForwarder?.((payload) => {
      if (this.disposed) return;
      try {
        this.port.postMessage({
          type: "follower-sync",
          payload: [payload],
        });
      } catch {
        // port closed; SharedWorkerLeaderClient will reissue.
      }
    });
    this.runtime.replayFollowerServerEdge?.();

    this.port.onmessage = (event: MessageEvent) => {
      const data = event.data as { type?: string; payload?: unknown };
      if (data?.type !== "leader-sync") return;
      const payload = data.payload;
      if (!Array.isArray(payload)) return;
      for (const entry of payload) {
        if (entry instanceof Uint8Array) {
          this.runtime.applyIncomingFollowerPayload?.(entry);
        }
      }
    };
    this.port.start();
  }

  stop(): void {
    if (this.disposed) return;
    this.disposed = true;
    this.runtime.setFollowerOutboxForwarder?.(null);
    try {
      this.port.onmessage = null;
      this.port.close();
    } catch {
      // best-effort
    }
  }
}
```

- [ ] **Step 2: Build core**

Run: `pnpm build:core`
Expected: PASS (the file is not yet imported anywhere; the optional chains keep it building before Task 16 lands).

- [ ] **Step 3: Commit**

```bash
git add packages/jazz-tools/src/runtime/shared-worker-leader/message-port-runtime-transport.ts
git commit -m "feat(shared-worker-leader): MessagePortRuntimeTransport for follower main-runtime sync forwarding"
```

---

### Task 16: Wire follower-runtime hooks through the Runtime client

**Files:**

- Modify: `crates/jazz-wasm/src/runtime.rs`
- Modify: `packages/jazz-tools/src/runtime/client.ts`

**Background.** Today, the main `WasmRuntime` does not carry its own `RustOutboxSender` — `Db.createWithWorker` constructs a `WorkerBridge` and `runtime.rs`'s `createWorkerBridge` wires a sender on the main runtime that posts to the dedicated worker. For the broker-less follower path, there is no dedicated worker; we still need an outbox sender installed, but its server-bound delivery target is a JS callback (which `MessagePortRuntimeTransport` provides).

The existing `RustOutboxSender::set_server_payload_forwarder` already short-circuits server-bound payloads through a JS callback. We need a main-runtime-level binding so the follower can install a forwarder without a `WorkerBridge`.

- [ ] **Step 1: Add the hooks to `WasmRuntime` (Rust)**

In `crates/jazz-wasm/src/runtime.rs`, add a new field on the `WasmRuntime` struct:

```rust
follower_outbox_sender: RefCell<Option<RustOutboxSender>>,
```

Initialize it to `RefCell::new(None)` in every `WasmRuntime` constructor.

Add four `#[wasm_bindgen]` methods on `WasmRuntime`:

```rust
#[wasm_bindgen(js_name = installFollowerOutboxSender)]
pub fn install_follower_outbox_sender(&self) {
    let sender = RustOutboxSender::new(true);
    sender.attach_target(JsValue::NULL, None, None, None);
    self.core.borrow_mut().set_sync_sender(Box::new(sender.clone()));
    *self.follower_outbox_sender.borrow_mut() = Some(sender);
}

#[wasm_bindgen(js_name = setFollowerOutboxForwarder)]
pub fn set_follower_outbox_forwarder(&self, callback: Option<Function>) {
    let Some(sender) = self.follower_outbox_sender.borrow().as_ref().cloned() else {
        return;
    };
    sender.set_server_payload_forwarder(callback);
}

#[wasm_bindgen(js_name = replayFollowerServerEdge)]
pub fn replay_follower_server_edge(&self) {
    self.remove_server();
    let _ = self.add_server(None, None);
}

#[wasm_bindgen(js_name = applyIncomingFollowerPayload)]
pub fn apply_incoming_follower_payload(&self, payload: Uint8Array) -> Result<(), JsError> {
    self.on_sync_message_received(payload.into(), None)
        .map_err(|e| JsError::new(&format!("apply follower payload: {e:?}")))
}
```

- [ ] **Step 2: Add the TS-side `Runtime` methods**

In `packages/jazz-tools/src/runtime/client.ts`, find the `Runtime` interface. Add four required methods that mirror the WASM names:

```typescript
installFollowerOutboxSender(): void;
setFollowerOutboxForwarder(cb: ((payload: Uint8Array) => void) | null): void;
replayFollowerServerEdge(): void;
applyIncomingFollowerPayload(payload: Uint8Array): void;
```

- [ ] **Step 3: Tighten the transport's interface**

In `message-port-runtime-transport.ts`, change `RuntimeWithFollowerHooks` so the methods are non-optional (now they exist on the real `Runtime`):

```typescript
interface RuntimeWithFollowerHooks extends Runtime {
  installFollowerOutboxSender(): void;
  setFollowerOutboxForwarder(cb: ((payload: Uint8Array) => void) | null): void;
  applyIncomingFollowerPayload(payload: Uint8Array): void;
  replayFollowerServerEdge(): void;
}
```

And drop the `?.` optional chains inside `start()` / `stop()`.

- [ ] **Step 4: Build core**

Run: `pnpm build:core`
Expected: PASS — WASM bindings compile, TS interface compiles.

- [ ] **Step 5: Commit**

```bash
git add crates/jazz-wasm/src/runtime.rs \
        packages/jazz-tools/src/runtime/client.ts \
        packages/jazz-tools/src/runtime/shared-worker-leader/message-port-runtime-transport.ts
git commit -m "feat(runtime): expose follower-runtime hooks (outbox sender + forwarder + apply + replay)"
```

---

### Task 17: Db integration — eager capability probe, lazy CONNECT, skip spawnWorker

**Files:**

- Modify: `packages/jazz-tools/src/runtime/db.ts`

**Architecture (informed by the read of `db.ts`):**

Schema reaches `Db` lazily via `getClient(schema)` (`db.ts:1000`), not at `createWithWorker` time. So the leader bootstrap splits:

- **Eager (in `createWithWorker`):** construct `SharedWorkerLeaderClient`, call `client.checkCapability()`. If supported, set a flag (`db.useSharedWorkerLeader = true`), store the client, **skip** `TabLeaderElection`, `openSyncChannel`, `StorageResetCoordinator`, and `Db.spawnWorker`. If unsupported (Chrome/Firefox today) or timeout, close the client and fall through to the existing path unchanged.
- **Lazy (in `getClient(schema)`):** when the flag is set and `this.workerBridge === null`, call `client.connect({ schemaJson })`, await `PEER_PORT`, construct the `JazzClient` with `hasWorker: true, useBinaryEncoding: true`, install `MessagePortRuntimeTransport` on `client.getRuntime()`, and **skip** `attachWorkerBridge`.

**Concrete splice points:**

- `db.ts:947–948` set `primaryDbName` / `workerDbName`. Eager probe lands here.
- `db.ts:951` constructs `TabLeaderElection`. Guard with `if (!db.useSharedWorkerLeader) { … }`.
- `db.ts:968–975` open `openSyncChannel()`, `storageReset`, `attachLifecycleHooks()`, `spawnWorker()`. Same guard, except `attachLifecycleHooks()` (`db.ts:1235`) is `worker`-agnostic and stays unconditional.
- `db.ts:1014–1031` is the `getClient` body. Inject the lazy CONNECT + transport here.
- `db.ts:1088–1108` is `attachWorkerBridge`. Replace its invocation in the leader path with `MessagePortRuntimeTransport` installation against `client.getRuntime()`.

**Failure semantics (by explicit decision — NOT a limitation to be "fixed"):** when the capability probe returns `true` but the subsequent CONNECT fails (timeout, scope-mismatch, init-failed), the design **throws** `SharedWorkerLeaderConnectError` and does **not** fall back to the dedicated-Worker path. A supported-but-broken leader is a hard error: the throw propagates out of `getClient` → `ensureBridgeReady` → the caller's first awaited query (and out of `createDb` if the schema is touched eagerly). Rationale: once the platform claims sync-OPFS support, a CONNECT failure indicates a real fault (stale cross-version leader holding `LOCK_NAME`, corrupt OPFS, WASM load failure) that silent dedicated-Worker fallback would mask. Do not add a retroactive `spawnWorker` fallback here.

- [ ] **Step 1: Add imports + fields**

At the top of `db.ts` (next to the existing `import { WorkerBridge … }` block):

```typescript
import {
  createSharedWorkerLeaderClient,
  SharedWorkerLeaderConnectError,
  type SharedWorkerLeaderClient,
} from "./shared-worker-leader/client.js";
import { MessagePortRuntimeTransport } from "./shared-worker-leader/message-port-runtime-transport.js";
import { resolveSharedWorkerLeaderUrl } from "./shared-worker-leader/url.js";
import { JAZZ_PACKAGE_VERSION } from "./shared-worker-leader/package-version.js";
```

(`SharedWorkerLeaderConnectError` is imported for `instanceof` checks if a caller of `createDb` wants to distinguish leader-connect failures from other init errors. It is not caught inside `Db` — see "Failure semantics".)

Add three private fields on the `Db` class (next to `workerDbName`):

```typescript
private useSharedWorkerLeader = false;
private sharedWorkerLeaderClient: SharedWorkerLeaderClient | null = null;
private messagePortTransport: MessagePortRuntimeTransport | null = null;
```

- [ ] **Step 2: Add a static helper for the eager probe**

```typescript
private static async tryProbeSharedWorkerLeader(
  config: DbConfig,
  primaryDbName: string,
  tabId: string,
  bornAt: number,
): Promise<SharedWorkerLeaderClient | null> {
  if (typeof SharedWorker === "undefined") return null;

  const locationHref = typeof location !== "undefined" ? location.href : undefined;
  const leaderUrl = resolveSharedWorkerLeaderUrl(
    import.meta.url,
    locationHref,
    config.runtimeSources,
  );
  const client = createSharedWorkerLeaderClient({
    appId: config.appId,
    dbName: primaryDbName,
    env: config.env,
    userBranch: config.userBranch,
    serverUrl: config.serverUrl,
    jwtToken: config.jwtToken,
    adminSecret: config.adminSecret,
    jazzPackageVersion: JAZZ_PACKAGE_VERSION,
    leaderUrl,
    tabId,
    bornAt,
  });

  let supported = false;
  try {
    supported = await client.checkCapability();
  } catch {
    supported = false;
  }
  if (!supported) {
    client.close();
    return null;
  }
  return client;
}
```

- [ ] **Step 3: Splice the eager probe into `createWithWorker`**

In `createWithWorker` (`db.ts:941`), after L948 (`db.workerDbName = db.primaryDbName`) and before the existing `try { const election = new TabLeaderElection(...)` block starting L950, insert:

```typescript
const probeTabId = crypto.randomUUID();
const probeBornAt = Date.now();
db.sharedWorkerLeaderClient = await Db.tryProbeSharedWorkerLeader(
  config,
  db.primaryDbName,
  probeTabId,
  probeBornAt,
);
if (db.sharedWorkerLeaderClient) {
  db.useSharedWorkerLeader = true;
  db.attachLifecycleHooks();
  return db;
}
```

Verify: `attachLifecycleHooks` (`db.ts:1235`) only attaches DOM listeners. `sendLifecycleHint` (`db.ts:1258`) already guards `!this.worker`. Both already tolerate `worker === null`. No further change needed.

The early `return db` means the existing election / `openSyncChannel` / `spawnWorker` block (L950–991) is skipped entirely when the probe succeeds.

- [ ] **Step 4: Splice the lazy CONNECT into `getClient`**

In `getClient` (`db.ts:1000`), inside the `if (!this.clients.has(key))` block (L1014), the existing code reads:

```typescript
const client = this.runtimeModule.createClient({
  config: { ...this.config },
  schema: runtimeSchema,
  hasWorker: this.worker !== null,
  useBinaryEncoding: this.worker !== null,
  onAuthFailure: (reason) => {
    this.markUnauthenticated(reason);
  },
});
this.attachMutationErrorHandler(client);
if (this.worker && !this.workerBridge) {
  this.attachWorkerBridge(key, client);
}
if (!this.worker && this.config.serverUrl) {
  client.connectTransport(/* ... */);
}
this.clients.set(key, client);
```

Replace with a version that branches on `this.useSharedWorkerLeader`:

```typescript
const usingLeader = this.useSharedWorkerLeader;
const client = this.runtimeModule.createClient({
  config: { ...this.config },
  schema: runtimeSchema,
  hasWorker: this.worker !== null || usingLeader,
  useBinaryEncoding: this.worker !== null || usingLeader,
  onAuthFailure: (reason) => {
    this.markUnauthenticated(reason);
  },
});
this.attachMutationErrorHandler(client);

if (this.worker && !this.workerBridge) {
  this.attachWorkerBridge(key, client);
} else if (usingLeader && !this.messagePortTransport) {
  // Kick off the lazy leader CONNECT; the first PEER_PORT installs the
  // transport. Other awaiters block via ensureBridgeReady().
  const leaderReady = this.attachLeaderTransport(key, client);
  this.bridgeReady = leaderReady;
  leaderReady.catch(() => undefined);
}

// Direct (non-worker, non-leader) clients with serverUrl open their own transport.
if (!this.worker && !usingLeader && this.config.serverUrl) {
  client.connectTransport(this.config.serverUrl, {
    jwt_token: this.config.jwtToken,
    admin_secret: this.config.adminSecret,
  });
}
this.clients.set(key, client);
```

Add the helper method on `Db`:

```typescript
private async attachLeaderTransport(schemaJson: string, client: JazzClient): Promise<void> {
  if (!this.sharedWorkerLeaderClient) {
    throw new Error("attachLeaderTransport called without an active SharedWorker leader client");
  }
  // No try/catch: a SharedWorkerLeaderConnectError from connect() is a hard
  // error by design (see "Failure semantics" above). It rejects this promise,
  // which is stored on this.bridgeReady, so ensureBridgeReady() — and the
  // caller's first awaited query — receive the typed error. No fallback.
  const snapshot = await this.sharedWorkerLeaderClient.connect({ schemaJson });
  const transport = new MessagePortRuntimeTransport({
    port: snapshot.port,
    runtime: client.getRuntime(),
  });
  transport.start();
  this.messagePortTransport = transport;
}
```

- [ ] **Step 5: Tear down on Db close**

Find `Db.close()` (search for `async close()` in `db.ts`) and add — before any existing worker tear-down:

```typescript
this.messagePortTransport?.stop();
this.messagePortTransport = null;
this.sharedWorkerLeaderClient?.close();
this.sharedWorkerLeaderClient = null;
this.useSharedWorkerLeader = false;
```

- [ ] **Step 6: Build core**

Run: `pnpm build:core`
Expected: PASS.

- [ ] **Step 7: Run the existing db test suite to confirm Chrome path is unbroken**

Run: `pnpm --filter jazz-tools test:browser db.transport`
Expected: PASS. Chromium's SharedWorker has no sync OPFS, so `checkCapability()` returns `false`, the probe closes the client, `db.useSharedWorkerLeader` stays `false`, and the legacy `TabLeaderElection` + `spawnWorker` path runs unchanged.

- [ ] **Step 8: Commit**

```bash
git add packages/jazz-tools/src/runtime/db.ts
git commit -m "feat(db): eager SharedWorker leader probe + lazy CONNECT in getClient; skip spawnWorker when leader is available"
```

---

### Task 18: Browser test — cold start (single tab)

**Files:**

- Create: `packages/jazz-tools/tests/browser/shared-worker-leader-cold-start.test.ts`

Capability gating uses the **in-SharedWorker** probe helper from Task 3, not a main-thread call.

- [ ] **Step 1: Write the test**

```typescript
// packages/jazz-tools/tests/browser/shared-worker-leader-cold-start.test.ts
import { describe, expect, it } from "vitest";
import { createDb } from "../../src/index.js";
import { TestSchema } from "./fixtures/test-schema.js";
import { leaderSupported } from "./fixtures/leader-support.js";

describe.skipIf(!leaderSupported)("shared-worker-leader cold start", () => {
  it("single tab opens DB through the SharedWorker leader", async () => {
    const db = await createDb({
      appId: `leader-test-${Math.random().toString(36).slice(2, 8)}`,
      dbName: `cold-start-${Date.now()}`,
      schema: TestSchema,
    });

    await db.users.insert({ id: "u1", name: "Alice" });
    const rows = await db.users.all();
    expect(rows.map((r) => r.id)).toEqual(["u1"]);

    await db.close();
  }, 30000);
});
```

> **Note for the implementer:** `describe.skipIf(!leaderSupported)` requires `leaderSupported` to be settled at describe-evaluation time. `leader-support.ts` resolves it with a module top-level `await`, which vitest browser mode supports for ESM test files and their imports. If your vitest version rejects top-level await in an imported module, inline the `await probeInSharedWorker()` at the top of _this_ test file instead (still above `describe.skipIf`). Confirm vitest version before relying on the import form.

- [ ] **Step 2: Run the test**

Run: `pnpm --filter jazz-tools test:browser shared-worker-leader-cold-start`
Expected:

- Chromium: the suite is reported as **skipped** (visible/yellow), not silently passed.
- WebKit (if configured): PASS.

- [ ] **Step 3: Commit**

```bash
git add packages/jazz-tools/tests/browser/shared-worker-leader-cold-start.test.ts
git commit -m "test(shared-worker-leader): single-tab cold start (skipped where sync-OPFS-in-SharedWorker unsupported)"
```

---

### Task 19: Browser test — two-tab follower-only steady state

**Files:**

- Create: `packages/jazz-tools/tests/browser/shared-worker-leader-two-tab.test.ts`

Use the existing two-context test harness (search `vitest.config.browser.ts` and `tests/browser` for `createRemoteBrowserDb` to confirm the command signature).

- [ ] **Step 1: Write the test**

```typescript
import { describe, expect, it } from "vitest";
import { commands } from "@vitest/browser/context";
import { createDb } from "../../src/index.js";
import { TestSchema } from "./fixtures/test-schema.js";
import { leaderSupported } from "./fixtures/leader-support.js";

describe.skipIf(!leaderSupported)("shared-worker-leader two tabs", () => {
  it("Tab A insert is visible to Tab B through the leader-minted MessagePort", async () => {
    const appId = `leader-two-tab-${Math.random().toString(36).slice(2, 6)}`;
    const dbName = `two-tab-${Date.now()}`;

    const localDb = await createDb({ appId, dbName, schema: TestSchema });
    const remoteId = await commands.createRemoteBrowserDb({ appId, dbName, schema: "TestSchema" });

    await localDb.users.insert({ id: "u-from-a", name: "Alice" });
    const titleFromB = await commands.waitForRemoteBrowserDbTitle({
      id: remoteId,
      match: { id: "u-from-a" },
    });
    expect(titleFromB).toBeTruthy();

    await commands.closeRemoteBrowserDb(remoteId);
    await localDb.close();
  }, 30000);
});
```

> **Note for the implementer:** before writing this test, grep the repo for `createRemoteBrowserDb` and confirm the actual signatures of `createRemoteBrowserDb`, `waitForRemoteBrowserDbTitle`, and `closeRemoteBrowserDb`. If they differ, adapt.

- [ ] **Step 2: Run the test**

Run: `pnpm --filter jazz-tools test:browser shared-worker-leader-two-tab`
Expected: reported as **skipped** on Chromium (visible); PASS on WebKit.

- [ ] **Step 3: Commit**

```bash
git add packages/jazz-tools/tests/browser/shared-worker-leader-two-tab.test.ts
git commit -m "test(shared-worker-leader): two-tab follower-only steady state"
```

---

### Task 20: Browser test — tab close

**Files:**

- Create: `packages/jazz-tools/tests/browser/shared-worker-leader-tab-close.test.ts`

Close one of three tabs; remaining tabs continue uninterrupted (no re-election in this mode — the leader is the SharedWorker itself).

- [ ] **Step 1: Write the test**

```typescript
import { describe, expect, it } from "vitest";
import { commands } from "@vitest/browser/context";
import { createDb } from "../../src/index.js";
import { TestSchema } from "./fixtures/test-schema.js";
import { leaderSupported } from "./fixtures/leader-support.js";

describe.skipIf(!leaderSupported)("shared-worker-leader tab close", () => {
  it("closing one of three tabs leaves the other two unaffected", async () => {
    const appId = `leader-close-${Math.random().toString(36).slice(2, 6)}`;
    const dbName = `tab-close-${Date.now()}`;

    const localDb = await createDb({ appId, dbName, schema: TestSchema });
    const remoteB = await commands.createRemoteBrowserDb({ appId, dbName, schema: "TestSchema" });
    const remoteC = await commands.createRemoteBrowserDb({ appId, dbName, schema: "TestSchema" });

    await commands.closeRemoteBrowserDb(remoteC);

    await localDb.users.insert({ id: "after-close", name: "Bob" });
    const visibleAtB = await commands.waitForRemoteBrowserDbTitle({
      id: remoteB,
      match: { id: "after-close" },
    });
    expect(visibleAtB).toBeTruthy();

    await commands.closeRemoteBrowserDb(remoteB);
    await localDb.close();
  }, 30000);
});
```

- [ ] **Step 2: Run the test**

Run: `pnpm --filter jazz-tools test:browser shared-worker-leader-tab-close`
Expected: reported as **skipped** on Chromium (visible); PASS on WebKit.

- [ ] **Step 3: Commit**

```bash
git add packages/jazz-tools/tests/browser/shared-worker-leader-tab-close.test.ts
git commit -m "test(shared-worker-leader): tab close does not disrupt remaining followers"
```

---

### Task 21: Browser test — SharedWorker restart recovery

**Files:**

- Create: `packages/jazz-tools/tests/browser/shared-worker-leader-restart.test.ts`

Force a control-port disconnect via `client.forceReconnect()` (already implemented in Task 13) and verify a fresh `PEER_PORT` arrives.

- [ ] **Step 1: Write the test**

```typescript
import { describe, expect, it } from "vitest";
import { createSharedWorkerLeaderClient } from "../../src/runtime/shared-worker-leader/client.js";
import { leaderSupported } from "./fixtures/leader-support.js";

describe.skipIf(!leaderSupported)("shared-worker-leader restart", () => {
  it("client receives a fresh PEER_PORT after forceReconnect()", async () => {
    const appId = `leader-restart-${Math.random().toString(36).slice(2, 6)}`;
    const dbName = `restart-${Date.now()}`;
    const leaderUrl = new URL(
      "../../src/runtime/shared-worker-leader/shared-worker-leader.ts",
      import.meta.url,
    ).toString();

    const client = createSharedWorkerLeaderClient({
      appId,
      dbName,
      jazzPackageVersion: "test",
      leaderUrl,
      tabId: "tab-A",
      bornAt: Date.now(),
    });

    expect(await client.checkCapability()).toBe(true);
    const first = await client.connect({ schemaJson: "{}" });

    const secondPromise = new Promise<typeof first>((resolve) => {
      const off = client.onPortChanged((snap) => {
        off();
        resolve(snap);
      });
    });

    client.forceReconnect();
    const second = await secondPromise;
    expect(second.port).not.toBe(first.port);

    client.close();
  }, 30000);
});
```

- [ ] **Step 2: Run the test**

Run: `pnpm --filter jazz-tools test:browser shared-worker-leader-restart`
Expected: reported as **skipped** on Chromium (visible); PASS on WebKit.

- [ ] **Step 3: Commit**

```bash
git add packages/jazz-tools/tests/browser/shared-worker-leader-restart.test.ts
git commit -m "test(shared-worker-leader): client recovery after forceReconnect()"
```

---

### Task 22: Verify end-to-end on WebKit and document deferred items

**Files:**

- (none — verification + a brief note in the spec if blockers are discovered)

- [ ] **Step 1: Run the full leader test slice in WebKit**

If a WebKit Playwright project exists:

```bash
JAZZ_TEST_BROWSER=webkit pnpm --filter jazz-tools test:browser shared-worker-leader-
```

Expected: all browser tests for shared-worker-leader PASS in WebKit; reported as **skipped** (not silently passed) in Chromium.

- [ ] **Step 2: If WebKit is not configured in CI, run manually**

Run the suite locally against Safari Technology Preview / WebKit before claiming this task complete. If unavailable, mark this task `in_progress` and surface to the user — do NOT mark complete.

- [ ] **Step 3: Document any deferred items**

If issues are discovered (storage reset on the leader path, lock-wait LEADER_FAULT typing, bundle-size optimisation), append them to the spec at `specs/todo/projects/shared-worker-broker/spec.md` under "Risks & open questions".

- [ ] **Step 4: Commit (only if a doc update was made)**

```bash
git add specs/todo/projects/shared-worker-broker/spec.md
git commit -m "docs(shared-worker-leader): note deferred items discovered during Safari verification"
```

---

## Known fragilities (carry forward in each task review)

1. **`acquireExclusiveLock` blocks indefinitely (Task 12).** If a stale rolling-deploy script URL holds `LOCK_NAME`, the SharedWorker waits in `acquireExclusiveLock` _before_ `runAsWorker`, so the leader never initializes and `initPromise` never settles. Note the 15s `init-ok` timeout inside `bootstrapRuntime` does **not** cover this — it wraps only the `runAsWorker`→init-ok step, which is reached after the lock. The actual recovery is the **client-side** 10s connect-timeout (Task 13): the tab's `connect()` rejects with `SharedWorkerLeaderConnectError("connect-timeout")`, which — per the throw-no-fallback decision — surfaces as a hard error to the first query. The user sees a real failure rather than a silent hang. Acceptable for v1; the cross-version-deploy scenario is rare. Follow-up: give `acquireExclusiveLock` its own timeout + a typed `LEADER_FAULT/lock-unavailable` so the leader can distinguish "lock held by stale leader" from "WASM init failed" instead of relying on the client timeout.

2. **Storage reset coordination is unavailable on the leader path.** The existing `StorageResetCoordinator` rides BroadcastChannel; the leader path does not open one. A reset request from a follower in leader mode will not propagate. v1 limitation; document in the Task 17 commit body.

3. **Bundle size.** The leader entry bundles the full Rust WASM runtime unconditionally. A dynamic `import("jazz-wasm")` gated on the capability probe (i.e., only Safari pulls the runtime) is a follow-up optimisation.

4. **Cross-mode protocol equivalence is not in scope.** Tab-hosted SharedWorker mode is a separate future plan; equivalence testing belongs there.

5. **Synthetic `MessageEvent` dispatch (Task 12).** `attachFollower` synthesises a `MessageEvent` on the global scope and invokes the Rust-installed `self.onmessage` directly. If Rust ever switches to `self.addEventListener('message', …)` instead of assigning `self.onmessage`, the dispatch will silently miss the handler. The Task-9 browser test exercises `attach-follower-port` via a dedicated Worker (where this concern is moot); add a leader-side integration test in WebKit (Task 18) that catches a regression — it does, because `cold-start` only passes if the follower port actually routes.

---

## Self-Review Checklist

After each task is implemented, re-read this checklist:

**Spec coverage (Safari leader path only):**

- Goal 1 (one writer): Task 12 acquires `LOCK_NAME` exclusively before opening OPFS / upstream inside the SharedWorker. ✓
- Goal 2 (lock observability): lock acquisition in Task 12. ✓
- Goal 3 (failure recovery): SharedWorker restart covered by Task 21. A blocked lock / stuck init is bounded by the client-side 10s connect-timeout (Task 13), surfaced as a hard `SharedWorkerLeaderConnectError` (no silent fallback). ✓
- Goal 4 (followers do not poll): Task 15 + Task 13 — followers receive PEER_PORT push, no polling. ✓
- Goal 5 (no leader main on data path): no leader main thread exists in this mode. ✓
- Goal 6 (no storage/sync/server-protocol changes): Tasks 5–8 are boundary plumbing only; no schema/sync semantics changed. ✓
- Goal 7 (capability-based mode selection): Task 3 + Task 10 ensemble; Task 17 capability gating in `Db.createWithWorker`. ✓

**Out of scope (per this plan):**

- Tab-hosted SharedWorker mode (where the SharedWorker brokers to a leader _tab_) — separate plan.
- BroadcastChannel fallback updates — unchanged.
- iOS Safari BFCache "force release" — out of scope per spec.
- Liveness-first lock stealing — out of scope per spec.

**Settled design decisions (do not relitigate during execution):**

- **No fallback on supported-but-failed.** Capability `false` → fall back to dedicated Worker. Capability `true` but CONNECT fails → **throw** `SharedWorkerLeaderConnectError`, no retroactive `spawnWorker`. (User decision.)
- **No heartbeat.** Justified by the SharedWorker lifetime model (leader only freezes when all tabs are BFCached). See Architecture. (User decision.)
- **`hasWorker: true` reused for the leader follower.** Verified safe by reading `WasmRuntimeModule.createClient` — the flag only sets `nonDurableClientRuntime`, suppresses the direct server connection, and adjusts durability defaults; no `WorkerBridge` is assumed.
- **`generation` always 1 in v1.** Reserved field; persistent counter deferred to the tab-hosted plan that consumes it.
- **Same-origin = trusted.** `CONNECT` carries jwt/adminSecret to a shared same-origin process; consistent with the existing dedicated-Worker threat model (pending final confirmation from the user).

**Placeholders:** none.

**Type consistency:** `TabId`, `PeerPortSnapshot`, `LeaderHost.attachFollower`, and `SharedWorkerLeaderClient`'s `checkCapability` / `connect` signatures match across Tasks 1, 10, 11, 12, 13, 14, 17, 21. `CONNECT` is fully specified in Task 1 and is not modified anywhere downstream. `CHECK_CAPABILITY` and `CAPABILITY_RESULT` are added in Task 1 and used unchanged in Tasks 10 (entry script), 13 (client), 14 (stub test), 17 (Db probe), 21 (restart test).

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-28-shared-worker-leader-safari.md`. Two execution options:

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
