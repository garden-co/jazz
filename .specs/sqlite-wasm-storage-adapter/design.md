# SQLite Wasm Storage Adapter for Jazz - Design Document

## Overview

This feature introduces a new storage adapter for Jazz applications running in browser environments, leveraging [SQLite Wasm](https://github.com/sqlite/sqlite-wasm) as an alternative to IndexedDB. SQLite Wasm provides a robust, SQL-based storage solution with support for OPFS (Origin Private File System) for persistence, offering better performance characteristics for certain workloads and improved debugging capabilities through standard SQL queries.

The adapter will integrate seamlessly with Jazz's existing storage architecture, implementing the `SQLiteDatabaseDriverAsync` interface and working with the `getSqliteStorageAsync` factory from `cojson`. All browser-facing framework providers (React, Svelte, and their Clerk auth variants) will be updated to accept the new storage option.

### Key Benefits

- **Better Performance**: SQLite can offer superior performance for complex queries and large datasets compared to IndexedDB
- **OPFS Support**: When available, provides persistent storage with better characteristics than IndexedDB
- **SQL Debugging**: Developers can inspect storage using standard SQL queries
- **Memory Fallback**: Gracefully falls back to in-memory storage when OPFS is unavailable
- **Compatibility**: Works in modern browsers with SharedArrayBuffer support (requires COOP/COEP headers)

## Architecture / Components

### Package Structure

A new package will be created: `packages/cojson-storage-sqlite-wasm/`

```
packages/cojson-storage-sqlite-wasm/
├── src/
│   ├── index.ts                           # Main entry point & getSqliteWasmStorage()
│   ├── SqliteWasmDriver.ts                # SQLiteDatabaseDriverAsync implementation
│   └── tests/
│       ├── storage.sqlite-wasm.test.ts    # Driver + integration tests
│       ├── testUtils.ts
│       └── messagesTestUtils.ts
├── package.json
├── tsconfig.json
├── vitest.config.ts                       # Browser mode via Playwright
```

### Component Interactions

`sqlite3Worker1Promiser` from `@sqlite.org/sqlite-wasm` already spawns and manages its own internal Web Worker. The driver uses the promiser API directly from the main thread -- no custom worker is needed.

```
┌─────────────────────────────────────────────────────────────┐
│              Framework Providers (all browser)                │
│                                                              │
│  React: JazzReactProvider → JazzBrowserContextManager        │
│  Svelte: Provider.svelte  → JazzBrowserContextManager        │
│  Svelte+Clerk: JazzSvelteProviderWithClerk → same            │
│                                                              │
│  All accept:  storage?: "indexedDB" | "sqlite-wasm"          │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│        createBrowserContext.ts (setupPeers)                   │
│                                                              │
│  if (storage === "sqlite-wasm")                              │
│    → dynamic import("cojson-storage-sqlite-wasm")            │
│    → getSqliteWasmStorage()                                  │
│  else                                                        │
│    → getIndexedDBStorage()  (default)                        │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│            cojson-storage-sqlite-wasm                        │
│  ┌────────────────────────────────────────────────────────┐ │
│  │              SqliteWasmDriver                          │ │
│  │  Implements: SQLiteDatabaseDriverAsync                │ │
│  │  Uses: sqlite3Worker1Promiser (main thread)           │ │
│  │                                                        │ │
│  │  - initialize()  → promiser('open', {filename})       │ │
│  │  - run(sql)       → promiser('exec', {sql, bind})     │ │
│  │  - query<T>(sql)  → promiser('exec', {sql, rowMode})  │ │
│  │  - get<T>(sql)    → query()[0]                        │ │
│  │  - transaction()  → BEGIN / callback / COMMIT|ROLLBACK│ │
│  │  - closeDb()      → promiser('close', {dbId})         │ │
│  └────────────────────────────────────────────────────────┘ │
│                             │                                │
│             sqlite3Worker1Promiser manages                   │
│             its own internal Web Worker                      │
│                             │                                │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  @sqlite.org/sqlite-wasm (internal worker)            │ │
│  │  - OPFS persistence when available                    │ │
│  │  - In-memory fallback                                 │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                   cojson/storage                             │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  getSqliteStorageAsync(driver)                        │ │
│  │  → runs migrations                                    │ │
│  │  → returns StorageApiAsync(new SqliteAsyncClient(db)) │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### Core Implementation Files

#### 1. SqliteWasmDriver.ts

Implements the `SQLiteDatabaseDriverAsync` interface using `sqlite3Worker1Promiser` directly. The promiser handles all worker communication internally.

```typescript
import type { SQLiteDatabaseDriverAsync } from "cojson";
import { sqlite3Worker1Promiser } from "@sqlite.org/sqlite-wasm";

type Promiser = (
  messageType: string,
  args: Record<string, unknown>,
) => Promise<{ dbId: string; result: Record<string, unknown> }>;

export class SqliteWasmDriver implements SQLiteDatabaseDriverAsync {
  private promiser!: Promiser;
  private dbId!: string;
  private readonly filename: string;
  private readonly useOPFS: boolean;

  constructor(filename = "jazz-cojson.sqlite3", useOPFS = true) {
    this.filename = filename;
    this.useOPFS = useOPFS;
  }

  async initialize(): Promise<void> {
    // sqlite3Worker1Promiser spawns its own internal worker
    this.promiser = await new Promise<Promiser>((resolve) => {
      const _promiser = sqlite3Worker1Promiser({
        onready: () => resolve(_promiser as Promiser),
      });
    });

    // Try OPFS first, fall back to in-memory
    const filename = this.useOPFS
      ? `file:${this.filename}?vfs=opfs`
      : ":memory:";

    try {
      const openResponse = await this.promiser("open", { filename });
      this.dbId = openResponse.dbId;
    } catch {
      // OPFS unavailable — fall back to in-memory
      console.warn(
        "OPFS not available, falling back to in-memory storage",
      );
      const openResponse = await this.promiser("open", {
        filename: ":memory:",
      });
      this.dbId = openResponse.dbId;
    }
  }

  async run(sql: string, params: unknown[]): Promise<void> {
    await this.promiser("exec", {
      dbId: this.dbId,
      sql,
      bind: params,
    });
  }

  async query<T>(sql: string, params: unknown[]): Promise<T[]> {
    const response = await this.promiser("exec", {
      dbId: this.dbId,
      sql,
      bind: params,
      returnValue: "resultRows",
      rowMode: "object",
    });
    return (response.result.resultRows ?? []) as T[];
  }

  async get<T>(sql: string, params: unknown[]): Promise<T | undefined> {
    const rows = await this.query<T>(sql, params);
    return rows[0];
  }

  async transaction(
    callback: (tx: SQLiteDatabaseDriverAsync) => unknown,
  ): Promise<unknown> {
    await this.run("BEGIN TRANSACTION", []);
    try {
      const result = await callback(this);
      await this.run("COMMIT", []);
      return result;
    } catch (error) {
      await this.run("ROLLBACK", []);
      throw error;
    }
  }

  async closeDb(): Promise<void> {
    await this.promiser("close", { dbId: this.dbId });
  }

  async getMigrationVersion(): Promise<number> {
    const row = await this.get<{ user_version: number }>(
      "PRAGMA user_version",
      [],
    );
    return row?.user_version ?? 0;
  }

  async saveMigrationVersion(version: number): Promise<void> {
    await this.run(`PRAGMA user_version = ${version}`, []);
  }
}
```

#### 2. index.ts

Main entry point. Mirrors the pattern used by other storage adapters (e.g. `cojson-storage-sqlite` calling `getSqliteStorage`).

```typescript
import { getSqliteStorageAsync } from "cojson";
import { SqliteWasmDriver } from "./SqliteWasmDriver.js";

export { SqliteWasmDriver };

/**
 * Create a SQLite Wasm storage adapter for Jazz.
 *
 * Uses `sqlite3Worker1Promiser` from `@sqlite.org/sqlite-wasm`,
 * which manages its own internal Web Worker. OPFS is used for
 * persistence when available; otherwise falls back to in-memory.
 *
 * **Requirements:**
 * - Server must set COOP/COEP headers for OPFS support
 * - `@sqlite.org/sqlite-wasm` must be excluded from bundler
 *   optimization (e.g. `optimizeDeps.exclude` in Vite)
 *
 * @param filename - Database file name for OPFS (default: `'jazz-cojson.sqlite3'`)
 * @param useOPFS - Whether to attempt OPFS persistence (default: `true`)
 */
export async function getSqliteWasmStorage(
  filename = "jazz-cojson.sqlite3",
  useOPFS = true,
) {
  const driver = new SqliteWasmDriver(filename, useOPFS);
  return await getSqliteStorageAsync(driver);
}
```

### Framework Provider Updates

The `storage` option needs to propagate through all browser-targeting framework providers. The type flows from `BaseBrowserContextOptions` → `JazzContextManagerProps` → each provider's props.

#### `BaseBrowserContextOptions` (the source of truth)

```typescript
// packages/jazz-tools/src/browser/createBrowserContext.ts

export type BaseBrowserContextOptions = {
  sync: SyncConfig;
  reconnectionTimeout?: number;
  storage?: "indexedDB" | "sqlite-wasm";  // ← add "sqlite-wasm"
  crypto?: CryptoProvider;
  authSecretStorage: AuthSecretStorage;
};

async function setupPeers(options: BaseBrowserContextOptions) {
  const crypto = options.crypto || (await WasmCrypto.create());
  // ...

  let storage;
  if (options.storage === "sqlite-wasm") {
    const { getSqliteWasmStorage } = await import("cojson-storage-sqlite-wasm");
    storage = await getSqliteWasmStorage();
  } else {
    // Default to IndexedDB (backward compatible)
    storage = await getIndexedDBStorage();
  }

  // ... rest unchanged
}
```

#### Files that reference `BaseBrowserContextOptions["storage"]` (all inherit the new type automatically)

These files already use `BaseBrowserContextOptions["storage"]` as their storage type, so they get `"sqlite-wasm"` support for free once the base type is updated:

| File | How it references the type |
|------|--------------------------|
| `packages/jazz-tools/src/browser/BrowserContextManager.ts` | `storage?: BaseBrowserContextOptions["storage"]` in `JazzContextManagerProps` |
| `packages/jazz-tools/src/react/provider.tsx` | `JazzProviderProps` extends `JazzContextManagerProps` — passes `storage` through |
| `packages/jazz-tools/src/svelte/Provider.svelte` | Props include `storage` from `JazzContextManagerProps` — passes to `contextManager.createContext()` |
| `packages/jazz-tools/src/svelte/auth/JazzSvelteProviderWithClerk.svelte` | `storage?: BaseBrowserContextOptions["storage"]` — passes to `JazzSvelteProvider` |

**No changes needed in the React, Svelte, or Svelte+Clerk providers** — they all derive their storage type from `BaseBrowserContextOptions["storage"]` and forward the value to `createBrowserContext`, which handles the actual instantiation.

#### React Native (out of scope)

React Native already accepts `SQLiteDatabaseDriverAsync | "disabled"` directly (see `BaseReactNativeContextOptions`). Since `@sqlite.org/sqlite-wasm` requires a browser environment (Web Workers + OPFS), it is not applicable to React Native and no changes are needed there.

## Data Models

### Database Schema

The adapter reuses the existing SQLite schema defined in `cojson/src/storage/sqliteAsync/sqliteMigrations.ts` via `getSqliteStorageAsync`. No schema changes are required -- the async migration runner handles everything.

Key tables:
- `coValues` — Stores CoValue headers and metadata
- `sessions` — Stores session information for each CoValue
- `transactions` — Stores individual transactions
- `signatureAfter` — Stores signatures for transaction batches
- `deletedCoValues` — Tracks CoValues pending deletion
- `syncState` — Tracks sync status with peers

## Testing Strategy

### Vitest Browser Mode Configuration

Tests **must** run in Vitest Browser Mode because `@sqlite.org/sqlite-wasm` requires a browser environment (Web Workers, Wasm). The configuration follows the exact pattern established by `cojson-storage-indexeddb`:

```typescript
// packages/cojson-storage-sqlite-wasm/vitest.config.ts
import { defineProject } from "vitest/config";
import { playwright } from "@vitest/browser-playwright";

export default defineProject({
  test: {
    name: "cojson-storage-sqlite-wasm",
    browser: {
      enabled: true,
      provider: playwright(),
      instances: [
        {
          headless: process.env.HEADLESS !== "false",
          browser: "chromium",
        },
      ],
    },
    include: ["src/**/*.test.ts"],
  },
});
```

### Driver Unit Tests

```typescript
// src/tests/storage.sqlite-wasm.test.ts
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { SqliteWasmDriver } from "../SqliteWasmDriver.js";

describe("SqliteWasmDriver", () => {
  let driver: SqliteWasmDriver;

  beforeEach(async () => {
    driver = new SqliteWasmDriver("test.db", false); // in-memory for tests
    await driver.initialize();
  });

  afterEach(async () => {
    await driver.closeDb();
  });

  it("should execute DDL and query", async () => {
    await driver.run(
      "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)",
      [],
    );
    await driver.run("INSERT INTO test (name) VALUES (?)", ["Alice"]);

    const rows = await driver.query<{ id: number; name: string }>(
      "SELECT * FROM test",
      [],
    );
    expect(rows).toEqual([{ id: 1, name: "Alice" }]);
  });

  it("should return single row with get()", async () => {
    await driver.run("CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)", []);
    await driver.run("INSERT INTO kv VALUES (?, ?)", ["key1", "val1"]);

    const row = await driver.get<{ k: string; v: string }>(
      "SELECT * FROM kv WHERE k = ?",
      ["key1"],
    );
    expect(row).toEqual({ k: "key1", v: "val1" });
  });

  it("should commit transactions", async () => {
    await driver.run("CREATE TABLE t (v INTEGER)", []);

    await driver.transaction(async (tx) => {
      await tx.run("INSERT INTO t VALUES (?)", [1]);
      await tx.run("INSERT INTO t VALUES (?)", [2]);
    });

    const rows = await driver.query<{ v: number }>(
      "SELECT v FROM t ORDER BY v",
      [],
    );
    expect(rows).toEqual([{ v: 1 }, { v: 2 }]);
  });

  it("should rollback failed transactions", async () => {
    await driver.run("CREATE TABLE t (v INTEGER)", []);

    await expect(
      driver.transaction(async (tx) => {
        await tx.run("INSERT INTO t VALUES (?)", [1]);
        throw new Error("boom");
      }),
    ).rejects.toThrow("boom");

    const rows = await driver.query<{ v: number }>(
      "SELECT COUNT(*) as c FROM t",
      [],
    );
    expect(rows[0]!.c).toBe(0);
  });

  it("should read and write migration version", async () => {
    expect(await driver.getMigrationVersion()).toBe(0);
    await driver.saveMigrationVersion(5);
    expect(await driver.getMigrationVersion()).toBe(5);
  });
});
```

### Integration Tests

Test the full storage API (`StorageApiAsync`) through `getSqliteWasmStorage()`, following the same patterns as `storage.indexeddb.test.ts`. These verify CoValue store/load roundtrips, dependency loading, transaction corrections, large data streaming, account persistence, sync state persistence, and sync resumption.

```typescript
// src/tests/storage-integration.test.ts
import { LocalNode, cojsonInternals } from "cojson";
import { WasmCrypto } from "cojson/crypto/WasmCrypto";
import { afterEach, beforeEach, describe, expect, test } from "vitest";
import { getSqliteWasmStorage } from "../index.js";
import { toSimplifiedMessages } from "./messagesTestUtils.js";
import {
  createTestNode,
  trackMessages,
  waitFor,
} from "./testUtils.js";

const Crypto = await WasmCrypto.create();
let syncMessages: ReturnType<typeof trackMessages>;

beforeEach(() => {
  syncMessages = trackMessages();
  cojsonInternals.setSyncStateTrackingBatchDelay(0);
  cojsonInternals.setCoValueLoadingRetryDelay(10);
});

afterEach(async () => {
  syncMessages.restore();
});

test("should sync and load data from storage", async () => {
  const node1 = createTestNode();
  node1.setStorage(await getSqliteWasmStorage("test.db", false));

  const group = node1.createGroup();
  const map = group.createMap();
  map.set("hello", "world");
  await map.core.waitForSync();

  node1.gracefulShutdown();
  syncMessages.clear();

  const node2 = createTestNode({ secret: node1.agentSecret });
  node2.setStorage(await getSqliteWasmStorage("test.db", false));

  const map2 = await node2.load(map.id);
  if (map2 === "unavailable") {
    throw new Error("Map is unavailable");
  }

  expect(map2.get("hello")).toBe("world");
});
```

### Testing Approach Summary

1. **All tests run in Vitest Browser Mode** via `@vitest/browser-playwright` with headless Chromium, because `@sqlite.org/sqlite-wasm` requires browser APIs (Web Workers, Wasm instantiation)
2. **Driver unit tests**: verify each method of the `SQLiteDatabaseDriverAsync` interface
3. **Integration tests**: mirror the `storage.indexeddb.test.ts` test suite, covering store/load roundtrips, dependency loading, transaction corrections, large data streaming, account persistence, sync state tracking, and sync resumption
4. **In-memory mode** (`useOPFS: false`) is used for all tests to avoid OPFS cross-test interference and ensure tests work in CI without COOP/COEP headers
