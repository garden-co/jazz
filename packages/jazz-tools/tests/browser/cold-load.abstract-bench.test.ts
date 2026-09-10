import { describe, expect, it } from "vitest";
import { commands } from "vitest/browser";
import { schema as s, generateAuthSecret } from "../../src/index.js";
import { createBrowserTestDb as createDb } from "./account-fixtures.js";
import type { Db } from "../../src/runtime/db.js";
import { INDEXEDDB_BTREE_DATABASE_VERSION } from "../../src/runtime/indexeddb-page-store.js";

declare const __JAZZ_ABSTRACT_BENCH__: string;
declare const __JAZZ_COLD_LOAD_FIXTURE__: string;
declare const __JAZZ_COLD_LOAD_RESPONSIVENESS__: boolean;

const app = s.defineApp({ tasks: s.table({ title: s.string(), done: s.boolean() }) });

// Opt-in diagnostic receipt, not a wall-clock correctness gate. Reopen is a
// fresh runtime in the same browser session (the WASM module cache stays warm).
// Build with `pnpm --filter jazz-wasm build:profiling`, then admit its verified
// manifest with `node dev/artifacts/stage-native-fingerprints.mjs --profiling`.
// Run this file with JAZZ_ABSTRACT_BENCH=1 and vitest.config.browser.ts.
// Phase reports and synthetic pre-update physical fixtures are written to
// .vitest-browser-bench. The 1500-row cases include ordinary updates while a
// subscription remains active; they are distinct from transaction staging.
// Set JAZZ_COLD_LOAD_FIXTURE_DIR to a previous receipt directory for the upgrade
// case. JAZZ_COLD_LOAD_RESPONSIVENESS=1 adds a timer observer; leave it unset for
// the primary comparison with earlier uninstrumented timing receipts.
describe.skipIf(__JAZZ_ABSTRACT_BENCH__ !== "1")("local cold-load phase receipt", () => {
  for (const count of [150, 300, 1500]) {
    for (const storage of ["memory", "persistent"] as const) {
      it(`${storage}: ${count} synthetic rows`, async () => {
        const appId = crypto.randomUUID();
        const secret = generateAuthSecret();
        const dbName = `cold-load-receipt-${count}-${crypto.randomUUID()}`;
        const config = {
          appId,
          secret,
          driver:
            storage === "memory"
              ? { type: "memory" as const }
              : { type: "persistent" as const, dbName },
          logLevel: "warn" as const,
        };
        let db: Db | undefined;
        const phases: Record<string, number> = {};
        const responsiveness: Record<
          string,
          ReturnType<ReturnType<typeof watchPageResponsiveness>>
        > = {};
        const measure = async <T>(name: string, operation: () => Promise<T>): Promise<T> => {
          const start = performance.now();
          const stopWatching = watchPageResponsiveness();
          try {
            return await operation();
          } finally {
            phases[name] = performance.now() - start;
            responsiveness[name] = stopWatching();
            await commands.writeRealisticBrowserReport(`cold-load-phases-${storage}-${count}`, {
              storage,
              count,
              phases,
              responsiveness,
            });
            console.info(
              "[cold-load-phase]",
              JSON.stringify({ storage, count, name, ms: phases[name] }),
            );
          }
        };
        const firstSubscription = async () => {
          let stop = () => {};
          try {
            await new Promise<void>((resolve, reject) => {
              stop = db!.subscribe(
                app.tasks,
                {
                  onUpdate(rows) {
                    if (rows.length === count) resolve();
                  },
                  onError: reject,
                },
                { tier: "local" },
              );
            });
          } finally {
            stop();
          }
        };
        try {
          db = await measure("initial_open", () => createDb(config));
          await measure("initial_empty_all", () => db!.all(app.tasks, { tier: "local" }));
          await measure("seed_local_durable", async () => {
            const result = await db!.transaction((tx) => {
              for (let i = 0; i < count; i++)
                tx.insert(app.tasks, { title: `Task ${i}`, done: false });
            });
            await result.wait({ tier: "local" });
          });
          expect(
            await measure("first_all_after_seed", () => db!.all(app.tasks, { tier: "local" })),
          ).toHaveLength(count);
          await measure("initial_subscription_after_all", firstSubscription);
          if (storage === "persistent") {
            await measure("shutdown_before_reopen", () => db!.shutdown());
            db = await measure("reopen_runtime", () => createDb(config));
            expect(
              await measure("reopen_first_all", () => db!.all(app.tasks, { tier: "local" })),
            ).toHaveLength(count);
            await measure("reopen_subscription_after_all", firstSubscription);
            await db.shutdown();
            db = await measure("subscription_first_reopen_runtime", () => createDb(config));
            await measure("reopen_subscription_without_all", firstSubscription);
            await db.shutdown();
            const physical = (await indexedDB.databases()).filter(({ name }) =>
              name?.startsWith(`${dbName}::jazz-browser-v1::`),
            );
            expect(physical).toHaveLength(1);
            const seededPageStats = await pageStats(physical[0]!.name!);
            await commands.writeRealisticBrowserReport(
              `cold-load-pages-before-${count}`,
              seededPageStats,
            );
            console.info(
              "[cold-load-page-stats]",
              JSON.stringify({ count, after: "seed_reopens", ...seededPageStats }),
            );
            const records = await snapshot(physical[0]!.name!);
            await commands.writeRealisticBrowserReport(`cold-load-fixture-${count}`, {
              appId,
              secret,
              dbName,
              physicalDbName: physical[0]!.name,
              records: Object.fromEntries(
                Object.entries(records).filter(([name]) => name !== "pages"),
              ),
              pageChunks: Math.ceil(records.pages.length / 128),
            });
            for (let offset = 0; offset < records.pages.length; offset += 128) {
              await commands.writeRealisticBrowserReport(
                `cold-load-fixture-${count}-pages-${offset / 128}`,
                records.pages.slice(offset, offset + 128),
              );
            }
            db = await createDb(config);
          }
          if (count === 1500) {
            const rows = await db.all(app.tasks, { tier: "local" });
            let completed = 0;
            const stop = db.subscribe(
              app.tasks,
              (rows) => {
                completed = rows.filter((row) => row.done).length;
              },
              { tier: "local" },
            );
            try {
              await measure("ordinary_updates_with_active_subscription", async () => {
                for (const row of rows.slice(0, 1350)) {
                  await db!.update(app.tasks, row.id, { done: true }).wait({ tier: "local" });
                }
              });
              assertSelectedRows(rows, await db.all(app.tasks, { tier: "local" }));
              await measure("final_subscription_delivery", async () => {
                const deadline = performance.now() + 30_000;
                while (completed !== 1350 && performance.now() < deadline) {
                  await new Promise((resolve) => setTimeout(resolve, 10));
                }
                expect(completed).toBe(1350);
              });
            } finally {
              stop();
            }
            if (storage === "persistent") {
              const names = (await indexedDB.databases()).filter(({ name }) =>
                name?.startsWith(`${dbName}::jazz-browser-v1::`),
              );
              await db.shutdown();
              db = await measure("post_update_reopen_runtime", () => createDb(config));
              const reopened = await measure("post_update_reopen_all", () =>
                db!.all(app.tasks, { tier: "local" }),
              );
              assertSelectedRows(rows, reopened);
              const stats = await pageStats(names[0]!.name!);
              await commands.writeRealisticBrowserReport(`cold-load-pages-after-${count}`, stats);
              console.info(
                "[cold-load-page-stats]",
                JSON.stringify({ count, after: "ordinary_updates", ...stats }),
              );
            }
          }
          console.info("[cold-load-receipt]", JSON.stringify({ storage, count, dbName, phases }));
        } finally {
          await db?.shutdown();
        }
      }, 600_000);
    }
  }
});

type FixtureEntries = [IDBValidKey, unknown][];
type ColdLoadFixture = {
  appId: string;
  secret: string;
  dbName: string;
  physicalDbName: string;
  records: Record<string, FixtureEntries>;
  pageChunks: number;
};

// The fixture directory is produced by a separate, pre-upgrade source run.
// Raw installation is setup only: the current production WASM reader performs
// checksum/format admission and verifies the logical rows through public Db APIs.
it.skipIf(__JAZZ_ABSTRACT_BENCH__ !== "1" || !__JAZZ_COLD_LOAD_FIXTURE__)(
  "reopens and updates 1500 pre-upgrade synthetic rows through the production reader",
  async () => {
    const fixtureCommands = commands as unknown as {
      readColdLoadFixture(chunk?: number): Promise<ColdLoadFixture | FixtureEntries>;
    };
    const fixture = (await fixtureCommands.readColdLoadFixture()) as ColdLoadFixture;
    expect(Number.isSafeInteger(fixture.pageChunks) && fixture.pageChunks > 0).toBe(true);
    expect((await indexedDB.databases()).some(({ name }) => name === fixture.physicalDbName)).toBe(
      false,
    );
    const open = indexedDB.open(fixture.physicalDbName, INDEXEDDB_BTREE_DATABASE_VERSION);
    open.onupgradeneeded = () => {
      for (const name of ["metadata", "pages", "storage-manifest"])
        open.result.createObjectStore(name);
    };
    const physical = await idbRequest(open);
    try {
      for (const [name, entries] of Object.entries(fixture.records)) {
        await putFixtureEntries(physical, name, entries);
      }
      for (let chunk = 0; chunk < fixture.pageChunks; chunk++) {
        const entries = (await fixtureCommands.readColdLoadFixture(chunk)) as FixtureEntries;
        await putFixtureEntries(physical, "pages", entries);
      }
    } finally {
      physical.close();
    }

    const before = await pageStats(fixture.physicalDbName);
    const phases: Record<string, number> = {};
    const responsiveness: Record<
      string,
      ReturnType<ReturnType<typeof watchPageResponsiveness>>
    > = {};
    const measure = async <T>(name: string, operation: () => Promise<T>) => {
      const started = performance.now();
      const stopWatching = watchPageResponsiveness();
      try {
        return await operation();
      } finally {
        phases[name] = performance.now() - started;
        responsiveness[name] = stopWatching();
        await commands.writeRealisticBrowserReport("cold-load-upgrade-phases", {
          phases,
          before,
          responsiveness,
        });
      }
    };
    const config = {
      appId: fixture.appId,
      secret: fixture.secret,
      driver: { type: "persistent" as const, dbName: fixture.dbName },
      logLevel: "warn" as const,
    };
    let db: Db | undefined;
    let stop = () => {};
    try {
      db = await measure("reopen_runtime", () => createDb(config));
      const rows = await measure("reopen_first_all", () => db!.all(app.tasks, { tier: "local" }));
      expect(rows).toHaveLength(1500);
      expect(rows.map((row) => row.title).sort()).toEqual(
        Array.from({ length: 1500 }, (_, i) => `Task ${i}`).sort(),
      );
      expect(rows.every((row) => !row.done)).toBe(true);
      let completed = 0;
      await measure(
        "initial_subscription_after_all",
        () =>
          new Promise<void>((resolve, reject) => {
            stop = db!.subscribe(
              app.tasks,
              {
                onUpdate(snapshot) {
                  completed = snapshot.filter((row) => row.done).length;
                  if (snapshot.length === 1500) resolve();
                },
                onError: reject,
              },
              { tier: "local" },
            );
          }),
      );
      await measure("ordinary_updates_with_active_subscription", async () => {
        for (const row of rows.slice(0, 1350)) {
          await db!.update(app.tasks, row.id, { done: true }).wait({ tier: "local" });
        }
      });
      await measure("final_subscription_delivery", async () => {
        const deadline = performance.now() + 30_000;
        while (completed !== 1350 && performance.now() < deadline)
          await new Promise((resolve) => setTimeout(resolve, 10));
        expect(completed).toBe(1350);
      });
      stop();
      await db.shutdown();
      db = await measure("post_update_reopen_runtime", () => createDb(config));
      const reopened = await measure("post_update_reopen_all", () =>
        db!.all(app.tasks, { tier: "local" }),
      );
      assertSelectedRows(rows, reopened);
      await commands.writeRealisticBrowserReport("cold-load-upgrade-result", {
        phases,
        responsiveness,
        before,
        after: await pageStats(fixture.physicalDbName),
      });
    } finally {
      stop();
      await db?.shutdown();
    }
  },
  600_000,
);

function idbRequest<T>(request: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

function assertSelectedRows(
  original: { id: string; title: string; done: boolean }[],
  current: { id: string; title: string; done: boolean }[],
) {
  expect(current.map((row) => [row.id, row.title]).sort()).toEqual(
    original.map((row) => [row.id, row.title]).sort(),
  );
  expect(
    current
      .filter((row) => row.done === true)
      .map((row) => row.id)
      .sort(),
  ).toEqual(
    original
      .slice(0, 1350)
      .map((row) => row.id)
      .sort(),
  );
  expect(
    current
      .filter((row) => row.done === false)
      .map((row) => row.id)
      .sort(),
  ).toEqual(
    original
      .slice(1350)
      .map((row) => row.id)
      .sort(),
  );
}

// A page-event-loop observation, not a React paint or user-input latency claim.
// Measure the trailing gap too, so a phase that blocks every timer is visible.
function watchPageResponsiveness() {
  if (!__JAZZ_COLD_LOAD_RESPONSIVENESS__) return () => ({ enabled: false as const });
  let last = performance.now();
  let maxGapMs = 0;
  let ticks = 0;
  const timer = setInterval(() => {
    const now = performance.now();
    maxGapMs = Math.max(maxGapMs, now - last);
    last = now;
    ticks++;
  }, 16);
  return () => {
    clearInterval(timer);
    return {
      enabled: true as const,
      max_timer_gap_ms: Math.max(maxGapMs, performance.now() - last),
      timer_ticks: ticks,
    };
  };
}

async function putFixtureEntries(database: IDBDatabase, name: string, entries: FixtureEntries) {
  const tx = database.transaction(name, "readwrite");
  const completed = new Promise<void>((resolve, reject) => {
    tx.oncomplete = () => resolve();
    tx.onabort = () => reject(tx.error ?? new Error("Fixture import aborted"));
  });
  for (const [key, value] of entries) tx.objectStore(name).put(reviveFixtureValue(value), key);
  await completed;
}

function reviveFixtureValue(value: unknown): unknown {
  if (!value || typeof value !== "object") return value;
  if ("base64" in value && typeof value.base64 === "string") {
    return Uint8Array.from(atob(value.base64), (character) => character.charCodeAt(0)).buffer;
  }
  if (Array.isArray(value)) return value.map(reviveFixtureValue);
  return Object.fromEntries(
    Object.entries(value).map(([key, nested]) => [key, reviveFixtureValue(nested)]),
  );
}

async function snapshot(name: string) {
  const request = <T>(req: IDBRequest<T>) =>
    new Promise<T>((resolve, reject) => {
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  const database = await request(indexedDB.open(name));
  try {
    const stores = Array.from(database.objectStoreNames);
    const tx = database.transaction(stores, "readonly");
    const entries = await Promise.all(
      stores.map(async (name) => {
        const store = tx.objectStore(name);
        const [keys, values] = await Promise.all([
          request(store.getAllKeys()),
          request(store.getAll()),
        ]);
        return [name, keys.map((key, index) => [key, values[index]])];
      }),
    );
    return JSON.parse(
      JSON.stringify(Object.fromEntries(entries), (_key, value) =>
        value instanceof ArrayBuffer ? { base64: binaryBase64(new Uint8Array(value)) } : value,
      ),
    );
  } finally {
    database.close();
  }
}

function binaryBase64(bytes: Uint8Array): string {
  let binary = "";
  for (let i = 0; i < bytes.length; i += 8192) {
    binary += String.fromCharCode(...bytes.subarray(i, i + 8192));
  }
  return btoa(binary);
}

async function pageStats(name: string) {
  const database = await new Promise<IDBDatabase>((resolve, reject) => {
    const req = indexedDB.open(name);
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
  try {
    const counts = await Promise.all(
      Array.from(database.objectStoreNames).map(
        (name) =>
          new Promise<[string, number]>((resolve, reject) => {
            const req = database.transaction(name, "readonly").objectStore(name).count();
            req.onsuccess = () => resolve([name, req.result]);
            req.onerror = () => reject(req.error);
          }),
      ),
    );
    let pageBytes = 0;
    let lastKey: IDBValidKey | undefined;
    for (;;) {
      const tx = database.transaction("pages", "readonly");
      const store = tx.objectStore("pages");
      const range = lastKey === undefined ? undefined : IDBKeyRange.lowerBound(lastKey, true);
      const [keys, values] = await Promise.all([
        idbRequest(store.getAllKeys(range, 128)),
        idbRequest(store.getAll(range, 128)),
      ]);
      for (const value of values) {
        if (!(value instanceof ArrayBuffer) && !ArrayBuffer.isView(value))
          throw new Error("Page fixture contains a non-binary page");
        pageBytes += value.byteLength;
      }
      if (keys.length < 128) break;
      lastKey = keys[keys.length - 1];
    }
    return { ...Object.fromEntries(counts), page_bytes: pageBytes };
  } finally {
    database.close();
  }
}
