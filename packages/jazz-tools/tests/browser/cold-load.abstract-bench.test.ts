import { describe, expect, it } from "vitest";
import { commands } from "vitest/browser";
import { schema as s, generateAuthSecret } from "../../src/index.js";
import { createBrowserTestDb as createDb } from "./account-fixtures.js";
import type { Db } from "../../src/runtime/db.js";

declare const __JAZZ_ABSTRACT_BENCH__: string;

const app = s.defineApp({ tasks: s.table({ title: s.string(), done: s.boolean() }) });

// Opt-in diagnostic receipt, not a wall-clock correctness gate. Reopen is a
// fresh runtime in the same browser session (the WASM module cache stays warm).
// Build with `pnpm --filter jazz-wasm build:profiling`, then admit its verified
// manifest with `node dev/artifacts/stage-native-fingerprints.mjs --profiling`.
// Run this file with JAZZ_ABSTRACT_BENCH=1 and vitest.config.browser.ts.
// Phase reports and synthetic pre-update physical fixtures are written to
// .vitest-browser-bench. The 1500-row cases include ordinary updates while a
// subscription remains active; they are distinct from transaction staging.
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
        const measure = async <T>(name: string, operation: () => Promise<T>): Promise<T> => {
          const start = performance.now();
          try {
            return await operation();
          } finally {
            phases[name] = performance.now() - start;
            await commands.writeRealisticBrowserReport(`cold-load-phases-${storage}-${count}`, {
              storage,
              count,
              phases,
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
            console.info(
              "[cold-load-page-stats]",
              JSON.stringify({
                count,
                after: "seed_reopens",
                ...(await pageStats(physical[0]!.name!)),
              }),
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
              expect(
                (await db.all(app.tasks, { tier: "local" })).filter((row) => row.done),
              ).toHaveLength(1350);
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
    return Object.fromEntries(counts);
  } finally {
    database.close();
  }
}
