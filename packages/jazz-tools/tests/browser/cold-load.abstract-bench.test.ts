import { describe, expect, it } from "vitest";
import { schema as s } from "../../src/index.js";
import { createBrowserTestDb as createDb, acquireBrowserTestAccount } from "./account-fixtures.js";
import type { Db } from "../../src/runtime/db.js";

declare const __JAZZ_ABSTRACT_BENCH__: string;

const app = s.defineApp({ tasks: s.table({ title: s.string(), done: s.boolean() }) });

// Opt-in diagnostic receipt, not a wall-clock correctness gate. Reopen is a
// fresh runtime in the same browser session (the WASM module cache stays warm).
describe.skipIf(__JAZZ_ABSTRACT_BENCH__ !== "1")("local cold-load phase receipt", () => {
  for (const count of [150, 300, 1500]) {
    for (const storage of ["memory", "persistent"] as const) {
      it(`${storage}: ${count} synthetic rows`, async () => {
        const appId = crypto.randomUUID();
        const account = await acquireBrowserTestAccount({ appId });
        const dbName = `cold-load-receipt-${count}-${crypto.randomUUID()}`;
        const config = {
          appId,
          account,
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
          }
          console.info("[cold-load-receipt]", JSON.stringify({ storage, count, dbName, phases }));
        } finally {
          await db?.shutdown();
        }
      }, 300_000);
    }
  }
});
