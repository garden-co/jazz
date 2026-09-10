import { describe, expect, it, vi } from "vitest";
import { schema } from "../../src/index.js";
import { loadWasmModule } from "../../src/runtime/wasm-loader.js";
import { createBrowserTestDb } from "./account-fixtures.js";

const app = schema.defineApp({
  todos: schema.table({ title: schema.string(), done: schema.boolean() }),
});

describe("exact local transaction write merging", () => {
  for (const count of [150, 300, 1500]) {
    it(`stages 90% of ${count} rows without table reads`, async () => {
      const db = await createBrowserTestDb({
        appId: `transaction-staging-${count}`,
        driver: { type: "memory" },
      });
      try {
        await db.all(app.todos);
        const seedStart = performance.now();
        const seed = await db.transaction((tx) => {
          for (let index = 0; index < count; index++) {
            tx.insert(app.todos, { title: `Item ${index}`, done: false });
          }
        });
        await seed.wait({ tier: "local" });
        const seedMs = performance.now() - seedStart;
        const readStart = performance.now();
        const rows = await db.all(app.todos);
        const readMs = performance.now() - readStart;
        const updates = rows.slice(0, Math.floor(count * 0.9));
        const { WasmDb } = await loadWasmModule();
        const all = vi.spyOn(WasmDb.prototype, "all");
        const exact = vi.spyOn(WasmDb.prototype, "localCurrentRow");
        const tx = db.beginTransaction();
        const stageStart = performance.now();
        try {
          for (const row of updates) tx.update(app.todos, row.id, { done: true });
          // A second patch must merge with this transaction's first patch.
          tx.update(app.todos, updates[0].id, { title: "Second patch" });
          expect(all).not.toHaveBeenCalled();
          expect(exact).toHaveBeenCalledTimes(updates.length);
        } finally {
          all.mockRestore();
          exact.mockRestore();
        }
        const stageMs = performance.now() - stageStart;
        const commitStart = performance.now();
        await tx.commit().wait({ tier: "local" });
        const commitMs = performance.now() - commitStart;
        const committed = await db.all(app.todos);
        expect(committed.filter((row) => row.done)).toHaveLength(updates.length);
        expect(committed.find((row) => row.id === updates[0].id)).toEqual({
          ...updates[0],
          title: "Second patch",
          done: true,
        });
        console.info(
          "transaction-staging-receipt",
          JSON.stringify({
            count,
            updated: updates.length,
            seedMs,
            readMs,
            stageMs,
            commitMs,
          }),
        );
      } finally {
        await db.shutdown();
      }
    }, 120_000);
  }
});
