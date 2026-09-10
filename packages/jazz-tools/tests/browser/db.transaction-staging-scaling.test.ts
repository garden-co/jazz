import { describe, expect, it, vi } from "vitest";
import { commands } from "vitest/browser";
import { schema } from "../../src/index.js";
import { loadWasmModule } from "../../src/runtime/wasm-loader.js";
import { createBrowserTestDb } from "./account-fixtures.js";

const app = schema.defineApp({
  todos: schema.table({ title: schema.string(), done: schema.boolean() }),
});

describe("exact local transaction write merging", () => {
  it("does not commit a rejected large-row patch after its error is caught", async () => {
    const db = await createBrowserTestDb({
      appId: "transaction-staging-large-rejection",
      driver: { type: "memory" },
    });
    try {
      const title = "large text ".repeat(20_000);
      const large = await db.insertStreaming(app.todos, {
        title: (async function* () {
          yield title;
        })(),
        done: false,
      });
      await large.wait({ tier: "local" });
      const small = db.insert(app.todos, { title: "small", done: false });
      await small.wait({ tier: "local" });
      const tx = db.beginTransaction();
      expect(() => tx.update(app.todos, large.value.id, { done: true })).toThrow(
        "synchronous WASM all/transaction reads cannot materialize a large value",
      );
      tx.update(app.todos, small.value.id, { done: true });
      await tx.commit().wait({ tier: "local" });
      expect(await db.all(app.todos)).toEqual(
        expect.arrayContaining([
          { id: large.value.id, title, done: false },
          { id: small.value.id, title: "small", done: true },
        ]),
      );
    } finally {
      await db.shutdown();
    }
  }, 30_000);

  it("does not cache a patch rejected by native staging", async () => {
    const db = await createBrowserTestDb({
      appId: "transaction-staging-native-rejection",
      driver: { type: "memory" },
    });
    try {
      const inserted = db.insert(app.todos, { title: "original", done: false });
      await inserted.wait({ tier: "local" });
      const tx = db.beginTransaction();
      const { WasmDb } = await loadWasmModule();
      const exact = vi.spyOn(WasmDb.prototype, "localCurrentRow");
      const nativeUpdate = vi.spyOn(WasmDb.prototype, "updateInTransaction");
      nativeUpdate.mockImplementationOnce(() => {
        throw new Error("synthetic staging failure");
      });
      try {
        expect(() => tx.update(app.todos, inserted.value.id, { title: "rejected patch" })).toThrow(
          "synthetic staging failure",
        );
      } finally {
        nativeUpdate.mockRestore();
      }
      try {
        tx.update(app.todos, inserted.value.id, { done: true });
        expect(exact).toHaveBeenCalledTimes(2);
      } finally {
        exact.mockRestore();
      }
      await tx.commit().wait({ tier: "local" });
      expect(await db.all(app.todos)).toEqual([
        { id: inserted.value.id, title: "original", done: true },
      ]);
    } finally {
      await db.shutdown();
    }
  });

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
        await commands.writeRealisticBrowserReport(`transaction-staging-${count}`, {
          count,
          updated: updates.length,
          seedMs,
          readMs,
          stageMs,
          commitMs,
        });
      } finally {
        await db.shutdown();
      }
    }, 120_000);
  }
});
