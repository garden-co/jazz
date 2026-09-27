import { describe, expect, it, vi } from "vitest";
import { schema as s } from "../../schema-namespace.js";
import { testAuthorBytes } from "../testing/account-fixtures.js";
import { loadWasmModuleForTest } from "../testing/wasm-runtime-test-utils.js";
import { openConfig, queryFromTable } from "./native-codec.js";
import { encodeSchema } from "./schema-codec.js";

const app = s.defineApp({ notes: s.table({ text: s.string() }, {}) });
// Raw-binding tests are required here: the public runtime otherwise supplies
// scheduling itself, hiding whether the WASM future actually emits a wake.
async function open() {
  const { WasmDb } = await loadWasmModuleForTest();
  return WasmDb.openMemory(
    encodeSchema(app.wasmSchema),
    openConfig(new Uint8Array(16).fill(45), testAuthorBytes("wasm-read-deadline"), 1, true),
  );
}

describe("WASM pending read deadlines", () => {
  it("wakes at the coverage deadline without intermediate polling", async () => {
    const db = await open();
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    let pending;
    try {
      pending = db.all(queryFromTable("notes"), { tier: "global" });
      expect(pending.setWake).toBeTypeOf("function");
      const wake = vi.fn();
      pending.setWake(wake);
      expect(pending.poll()).toBeNull();
      await vi.advanceTimersByTimeAsync(14_999);
      expect(wake).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(1);
      expect(wake).toHaveBeenCalledTimes(1);
      expect(() => pending.poll()).toThrow("Timed out waiting for query coverage");
    } finally {
      pending?.cancel();
      vi.useRealTimers();
      await db.close();
    }
  });

  it("removes the deadline and stored callback when cancelled asleep", async () => {
    const db = await open();
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    let pending;
    try {
      pending = db.all(queryFromTable("notes"), { tier: "global" });
      const wake = vi.fn();
      pending.setWake(wake);
      expect(pending.poll()).toBeNull();
      expect(vi.getTimerCount()).toBe(1);
      pending.cancel();
      expect(vi.getTimerCount()).toBe(0);
      await vi.advanceTimersByTimeAsync(30_000);
      expect(wake).not.toHaveBeenCalled();
    } finally {
      pending?.cancel();
      vi.useRealTimers();
      await db.close();
    }
  });
});
