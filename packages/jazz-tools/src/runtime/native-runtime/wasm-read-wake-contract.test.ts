import { describe, expect, it, vi } from "vitest";
import { schema as s } from "../../schema-namespace.js";
import { testAuthorBytes } from "../testing/account-fixtures.js";
import { loadWasmModuleForTest } from "../testing/wasm-runtime-test-utils.js";
import { openConfig, queryFromTable } from "./native-codec.js";
import { encodeSchema } from "./schema-codec.js";

type PendingRead = {
  setWake(callback: () => void): void;
  poll(): Uint8Array | null;
  cancel(): void;
};

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
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    let pending: PendingRead | undefined;
    try {
      const read: PendingRead = db.all(queryFromTable("notes"), { tier: "global" });
      pending = read;
      expect(read.setWake).toBeTypeOf("function");
      const wake = vi.fn();
      read.setWake(wake);
      expect(read.poll()).toBeNull();
      await vi.advanceTimersByTimeAsync(14_999);
      expect(wake).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(1);
      expect(wake).toHaveBeenCalledTimes(1);
      expect(() => read.poll()).toThrow("Timed out waiting for query coverage");
    } finally {
      pending?.cancel();
      vi.useRealTimers();
      await db.close();
    }
  });

  it("removes the deadline and stored callback when cancelled asleep", async () => {
    const db = await open();
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    let pending: PendingRead | undefined;
    try {
      const read: PendingRead = db.all(queryFromTable("notes"), { tier: "global" });
      pending = read;
      const wake = vi.fn();
      read.setWake(wake);
      expect(read.poll()).toBeNull();
      expect(vi.getTimerCount()).toBe(1);
      read.cancel();
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
