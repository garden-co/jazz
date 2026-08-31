import { describe, expect, it } from "vitest";
import {
  acquireBrowserPhysicalDatabaseEpoch,
  BrowserPhysicalDatabaseBusyError,
} from "./browser-physical-database-epoch.js";

function testLocks() {
  const held = new Set<string>();
  return {
    async request<T>(
      name: string,
      _options: { mode: "exclusive"; ifAvailable: true },
      callback: (lock: object | null) => Promise<T> | T,
    ): Promise<T> {
      if (held.has(name)) return await callback(null);
      held.add(name);
      try {
        return await callback({});
      } finally {
        held.delete(name);
      }
    },
  };
}

describe("browser physical database epoch", () => {
  it("does not let a second worker realm recover a live physical root", async () => {
    const locks = testLocks();
    const first = await acquireBrowserPhysicalDatabaseEpoch("same-root", locks);

    // Planted positive: without an origin-wide lifetime lock, two asset-scoped
    // SharedWorkers both consider themselves fresh and each retires the
    // other's live foreground-node lease pool.
    await expect(acquireBrowserPhysicalDatabaseEpoch("same-root", locks)).rejects.toBeInstanceOf(
      BrowserPhysicalDatabaseBusyError,
    );

    first.release();
    // The Web Locks callback relinquishes after its release continuation.
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    const successor = await acquireBrowserPhysicalDatabaseEpoch("same-root", locks);
    expect(successor.id).not.toBe(first.id);
    successor.release();
  });
});
