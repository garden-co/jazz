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
