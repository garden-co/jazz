import { describe, expect, it } from "vitest";
import { probeInSharedWorker } from "./fixtures/leader-support.js";
// Direct import drives the red: before capability.ts exists, this module fails
// to resolve and the whole test file fails to load. (The probe helper swallows
// a missing module into `false`, so it cannot be relied on for the red.)
import { detectSyncOpfsInWorkerScope } from "../../src/runtime/shared-worker-leader/capability.js";

async function readOpfsEntryNames(): Promise<string[] | null> {
  try {
    const root = await navigator.storage.getDirectory();
    const entries: string[] = [];
    // @ts-expect-error -- FileSystemDirectoryHandle is async-iterable in browsers.
    for await (const [name] of root) entries.push(name);
    return entries;
  } catch {
    return null;
  }
}

describe("shared-worker-leader capability probe", () => {
  it("exports detectSyncOpfsInWorkerScope", () => {
    expect(typeof detectSyncOpfsInWorkerScope).toBe("function");
  });

  it("returns a boolean from inside a SharedWorker", async () => {
    const result = await probeInSharedWorker();
    expect(typeof result).toBe("boolean");
  });

  it("does not leave a residual OPFS file behind when OPFS is inspectable", async () => {
    const supported = await probeInSharedWorker();
    const entries = await readOpfsEntryNames();

    if (!entries) {
      expect(supported).toBe(false);
      return;
    }

    expect(entries.some((n) => n.startsWith("__jazz_leader_probe_"))).toBe(false);
  });
});
