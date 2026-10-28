import { createRoot } from "solid-js";
import { describe, expect, it, vi } from "vitest";
import type { SyncStatus } from "../runtime/sync-status.js";

const listeners = new Set<(status: SyncStatus) => void>();
let current: SyncStatus = { link: { state: "connecting" }, pendingUploads: 1, synced: false };
const db = {
  getSyncStatus: () => current,
  onSyncStatusChange(listener: (status: SyncStatus) => void) {
    listeners.add(listener);
    return () => listeners.delete(listener);
  },
};

vi.mock("./provider.js", () => ({ useDb: () => () => db }));

const { useSyncStatus } = await import("./use-sync-status.js");

describe("useSyncStatus", () => {
  it("follows the Db's status until its owner is disposed", async () => {
    const { status, dispose } = createRoot((dispose) => ({ status: useSyncStatus(), dispose }));
    expect(status()).toBe(current);
    // Effects run after the root's synchronous setup.
    await Promise.resolve();
    expect(listeners.size).toBe(1);

    const connected: SyncStatus = { link: { state: "connected" }, pendingUploads: 0, synced: true };
    current = connected;
    for (const listener of listeners) listener(connected);
    expect(status()).toBe(connected);

    dispose();
    expect(listeners.size).toBe(0);
  });
});
