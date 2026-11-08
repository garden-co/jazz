import { effectScope } from "vue";
import { describe, expect, it, vi } from "vitest";
import type { SyncStatus } from "../runtime/sync-status.js";

const listeners = new Set<(status: SyncStatus) => void>();
let current: SyncStatus = { link: { state: "connecting" }, pendingUploads: 1, synced: false };

vi.mock("./provider.js", () => ({
  useDb: () => ({
    getSyncStatus: () => current,
    onSyncStatusChange(listener: (status: SyncStatus) => void) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
  }),
}));

const { useSyncStatus } = await import("./use-sync-status.js");

describe("useSyncStatus", () => {
  it("follows the Db's status until its scope is disposed", () => {
    const scope = effectScope();
    const status = scope.run(() => useSyncStatus())!;
    expect(status.value).toEqual(current);

    const connected: SyncStatus = { link: { state: "connected" }, pendingUploads: 0, synced: true };
    current = connected;
    for (const listener of listeners) listener(connected);
    expect(status.value).toEqual(connected);

    scope.stop();
    expect(listeners.size).toBe(0);
  });
});
