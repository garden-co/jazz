import { createEffect, createSignal, onCleanup, type Accessor } from "solid-js";
import type { SyncStatus } from "../runtime/sync-status.js";
import { useDb } from "./provider.js";

/**
 * Whether this client's writes have reached the server: the upstream
 * connection and how many committed transactions still wait for upload.
 */
export function useSyncStatus(): Accessor<SyncStatus> {
  const db = useDb();
  const [status, setStatus] = createSignal(db().getSyncStatus());
  createEffect(() => {
    const current = db();
    setStatus(current.getSyncStatus());
    onCleanup(current.onSyncStatusChange((next) => setStatus(next)));
  });
  return status;
}
