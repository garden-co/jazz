import { useCallback, useSyncExternalStore } from "react";
import { useJazzClient } from "./provider.js";
import type { SyncStatus } from "../runtime/sync-status.js";

/**
 * Whether this client's writes have reached the server: the upstream
 * connection and how many committed transactions still wait for upload.
 * Re-renders only when the status changes.
 */
export function useSyncStatus(): SyncStatus {
  const client = useJazzClient();
  return useSyncExternalStore(
    useCallback((onChange) => client.db.onSyncStatusChange(onChange), [client.db]),
    () => client.db.getSyncStatus(),
    () => client.db.getSyncStatus(),
  );
}
