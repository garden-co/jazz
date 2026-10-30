import { onScopeDispose, readonly, shallowRef, type ShallowRef } from "vue";
import type { SyncStatus } from "../runtime/sync-status.js";
import { useDb } from "./provider.js";

/**
 * Whether this client's writes have reached the server, as a reactive ref:
 * the upstream connection and how many committed transactions still wait
 * for upload. The subscription ends with the current scope.
 */
export function useSyncStatus(): Readonly<ShallowRef<SyncStatus>> {
  const db = useDb();
  const status = shallowRef(db.getSyncStatus());
  onScopeDispose(
    db.onSyncStatusChange((next) => {
      status.value = next;
    }),
  );
  return readonly(status);
}
