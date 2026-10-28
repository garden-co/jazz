import { readable, type Readable } from "svelte/store";
import type { Db } from "../runtime/db.js";
import type { SyncStatus } from "../runtime/sync-status.js";
import { getDb } from "./context.svelte.js";

/**
 * Whether this client's writes have reached the server, as a store (read it
 * as `$status`): the upstream connection and how many committed transactions
 * still wait for upload. Call it during component initialisation, or pass a
 * {@link Db}. Only mounted subscribers observe the database.
 */
export function syncStatus(db: Db = getDb()): Readable<SyncStatus> {
  return readable(db.getSyncStatus(), (set) => {
    const unsubscribe = db.onSyncStatusChange(set);
    set(db.getSyncStatus());
    return unsubscribe;
  });
}
