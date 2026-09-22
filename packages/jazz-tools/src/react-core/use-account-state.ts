import { useSyncExternalStore } from "react";
import type { AccountManager, AccountSnapshot } from "../accounts/state.js";

/** Observe the shared manager; actions remain on the manager itself. */
export function useAccountState<Auth>(manager: AccountManager<Auth>): AccountSnapshot {
  return useSyncExternalStore(manager.subscribe, manager.getSnapshot, manager.getSnapshot);
}
