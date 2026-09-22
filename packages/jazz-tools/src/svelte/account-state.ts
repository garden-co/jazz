import { readable, type Readable } from "svelte/store";
import type { AccountManager, AccountSnapshot } from "../accounts/state.js";

/** A lazy store: only mounted subscribers observe the shared account manager. */
export function accountState<Auth>(manager: AccountManager<Auth>): Readable<AccountSnapshot> {
  return readable(manager.getSnapshot(), (set) => {
    const unsubscribe = manager.subscribe(() => set(manager.getSnapshot()));
    set(manager.getSnapshot());
    return unsubscribe;
  });
}
