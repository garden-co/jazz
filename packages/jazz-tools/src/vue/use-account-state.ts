import { onScopeDispose, readonly, shallowRef, type ShallowRef } from "vue";
import type { AccountManager, AccountSnapshot } from "../accounts/state.js";

/** Reactive selection with automatic subscription cleanup in the current scope. */
export function useAccountState<Auth>(
  manager: AccountManager<Auth>,
): Readonly<ShallowRef<AccountSnapshot>> {
  const state = shallowRef(manager.getSnapshot());
  onScopeDispose(
    manager.subscribe(() => {
      state.value = manager.getSnapshot();
    }),
  );
  return readonly(state);
}
