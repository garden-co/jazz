import { createSignal, onCleanup } from "solid-js";
import type { AccountManager, AccountSnapshot } from "../accounts/state.js";

/** Observe account selection inside a Solid owner. */
export function createAccountState<Auth>(manager: AccountManager<Auth>): () => AccountSnapshot {
  const [state, setState] = createSignal(manager.getSnapshot());
  onCleanup(manager.subscribe(() => setState(manager.getSnapshot())));
  return state;
}
