import { getContext, setContext } from "svelte";
const key = Symbol("auth-actions");
export function setAuthActions(actions: { signOut(): Promise<void> }) {
  setContext(key, actions);
}
export function getAuthActions(): { signOut(): Promise<void> } {
  const actions = getContext<{ signOut(): Promise<void> }>(key);
  if (!actions) throw new Error("Auth provider is not ready");
  return actions;
}
