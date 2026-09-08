import { getContext, setContext } from "svelte";
const key = Symbol("provider-auth-actions");
export interface AuthActions {
  signOut(): Promise<void>;
}
export function setAuthActions(actions: AuthActions) {
  setContext(key, actions);
}
export function getAuthActions(): AuthActions {
  const actions = getContext<AuthActions>(key);
  if (!actions) throw new Error("Auth provider is not ready");
  return actions;
}
