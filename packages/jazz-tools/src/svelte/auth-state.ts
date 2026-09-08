import { getContext, setContext } from "svelte";
import type { Readable } from "svelte/store";
import type { JazzAppSnapshot } from "../session/app.js";
import type { JazzClient } from "./create-jazz-client.js";

export type JazzAuthState = Readable<JazzAppSnapshot<JazzClient>> & {
  logout(): Promise<void>;
  retry(): Promise<void>;
};
const AUTH_CONTEXT = Symbol("jazz-auth");
/** @internal */
export function setJazzAuth(auth: JazzAuthState): void {
  setContext(AUTH_CONTEXT, auth);
}
/** Read `$auth.status` / `$auth.account`, and call `auth.logout()` or `auth.retry()`. */
export function useJazzAuth(): JazzAuthState {
  const auth = getContext<JazzAuthState | undefined>(AUTH_CONTEXT);
  if (!auth) throw new Error("useJazzAuth must be used within JazzProvider");
  return auth;
}
