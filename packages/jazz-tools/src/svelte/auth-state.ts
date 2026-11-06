import { getContext, setContext } from "svelte";
import type { Readable } from "svelte/store";
import type { JazzAppSnapshot } from "../session/app.js";
import type { JazzSessionActions } from "../session/state.js";
import type { JazzClient } from "./create-jazz-client.js";

export type JazzAuthState = Readable<JazzAppSnapshot<JazzClient>> & {
  readonly sessionActions: JazzSessionActions;
  /**
   * Your auth changed: call this after your own sign-in, sign-out or account
   * switch. Jazz asks your backend who is signed in now and follows its
   * answer. Failures are represented in the snapshot.
   */
  reauthenticate(): Promise<void>;
  logout(): Promise<void>;
  retry(): Promise<void>;
};
const AUTH_CONTEXT = Symbol("jazz-auth");
/** @internal */
export function setJazzAuth(auth: JazzAuthState): void {
  setContext(AUTH_CONTEXT, auth);
}
/**
 * Read `$auth.status`, `$auth.user` and `$auth.account`, and call
 * `auth.reauthenticate()` after your own sign-in or sign-out.
 */
export function useJazzAuth(): JazzAuthState {
  const auth = getContext<JazzAuthState | undefined>(AUTH_CONTEXT);
  if (!auth) throw new Error("useJazzAuth must be used within JazzProvider");
  return auth;
}
