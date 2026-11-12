import { getContext, setContext } from "svelte";
import { readable, type Readable } from "svelte/store";
import {
  jazzSessionActions,
  type JazzSession,
  type JazzSessionSnapshot,
} from "../session/jazz-session.js";
import type { JazzClient } from "./create-jazz-client.js";

const SESSION_CONTEXT = Symbol("jazz-session");

/**
 * A session as a Svelte store (`$session.status`, `$session.user`), with its
 * operations (`session.createDeviceAccount()`, `session.refresh()`,
 * `session.signOut()`, ...) bound to it.
 */
export type JazzSessionState<Client = JazzClient> = Readable<JazzSessionSnapshot<Client>> &
  Omit<JazzSession<Client>, "subscribe" | "getSnapshot" | "db">;

/** A session you own (from `createJazzClient`), as a Svelte store. */
export function sessionState<Client>(session: JazzSession<Client>): JazzSessionState<Client> {
  const state = readable(session.getSnapshot(), (set) => {
    const unsubscribe = session.subscribe(() => set(session.getSnapshot()));
    set(session.getSnapshot());
    return unsubscribe;
  });
  return { ...jazzSessionActions(session), subscribe: state.subscribe };
}

/** @internal */
export function setJazzSession(session: JazzSession<JazzClient>): void {
  setContext(SESSION_CONTEXT, sessionState(session));
}

/**
 * The session of the enclosing `<JazzProvider>`, in every one of its views:
 * read `$session.status` and `$session.user`, and call
 * `session.createDeviceAccount()`, `session.refresh()` or `session.signOut()`.
 * Operations reject on failure; the failure also shows in `$session.error`.
 */
export function getJazzSession(): JazzSessionState<JazzClient> {
  const session = getContext<JazzSessionState<JazzClient> | undefined>(SESSION_CONTEXT);
  if (!session) throw new Error("getJazzSession must be used within <JazzProvider>");
  return session;
}
