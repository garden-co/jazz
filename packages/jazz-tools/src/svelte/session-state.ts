import { getContext, setContext } from "svelte";
import { readable, type Readable } from "svelte/store";
import type { JazzSession, JazzSessionSnapshot } from "../session/state.js";
import type { JazzClient } from "./create-jazz-client.js";

const SESSION_CONTEXT = Symbol("jazz-session");
export type JazzSessionState<Client> = Readable<JazzSessionSnapshot<Client>> &
  Omit<JazzSession<Client>, "subscribe">;

/** Observe the shared session using a Svelte store; commands remain bound to its owner. */
export function sessionState<Client>(session: JazzSession<Client>): JazzSessionState<Client> {
  const state = readable(session.getSnapshot(), (set) => {
    const unsubscribe = session.subscribe(() => set(session.getSnapshot()));
    set(session.getSnapshot());
    return unsubscribe;
  });
  return { ...session, subscribe: state.subscribe } as JazzSessionState<Client>;
}

/** @internal */
export function setJazzSession(session: JazzSession<JazzClient>): void {
  setContext(SESSION_CONTEXT, sessionState(session));
}

/** Read `$session.status` / `$session.account` and call `session.linkJWT(...)`. */
export function getJazzSession(): JazzSessionState<JazzClient> {
  const session = getContext<JazzSessionState<JazzClient> | undefined>(SESSION_CONTEXT);
  if (!session) throw new Error("getJazzSession must be used within JazzSessionProvider");
  return session;
}
