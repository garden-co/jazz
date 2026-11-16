import { getContext, setContext } from "svelte";
import { readable, toStore, type Readable } from "svelte/store";
import { getOptionalJazzContext } from "./context.svelte.js";
import {
  clientOnlySessionActions,
  clientOnlySnapshot,
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

/**
 * @internal A `<JazzSvelteClientProvider client>` hides an enclosing
 * `<JazzProvider>`'s session from its children: they read that client,
 * which is not the session's.
 */
export function hideJazzSession(): void {
  setContext(SESSION_CONTEXT, undefined);
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
  if (session) return session;
  // A client opened outside a session (`<JazzSvelteClientProvider client>`):
  // its user, always ready, with operations that reject naming why.
  const ctx = getOptionalJazzContext();
  if (!ctx)
    throw new Error(
      "getJazzSession must be used within <JazzProvider> or <JazzSvelteClientProvider>",
    );
  const state = toStore<JazzSessionSnapshot<JazzClient>>(() =>
    ctx.client ? clientOnlySnapshot(ctx.client as JazzClient, ctx.session) : STARTING_SNAPSHOT,
  );
  return { ...clientOnlySessionActions<JazzClient>(), subscribe: state.subscribe };
}

const STARTING_SNAPSHOT: JazzSessionSnapshot<never> = Object.freeze({
  status: "starting",
  user: null,
  localFirst: false,
});
