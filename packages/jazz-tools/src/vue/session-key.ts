import type { InjectionKey, ShallowRef } from "vue";
import type { JazzSession, JazzSessionSnapshot } from "../session/jazz-session.js";
import type { JazzClient } from "../web/create-jazz-client.js";

/** @internal The enclosing `<JazzProvider>`'s session. */
export type ProvidedSession = {
  session: JazzSession<JazzClient>;
  snapshot: Readonly<ShallowRef<JazzSessionSnapshot<JazzClient>>>;
};

/**
 * @internal A `<JazzClientProvider client>` resets it for its children: they
 * read that client, which is not the session's. `<JazzProvider>` renders its
 * own client with `inSession`, which keeps it.
 */
export const SessionKey: InjectionKey<ProvidedSession | undefined> = Symbol("JazzSession");
