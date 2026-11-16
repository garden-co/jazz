import { createContext, type Accessor } from "solid-js";
import type { JazzSession, JazzSessionSnapshot } from "../session/jazz-session.js";
import type { JazzClient } from "../web/create-jazz-client.js";

/** @internal The enclosing `<JazzProvider>`'s session. */
export type ProvidedSession = {
  session: JazzSession<JazzClient>;
  snapshot: Accessor<JazzSessionSnapshot<JazzClient>>;
};

/**
 * @internal A `<JazzClientProvider client>` resets it for its children: they
 * read that client, which is not the session's.
 */
export const SessionContext = createContext<ProvidedSession>();
