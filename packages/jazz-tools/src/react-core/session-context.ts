import { createContext } from "react";

/**
 * @internal The enclosing `<JazzProvider>`'s session state. A
 * `<JazzClientProvider client>` resets it to null for its children: they read
 * that client, which is not the session's.
 */
export const SessionContext = createContext<unknown>(null);
