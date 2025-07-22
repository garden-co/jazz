import type {
  AccountClass,
  AuthSecretStorage,
  CoValueOrZodSchema,
  ID,
  InstanceOfSchema,
  JazzContextType,
} from "jazz-tools";
import { Account } from "jazz-tools";
import { consumeInviteLinkFromWindowLocation } from "jazz-tools/browser";
import {
  Accessor,
  createContext,
  createEffect,
  untrack,
  useContext,
} from "solid-js";
import { Provider } from "./Provider.js";

export { Provider as JazzProvider };

export type JazzContextValue<Acc extends Account = Account> = {
  readonly current?: Accessor<JazzContextType<Acc> | undefined>;
};

/**
 * The key for the Jazz context.
 */
export const JAZZ_CTX = {};
export const JAZZ_AUTH_CTX = {};

/**
 * The Jazz context.
 */
export const JazzContext = createContext<JazzContextValue>(JAZZ_CTX);
export const JazzAuthContext = createContext<Accessor<AuthSecretStorage>>();

/**
 * Get the current Jazz context.
 * @returns The current Jazz context.
 */
export function useJazzContext<Acc extends Account>() {
  const context = useContext(JazzContext) as JazzContextValue<Acc>;

  if (!context) {
    throw new Error("useJazzContext must be used within a JazzProvider");
  }

  if (!context.current || !context.current()) {
    throw new Error("Jazz context is not initialized");
  }

  return context.current as Accessor<JazzContextType<Acc>>;
}

export function useAuthSecretStorage() {
  const context = useContext(JazzAuthContext);

  if (!context) {
    throw new Error("useAuthSecretStorage must be used within a JazzProvider");
  }

  return context;
}

/**
 * Triggers the `onAccept` function when an invite link is detected in the URL.
 *
 * @param invitedObjectSchema - The invited object schema.
 * @param onAccept - Function to call when the invite is accepted.
 * @param forValueHint - Hint for the value.
 * @returns The accept invite hook.
 */
export class InviteListener<V extends CoValueOrZodSchema> {
  constructor({
    invitedObjectSchema,
    onAccept,
    forValueHint,
  }: {
    invitedObjectSchema: V;
    onAccept: (projectID: ID<V>) => void;
    forValueHint?: string;
  }) {
    const jazz = useJazzContext<InstanceOfSchema<AccountClass<Account>>>();

    // Subscribe to the onAccept function.
    createEffect(() => {
      // Subscribe to the onAccept function.
      untrack(() => {
        const _ctx = jazz();
        // If there is no context, return.
        if (!_ctx) return;
        if (!("me" in _ctx)) return;

        // Consume the invite link from the window location.
        const result = consumeInviteLinkFromWindowLocation({
          as: _ctx.me,
          invitedObjectSchema,
          forValueHint,
        });

        // If the result is valid, call the onAccept function.
        result
          .then((result) => result && onAccept(result?.valueID))
          .catch((e) => {
            console.error("Failed to accept invite", e);
          });
      });
    });
  }
}
