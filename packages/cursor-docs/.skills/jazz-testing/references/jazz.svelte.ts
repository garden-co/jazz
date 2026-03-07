import type {
  AccountClass,
  AuthSecretStorage,
  CoValueClassOrSchema,
  ID,
  InstanceOfSchema,
  JazzContextType,
} from "jazz-tools";
import { Account } from "jazz-tools";
import { consumeInviteLinkFromWindowLocation } from "jazz-tools/browser";
import { getContext, onDestroy, untrack } from "svelte";
import Provider from "./Provider.svelte";

export { Provider as JazzSvelteProvider };

/**
 * The key for the Jazz context.
 */
export const JAZZ_CTX = {};
export const JAZZ_AUTH_CTX = {};

/**
 * The Jazz context.
 */
export type JazzContext<Acc extends Account> = {
  current?: JazzContextType<Acc>;
};

/**
 * Get the current Jazz context.
 * @returns The current Jazz context.
 */
export function getJazzContext<Acc extends Account>() {
  const context = getContext<JazzContext<Acc>>(JAZZ_CTX);

  if (!context) {
    throw new Error("useJazzContext must be used within a JazzSvelteProvider");
  }

  if (!context.current) {
    throw new Error("Jazz context is not initialized");
  }

  return context as {
    current: JazzContextType<Acc>;
  };
}

export function getAuthSecretStorage() {
  const context = getContext<AuthSecretStorage>(JAZZ_AUTH_CTX);

  if (!context) {
    throw new Error(
      "getAuthSecretStorage must be used within a JazzSvelteProvider",
    );
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
export class InviteListener<V extends CoValueClassOrSchema> {
  constructor({
    invitedObjectSchema,
    onAccept,
    forValueHint,
  }: {
    invitedObjectSchema: V;
    onAccept: (coValueID: ID<V>) => void;
    forValueHint?: string;
  }) {
    const _onAccept = onAccept;
    const ctx = getJazzContext<InstanceOfSchema<AccountClass<Account>>>();

    const tryConsume = () => {
      if (!ctx.current || !("me" in ctx.current)) return;

      consumeInviteLinkFromWindowLocation({
        as: ctx.current.me,
        invitedObjectSchema,
        forValueHint,
      })
        .then((result) => result && _onAccept(result.valueID))
        .catch((e) => console.error("Failed to accept invite", e));
    };

    // run once when instantiated
    $effect(() => {
      untrack(tryConsume);
    });

    window.addEventListener("hashchange", tryConsume);

    onDestroy(() => {
      window.removeEventListener("hashchange", tryConsume);
    });
  }
}
