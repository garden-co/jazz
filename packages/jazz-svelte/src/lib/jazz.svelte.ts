import { consumeInviteLinkFromWindowLocation } from 'jazz-browser';
import type {
  AccountClass,
  AuthSecretStorage,
  CoValueOrZodSchema,
  ID,
  InstanceOfSchema,
  JazzContextType
} from 'jazz-tools';
import { Account } from 'jazz-tools';
import { getContext, untrack } from 'svelte';
import Provider from './Provider.svelte';

export { Provider as JazzProvider };

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
    throw new Error('useJazzContext must be used within a JazzProvider');
  }

  if (!context.current) {
    throw new Error('Jazz context is not initialized');
  }

  return context as {
    current: JazzContextType<Acc>;
  };
}

export function getAuthSecretStorage() {
  const context = getContext<AuthSecretStorage>(JAZZ_AUTH_CTX);

  if (!context) {
    throw new Error('getAuthSecretStorage must be used within a JazzProvider');
  }

  return context;
}

/**
 * Use the accept invite hook.
 * @param invitedObjectSchema - The invited object schema.
 * @param onAccept - Function to call when the invite is accepted.
 * @param forValueHint - Hint for the value.
 * @returns The accept invite hook.
 */
export function useAcceptInvite<V extends CoValueOrZodSchema>({
  invitedObjectSchema,
  onAccept,
  forValueHint
}: {
  invitedObjectSchema: V;
  onAccept: (projectID: ID<V>) => void;
  forValueHint?: string;
}): void {
  const _onAccept = onAccept;

  // Subscribe to the onAccept function.
  $effect(() => {
    const ctx = getJazzContext<InstanceOfSchema<AccountClass<Account>>>();

    // eslint-disable-next-line @typescript-eslint/no-unused-expressions
    _onAccept;
    // Subscribe to the onAccept function.
    untrack(() => {
      // If there is no context, return.
      if (!ctx.current) return;
      if (!('me' in ctx.current)) return;

      // Consume the invite link from the window location.
      const result = consumeInviteLinkFromWindowLocation({
        as: ctx.current.me,
        invitedObjectSchema,
        forValueHint
      });
      // If the result is valid, call the onAccept function.
      result
        .then((result) => result && _onAccept(result?.valueID))
        .catch((e) => {
          console.error('Failed to accept invite', e);
        });
    });
  });
}
