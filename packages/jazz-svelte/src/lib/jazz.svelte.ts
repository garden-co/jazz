import { consumeInviteLinkFromWindowLocation } from 'jazz-browser';
import type {
  AnonymousJazzAgent,
  AuthSecretStorage,
  CoValue,
  CoValueClass,
  ID,
  JazzAuthContext,
  JazzContextType,
  JazzGuestContext,
  RefsToResolve,
  Resolved
} from 'jazz-tools';
import { Account, subscribeToCoValue } from 'jazz-tools';
import { getContext, untrack } from 'svelte';
import Provider from './Provider.svelte';
import type { RefsToResolveStrict } from 'jazz-tools';

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

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export interface Register {}

export type RegisteredAccount = Register extends { Account: infer Acc } ? Acc : Account;

declare module "jazz-tools" {
  export interface Register {
    Account: RegisteredAccount;
  }
}

  export function useAccount<const R extends RefsToResolve<RegisteredAccount>>(
    options?: { resolve?: RefsToResolveStrict<RegisteredAccount, R> }
  ): { me: Resolved<RegisteredAccount, R> | undefined | null; logOut: () => void };
  export function useAccount<const R extends RefsToResolve<RegisteredAccount>>(
    options?: { resolve?: RefsToResolveStrict<RegisteredAccount, R> }
  ): { me: RegisteredAccount | Resolved<RegisteredAccount, R> | undefined | null; logOut: () => void } {
  const ctx = getJazzContext<RegisteredAccount>();
  if (!ctx?.current) {
    throw new Error('useAccount must be used within a JazzProvider');
  }
  if (!('me' in ctx.current)) {
    throw new Error(
      "useAccount can't be used in a JazzProvider with auth === 'guest' - consider using useAccountOrGuest()"
    );
  }

  // If no depth is specified, return the context's me directly
  if (options?.resolve === undefined) {
    return {
      get me() {
        return (ctx.current as JazzAuthContext<RegisteredAccount>).me;
      },
      logOut() {
        return ctx.current?.logOut();
      }
    };
  }

  // If depth is specified, use useCoState to get the deeply loaded version
  const me = useCoState<RegisteredAccount, R>(
    ctx.current.me.constructor as CoValueClass<RegisteredAccount>,
    (ctx.current as JazzAuthContext<RegisteredAccount>).me.id,
    options
  );

  return {
    get me() {
      return me.current;
    },
    logOut() {
      return ctx.current?.logOut();
    }
  };
}

export function useAccountOrGuest(): { me: RegisteredAccount | AnonymousJazzAgent };
export function useAccountOrGuest<R extends RefsToResolve<RegisteredAccount>>(
  options?: { resolve?: RefsToResolveStrict<RegisteredAccount, R> }
): { me: Resolved<RegisteredAccount, R> | undefined  | null| AnonymousJazzAgent };
export function useAccountOrGuest<R extends RefsToResolve<RegisteredAccount>>(
  options?: { resolve?: RefsToResolveStrict<RegisteredAccount, R> }
): { me: RegisteredAccount | Resolved<RegisteredAccount, R> | undefined  | null| AnonymousJazzAgent } {
  const ctx = getJazzContext<RegisteredAccount>();

  if (!ctx?.current) {
    throw new Error('useAccountOrGuest must be used within a JazzProvider');
  }

  const contextMe = 'me' in ctx.current ? ctx.current.me : undefined;

  const me = useCoState<RegisteredAccount, R>(
    contextMe?.constructor as CoValueClass<RegisteredAccount>,
    contextMe?.id,
    options
  );

  // If the context has a me, return the account.
  if ('me' in ctx.current) {
    return {
      get me() {
        return options?.resolve === undefined
          ? me.current || (ctx.current as JazzAuthContext<RegisteredAccount>)?.me
          : me.current;
      }
    };
  }
  // If the context has no me, return the guest.
  else {
    return {
      get me() {
        return (ctx.current as JazzGuestContext)?.guest;
      }
    };
  }
}

export function useCoState<V extends CoValue, R extends RefsToResolve<V>>(
  Schema: CoValueClass<V>,
  id: ID<CoValue> | undefined,
  options?: { resolve?: RefsToResolveStrict<V, R> }
): {
  current: Resolved<V, R> | undefined | null;
} {
  const ctx = getJazzContext<RegisteredAccount>();

  // Create state and a stable observable
  let state = $state.raw<Resolved<V, R> | undefined | null>(undefined);

  // Effect to handle subscription
  $effect(() => {
    // Reset state when dependencies change
    state = undefined;

    // Return early if no context or id, effectively cleaning up any previous subscription
    if (!ctx?.current || !id) return;

    const agent = "me" in ctx.current ? ctx.current.me : ctx.current.guest;

    // Setup subscription with current values
    return subscribeToCoValue<V, R>(
      Schema,
      id,
      {
        resolve: options?.resolve,
        loadAs: agent,
        onUnavailable: () => {
          state = null;
        },
        onUnauthorized: () => {
          state = null;
        },
        syncResolution: true,
      },
      (value) => {
        // Get current value from our stable observable
        state = value;
      },
    );
  });

  return {
    get current() {
      return state;
    }
  };
}

/**
 * Use the accept invite hook.
 * @param invitedObjectSchema - The invited object schema.
 * @param onAccept - Function to call when the invite is accepted.
 * @param forValueHint - Hint for the value.
 * @returns The accept invite hook.
 */
export function useAcceptInvite<V extends CoValue>({
  invitedObjectSchema,
  onAccept,
  forValueHint
}: {
  invitedObjectSchema: CoValueClass<V>;
  onAccept: (projectID: ID<V>) => void;
  forValueHint?: string;
}): void {
  const ctx = getJazzContext<RegisteredAccount>();
  const _onAccept = onAccept;

  if (!ctx.current) {
    throw new Error('useAcceptInvite must be used within a JazzProvider');
  }

  if (!('me' in ctx.current)) {
    throw new Error("useAcceptInvite can't be used in a JazzProvider with auth === 'guest'.");
  }

  // Subscribe to the onAccept function.
  $effect(() => {
    // eslint-disable-next-line @typescript-eslint/no-unused-expressions
    _onAccept;
    // Subscribe to the onAccept function.
    untrack(() => {
      // If there is no context, return.
      if (!ctx.current) return;
      // Consume the invite link from the window location.
      const result = consumeInviteLinkFromWindowLocation({
        as: (ctx.current as JazzAuthContext<RegisteredAccount>).me,
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
