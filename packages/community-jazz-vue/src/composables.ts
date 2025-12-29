import {
  Account,
  AccountClass,
  AnonymousJazzAgent,
  AnyAccountSchema,
  type AuthSecretStorage,
  type CoValue,
  CoValueClassOrSchema,
  InboxSender,
  InstanceOfSchema,
  type JazzAuthContext,
  JazzContextManager,
  type JazzContextType,
  type Loaded,
  type MaybeLoaded,
  CoValueLoadingState,
  type ResolveQuery,
  ResolveQueryStrict,
  coValueClassFromCoValueClassOrSchema,
  subscribeToCoValue,
  createUnloadedCoValue,
} from "jazz-tools";
import { consumeInviteLinkFromWindowLocation } from "jazz-tools/browser";
import {
  type ComputedRef,
  type MaybeRefOrGetter,
  type Ref,
  type ShallowRef,
  computed,
  inject,
  markRaw,
  onMounted,
  onUnmounted,
  ref,
  shallowRef,
  toRaw,
  toValue,
  watch,
} from "vue";

import {
  JazzAuthContextSymbol,
  JazzContextManagerSymbol,
  JazzContextSymbol,
} from "./provider.js";

export const logoutHandler = ref<() => void>();

function getCurrentAccountFromContextManager<Acc extends Account>(
  contextManager: JazzContextManager<Acc, any>,
) {
  const context = contextManager.getCurrentValue();

  if (!context) {
    throw new Error("No context found");
  }

  return "me" in context ? context.me : context.guest;
}

export function useJazzContext<Acc extends Account>(): Ref<
  JazzContextType<Acc>,
  JazzContextType<Acc>
> {
  const context = inject<Ref<JazzContextType<Acc>>>(JazzContextSymbol);
  if (!context) {
    throw new Error("useJazzContext must be used within a JazzProvider");
  }
  return context;
}

export function useJazzContextManager<Acc extends Account>() {
  const context = inject<Ref<JazzContextManager<Acc, {}>>>(
    JazzContextManagerSymbol,
  );

  if (!context?.value) {
    throw new Error(
      "You need to set up a JazzProvider on top of your app to use this hook.",
    );
  }

  return context;
}

export function useAuthSecretStorage() {
  const context = inject<AuthSecretStorage>(JazzAuthContextSymbol);
  if (!context) {
    throw new Error("useAuthSecretStorage must be used within a JazzProvider");
  }
  return context;
}

export function useAccount<
  A extends AccountClass<Account> | AnyAccountSchema = typeof Account,
  R extends ResolveQuery<A> = true,
>(
  AccountSchema: A = Account as unknown as A,
  options?: {
    resolve?: ResolveQueryStrict<A, R>;
  },
): ComputedRef<MaybeLoaded<Loaded<A, R>>> {
  const context = useJazzContext();

  if (!context.value) {
    throw new Error("useAccount must be used within a JazzProvider");
  }

  // Handle guest mode - return null for the account data
  if (!("me" in context.value)) {
    return computed(() => null) as any;
  }

  const contextMe = context.value.me as InstanceOfSchema<A>;

  const me = useCoState(
    AccountSchema as any,
    toRaw(contextMe).$jazz.id,
    options as any,
  );

  return computed(() => {
    const value =
      options?.resolve === undefined ? me.value || contextMe : me.value;
    return value ? markRaw(value) : value;
  }) as any;
}

/**
 * Returns a function for logging out the current account.
 */
export function useLogOut(): () => void {
  const context = useJazzContext();
  if (!context.value) {
    throw new Error("useLogOut must be used within a JazzProvider");
  }
  return context.value.logOut;
}

/**
 * Hook for accessing the current agent. An agent can either be:
 * - an Authenticated Account, if the user is logged in
 * - an Anonymous Account, if the user didn't log in
 * - or an anonymous agent, if in guest mode
 *
 * The agent can be used as the `loadAs` parameter for load and subscribe methods.
 */
export function useAgent<
  A extends AccountClass<Account> | AnyAccountSchema = typeof Account,
>(): AnonymousJazzAgent | Loaded<A, true> {
  const contextManager = useJazzContextManager<InstanceOfSchema<A>>();
  const agent = getCurrentAccountFromContextManager(contextManager.value);
  return agent as AnonymousJazzAgent | Loaded<A, true>;
}

export function useCoState<
  S extends CoValueClassOrSchema,
  const R extends ResolveQuery<S> = true,
>(
  Schema: S,
  id: MaybeRefOrGetter<string | undefined>,
  options?: { resolve?: ResolveQueryStrict<S, R> },
): Ref<MaybeLoaded<Loaded<S, R>>> {
  const state: ShallowRef<MaybeLoaded<Loaded<S, R>>> = shallowRef(
    createUnloadedCoValue(toValue(id) ?? "", CoValueLoadingState.LOADING),
  );
  const context = useJazzContext();

  if (!context.value) {
    throw new Error("useCoState must be used within a JazzProvider");
  }

  let unsubscribe: (() => void) | undefined;

  const updateState = (value: MaybeLoaded<Loaded<S, R>>) => {
    if (shouldSkipUpdate(value, state.value)) return;
    // Use markRaw to prevent Vue from making Jazz objects reactive
    // but still allow property access and mutations
    state.value = markRaw(value);
  };

  watch(
    [() => toValue(id), context],
    ([currentId, currentContext]) => {
      if (unsubscribe) {
        unsubscribe();
        unsubscribe = undefined;
      }

      if (!currentId || !currentContext) {
        updateState(
          createUnloadedCoValue(
            currentId ?? "",
            CoValueLoadingState.UNAVAILABLE,
          ),
        );
        return;
      }

      const loadAsAgent =
        "me" in currentContext ? currentContext.me : currentContext.guest;
      if (!loadAsAgent) {
        updateState(
          createUnloadedCoValue(currentId, CoValueLoadingState.UNAVAILABLE),
        );
        return;
      }

      const safeLoadAsAgent = toRaw(loadAsAgent);

      try {
        unsubscribe = subscribeToCoValue(
          coValueClassFromCoValueClassOrSchema(Schema),
          currentId as any,
          {
            resolve: options?.resolve as any,
            loadAs: safeLoadAsAgent,
            onError: (value) => {
              updateState(value);
            },
            syncResolution: true,
          },
          (value: any) => {
            updateState(value);
          },
        );
      } catch (error) {
        console.error("Error in useCoState subscription:", error);
        updateState(
          createUnloadedCoValue(currentId, CoValueLoadingState.UNAVAILABLE),
        );
      }
    },
    { immediate: true },
  );

  onUnmounted(() => {
    if (unsubscribe) {
      unsubscribe();
      unsubscribe = undefined;
    }
  });

  return state;
}

export function useAcceptInvite<S extends CoValueClassOrSchema>({
  invitedObjectSchema,
  onAccept,
  forValueHint,
}: {
  invitedObjectSchema: S;
  onAccept: (projectID: string) => void;
  forValueHint?: string;
}): void {
  const context = useJazzContext();

  if (!context.value) {
    throw new Error("useAcceptInvite must be used within a JazzProvider");
  }

  if (!("me" in context.value)) {
    throw new Error(
      "useAcceptInvite can't be used in a JazzProvider with auth === 'guest'.",
    );
  }

  const handleInvite = () => {
    const result = consumeInviteLinkFromWindowLocation({
      as: toRaw((context.value as JazzAuthContext<Account>).me),
      invitedObjectSchema,
      forValueHint,
    });

    result
      .then((res) => res && onAccept(res.valueID))
      .catch((e) => {
        console.error("Failed to accept invite", e);
      });
  };

  onMounted(() => {
    handleInvite();
    window.addEventListener("hashchange", handleInvite);
  });

  onUnmounted(() => {
    window.removeEventListener("hashchange", handleInvite);
  });

  watch(
    () => onAccept,
    (newOnAccept, oldOnAccept) => {
      if (newOnAccept !== oldOnAccept) {
        handleInvite();
      }
    },
  );
}

export function experimental_useInboxSender<
  I extends CoValue,
  O extends CoValue | undefined,
>(inboxOwnerID: MaybeRefOrGetter<string | undefined>) {
  const context = useJazzContext();

  if (!context.value) {
    throw new Error(
      "experimental_useInboxSender must be used within a JazzProvider",
    );
  }

  if (!("me" in context.value)) {
    throw new Error(
      "experimental_useInboxSender can't be used in a JazzProvider with auth === 'guest'.",
    );
  }

  const me = computed(() => (context.value as JazzAuthContext<Account>).me);
  const inboxRef = ref<Promise<InboxSender<I, O>> | undefined>(undefined);

  const sendMessage = async (message: I) => {
    const resolvedInboxOwnerID = toValue(inboxOwnerID);
    if (!resolvedInboxOwnerID) throw new Error("Inbox owner ID is required");

    if (!inboxRef.value) {
      const inbox = InboxSender.load<I, O>(
        resolvedInboxOwnerID,
        toRaw(me.value),
      );
      inboxRef.value = inbox;
    }

    let inbox = await inboxRef.value;

    if (inbox.owner.id !== resolvedInboxOwnerID) {
      const req = InboxSender.load<I, O>(resolvedInboxOwnerID, toRaw(me.value));
      inboxRef.value = req;
      inbox = await req;
    }

    return inbox.sendMessage(message);
  };

  watch(
    () => toValue(inboxOwnerID),
    () => {
      inboxRef.value = undefined;
    },
  );

  return sendMessage;
}

// useAccountOrGuest has been removed in v0.15.4 to match React library.
// It has been merged into useAccount which now handles both authenticated and guest scenarios.
// This change maintains 1:1 API compatibility with the React Jazz library.
// If you were using useAccountOrGuest, please migrate to useAccount.

function shouldSkipUpdate(
  newValue: MaybeLoaded<CoValue>,
  previousValue: MaybeLoaded<CoValue>,
) {
  if (previousValue === newValue) return true;
  // Avoid re-renders if the value is not loaded and didn't change
  return (
    previousValue.$jazz.id === newValue.$jazz.id &&
    !previousValue.$isLoaded &&
    !newValue.$isLoaded &&
    previousValue.$jazz.loadingState === newValue.$jazz.loadingState
  );
}
