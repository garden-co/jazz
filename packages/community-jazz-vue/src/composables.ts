import {
  Account,
  AccountClass,
  AnonymousJazzAgent,
  AnyAccountSchema,
  type AuthSecretStorage,
  type CoValue,
  CoValueOrZodSchema,
  InboxSender,
  InstanceOfSchema,
  type JazzAuthContext,
  JazzContextManager,
  type JazzContextType,
  type Loaded,
  type RefsToResolve,
  ResolveQuery,
  ResolveQueryStrict,
  anySchemaToCoSchema,
  subscribeToCoValue,
} from "jazz-tools";
import { consumeInviteLinkFromWindowLocation } from "jazz-tools/browser";
import {
  type ComputedRef,
  type Ref,
  type ShallowRef,
  computed,
  inject,
  markRaw,
  nextTick,
  onMounted,
  onUnmounted,
  ref,
  shallowRef,
  toRaw,
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
): {
  me: ComputedRef<Loaded<A, R> | undefined | null>;
  agent: AnonymousJazzAgent | Loaded<A, true>;
  logOut: () => void;
} {
  const context = useJazzContext();
  const contextManager = useJazzContextManager<InstanceOfSchema<A>>();

  if (!context.value) {
    throw new Error("useAccount must be used within a JazzProvider");
  }

  const agent = getCurrentAccountFromContextManager(contextManager.value);

  // Handle guest mode - return null for me and the guest agent
  if (!("me" in context.value)) {
    return {
      me: computed(() => null) as any,
      agent: agent,
      logOut: context.value.logOut,
    };
  }

  const contextMe = context.value.me as InstanceOfSchema<A>;

  const me = useCoState(AccountSchema as any, contextMe.id, options as any);

  return {
    me: computed(() => {
      const value =
        options?.resolve === undefined ? me.value || contextMe : me.value;
      return value ? markRaw(value) : value;
    }) as any,
    agent: agent,
    logOut: context.value.logOut,
  };
}

export function useCoState<
  S extends CoValueOrZodSchema,
  const R extends RefsToResolve<S> = true,
>(
  Schema: S,
  id: string | undefined,
  options?: { resolve?: ResolveQueryStrict<S, R> },
): Ref<Loaded<S, R> | undefined | null> {
  const state: ShallowRef<Loaded<S, R> | undefined | null> =
    shallowRef(undefined);
  const context = useJazzContext();

  if (!context.value) {
    throw new Error("useCoState must be used within a JazzProvider");
  }

  let unsubscribe: (() => void) | undefined;

  watch(
    () => id,
    () => {
      if (unsubscribe) {
        unsubscribe();
        unsubscribe = undefined;
      }

      if (!id || !context.value) {
        state.value = undefined;
        return;
      }

      const loadAsAgent =
        "me" in context.value ? context.value.me : context.value.guest;
      if (!loadAsAgent) {
        state.value = undefined;
        return;
      }

      const safeLoadAsAgent = toRaw(loadAsAgent);

      try {
        unsubscribe = subscribeToCoValue(
          anySchemaToCoSchema(Schema),
          id as any,
          {
            resolve: options?.resolve as any,
            loadAs: safeLoadAsAgent,
            onUnavailable: () => {
              state.value = null;
            },
            onUnauthorized: () => {
              state.value = null;
            },
            syncResolution: true,
          },
          (value: any) => {
            // Use markRaw to prevent Vue from making Jazz objects reactive
            // but still allow property access and mutations
            state.value = value ? markRaw(value) : value;
          },
        );
      } catch (error) {
        console.error("Error in useCoState subscription:", error);
        state.value = null;
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

export function useAcceptInvite<S extends CoValueOrZodSchema>({
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
>(inboxOwnerID: string | undefined) {
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
    if (!inboxOwnerID) throw new Error("Inbox owner ID is required");

    if (!inboxRef.value) {
      const inbox = InboxSender.load<I, O>(inboxOwnerID, toRaw(me.value));
      inboxRef.value = inbox;
    }

    let inbox = await inboxRef.value;

    if (inbox.owner.id !== inboxOwnerID) {
      const req = InboxSender.load<I, O>(inboxOwnerID, toRaw(me.value));
      inboxRef.value = req;
      inbox = await req;
    }

    return inbox.sendMessage(message);
  };

  watch(
    () => inboxOwnerID,
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
