import { consumeInviteLinkFromWindowLocation } from "jazz-browser";
import {
  Account,
  AnonymousJazzAgent,
  AuthSecretStorage,
  CoValue,
  CoValueClass,
  ID,
  JazzAuthContext,
  JazzContextType,
  JazzGuestContext,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  subscribeToCoValue,
} from "jazz-tools";
/* eslint-disable @typescript-eslint/no-explicit-any */
import {
  ComputedRef,
  MaybeRef,
  Ref,
  ShallowRef,
  computed,
  inject,
  onMounted,
  onUnmounted,
  ref,
  shallowRef,
  toRaw,
  unref,
  watch,
} from "vue";
import {
  JazzAuthContextSymbol,
  JazzContextSymbol,
  RegisteredAccount,
} from "./provider.js";

export const logoutHandler = ref<() => void>();

export function useJazzContext() {
  const context =
    inject<Ref<JazzContextType<RegisteredAccount>>>(JazzContextSymbol);
  if (!context?.value) {
    throw new Error("useJazzContext must be used within a JazzProvider");
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

export function createUseAccountComposables<Acc extends Account>() {
  function useAccount(): {
    me: ComputedRef<Acc>;
    logOut: () => void;
  };
  function useAccount<const R extends RefsToResolve<Acc>>(options?: {
    resolve?: RefsToResolveStrict<Acc, R>;
  }): {
    me: ComputedRef<Resolved<Acc, R> | undefined | null>;
    logOut: () => void;
  };
  function useAccount<const R extends RefsToResolve<Acc>>(options?: {
    resolve?: RefsToResolveStrict<Acc, R>;
  }): {
    me: ComputedRef<Acc | Resolved<Acc, R> | undefined | null>;
    logOut: () => void;
  } {
    const context = useJazzContext();

    if (!context.value) {
      throw new Error("useAccount must be used within a JazzProvider");
    }

    if (!("me" in context.value)) {
      throw new Error(
        "useAccount can't be used in a JazzProvider with auth === 'guest' - consider using useAccountOrGuest()",
      );
    }

    const contextMe = context.value.me as Acc;

    const me = useCoState<Acc, R>(
      contextMe.constructor as CoValueClass<Acc>,
      contextMe.id,
      options,
    );

    return {
      me: computed(() => {
        const value =
          options?.resolve === undefined
            ? me.value || toRaw((context.value as JazzAuthContext<Acc>).me)
            : me.value;

        return value ? toRaw(value) : value;
      }),
      logOut: context.value.logOut,
    };
  }

  function useAccountOrGuest(): {
    me: ComputedRef<Acc | AnonymousJazzAgent>;
  };
  function useAccountOrGuest<const R extends RefsToResolve<Acc>>(options?: {
    resolve?: RefsToResolveStrict<Acc, R>;
  }): {
    me: ComputedRef<Resolved<Acc, R> | undefined | null | AnonymousJazzAgent>;
  };
  function useAccountOrGuest<const R extends RefsToResolve<Acc>>(options?: {
    resolve?: RefsToResolveStrict<Acc, R>;
  }): {
    me: ComputedRef<
      Acc | Resolved<Acc, R> | undefined | null | AnonymousJazzAgent
    >;
  } {
    const context = useJazzContext();

    if (!context.value) {
      throw new Error("useAccountOrGuest must be used within a JazzProvider");
    }

    const contextMe = computed(() =>
      "me" in context.value ? (context.value.me as Acc) : undefined,
    );

    const me = useCoState<Acc, R>(
      contextMe.value?.constructor as CoValueClass<Acc>,
      contextMe.value?.id,
      options,
    );

    if ("me" in context.value) {
      return {
        me: computed(() =>
          options?.resolve === undefined
            ? me.value || toRaw((context.value as JazzAuthContext<Acc>).me)
            : me.value,
        ),
      };
    } else {
      return {
        me: computed(() => toRaw((context.value as JazzGuestContext).guest)),
      };
    }
  }

  return {
    useAccount,
    useAccountOrGuest,
  };
}

const { useAccount, useAccountOrGuest } =
  createUseAccountComposables<RegisteredAccount>();

export { useAccount, useAccountOrGuest };

export function useCoState<V extends CoValue, const R extends RefsToResolve<V>>(
  Schema: CoValueClass<V>,
  id: MaybeRef<ID<CoValue> | undefined>,
  options?: { resolve?: RefsToResolveStrict<V, R> },
): Ref<Resolved<V, R> | undefined | null> {
  const state: ShallowRef<Resolved<V, R> | undefined | null> =
    shallowRef(undefined);
  const context = useJazzContext();

  if (!context.value) {
    throw new Error("useCoState must be used within a JazzProvider");
  }

  let unsubscribe: (() => void) | undefined;

  watch(
    [() => unref(id), () => context, () => Schema, () => options],
    () => {
      if (unsubscribe) unsubscribe();

      const idValue = unref(id);
      if (!idValue) return;

      unsubscribe = subscribeToCoValue(
        Schema,
        idValue,
        {
          resolve: options?.resolve,
          loadAs:
            "me" in context.value
              ? toRaw(context.value.me)
              : toRaw(context.value.guest),
          onUnavailable: () => {
            state.value = null;
          },
          onUnauthorized: () => {
            state.value = null;
          },
          syncResolution: true,
        },
        (value) => {
          state.value = value;
        },
      );
    },
    { deep: true, immediate: true },
  );

  onUnmounted(() => {
    if (unsubscribe) unsubscribe();
  });

  const computedState = computed(() => state.value);

  return computedState;
}

export function useAcceptInvite<V extends CoValue>({
  invitedObjectSchema,
  onAccept,
  forValueHint,
}: {
  invitedObjectSchema: CoValueClass<V>;
  onAccept: (projectID: ID<V>) => void;
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

  const runInviteAcceptance = () => {
    const result = consumeInviteLinkFromWindowLocation({
      as: toRaw((context.value as JazzAuthContext<RegisteredAccount>).me),
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
    runInviteAcceptance();
  });

  watch(
    () => onAccept,
    (newOnAccept, oldOnAccept) => {
      if (newOnAccept !== oldOnAccept) {
        runInviteAcceptance();
      }
    },
  );
}
