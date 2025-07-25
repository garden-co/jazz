import React, {
  useCallback,
  useContext,
  useRef,
  useSyncExternalStore,
} from "react";

import {
  Account,
  AccountClass,
  AnonymousJazzAgent,
  AnyAccountSchema,
  CoValue,
  CoValueClassOrSchema,
  InboxSender,
  InstanceOfSchema,
  JazzContextManager,
  JazzContextType,
  Loaded,
  ResolveQuery,
  ResolveQueryStrict,
  SubscriptionScope,
  coValueClassFromCoValueClassOrSchema,
} from "jazz-tools";
import { JazzContext, JazzContextManagerContext } from "./provider.js";
import { getCurrentAccountFromContextManager } from "./utils.js";

export function useJazzContext<Acc extends Account>() {
  const value = useContext(JazzContext) as JazzContextType<Acc>;

  if (!value) {
    throw new Error(
      "You need to set up a JazzProvider on top of your app to use this hook.",
    );
  }

  return value;
}

export function useJazzContextManager<Acc extends Account>() {
  const value = useContext(JazzContextManagerContext) as JazzContextManager<
    Acc,
    {}
  >;

  if (!value) {
    throw new Error(
      "You need to set up a JazzProvider on top of your app to use this hook.",
    );
  }

  return value;
}

export function useAuthSecretStorage() {
  const value = useContext(JazzContextManagerContext);

  if (!value) {
    throw new Error(
      "You need to set up a JazzProvider on top of your app to use this useAuthSecretStorage.",
    );
  }

  return value.getAuthSecretStorage();
}

export function useIsAuthenticated() {
  const authSecretStorage = useAuthSecretStorage();

  return useSyncExternalStore(
    useCallback(
      (callback) => {
        return authSecretStorage.onUpdate(callback);
      },
      [authSecretStorage],
    ),
    () => authSecretStorage.isAuthenticated,
    () => authSecretStorage.isAuthenticated,
  );
}

function useCoValueSubscription<
  S extends CoValueClassOrSchema,
  const R extends ResolveQuery<S>,
>(
  Schema: S,
  id: string | undefined | null,
  options?: {
    resolve?: ResolveQueryStrict<S, R>;
  },
) {
  const contextManager = useJazzContextManager();

  const createSubscription = () => {
    if (!id) {
      return {
        subscription: null,
        contextManager,
        id,
        Schema,
      };
    }

    const node = contextManager.getCurrentValue()!.node;
    const subscription = new SubscriptionScope<any>(
      node,
      options?.resolve ?? true,
      id,
      {
        ref: coValueClassFromCoValueClassOrSchema(Schema),
        optional: true,
      },
    );

    return {
      subscription,
      contextManager,
      id,
      Schema,
    };
  };

  const [subscription, setSubscription] = React.useState(createSubscription);

  React.useLayoutEffect(() => {
    if (
      subscription.contextManager !== contextManager ||
      subscription.id !== id ||
      subscription.Schema !== Schema
    ) {
      subscription.subscription?.destroy();
      setSubscription(createSubscription());
    }

    return contextManager.subscribe(() => {
      subscription.subscription?.destroy();
      setSubscription(createSubscription());
    });
  }, [Schema, id, contextManager]);

  return subscription.subscription;
}

export function useCoState<
  S extends CoValueClassOrSchema,
  const R extends ResolveQuery<S> = true,
>(
  Schema: S,
  id: string | undefined,
  options?: {
    resolve?: ResolveQueryStrict<S, R>;
  },
): Loaded<S, R> | undefined | null {
  const subscription = useCoValueSubscription(Schema, id, options);

  const value = React.useSyncExternalStore<Loaded<S, R> | undefined | null>(
    React.useCallback(
      (callback) => {
        if (!subscription) {
          return () => {};
        }

        return subscription.subscribe(callback);
      },
      [subscription],
    ),
    () => (subscription ? subscription.getCurrentValue() : null),
    () => (subscription ? subscription.getCurrentValue() : null),
  );

  return value;
}

function useAccountSubscription<
  S extends AccountClass<Account> | AnyAccountSchema,
  const R extends ResolveQuery<S>,
>(
  Schema: S,
  options?: {
    resolve?: ResolveQueryStrict<S, R>;
  },
) {
  const contextManager = useJazzContextManager();

  const createSubscription = () => {
    const agent = getCurrentAccountFromContextManager(contextManager);

    if (agent._type === "Anonymous") {
      return {
        subscription: null,
        contextManager,
        agent,
      };
    }

    // We don't need type validation here, since it's mostly to help users on public API
    const resolve: any = options?.resolve ?? true;

    const node = contextManager.getCurrentValue()!.node;
    const subscription = new SubscriptionScope<any>(node, resolve, agent.id, {
      ref: coValueClassFromCoValueClassOrSchema(Schema),
      optional: true,
    });

    return {
      subscription,
      contextManager,
      Schema,
    };
  };

  const [subscription, setSubscription] = React.useState(createSubscription);

  React.useLayoutEffect(() => {
    if (
      subscription.contextManager !== contextManager ||
      subscription.Schema !== Schema
    ) {
      subscription.subscription?.destroy();
      setSubscription(createSubscription());
    }

    return contextManager.subscribe(() => {
      subscription.subscription?.destroy();
      setSubscription(createSubscription());
    });
  }, [Schema, contextManager]);

  return subscription.subscription;
}

export function useAccount<
  A extends AccountClass<Account> | AnyAccountSchema,
  R extends ResolveQuery<A> = true,
>(
  AccountSchema: A = Account as unknown as A,
  options?: {
    resolve?: ResolveQueryStrict<A, R>;
  },
): {
  me: Loaded<A, R> | undefined | null;
  agent: AnonymousJazzAgent | Loaded<A, true>;
  logOut: () => void;
} {
  const contextManager = useJazzContextManager<InstanceOfSchema<A>>();
  const subscription = useAccountSubscription(AccountSchema, options);

  const agent = getCurrentAccountFromContextManager(contextManager);

  const value = React.useSyncExternalStore<Loaded<A, R> | undefined | null>(
    React.useCallback(
      (callback) => {
        if (!subscription) {
          return () => {};
        }

        return subscription.subscribe(callback);
      },
      [subscription],
    ),
    () => (subscription ? subscription.getCurrentValue() : null),
    () => (subscription ? subscription.getCurrentValue() : null),
  );

  return {
    me: value,
    agent,
    logOut: contextManager.logOut,
  };
}

export function experimental_useInboxSender<
  I extends CoValue,
  O extends CoValue | undefined,
>(inboxOwnerID: string | undefined) {
  const context = useJazzContext();

  if (!("me" in context)) {
    throw new Error(
      "useInboxSender can't be used in a JazzProvider with auth === 'guest'.",
    );
  }

  const me = context.me;
  const inboxRef = useRef<Promise<InboxSender<I, O>> | undefined>(undefined);

  const sendMessage = useCallback(
    async (message: I) => {
      if (!inboxOwnerID) throw new Error("Inbox owner ID is required");

      if (!inboxRef.current) {
        const inbox = InboxSender.load<I, O>(inboxOwnerID, me);
        inboxRef.current = inbox;
      }

      let inbox = await inboxRef.current;

      if (inbox.owner.id !== inboxOwnerID) {
        const req = InboxSender.load<I, O>(inboxOwnerID, me);
        inboxRef.current = req;
        inbox = await req;
      }

      return inbox.sendMessage(message);
    },
    [inboxOwnerID],
  );

  return sendMessage;
}
