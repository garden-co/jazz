import React, {
  useCallback,
  useContext,
  useRef,
  useSyncExternalStore,
} from "react";

import {
  Account,
  AnonymousJazzAgent,
  CoValue,
  CoValueClass,
  ID,
  InboxSender,
  JazzContextManager,
  JazzContextType,
  RefsToResolve,
  RefsToResolveStrict,
  Resolved,
  createCoValueObservable,
} from "jazz-tools";
import {
  JazzContext,
  JazzContextManagerContext,
  RegisteredAccount,
} from "./provider.js";
import { getCurrentAccountFromContextManager } from "./utils.js";
import { subscribeToContextManager } from "./utils.js";

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

function useCoValueObservable<
  V extends CoValue,
  const R extends RefsToResolve<V>,
>() {
  const [initialValue] = React.useState(() => createCoValueObservable<V, R>());
  const ref = useRef(initialValue);

  return {
    getCurrentValue() {
      return ref.current.getCurrentValue();
    },
    getCurrentObservable() {
      return ref.current;
    },
    reset() {
      ref.current = createCoValueObservable<V, R>();
    },
  };
}

export function useCoState<
  V extends CoValue,
  const R extends RefsToResolve<V> = true,
>(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  Schema: CoValueClass<V>,
  id: ID<CoValue> | undefined,
  options?: { resolve?: RefsToResolveStrict<V, R> },
): Resolved<V, R> | undefined | null {
  const contextManager = useJazzContextManager();

  const observable = useCoValueObservable<V, R>();

  const value = React.useSyncExternalStore<Resolved<V, R> | undefined | null>(
    React.useCallback(
      (callback) => {
        observable.reset();

        if (!id) return () => {};

        // We subscribe to the context manager to react to the account updates
        // faster than the useSyncExternalStore callback update to keep the isAuthenticated state
        // up to date with the data when logging in and out.
        return subscribeToContextManager(contextManager, () => {
          const agent = getCurrentAccountFromContextManager(contextManager);
          observable.reset();

          return observable.getCurrentObservable().subscribe(
            Schema,
            id,
            {
              loadAs: agent,
              resolve: options?.resolve,
              onUnauthorized: callback,
              onUnavailable: callback,
              syncResolution: true,
            },
            callback,
          );
        });
      },
      [Schema, id, contextManager],
    ),
    () => observable.getCurrentValue(),
    () => observable.getCurrentValue(),
  );

  return value;
}

function useAccount<A extends RegisteredAccount>(): {
  me: A;
  logOut: () => void;
};
function useAccount<
  A extends RegisteredAccount,
  R extends RefsToResolve<A>,
>(options?: {
  resolve?: RefsToResolveStrict<A, R>;
}): { me: Resolved<A, R> | undefined | null; logOut: () => void };
function useAccount<
  A extends RegisteredAccount,
  R extends RefsToResolve<A>,
>(options?: {
  resolve?: RefsToResolveStrict<A, R>;
}): { me: A | Resolved<A, R> | undefined | null; logOut: () => void } {
  const context = useJazzContext<A>();
  const contextManager = useJazzContextManager<A>();

  if (!("me" in context)) {
    throw new Error(
      "useAccount can't be used in a JazzProvider with auth === 'guest' - consider using useAccountOrGuest()",
    );
  }

  const observable = useCoValueObservable<A, R>();

  const me = React.useSyncExternalStore<Resolved<A, R> | undefined | null>(
    React.useCallback(
      (callback) => {
        return subscribeToContextManager(contextManager, () => {
          const agent = getCurrentAccountFromContextManager(contextManager);

          if (agent._type === "Anonymous") {
            throw new Error(
              "useAccount can't be used in a JazzProvider with auth === 'guest' - consider using useAccountOrGuest()",
            );
          }

          observable.reset();

          const Schema = agent.constructor as CoValueClass<A>;

          return observable.getCurrentObservable().subscribe(
            Schema,
            (agent as A).id,
            {
              loadAs: agent,
              resolve: options?.resolve,
              onUnauthorized: callback,
              onUnavailable: callback,
              syncResolution: true,
            },
            callback,
          );
        });
      },
      [contextManager],
    ),
    () => observable.getCurrentValue(),
    () => observable.getCurrentValue(),
  );

  return {
    me: options?.resolve === undefined ? me || context.me : me,
    logOut: contextManager.logOut,
  };
}

function useAccountOrGuest<A extends RegisteredAccount>(): {
  me: A | AnonymousJazzAgent;
};
function useAccountOrGuest<
  A extends RegisteredAccount,
  R extends RefsToResolve<A>,
>(options?: { resolve?: RefsToResolveStrict<A, R> }): {
  me: Resolved<A, R> | undefined | null | AnonymousJazzAgent;
};
function useAccountOrGuest<
  A extends RegisteredAccount,
  R extends RefsToResolve<A>,
>(options?: { resolve?: RefsToResolveStrict<A, R> }): {
  me: A | Resolved<A, R> | undefined | null | AnonymousJazzAgent;
} {
  const context = useJazzContext<A>();
  const contextManager = useJazzContextManager<A>();

  const observable = useCoValueObservable<A, R>();

  const me = React.useSyncExternalStore<Resolved<A, R> | undefined | null>(
    React.useCallback(
      (callback) => {
        return subscribeToContextManager(contextManager, () => {
          const agent = getCurrentAccountFromContextManager(contextManager);

          if (agent._type === "Anonymous") {
            return () => {};
          }

          observable.reset();

          const Schema = agent.constructor as CoValueClass<A>;

          return observable.getCurrentObservable().subscribe(
            Schema,
            (agent as A).id,
            {
              loadAs: agent,
              resolve: options?.resolve,
              onUnauthorized: callback,
              onUnavailable: callback,
              syncResolution: true,
            },
            callback,
          );
        });
      },
      [contextManager],
    ),
    () => observable.getCurrentValue(),
    () => observable.getCurrentValue(),
  );

  if ("me" in context) {
    return {
      me: options?.resolve === undefined ? me || context.me : me,
    };
  } else {
    return { me: context.guest };
  }
}

export { useAccount, useAccountOrGuest };

export function experimental_useInboxSender<
  I extends CoValue,
  O extends CoValue | undefined,
>(inboxOwnerID: ID<RegisteredAccount> | undefined) {
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

      // @ts-expect-error inbox.owner.id is typed as RawAccount id
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
