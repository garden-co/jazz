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
  DeeplyLoaded,
  DepthsIn,
  ID,
  InboxSender,
  JazzContextManager,
  JazzContextType,
  createCoValueObservable,
} from "jazz-tools";
import { JazzContext, JazzContextManagerContext } from "./provider.js";
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

function useCoValueObservable<V extends CoValue, D>() {
  const [initialValue] = React.useState(() =>
    createCoValueObservable<V, D>({
      syncResolution: true,
    }),
  );
  const ref = useRef(initialValue);

  return {
    getCurrentValue() {
      return ref.current.getCurrentValue();
    },
    getCurrentObservable() {
      return ref.current;
    },
    reset() {
      ref.current = createCoValueObservable<V, D>({
        syncResolution: true,
      });
    },
  };
}

export function useCoState<V extends CoValue, D>(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  Schema: CoValueClass<V>,
  id: ID<CoValue> | undefined,
  depth: D & DepthsIn<V> = [] as D & DepthsIn<V>,
): DeeplyLoaded<V, D> | undefined | null {
  const contextManager = useJazzContextManager();

  const observable = useCoValueObservable<V, D>();

  const value = React.useSyncExternalStore<
    DeeplyLoaded<V, D> | undefined | null
  >(
    React.useCallback(
      (callback) => {
        if (!id) return () => {};

        // We subscribe to the context manager to react to the account updates
        // faster than the useSyncExternalStore callback update to keep the isAuthenticated state
        // up to date with the data when logging in and out.
        return subscribeToContextManager(contextManager, () => {
          const agent = getCurrentAccountFromContextManager(contextManager);

          observable.reset();

          return observable
            .getCurrentObservable()
            .subscribe(Schema, id, agent, depth, callback, callback);
        });
      },
      [Schema, id, contextManager],
    ),
    () => observable.getCurrentValue(),
    () => observable.getCurrentValue(),
  );

  return React.useMemo(() => {
    return id ? value : undefined;
  }, [id, value]);
}

export function createUseAccountHooks<Acc extends Account>() {
  function useAccount(): {
    me: Acc;
    logOut: () => void;
  };
  function useAccount<D extends DepthsIn<Acc>>(
    depth: D,
  ): { me: DeeplyLoaded<Acc, D> | undefined | null; logOut: () => void };
  function useAccount<D extends DepthsIn<Acc>>(
    depth?: D,
  ): { me: Acc | DeeplyLoaded<Acc, D> | undefined | null; logOut: () => void } {
    const context = useJazzContext<Acc>();
    const contextManager = useJazzContextManager<Acc>();

    if (!("me" in context)) {
      throw new Error(
        "useAccount can't be used in a JazzProvider with auth === 'guest' - consider using useAccountOrGuest()",
      );
    }

    const observable = useCoValueObservable<Acc, D>();

    const me = React.useSyncExternalStore<
      DeeplyLoaded<Acc, D> | undefined | null
    >(
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

            const Schema = agent.constructor as CoValueClass<Acc>;

            return observable
              .getCurrentObservable()
              .subscribe(
                Schema,
                agent.id,
                agent,
                depth ?? ([] as D),
                callback,
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
      me: depth === undefined ? me || context.me : me,
      logOut: context.logOut,
    };
  }

  function useAccountOrGuest(): {
    me: Acc | AnonymousJazzAgent;
  };
  function useAccountOrGuest<D extends DepthsIn<Acc>>(
    depth: D,
  ): { me: DeeplyLoaded<Acc, D> | undefined | null | AnonymousJazzAgent };
  function useAccountOrGuest<D extends DepthsIn<Acc>>(
    depth?: D,
  ): {
    me: Acc | DeeplyLoaded<Acc, D> | undefined | null | AnonymousJazzAgent;
  } {
    const context = useJazzContext<Acc>();
    const contextManager = useJazzContextManager<Acc>();

    const observable = useCoValueObservable<Acc, D>();

    const me = React.useSyncExternalStore<
      DeeplyLoaded<Acc, D> | undefined | null
    >(
      React.useCallback(
        (callback) => {
          return subscribeToContextManager(contextManager, () => {
            const agent = getCurrentAccountFromContextManager(contextManager);

            if (agent._type === "Anonymous") {
              return () => {};
            }

            observable.reset();

            const Schema = agent.constructor as CoValueClass<Acc>;

            return observable
              .getCurrentObservable()
              .subscribe(
                Schema,
                agent.id,
                agent,
                depth ?? ([] as D),
                callback,
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
        me: depth === undefined ? me || context.me : me,
      };
    } else {
      return { me: context.guest };
    }
  }

  return {
    useAccount,
    useAccountOrGuest,
  };
}

export function experimental_useInboxSender<
  I extends CoValue,
  O extends CoValue | undefined,
>(inboxOwnerID: ID<Account> | undefined) {
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
