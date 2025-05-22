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
  CoValueOrZodSchema,
  InboxSender,
  InstanceOfSchema,
  JazzContextManager,
  JazzContextType,
  Loaded,
  ResolveQuery,
  ResolveQueryStrict,
  createCoValueObservable,
  z,
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

function useCoValueObservable<
  S extends CoValueOrZodSchema,
  const R extends ResolveQuery<S>,
>() {
  const [initialValue] = React.useState(() => createCoValueObservable<S, R>());
  const ref = useRef(initialValue);

  return {
    getCurrentValue() {
      return ref.current.getCurrentValue();
    },
    getCurrentObservable() {
      return ref.current;
    },
    reset(initialValue?: undefined | null) {
      ref.current = createCoValueObservable<S, R>(initialValue);
    },
  };
}

export function useCoState<
  S extends CoValueOrZodSchema,
  const R extends ResolveQuery<S> = true,
>(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  Schema: S,
  id: string | undefined,
  options?: {
    resolve?: ResolveQueryStrict<S, R>;
  },
): Loaded<S, R> | undefined | null {
  const contextManager = useJazzContextManager();

  const observable = useCoValueObservable<S, R>();

  const value = React.useSyncExternalStore<Loaded<S, R> | undefined | null>(
    React.useCallback(
      (callback) => {
        if (!id) {
          observable.reset(null);

          return () => {};
        }

        observable.reset();

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

function useAccount<
  A extends AccountClass<Account> | AnyAccountSchema,
  R extends ResolveQuery<A>,
>(
  AccountSchema: A = Account as unknown as A,
  options?: {
    resolve?: ResolveQueryStrict<A, R>;
  },
): {
  me: Loaded<A, R> | undefined | null;
  agent: AnonymousJazzAgent | InstanceOfSchema<A>;
  logOut: () => void;
} {
  const context = useJazzContext<InstanceOfSchema<A>>();
  const contextManager = useJazzContextManager<InstanceOfSchema<A>>();

  const agent = "me" in context ? context.me : context.guest;

  const observable = useCoValueObservable<A, R>();

  const me = React.useSyncExternalStore<Loaded<A, R> | undefined | null>(
    React.useCallback(
      (callback) => {
        return subscribeToContextManager(contextManager, () => {
          const agent = getCurrentAccountFromContextManager(contextManager);

          observable.reset();

          if (agent._type === "Anonymous") {
            return () => {};
          }

          return observable.getCurrentObservable().subscribe(
            AccountSchema,
            agent.id,
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
    () => observable.getCurrentValue() as Loaded<A, R> | undefined | null,
    () => observable.getCurrentValue() as Loaded<A, R> | undefined | null,
  );

  return {
    me,
    agent,
    logOut: contextManager.logOut,
  };
}

export { useAccount };

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
