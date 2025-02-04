import React, { useCallback, useContext, useRef } from "react";

import {
  Account,
  AnonymousJazzAgent,
  CoValue,
  CoValueClass,
  ID,
  InboxSender,
  RefsToResolve,
  Resolved,
  createCoValueObservable,
} from "jazz-tools";
import { JazzContext, JazzContextType } from "./provider.js";

export function useJazzContext<Acc extends Account>() {
  const value = useContext(JazzContext) as JazzContextType<Acc>;

  if (!value) {
    throw new Error(
      "You need to set up a JazzProvider on top of your app to use this hook.",
    );
  }

  return value;
}

export function useCoState<
  V extends CoValue,
  const R extends RefsToResolve<V> = true,
>(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  Schema: CoValueClass<V>,
  id: ID<V> | undefined,
  options?: { resolve?: R },
): Resolved<V, R> | undefined {
  const context = useJazzContext();

  const [observable] = React.useState(() =>
    createCoValueObservable<V, R>({
      syncResolution: true,
    }),
  );

  const value = React.useSyncExternalStore<Resolved<V, R> | undefined>(
    React.useCallback(
      (callback) => {
        if (!id) return () => {};

        const agent = "me" in context ? context.me : context.guest;

        return observable.subscribe(
          Schema,
          id,
          {
            loadAs: agent,
            resolve: options?.resolve,
          },
          callback,
        );
      },
      [Schema, id, context],
    ),
    () => observable.getCurrentValue(),
    () => observable.getCurrentValue(),
  );

  return value;
}

export function createUseAccountHooks<Acc extends Account>() {
  function useAccount(): {
    me: Acc;
    logOut: () => void;
  };
  function useAccount<const R extends RefsToResolve<Acc> = true>(options?: {
    resolve?: R;
  }): { me: Resolved<Acc, R> | undefined; logOut: () => void };
  function useAccount<const R extends RefsToResolve<Acc>>(options?: {
    resolve?: R;
  }): { me: Acc | Resolved<Acc, R> | undefined; logOut: () => void } {
    const context = useJazzContext<Acc>();

    if (!("me" in context)) {
      throw new Error(
        "useAccount can't be used in a JazzProvider with auth === 'guest' - consider using useAccountOrGuest()",
      );
    }

    const me = useCoState<Acc, R>(
      context.AccountSchema,
      context.me.id,
      options,
    );

    return {
      me: options?.resolve === undefined ? me || context.me : me,
      logOut: context.logOut,
    };
  }

  function useAccountOrGuest(): {
    me: Acc | AnonymousJazzAgent;
  };
  function useAccountOrGuest<
    const R extends RefsToResolve<Acc> = true,
  >(options?: {
    resolve?: R;
  }): { me: Resolved<Acc, R> | undefined | AnonymousJazzAgent };
  function useAccountOrGuest<const R extends RefsToResolve<Acc>>(options?: {
    resolve?: R;
  }): { me: Acc | Resolved<Acc, R> | undefined | AnonymousJazzAgent } {
    const context = useJazzContext<Acc>();

    const contextMe = "me" in context ? context.me : undefined;

    const me = useCoState<Acc, R>(
      context.AccountSchema,
      contextMe?.id,
      options,
    );

    if ("me" in context) {
      return {
        me: options?.resolve === undefined ? me || context.me : me,
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
