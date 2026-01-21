import React from "react";
import {
  Account,
  AccountClass,
  AnyAccountSchema,
  CoValueClassOrSchema,
  CoValueLoadingState,
  Loaded,
  MaybeLoaded,
  ResolveQuery,
  ResolveQueryStrict,
} from "jazz-tools";
import {
  useAccountSubscription,
  useCoValueSubscription,
  useSubscriptionSelector,
} from "./hooks.js";
import type { CoValueSubscription } from "./types.js";

export function createCoValueSubscriptionContext<
  S extends CoValueClassOrSchema,
  const R extends ResolveQuery<S> = true,
>(schema: S, resolve?: ResolveQueryStrict<S, R>) {
  const Context = React.createContext<CoValueSubscription<S, R>>(null);

  return {
    Provider: ({
      id,
      options,
      loadingFallback,
      unavailableFallback,
      children,
    }: React.PropsWithChildren<{
      id: string | undefined | null;
      options?: Omit<
        Parameters<typeof useCoValueSubscription<S, R>>[2],
        "resolve"
      >;
      loadingFallback?: React.ReactNode;
      unavailableFallback?: React.ReactNode;
    }>) => {
      const subscription = useCoValueSubscription(
        schema,
        id,
        {
          ...options,
          resolve: resolve,
        },
        "CoValueSubscriptionProvider",
      );

      const loadState = useSubscriptionSelector(subscription, {
        select: (value) => value.$jazz.loadingState,
      });

      if (loadState === CoValueLoadingState.LOADING) {
        return loadingFallback ?? null;
      }
      if (
        loadState === CoValueLoadingState.UNAUTHORIZED ||
        loadState === CoValueLoadingState.UNAVAILABLE
      ) {
        return unavailableFallback ?? null;
      }

      return (
        <Context.Provider value={subscription}>{children}</Context.Provider>
      );
    },
    useSelector: <TSelectorReturn = Loaded<S, R>>(options?: {
      select?: (value: Loaded<S, R>) => TSelectorReturn;
      equalityFn?: (a: TSelectorReturn, b: TSelectorReturn) => boolean;
    }) => {
      const subscription = React.useContext(Context);

      if (!subscription) {
        throw new Error(
          "useSelector must be used within a coValue subscription provider",
        );
      }

      return useSubscriptionSelector<S, R, TSelectorReturn>(
        subscription,
        options as Parameters<
          typeof useSubscriptionSelector<S, R, TSelectorReturn>
        >[1],
      );
    },
  };
}

export function createAccountSubscriptionContext<
  A extends AccountClass<Account> | AnyAccountSchema,
  const R extends ResolveQuery<A> = true,
>(schema: A, resolve?: ResolveQueryStrict<A, R>) {
  const Context = React.createContext<CoValueSubscription<A, R>>(null);

  return {
    Provider: ({
      options,
      loadingFallback,
      unavailableFallback,
      children,
    }: React.PropsWithChildren<{
      options?: Omit<
        Parameters<typeof useAccountSubscription<A, R>>[1],
        "resolve"
      >;
      loadingFallback?: React.ReactNode;
      unavailableFallback?: React.ReactNode;
    }>) => {
      const subscription = useAccountSubscription(
        schema,
        {
          ...options,
          resolve: resolve,
        },
        "AccountSubscriptionProvider",
      );

      const loadState = useSubscriptionSelector(subscription, {
        select: (value) => value.$jazz.loadingState,
      });

      if (loadState === CoValueLoadingState.LOADING) {
        return loadingFallback ?? null;
      }

      if (
        loadState === CoValueLoadingState.UNAUTHORIZED ||
        loadState === CoValueLoadingState.UNAVAILABLE
      ) {
        return unavailableFallback ?? null;
      }

      return (
        <Context.Provider value={subscription}>{children}</Context.Provider>
      );
    },
    useSelector: <TSelectorReturn = Loaded<A, R>>(options?: {
      select?: (value: Loaded<A, R>) => TSelectorReturn;
      equalityFn?: (a: TSelectorReturn, b: TSelectorReturn) => boolean;
    }) => {
      const subscription = React.useContext(Context);

      if (!subscription) {
        throw new Error(
          "useSelector must be used within an account subscription provider",
        );
      }

      return useSubscriptionSelector<A, R, TSelectorReturn>(
        subscription,
        options as Parameters<
          typeof useSubscriptionSelector<A, R, TSelectorReturn>
        >[1],
      );
    },
  };
}
