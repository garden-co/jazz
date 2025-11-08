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
  useSubscriptionRef,
  useSubscriptionSelector,
} from "./hooks.js";
import type {
  CoValueSubscription,
  CoValueRef,
  UseSubscriptionOptions,
} from "./types.js";
import { NotLoadedCoValueState } from "../tools/subscribe/types.js";

interface FallbackProps {
  loadingFallback?: React.ReactNode | (() => React.ReactNode);
  unavailableFallback?:
    | React.ReactNode
    | ((props: {
        loadingState: Exclude<
          NotLoadedCoValueState,
          CoValueLoadingState.LOADING
        >;
      }) => React.ReactNode);
}

const UninitializedContextSymbol = Symbol("UninitializedContext");

/**
 * We provide `ref` through a context instead of calling `useSubscriptionRef` repeatedly time for
 * every single consumer since the returned `ref` will always be the same, thus reducing the number
 * of subscribers.
 */
function RefProvider({
  RefContext,
  subscription,
  children,
}: {
  RefContext: React.Context<CoValueRef<any>>;
  subscription: CoValueSubscription<any, any>;
  children?: React.ReactNode | undefined;
}) {
  const ref = useSubscriptionRef(subscription);

  return <RefContext.Provider value={ref}>{children}</RefContext.Provider>;
}

function InternalProvider<
  S extends CoValueClassOrSchema,
  const R extends ResolveQuery<S> = true,
>({
  Context,
  RefContext,
  subscription,
  passthroughNotLoaded,
  loadingFallback,
  unavailableFallback,
  children,
}: {
  Context: React.Context<
    CoValueSubscription<S, R> | typeof UninitializedContextSymbol
  >;
  RefContext: React.Context<any>;
  subscription: CoValueSubscription<S, R>;
  children?: React.ReactNode | undefined;
  passthroughNotLoaded?: boolean;
} & FallbackProps) {
  const loadingState = useSubscriptionSelector(subscription, {
    select: (value) => (passthroughNotLoaded ? null : value.$jazz.loadingState),
  });

  if (loadingState === CoValueLoadingState.LOADING) {
    const LoadingFallback = loadingFallback;

    if (typeof LoadingFallback === "function") {
      return <LoadingFallback />;
    }

    return LoadingFallback ?? null;
  }

  if (
    loadingState === CoValueLoadingState.UNAUTHORIZED ||
    loadingState === CoValueLoadingState.UNAVAILABLE
  ) {
    const UnavailableFallback = unavailableFallback;

    if (typeof UnavailableFallback === "function") {
      return <UnavailableFallback loadingState={loadingState} />;
    }

    return UnavailableFallback ?? null;
  }

  return (
    <Context.Provider value={subscription}>
      <RefProvider RefContext={RefContext} subscription={subscription}>
        {children}
      </RefProvider>
    </Context.Provider>
  );
}

interface AccountSubscriptionContext<
  S extends CoValueClassOrSchema,
  R extends ResolveQuery<S> = true,
  P extends boolean = false,
  T = P extends true ? MaybeLoaded<Loaded<S, R>> : Loaded<S, R>,
> {
  Provider: React.FC<
    {
      options?: Omit<UseSubscriptionOptions<S, R>, "resolve">;
      children?: React.ReactNode | undefined;
    } & (P extends false ? FallbackProps : unknown)
  >;
  useSelector: <TSelectorReturn = T>(options?: {
    select?: (value: T) => TSelectorReturn;
    equalityFn?: (a: TSelectorReturn, b: TSelectorReturn) => boolean;
  }) => TSelectorReturn;
  useRef: () => CoValueRef<T>;
}

interface CoValueSubscriptionContext<
  S extends CoValueClassOrSchema,
  R extends ResolveQuery<S> = true,
  P extends boolean = false,
  T = P extends true ? MaybeLoaded<Loaded<S, R>> : Loaded<S, R>,
> extends Omit<AccountSubscriptionContext<S, R, P, T>, "Provider"> {
  Provider: React.FC<
    {
      id: string | undefined | null;
      options?: Omit<UseSubscriptionOptions<S, R>, "resolve">;
      children?: React.ReactNode | undefined;
    } & (P extends false ? FallbackProps : unknown)
  >;
}

interface ContextOptions<P extends boolean = false> {
  /**
   * If `false` (default behavior), the provider will render fallback components or `null` instead of its children
   * until the CoValue is loaded. In this case, the `useSelector` and `useRef` hooks will be guaranteed to return a
   * loaded value, and there would be no need to assert the value of `$isLoaded`.
   *
   * If `true`, the provider will always render its children even if the CoValue is not loaded, and you must handle
   * loading states in `useSelector` and `useRef` hooks return values.
   * This is useful for creating providers for optional CoValues.
   */
  passthroughNotLoaded?: P;
}

export function createCoValueSubscriptionContext<
  S extends CoValueClassOrSchema,
  const R extends ResolveQuery<S> = true,
  const P extends boolean = false,
  T = P extends true ? MaybeLoaded<Loaded<S, R>> : Loaded<S, R>,
>(
  /** The CoValue schema or class constructor */
  schema: S,
  /** Resolve query to specify which nested CoValues to load from the CoValue */
  resolve?: ResolveQueryStrict<S, R>,
  /** Optional behavior customization */
  contextOptions?: {
    passthroughNotLoaded?: P;
  },
): CoValueSubscriptionContext<S, R, P, T> {
  const Context = React.createContext<
    CoValueSubscription<S, R> | typeof UninitializedContextSymbol
  >(UninitializedContextSymbol);
  const RefContext = React.createContext<CoValueRef<T> | undefined>(undefined);

  return {
    Provider: ({ id, options, ...props }) => {
      const subscription = useCoValueSubscription(schema, id, {
        ...options,
        resolve,
      });

      return (
        <InternalProvider
          Context={Context}
          RefContext={RefContext}
          passthroughNotLoaded={contextOptions?.passthroughNotLoaded}
          subscription={subscription}
          {...props}
        />
      );
    },
    useSelector: (options) => {
      const subscription = React.useContext(Context);

      if (subscription === UninitializedContextSymbol) {
        throw new Error(
          "useSelector must be used within a CoValue subscription Provider",
        );
      }

      return useSubscriptionSelector(subscription, options as any);
    },
    useRef: () => {
      const subscription = React.useContext(Context);

      if (subscription === UninitializedContextSymbol) {
        throw new Error(
          "useRef must be used within a CoValue subscription Provider",
        );
      }

      return React.useContext(RefContext) as CoValueRef<T>;
    },
  };
}

export function createAccountSubscriptionContext<
  A extends AccountClass<Account> | AnyAccountSchema,
  const R extends ResolveQuery<A> = true,
  const P extends boolean = false,
  T = P extends true ? MaybeLoaded<Loaded<A, R>> : Loaded<A, R>,
>(
  /** The account schema to use. Defaults to the base Account schema */
  schema: A = Account as unknown as A,
  /** Resolve query to specify which nested CoValues to load from the account */
  resolve?: ResolveQueryStrict<A, R>,
  /** Optional behavior customization */
  contextOptions?: ContextOptions<P>,
): AccountSubscriptionContext<A, R, P, T> {
  const Context = React.createContext<
    CoValueSubscription<A, R> | typeof UninitializedContextSymbol
  >(UninitializedContextSymbol);
  const RefContext = React.createContext<CoValueRef<T> | undefined>(undefined);

  return {
    Provider: ({ options, ...props }) => {
      const subscription = useAccountSubscription(schema, {
        ...options,
        resolve,
      });

      return (
        <InternalProvider
          Context={Context}
          RefContext={RefContext}
          passthroughNotLoaded={contextOptions?.passthroughNotLoaded}
          subscription={subscription}
          {...props}
        />
      );
    },
    useSelector: (options) => {
      const subscription = React.useContext(Context);

      if (subscription === UninitializedContextSymbol) {
        throw new Error(
          "useSelector must be used within an account subscription Provider",
        );
      }

      return useSubscriptionSelector(subscription, options as any);
    },
    useRef: () => {
      const subscription = React.useContext(Context);

      if (subscription === UninitializedContextSymbol) {
        throw new Error(
          "useRef must be used within an account subscription Provider",
        );
      }

      return React.useContext(RefContext) as CoValueRef<T>;
    },
  };
}
