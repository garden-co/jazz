import {
  Account,
  AccountClass,
  AnyAccountSchema,
  CoValueFromRaw,
} from "jazz-tools";
import {
  JazzBrowserContextManager,
  JazzContextManagerProps,
} from "jazz-tools/browser";
import {
  JazzContext,
  JazzContextManagerContext,
  use,
} from "jazz-tools/react-core";
import React, { Suspense, useEffect, useRef } from "react";

export type JazzProviderProps<
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
> = {
  children: React.ReactNode;
  fallback?: React.ReactNode | null;
  authSecretStorageKey?: string;
} & JazzContextManagerProps<S>;

/** @category Context & Hooks */
export function JazzReactProvider<
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
>({
  children,
  guestMode,
  sync,
  storage,
  AccountSchema,
  defaultProfileName,
  onLogOut,
  logOutReplacement,
  onAnonymousAccountDiscarded,
  fallback = null,
  authSecretStorageKey,
}: JazzProviderProps<S>) {
  const [contextManager] = React.useState(
    () =>
      new JazzBrowserContextManager<S>({
        authSecretStorageKey,
      }),
  );

  const onLogOutRefCallback = useRefCallback(onLogOut);
  const logOutReplacementRefCallback = useRefCallback(logOutReplacement);
  const onAnonymousAccountDiscardedRefCallback = useRefCallback(
    onAnonymousAccountDiscarded,
  );
  const logoutReplacementActiveRef = useRef(false);
  logoutReplacementActiveRef.current = Boolean(logOutReplacement);

  const props = {
    AccountSchema,
    guestMode,
    sync,
    storage,
    defaultProfileName,
    onLogOut: onLogOutRefCallback,
    logOutReplacement: logoutReplacementActiveRef.current
      ? logOutReplacementRefCallback
      : undefined,
    onAnonymousAccountDiscarded: onAnonymousAccountDiscardedRefCallback,
  } satisfies JazzContextManagerProps<S>;

  if (contextManager.propsChanged(props)) {
    contextManager.createContext(props).catch((error) => {
      console.log(error.stack);
      console.error("Error creating Jazz browser context:", error);
    });
  }

  const promise = React.useSyncExternalStore(
    React.useCallback((callback) => {
      return contextManager.subscribe(callback);
    }, []),
    () => contextManager.contextPromise,
    () => contextManager.contextPromise,
  );

  useEffect(() => {
    // In development mode we don't return a cleanup function because otherwise
    // the double effect execution would mark the context as done immediately.
    if (process.env.NODE_ENV === "development") return;

    return () => {
      contextManager.done();
    };
  }, []);

  return (
    <Suspense fallback={fallback}>
      <WaitFor promise={promise}>
        {(value) => (
          <JazzContext.Provider value={value}>
            <JazzContextManagerContext.Provider value={contextManager}>
              {children}
            </JazzContextManagerContext.Provider>
          </JazzContext.Provider>
        )}
      </WaitFor>
    </Suspense>
  );
}

function WaitFor<T>(props: {
  promise: React.Usable<T> | undefined;
  children: (value: NonNullable<T>) => React.ReactNode;
}) {
  if (!props.promise) {
    throw new Error("The Jazz context has not been initialized yet");
  }
  const value = use(props.promise);

  if (!value) {
    throw new Error("The Jazz context promise has returned undefined");
  }

  return props.children(value);
}

function useRefCallback<T extends (...args: any[]) => any>(callback?: T) {
  const callbackRef = React.useRef(callback);
  callbackRef.current = callback;
  return useRef(
    (...args: Parameters<T>): ReturnType<T> => callbackRef.current?.(...args),
  ).current;
}
