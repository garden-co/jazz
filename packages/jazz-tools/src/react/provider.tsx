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
import { JazzContext, useJazzProviderCheck } from "jazz-tools/react-core";
import React, { useEffect, useMemo, useRef, useSyncExternalStore } from "react";

export type JazzProviderProps<
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
> = {
  children: React.ReactNode;
  enableSSR?: boolean;
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
  enableSSR,
  fallback = null,
  authSecretStorageKey,
}: JazzProviderProps<S>) {
  useJazzProviderCheck();

  const [contextManager] = React.useState(
    () =>
      new JazzBrowserContextManager<S>({
        useAnonymousFallback: enableSSR,
        authSecretStorageKey,
      }),
  );

  const onLogOutRefCallback = useRefCallback(onLogOut);
  const logOutReplacementRefCallback = useRefCallback(logOutReplacement);
  const onAnonymousAccountDiscardedRefCallback = useRefCallback(
    onAnonymousAccountDiscarded,
  );

  const props = useMemo(() => {
    return {
      AccountSchema,
      guestMode,
      sync,
      storage,
      defaultProfileName,
      onLogOut: onLogOutRefCallback,
      logOutReplacement: logOutReplacement
        ? logOutReplacementRefCallback
        : undefined,
      onAnonymousAccountDiscarded: onAnonymousAccountDiscarded
        ? onAnonymousAccountDiscardedRefCallback
        : undefined,
    } satisfies JazzContextManagerProps<S>;
  }, [guestMode, sync.peer, sync.when, storage]);

  if (contextManager.propsChanged(props)) {
    contextManager.createContext(props).catch((error) => {
      console.log(error.stack);
      console.error("Error creating Jazz browser context:", error);
    });
  }

  const isReady = useSyncExternalStore(
    useCallback(
      (callback) => {
        return contextManager.subscribe(callback);
      },
      [contextManager],
    ),
    () => Boolean(contextManager.getCurrentValue()),
    () => Boolean(contextManager.getCurrentValue()),
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
    <JazzContext.Provider value={contextManager}>
      {isReady ? children : fallback}
    </JazzContext.Provider>
  );
}

function useRefCallback<T extends (...args: any[]) => any>(callback?: T) {
  const callbackRef = React.useRef(callback);
  callbackRef.current = callback;
  return useRef(
    (...args: Parameters<T>): ReturnType<T> => callbackRef.current?.(...args),
  ).current;
}
