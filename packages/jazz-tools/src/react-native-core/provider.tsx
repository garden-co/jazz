import {
  Account,
  AccountClass,
  AnyAccountSchema,
  CoValueFromRaw,
  KvStore,
} from "jazz-tools";
import { JazzContext } from "jazz-tools/react-core";
import React, {
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useSyncExternalStore,
} from "react";
import type { JazzContextManagerProps } from "./ReactNativeContextManager.js";
import { ReactNativeContextManager } from "./ReactNativeContextManager.js";
import { setupKvStore } from "./platform.js";

export type JazzProviderProps<
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
> = {
  children: React.ReactNode;
  kvStore?: KvStore;
  fallback?: React.ReactNode | null;
  authSecretStorageKey?: string;
} & JazzContextManagerProps<S>;

/** @category Context & Hooks */
export function JazzProviderCore<
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
  kvStore,
  authSecretStorageKey,
  fallback = null,
}: JazzProviderProps<S>) {
  if (useContext(JazzContext)) {
    throw new Error(
      "You can't nest a JazzProvider inside another JazzProvider.",
    );
  }

  setupKvStore(kvStore);

  const [contextManager] = React.useState(
    () => new ReactNativeContextManager<S>({ authSecretStorageKey }),
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
      console.error("Error creating Jazz React Native context:", error);
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
