import {
  Account,
  AccountClass,
  AnyAccountSchema,
  CoValueFromRaw,
  InstanceOfSchema,
  JazzContextType,
  KvStore,
} from "jazz-tools";
import { JazzContext, JazzContextManagerContext } from "jazz-tools/react-core";
import React, { useEffect, useRef } from "react";
import { JazzContextManagerProps } from "./ReactNativeContextManager.js";
import { ReactNativeContextManager } from "./ReactNativeContextManager.js";
import { setupKvStore } from "./platform.js";

export type JazzProviderProps<
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
> = {
  children: React.ReactNode;
  kvStore?: KvStore;
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
  kvStore,
  onAnonymousAccountDiscarded,
  CryptoProvider,
  logOutReplacement,
}: JazzProviderProps<S>) {
  setupKvStore(kvStore);

  const [contextManager] = React.useState(
    () => new ReactNativeContextManager<S>(),
  );

  const onAnonymousAccountDiscardedRefCallback = useRefCallback(
    onAnonymousAccountDiscarded,
  );
  const onLogOutRefCallback = useRefCallback(onLogOut);
  const logOutReplacementRefCallback = useRefCallback(logOutReplacement);
  const logoutReplacementActiveRef = useRef(false);
  logoutReplacementActiveRef.current = Boolean(logOutReplacement);

  const value = React.useSyncExternalStore<
    JazzContextType<InstanceOfSchema<S>> | undefined
  >(
    React.useCallback(
      (callback) => {
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
          CryptoProvider,
        } satisfies JazzContextManagerProps<S>;

        if (contextManager.propsChanged(props)) {
          contextManager.createContext(props).catch((error) => {
            console.log(error.stack);
            console.error("Error creating Jazz context:", error);
          });
        }

        return contextManager.subscribe(callback);
      },
      [sync, guestMode].concat(storage as any),
    ),
    () => contextManager.getCurrentValue(),
    () => contextManager.getCurrentValue(),
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
    <JazzContext.Provider value={value}>
      <JazzContextManagerContext.Provider value={contextManager}>
        {value && children}
      </JazzContextManagerContext.Provider>
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
