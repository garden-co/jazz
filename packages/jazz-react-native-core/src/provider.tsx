import { JazzContext, JazzContextManagerContext } from "jazz-react-core";
import { Account, JazzContextType, KvStore } from "jazz-tools";
import React, { useEffect, useRef } from "react";
import { JazzContextManagerProps } from "./ReactNativeContextManager.js";
import { ReactNativeContextManager } from "./ReactNativeContextManager.js";
import { setupKvStore } from "./platform.js";

export interface Register {}

export type RegisteredAccount = Register extends { Account: infer Acc }
  ? Acc
  : Account;

export type JazzProviderProps<Acc extends Account = RegisteredAccount> = {
  children: React.ReactNode;
  kvStore?: KvStore;
} & JazzContextManagerProps<Acc>;

/** @category Context & Hooks */
export function JazzProviderCore<Acc extends Account = RegisteredAccount>({
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
}: JazzProviderProps<Acc>) {
  setupKvStore(kvStore);

  const [contextManager] = React.useState(
    () => new ReactNativeContextManager<Acc>(),
  );

  const onAnonymousAccountDiscardedRefCallback = useRefCallback(
    onAnonymousAccountDiscarded,
  );
  const onLogOutRefCallback = useRefCallback(onLogOut);

  const value = React.useSyncExternalStore<JazzContextType<Acc> | undefined>(
    React.useCallback(
      (callback) => {
        const props = {
          AccountSchema,
          guestMode,
          sync,
          storage,
          defaultProfileName,
          onLogOut: onLogOutRefCallback,
          onAnonymousAccountDiscarded: onAnonymousAccountDiscardedRefCallback,
          CryptoProvider,
        };
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
