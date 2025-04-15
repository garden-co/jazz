import {
  JazzBrowserContextManager,
  JazzContextManagerProps,
} from "jazz-browser";
import { JazzContext, JazzContextManagerContext } from "jazz-react-core";
import { Account, JazzContextType } from "jazz-tools";
import React, { useEffect, useRef } from "react";

export interface Register {}

export type RegisteredAccount = Register extends { Account: infer Acc }
  ? Acc
  : Account;

export type JazzProviderProps<Acc extends Account = RegisteredAccount> = {
  children: React.ReactNode;
} & JazzContextManagerProps<Acc>;

/** @category Context & Hooks */
export function JazzProvider<Acc extends Account = RegisteredAccount>({
  children,
  guestMode,
  sync,
  storage,
  AccountSchema,
  defaultProfileName,
  onLogOut,
  logOutReplacement,
  onAnonymousAccountDiscarded,
}: JazzProviderProps<Acc>) {
  const [contextManager] = React.useState(
    () => new JazzBrowserContextManager<Acc>(),
  );

  const onLogOutRefCallback = useRefCallback(onLogOut);
  const logOutReplacementRefCallback = useRefCallback(logOutReplacement);
  const onAnonymousAccountDiscardedRefCallback = useRefCallback(
    onAnonymousAccountDiscarded,
  );
  const logoutReplacementActiveRef = useRef(false);
  logoutReplacementActiveRef.current = Boolean(logOutReplacement);

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
          logOutReplacement: logoutReplacementActiveRef.current
            ? logOutReplacementRefCallback
            : undefined,
          onAnonymousAccountDiscarded: onAnonymousAccountDiscardedRefCallback,
        } satisfies JazzContextManagerProps<Acc>;

        if (contextManager.propsChanged(props)) {
          contextManager.createContext(props).catch((error) => {
            console.error("Error creating Jazz browser context:", error);
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
