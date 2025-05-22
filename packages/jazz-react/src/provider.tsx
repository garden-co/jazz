import {
  JazzBrowserContextManager,
  JazzContextManagerProps,
} from "jazz-browser";
import { JazzContext, JazzContextManagerContext } from "jazz-react-core";
import {
  Account,
  AccountClass,
  AnyAccountSchema,
  CoValueFromRaw,
  InstanceOfSchema,
  JazzContextType,
} from "jazz-tools";
import React, { useEffect, useRef } from "react";

export type JazzProviderProps<
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
> = {
  children: React.ReactNode;
  enableSSR?: boolean;
} & JazzContextManagerProps<S>;

/** @category Context & Hooks */
export function JazzProvider<
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
}: JazzProviderProps<S>) {
  const [contextManager] = React.useState(
    () =>
      new JazzBrowserContextManager<S>({
        useAnonymousFallback: enableSSR,
      }),
  );

  const onLogOutRefCallback = useRefCallback(onLogOut);
  const logOutReplacementRefCallback = useRefCallback(logOutReplacement);
  const onAnonymousAccountDiscardedRefCallback = useRefCallback(
    onAnonymousAccountDiscarded,
  );
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
        } satisfies JazzContextManagerProps<S>;

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
