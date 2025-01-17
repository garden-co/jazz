import {
  BaseBrowserContextOptions,
  createJazzBrowserContext,
} from "jazz-browser";
import React, { useEffect, useRef, useState } from "react";

import { JazzContext, JazzContextType } from "jazz-react-core";
import { Account, AccountClass, AuthMethod } from "jazz-tools";

export interface Register {}

export type RegisteredAccount = Register extends { Account: infer Acc }
  ? Acc
  : Account;

export type JazzProviderProps<Acc extends Account = RegisteredAccount> = {
  children: React.ReactNode;
  auth?: AuthMethod | "guest";
  peer: `wss://${string}` | `ws://${string}`;
  localOnly?: boolean;
  storage?: BaseBrowserContextOptions["storage"];
  AccountSchema?: AccountClass<Acc>;
};

/** @category Context & Hooks */
export function JazzProvider<Acc extends Account = RegisteredAccount>({
  children,
  auth,
  peer,
  storage,
  AccountSchema = Account as unknown as AccountClass<Acc>,
  localOnly,
}: JazzProviderProps<Acc>) {
  const [ctx, setCtx] = useState<JazzContextType<Acc> | undefined>();

  const [sessionCount, setSessionCount] = useState(0);

  const effectExecuted = useRef(false);
  effectExecuted.current = false;

  useEffect(
    () => {
      // Avoid double execution of the effect in development mode for easier debugging.
      if (process.env.NODE_ENV === "development") {
        if (effectExecuted.current) {
          return;
        }
        effectExecuted.current = true;

        // In development mode we don't return a cleanup function because otherwise
        // the double effect execution would mark the context as done immediately.
        //
        // So we mark it as done in the subsequent execution.
        const previousContext = ctx;

        if (previousContext) {
          previousContext.done();
        }
      }

      async function createContext() {
        const currentContext = await createJazzBrowserContext<Acc>(
          auth === "guest"
            ? {
                guest: true,
                peer,
                storage,
                localOnly,
              }
            : {
                guest: false,
                AccountSchema,
                auth,
                peer,
                storage,
                localOnly,
              },
        );

        const logOut = () => {
          currentContext.logOut();
          setCtx(undefined);
          setSessionCount(sessionCount + 1);

          if (process.env.NODE_ENV === "development") {
            // In development mode we don't return a cleanup function
            // so we mark the context as done here.
            currentContext.done();
          }
        };

        const refresh = () => {
          setCtx(undefined);
          setSessionCount(sessionCount + 1);

          if (process.env.NODE_ENV === "development") {
            // In development mode we don't return a cleanup function
            // so we mark the context as done here.
            currentContext.done();
          }
        };

        setCtx({
          ...currentContext,
          AccountSchema,
          logOut,
          refreshContext: refresh,
        });

        return currentContext;
      }

      const promise = createContext();

      // In development mode we don't return a cleanup function because otherwise
      // the double effect execution would mark the context as done immediately.
      if (process.env.NODE_ENV === "development") {
        return;
      }

      return () => {
        void promise.then((context) => context.done());
      };
    },
    [AccountSchema, auth, peer, sessionCount].concat(storage as any),
  );

  useEffect(() => {
    if (ctx) {
      ctx.toggleNetwork?.(!localOnly);
    }
  }, [ctx, localOnly]);

  return (
    <JazzContext.Provider value={ctx}>{ctx && children}</JazzContext.Provider>
  );
}
