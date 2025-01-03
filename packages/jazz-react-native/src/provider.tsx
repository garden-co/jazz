import { JazzContext, JazzContextType } from "jazz-react-core";
import { Account, AccountClass, AuthMethod } from "jazz-tools";
import { useState } from "react";
import { useEffect, useRef } from "react";
import {
  BaseReactNativeContextOptions,
  createJazzRNContext,
} from "./platform.js";
export interface Register {}

export type RegisteredAccount = Register extends { Account: infer Acc }
  ? Acc
  : Account;

export type JazzProviderProps<Acc extends Account = RegisteredAccount> = {
  children: React.ReactNode;
  auth: AuthMethod | "guest";
  peer: `wss://${string}` | `ws://${string}`;
  AccountSchema?: AccountClass<Acc>;
  CryptoProvider?: BaseReactNativeContextOptions["CryptoProvider"];
};

/** @category Context & Hooks */
export function JazzProvider<Acc extends Account = RegisteredAccount>({
  children,
  auth,
  peer,
  AccountSchema = Account as unknown as AccountClass<Acc>,
  CryptoProvider,
}: JazzProviderProps<Acc>) {
  const [ctx, setCtx] = useState<JazzContextType<Acc> | undefined>();

  const [sessionCount, setSessionCount] = useState(0);

  const effectExecuted = useRef(false);
  effectExecuted.current = false;

  useEffect(() => {
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
      const currentContext = await createJazzRNContext<Acc>(
        auth === "guest"
          ? {
              peer,
              CryptoProvider,
            }
          : {
              AccountSchema,
              auth: auth,
              peer,
              CryptoProvider,
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

      setCtx({
        ...currentContext,
        AccountSchema,
        logOut,
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
  }, [AccountSchema, auth, peer, sessionCount]);

  return (
    <JazzContext.Provider value={ctx}>{ctx && children}</JazzContext.Provider>
  );
}
