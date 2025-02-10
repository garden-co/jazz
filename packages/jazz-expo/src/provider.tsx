import {
  JazzContext,
  JazzContextType,
  RegisteredAccount,
} from "jazz-react-core";
import { JazzProviderProps, createJazzRNContext } from "jazz-react-native";
import { Account, AccountClass } from "jazz-tools";
import React, { useState } from "react";
import { useEffect, useRef } from "react";

import { ExpoSQLiteAdapter } from "cojson-storage-rn-expo-sqlite-adapter";

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
              storage: new ExpoSQLiteAdapter(),
            }
          : {
              AccountSchema,
              auth: auth,
              peer,
              CryptoProvider,
              storage: new ExpoSQLiteAdapter(),
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
