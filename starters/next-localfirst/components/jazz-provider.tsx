"use client";

import { createContext, useContext } from "react";
import {
  JazzProvider as JazzBaseProvider,
  useLocalFirstAuth,
  type LocalFirstAuth,
} from "jazz-tools/react";

const APP_ID = process.env.NEXT_PUBLIC_JAZZ_APP_ID;
const SERVER_URL = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL;
const JazzAuthContext = createContext<LocalFirstAuth | null>(null);

export function useJazzAuth(): LocalFirstAuth {
  const auth = useContext(JazzAuthContext);
  if (!auth) throw new Error("useJazzAuth must be used inside JazzProvider");
  return auth;
}

export function JazzProvider({ children }: React.PropsWithChildren) {
  if (!APP_ID || !SERVER_URL) {
    const missing = [
      !APP_ID && "NEXT_PUBLIC_JAZZ_APP_ID",
      !SERVER_URL && "NEXT_PUBLIC_JAZZ_SERVER_URL",
    ]
      .filter((v) => !!v)
      .join(" & ");
    throw new Error(
      `${missing} not set. The withJazz Next plugin injects these at dev time; in production, set them explicitly in your environment.`,
    );
  }

  const auth = useLocalFirstAuth();
  const { secret, isLoading } = auth;
  if (isLoading || !secret) return null;

  return (
    <JazzBaseProvider
      config={{ appId: APP_ID, serverUrl: SERVER_URL, secret }}
      fallback={<p>Loading...</p>}
    >
      <JazzAuthContext.Provider value={auth}>{children}</JazzAuthContext.Provider>
    </JazzBaseProvider>
  );
}
