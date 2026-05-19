"use client";

import { JazzProvider as JazzBaseProvider, useLocalFirstAuth } from "jazz-tools/react";

const APP_ID = process.env.NEXT_PUBLIC_JAZZ_APP_ID;
const SERVER_URL = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL;

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

  const { secret, isLoading } = useLocalFirstAuth();
  if (isLoading || !secret) return null;

  return (
    <JazzBaseProvider
      config={{ appId: APP_ID, serverUrl: SERVER_URL, secret }}
      fallback={<p>Loading...</p>}
    >
      {children}
    </JazzBaseProvider>
  );
}
