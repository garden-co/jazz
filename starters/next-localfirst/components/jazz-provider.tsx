"use client";

import { JazzSessionProvider, useJazzSession } from "jazz-tools/react";

const APP_ID = process.env.NEXT_PUBLIC_JAZZ_APP_ID;
const SERVER_URL = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL;

export function useLocalAccount() {
  const { account, restoreLocalFirst } = useJazzSession();
  if (!account) throw new Error("Jazz account is not ready");
  return { account, restore: restoreLocalFirst };
}

function SessionFallback() {
  const { error, retry } = useJazzSession();
  return error ? (
    <p role="alert">
      {error.message} <button onClick={() => void retry().catch(() => {})}>Retry</button>
    </p>
  ) : (
    <p>Loading...</p>
  );
}

function SessionContent({ children }: React.PropsWithChildren) {
  const { error } = useJazzSession();
  return (
    <>
      {error && <p role="alert">{error.message}</p>}
      {children}
    </>
  );
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

  return (
    <JazzSessionProvider
      config={{ appId: APP_ID, serverUrl: SERVER_URL, initial: "local-first" }}
      fallback={<SessionFallback />}
    >
      <SessionContent>{children}</SessionContent>
    </JazzSessionProvider>
  );
}
