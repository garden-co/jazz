"use client";

import { createContext, useContext, useEffect, useState } from "react";
import { JazzSessionProvider, useJazzSession, useJazzSessionOwner } from "jazz-tools/react";
import { authClient } from "@/lib/auth-client";
import { getToken } from "@/lib/accounts";
const APP_ID = process.env.NEXT_PUBLIC_JAZZ_APP_ID;
const SERVER_URL = process.env.NEXT_PUBLIC_JAZZ_SERVER_URL;
const ProviderErrorContext = createContext<React.Dispatch<React.SetStateAction<Error | undefined>>>(
  () => {},
);
export const useProviderError = () => useContext(ProviderErrorContext);

function SessionContent({
  children,
  providerError,
}: React.PropsWithChildren<{ providerError?: Error }>) {
  const {
    status,
    error: sessionError,
    loginOrRegisterJWT,
    linkJWT,
    createLocalFirst,
    retry,
  } = useJazzSession();
  const error = sessionError ?? providerError;
  const reportError = useProviderError();
  const clearProviderError = () =>
    reportError((current) => (current === providerError ? undefined : current));

  return (
    <>
      {error && (
        <aside className="alert-error" role="alert">
          {error.message}
          <button
            type="button"
            onClick={() =>
              void linkJWT({ getToken })
                .then(clearProviderError)
                .catch(() => {})
            }
          >
            Retry linking
          </button>
          <button
            type="button"
            onClick={() =>
              void retry()
                .then(clearProviderError)
                .catch(() => {})
            }
          >
            Retry account preparation
          </button>
        </aside>
      )}
      {status === "ready" ? (
        children
      ) : status === "signed-out" ? (
        <div>
          <button
            onClick={() =>
              void loginOrRegisterJWT({ getToken })
                .then(clearProviderError)
                .catch(() => {})
            }
          >
            Retry sign in
          </button>
          <button
            onClick={() =>
              void createLocalFirst()
                .then(clearProviderError)
                .catch(() => {})
            }
          >
            Continue locally
          </button>
        </div>
      ) : (
        <p>Loading...</p>
      )}
    </>
  );
}

export function JazzProvider({ children }: React.PropsWithChildren) {
  if (!APP_ID || !SERVER_URL) throw new Error("Jazz app ID and server URL must be set");
  const { session, error, retry } = useJazzSessionOwner({
    appId: APP_ID,
    serverUrl: SERVER_URL,
    initial: "local-first",
  });
  const [providerError, setProviderError] = useState<Error>();
  useEffect(() => {
    if (!session) return;
    void authClient
      .getSession()
      .then((auth) => (auth.data?.session ? session.loginOrRegisterJWT({ getToken }) : undefined))
      .catch((cause) =>
        setProviderError(cause instanceof Error ? cause : new Error(String(cause))),
      );
  }, [session]);
  if (!session)
    return error ? (
      <p role="alert">
        {error.message}{" "}
        <button onClick={() => void retry().catch(() => {})}>Retry account preparation</button>
      </p>
    ) : (
      <p>Loading...</p>
    );
  return (
    <ProviderErrorContext.Provider value={setProviderError}>
      <JazzSessionProvider
        session={session}
        fallback={<SessionContent providerError={providerError} />}
      >
        <SessionContent providerError={providerError}>{children}</SessionContent>
      </JazzSessionProvider>
    </ProviderErrorContext.Provider>
  );
}
