import { useEffect, useState, StrictMode } from "react";
import { JazzSessionProvider, useJazzSession } from "jazz-tools/react";
import { authClient } from "./auth-client";
import { getToken } from "./accounts";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import "./App.css";

const APP_ID = import.meta.env.VITE_JAZZ_APP_ID;
const SERVER_URL = import.meta.env.VITE_JAZZ_SERVER_URL;
const restored = new WeakSet<() => Promise<void>>();

function SessionContent({ children }: React.PropsWithChildren) {
  const { status, error: sessionError, logout, loginJWT, linkJWT, retry } = useJazzSession();
  const [providerError, setProviderError] = useState<Error>();
  const error = sessionError ?? providerError;
  useEffect(() => {
    if (status !== "ready" || restored.has(logout)) return;
    restored.add(logout);
    void authClient
      .getSession()
      .then((auth) => (auth.data?.session ? loginJWT({ getToken }) : undefined))
      .catch((cause) =>
        setProviderError(cause instanceof Error ? cause : new Error(String(cause))),
      );
  }, [status, loginJWT, logout]);
  return (
    <>
      {error && (
        <aside className="alert-error" role="alert">
          {error.message}
          <button type="button" onClick={() => void linkJWT({ getToken }).catch(() => {})}>
            Retry linking
          </button>
          <button type="button" onClick={() => void retry().catch(() => {})}>
            Retry account preparation
          </button>
        </aside>
      )}
      {status === "ready" ? children : <p>Loading...</p>}
    </>
  );
}

export function JazzProvider({ children }: React.PropsWithChildren) {
  if (!APP_ID || !SERVER_URL)
    throw new Error("VITE_JAZZ_APP_ID and VITE_JAZZ_SERVER_URL must be set");
  return (
    <JazzSessionProvider
      config={{ appId: APP_ID, serverUrl: SERVER_URL, initial: "local-first" }}
      fallback={<SessionContent />}
    >
      <SessionContent>{children}</SessionContent>
    </JazzSessionProvider>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <JazzProvider>
      <App />
    </JazzProvider>
  </StrictMode>,
);
