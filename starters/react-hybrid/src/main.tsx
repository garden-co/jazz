import { createContext, useContext, useEffect, useState, useRef, StrictMode } from "react";
import { JazzProvider as Provider, useJazzAuth } from "jazz-tools/react";
import { authClient } from "./auth-client";
import { getToken } from "./accounts";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import "./App.css";
const APP_ID = import.meta.env.VITE_JAZZ_APP_ID;
const SERVER_URL = import.meta.env.VITE_JAZZ_SERVER_URL;
const ProviderErrorContext = createContext<React.Dispatch<React.SetStateAction<Error | undefined>>>(
  () => {},
);
export const useProviderError = () => useContext(ProviderErrorContext);

function SessionContent({
  children,
  providerError,
}: React.PropsWithChildren<{ providerError?: Error }>) {
  const { status, error: sessionError, sessionActions } = useJazzAuth();
  const { loginOrRegisterJWT, linkJWT, createLocalFirst, retry } = sessionActions;
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
  const [providerError, setProviderError] = useState<Error>();
  const restored = useRef(false);
  const fallback = <SessionContent providerError={providerError} />;
  return (
    <ProviderErrorContext.Provider value={setProviderError}>
      <Provider
        appId={APP_ID}
        serverUrl={SERVER_URL}
        initial="local-first"
        signedOut={fallback}
        loading={fallback}
        error={fallback}
      >
        <RestoreProviderSession restored={restored} />
        <SessionContent providerError={providerError}>{children}</SessionContent>
      </Provider>
    </ProviderErrorContext.Provider>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <JazzProvider>
      <App />
    </JazzProvider>
  </StrictMode>,
);

function RestoreProviderSession({ restored }: { restored: React.RefObject<boolean> }) {
  const { sessionActions } = useJazzAuth();
  const reportError = useProviderError();
  useEffect(() => {
    if (restored.current) return;
    restored.current = true;
    // Manual source: signup explicitly links the fresh provider identity first.
    void authClient
      .getSession()
      .then((auth) =>
        auth.data?.session ? sessionActions.loginOrRegisterJWT({ getToken }) : undefined,
      )
      .catch((cause) => reportError(cause instanceof Error ? cause : new Error(String(cause))));
  }, [restored, sessionActions, reportError]);
  return null;
}
