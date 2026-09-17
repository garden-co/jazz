import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { JazzProvider, useJazzAuth } from "jazz-tools/react";
import { App } from "./App";
import "./App.css";

const APP_ID = import.meta.env.VITE_JAZZ_APP_ID as string | undefined;
const SERVER_URL = import.meta.env.VITE_JAZZ_SERVER_URL as string | undefined;

if (!APP_ID || !SERVER_URL) {
  const missing = [!APP_ID && "VITE_JAZZ_APP_ID", !SERVER_URL && "VITE_JAZZ_SERVER_URL"]
    .filter((v) => !!v)
    .join(" & ");
  throw new Error(
    `${missing} not set. The jazzPlugin Vite plugin injects these at dev time; in production, set them explicitly in your environment.`,
  );
}

function AccountApp() {
  const { account, sessionActions, error } = useJazzAuth();
  return (
    <>
      {error && <p role="alert">{error.message}</p>}
      <App account={account!} onRestore={sessionActions.restoreLocalFirst} />
    </>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <JazzProvider
      appId={APP_ID}
      serverUrl={SERVER_URL}
      initial="local-first"
      loading={<p>Loading...</p>}
      error={({ error, retry }) => (
        <p role="alert">
          {error?.message} <button onClick={() => void retry()}>Retry</button>
        </p>
      )}
    >
      <AccountApp />
    </JazzProvider>
  </StrictMode>,
);
