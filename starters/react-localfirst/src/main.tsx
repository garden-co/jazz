import { StrictMode, useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import { type DbConfig, BrowserAuthSecretStore } from "jazz-tools";
import { JazzProvider } from "jazz-tools/react";
import { App } from "./App";
import "./App.css";

const APP_ID = import.meta.env.VITE_JAZZ_APP_ID as string | undefined;
const SERVER_URL = import.meta.env.VITE_JAZZ_SERVER_URL as string | undefined;

function buildConfig(secret: string): DbConfig {
  if (!APP_ID || !SERVER_URL) {
    const missing = [!APP_ID && "VITE_JAZZ_APP_ID", !SERVER_URL && "VITE_JAZZ_SERVER_URL"]
      .filter((v) => !!v)
      .join(" & ");
    throw new Error(
      `${missing} not set. The jazzPlugin Vite plugin injects these at dev time; in production, set them explicitly in your environment.`,
    );
  }
  return {
    appId: APP_ID,
    serverUrl: SERVER_URL,
    secret,
  };
}

function LocalFirstProvider({ children }: React.PropsWithChildren) {
  const [config, setConfig] = useState<DbConfig | null>(null);

  useEffect(() => {
    BrowserAuthSecretStore.getOrCreateSecret().then((secret) => {
      setConfig(buildConfig(secret));
    });
  }, []);

  if (!config) return null;

  return (
    <JazzProvider config={config} fallback={<p>Loading...</p>}>
      {children}
    </JazzProvider>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <LocalFirstProvider>
      <App />
    </LocalFirstProvider>
  </StrictMode>,
);
