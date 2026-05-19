import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { JazzProvider, useLocalFirstAuth } from "jazz-tools/react";
import { App } from "./App";
import "./App.css";

const APP_ID = import.meta.env.VITE_JAZZ_APP_ID as string | undefined;
const SERVER_URL = import.meta.env.VITE_JAZZ_SERVER_URL as string | undefined;

function LocalFirstProvider({ children }: React.PropsWithChildren) {
  if (!APP_ID || !SERVER_URL) {
    const missing = [!APP_ID && "VITE_JAZZ_APP_ID", !SERVER_URL && "VITE_JAZZ_SERVER_URL"]
      .filter((v) => !!v)
      .join(" & ");
    throw new Error(
      `${missing} not set. The jazzPlugin Vite plugin injects these at dev time; in production, set them explicitly in your environment.`,
    );
  }

  const { secret, isLoading } = useLocalFirstAuth();
  if (isLoading || !secret) return null;

  return (
    <JazzProvider
      config={{ appId: APP_ID, serverUrl: SERVER_URL, secret }}
      fallback={<p>Loading...</p>}
    >
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
