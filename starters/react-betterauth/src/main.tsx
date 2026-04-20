import { StrictMode, useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import { JazzProvider as JazzBaseProvider, useDb } from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { authClient } from "./auth-client";
import { App } from "./App";
import "./App.css";

const APP_ID = import.meta.env.VITE_JAZZ_APP_ID as string | undefined;
const SERVER_URL = import.meta.env.VITE_JAZZ_SERVER_URL as string | undefined;

function BetterAuthProvider({ children }: React.PropsWithChildren) {
  const { data: session } = authClient.useSession();
  const [config, setConfig] = useState<DbConfig | null>(null);

  useEffect(() => {
    if (!session) {
      setConfig(null);
      return;
    }
    let cancelled = false;
    authClient.token().then(({ data, error }) => {
      if (cancelled || error || !data?.token) return;
      const { token: jwtToken } = data;
      if (!APP_ID || !SERVER_URL) {
        const missing = [!APP_ID && "VITE_JAZZ_APP_ID", !SERVER_URL && "VITE_JAZZ_SERVER_URL"]
          .filter((v) => !!v)
          .join(" & ");
        throw new Error(
          `${missing} not set. The jazzPlugin Vite plugin injects these at dev time; in production, set them explicitly in your environment.`,
        );
      }
      setConfig({ appId: APP_ID, serverUrl: SERVER_URL, jwtToken });
    });
    return () => {
      cancelled = true;
    };
  }, [session]);

  if (!session) return <>{children}</>;
  if (!config) return null;

  return (
    <JazzBaseProvider config={config} fallback={null}>
      <JwtRefresh />
      {children}
    </JazzBaseProvider>
  );
}

function JwtRefresh() {
  const db = useDb();
  useEffect(
    () =>
      db.onAuthChanged((state) => {
        if (state.error !== "expired") return;
        authClient.token().then(({ data, error }) => {
          if (!error && data?.token) db.updateAuthToken(data.token);
        });
      }),
    [db],
  );
  return null;
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <BetterAuthProvider>
      <App />
    </BetterAuthProvider>
  </StrictMode>,
);
