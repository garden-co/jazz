import { StrictMode, useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import { JazzProvider as JazzBaseProvider, useDb } from "jazz-tools/react";
import { type DbConfig, BrowserAuthSecretStore } from "jazz-tools";
import { authClient } from "./auth-client";
import { App } from "./App";
import "./App.css";

const APP_ID = import.meta.env.VITE_JAZZ_APP_ID as string | undefined;
const SERVER_URL = import.meta.env.VITE_JAZZ_SERVER_URL as string | undefined;

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

function baseConfig(): Omit<DbConfig, "jwtToken" | "secret"> {
  if (!APP_ID || !SERVER_URL) {
    const missing = [!APP_ID && "VITE_JAZZ_APP_ID", !SERVER_URL && "VITE_JAZZ_SERVER_URL"]
      .filter((v) => !!v)
      .join(" & ");
    throw new Error(
      `${missing} not set. The jazzPlugin Vite plugin injects these at dev time; in production, set them explicitly in your environment.`,
    );
  }
  return { appId: APP_ID, serverUrl: SERVER_URL };
}

async function buildLocalFirstConfig(): Promise<DbConfig> {
  const secret = await BrowserAuthSecretStore.getOrCreateSecret();
  return { ...baseConfig(), secret };
}

async function buildJwtConfig(): Promise<DbConfig | null> {
  const { data, error } = await authClient.token();
  if (error || !data?.token) return null;
  return { ...baseConfig(), jwtToken: data.token };
}

/**
 * Hybrid provider: renders JazzProvider regardless of BetterAuth session state.
 * Anonymous visitors get a local-first identity (from BrowserAuthSecretStore);
 * signed-in users get a BetterAuth-issued JWT. Switching between the two
 * triggers JazzProvider to rebuild against the new config.
 */
function HybridProvider({ children }: React.PropsWithChildren) {
  const { data: authSession, isPending } = authClient.useSession();
  const [config, setConfig] = useState<DbConfig | null>(null);
  const authenticated = Boolean(authSession?.session);

  useEffect(() => {
    if (isPending) return;
    let cancelled = false;
    setConfig(null);

    (async () => {
      const next = authenticated ? await buildJwtConfig() : await buildLocalFirstConfig();
      if (!cancelled && next) setConfig(next);
    })();

    return () => {
      cancelled = true;
    };
  }, [isPending, authenticated]);

  if (isPending || !config) return null;

  return (
    <JazzBaseProvider config={config} fallback={<p>Loading...</p>}>
      {authenticated && <JwtRefresh />}
      {children}
    </JazzBaseProvider>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <HybridProvider>
      <App />
    </HybridProvider>
  </StrictMode>,
);
