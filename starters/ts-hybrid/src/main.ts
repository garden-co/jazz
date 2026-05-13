import { BrowserAuthSecretStore, createDb, type DbConfig } from "jazz-tools";
import { authClient, type AuthSession } from "./auth-client.js";
import { mountApp, type AppHandle } from "./app.js";
import "./app.css";

const APP_ID = import.meta.env.VITE_JAZZ_APP_ID as string | undefined;
const SERVER_URL = import.meta.env.VITE_JAZZ_SERVER_URL as string | undefined;

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

function isAuthenticated(session: AuthSession): boolean {
  return Boolean(session.data?.session);
}

async function boot() {
  const root = document.getElementById("root");
  if (!root) throw new Error("#root not found");

  // Wait until BetterAuth resolves its initial session before booting Jazz —
  // mirrors the React `isPending` gate.
  const sessionAtom = authClient.useSession;
  if (sessionAtom.get().isPending) {
    await new Promise<void>((resolve) => {
      const off = sessionAtom.subscribe((next: AuthSession) => {
        if (!next.isPending) {
          off();
          resolve();
        }
      });
    });
  }

  let currentlyAuthenticated = isAuthenticated(sessionAtom.get());
  const initialConfig = currentlyAuthenticated
    ? ((await buildJwtConfig()) ?? (await buildLocalFirstConfig()))
    : await buildLocalFirstConfig();
  let db = await createDb(initialConfig);

  const app: AppHandle = mountApp(root, db);

  // JWT refresh: when Jazz reports the token has expired, mint a fresh one
  // from BetterAuth and hand it back.
  db.onAuthChanged((state) => {
    if (state.error !== "expired") return;
    authClient.token().then(({ data, error }) => {
      if (!error && data?.token) db.updateAuthToken(data.token);
    });
  });

  // Rebuild Db when the session flips between anonymous and signed-in.
  sessionAtom.subscribe(async (next: AuthSession) => {
    if (next.isPending) return;
    const nowAuth = isAuthenticated(next);
    if (nowAuth === currentlyAuthenticated) return;
    currentlyAuthenticated = nowAuth;

    const nextConfig = nowAuth
      ? ((await buildJwtConfig()) ?? (await buildLocalFirstConfig()))
      : await buildLocalFirstConfig();
    db = await createDb(nextConfig);
    app.setDb(db);
  });
}

boot();
