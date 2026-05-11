import { createDb, type DbConfig, type Db } from "jazz-tools";
import { authClient, type AuthSession } from "./auth-client.js";
import { mountApp, type AppHandle } from "./app.js";
import "./app.css";

const APP_ID = import.meta.env.VITE_JAZZ_APP_ID as string | undefined;
const SERVER_URL = import.meta.env.VITE_JAZZ_SERVER_URL as string | undefined;

function baseConfig(): Omit<DbConfig, "jwtToken"> {
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

  let db: Db | null = null;
  if (isAuthenticated(sessionAtom.get())) {
    const config = await buildJwtConfig();
    if (config) db = await createDb(config);
  }

  const app: AppHandle = mountApp(root, db);

  // JWT refresh: when Jazz reports the token has expired, mint a fresh one
  // from BetterAuth and hand it back.
  function wireJwtRefresh(d: Db) {
    d.onAuthChanged((state) => {
      if (state.error !== "expired") return;
      authClient.token().then(({ data, error }) => {
        if (!error && data?.token) d.updateAuthToken(data.token);
      });
    });
  }
  if (db) wireJwtRefresh(db);

  let currentlyAuthenticated = isAuthenticated(sessionAtom.get());
  sessionAtom.subscribe(async (next: AuthSession) => {
    if (next.isPending) return;
    const nowAuth = isAuthenticated(next);
    if (nowAuth === currentlyAuthenticated) return;
    currentlyAuthenticated = nowAuth;

    if (nowAuth) {
      const config = await buildJwtConfig();
      if (config) {
        db = await createDb(config);
        wireJwtRefresh(db);
        app.setDb(db);
      }
    } else {
      db = null;
      app.setDb(null);
    }
  });
}

boot();
