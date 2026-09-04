import { BrowserAuthSecretStore, createDb, type Db, type DbConfig } from "jazz-tools";
import { authClient, type AuthSession } from "./auth-client.js";
import { mountApp, type AppHandle } from "./app.js";
import "./app.css";

const APP_ID = import.meta.env.VITE_JAZZ_APP_ID as string | undefined;
const SERVER_URL = import.meta.env.VITE_JAZZ_SERVER_URL as string | undefined;

function baseConfig(): Omit<DbConfig, "jwtToken" | "secret" | "cookieSession"> {
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
  const { data, error } = await authClient.$fetch<{ token: string }>("/token", {
    method: "GET",
  });
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
  let authGeneration = 0;

  const app: AppHandle = mountApp(root, db);

  // JWT refresh: when Jazz reports the token has expired, mint a fresh one
  // from BetterAuth and hand it back.
  function wireJwtRefresh(ownedDb: Db): () => void {
    return ownedDb.onAuthChanged((state) => {
      if (state.error !== "expired" || db !== ownedDb) return;
      void authClient.$fetch<{ token: string }>("/token", { method: "GET" }).then(
        ({ data, error }) => {
          // The request can finish after a login/logout transition. A token
          // minted for the retired session must never be installed on its
          // replacement Db.
          if (!error && data?.token && db === ownedDb) {
            ownedDb.updateAuthToken(data.token);
          }
        },
        () => {},
      );
    });
  }
  let stopJwtRefresh = wireJwtRefresh(db);

  // Rebuild Db when the session flips between anonymous and signed-in.
  async function transitionToSession(next: AuthSession): Promise<void> {
    if (next.isPending) return;
    const nowAuth = isAuthenticated(next);
    if (nowAuth === currentlyAuthenticated) return;
    currentlyAuthenticated = nowAuth;
    const generation = ++authGeneration;

    const nextConfig = nowAuth
      ? ((await buildJwtConfig()) ?? (await buildLocalFirstConfig()))
      : await buildLocalFirstConfig();
    if (generation !== authGeneration) return;

    const nextDb = await createDb(nextConfig);
    if (generation !== authGeneration) {
      await nextDb.shutdown();
      return;
    }

    const previousDb = db;
    const stopPreviousJwtRefresh = stopJwtRefresh;
    db = nextDb;
    stopJwtRefresh = wireJwtRefresh(nextDb);
    app.setDb(nextDb);
    stopPreviousJwtRefresh();
    await previousDb.shutdown();
  }

  function handleSession(next: AuthSession) {
    void transitionToSession(next).catch((error: unknown) => {
      // Session subscriptions do not await listener promises. Do not turn a
      // teardown failure into an unhandled rejection, but retain diagnostics.
      console.error("Failed to transition Jazz authentication", error);
    });
  }

  sessionAtom.subscribe(handleSession);
  // A session may have changed while the initial configuration or Db was
  // opening. Re-read after subscribing so that transition is not lost.
  handleSession(sessionAtom.get());
}

boot();
