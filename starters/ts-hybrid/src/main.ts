import { createJazzSession } from "jazz-tools/client";
import { mountApp } from "./app.js";
import { authClient } from "./auth-client.js";
import { getToken } from "./accounts.js";
import "./app.css";

const APP_ID = import.meta.env.VITE_JAZZ_APP_ID as string | undefined;
const SERVER_URL = import.meta.env.VITE_JAZZ_SERVER_URL as string | undefined;

async function boot() {
  const root = document.getElementById("root");
  if (!root) throw new Error("#root not found");
  if (!APP_ID || !SERVER_URL)
    throw new Error("VITE_JAZZ_APP_ID and VITE_JAZZ_SERVER_URL must be set");

  const session = await createJazzSession({
    appId: APP_ID,
    serverUrl: SERVER_URL,
    initial: "local-first",
  });
  let providerLinkError: Error | undefined;
  try {
    const auth = await authClient.getSession();
    if (auth.data?.session) await session.loginOrRegisterJWT({ getToken });
  } catch (cause) {
    if (session.getSnapshot().account?.identity.issuer !== "urn:jazz:local-first") {
      await session.close();
      throw cause;
    }
    providerLinkError = cause instanceof Error ? cause : new Error(String(cause));
  }
  const client = session.getSnapshot().client;
  if (!client) throw session.getSnapshot().error ?? new Error("Jazz client is unavailable");
  const app = mountApp(root, client.db, session, providerLinkError);
  const unsubscribe = session.subscribe(() => app.setDb(session.getSnapshot().client?.db));
  window.addEventListener("pagehide", (event) => {
    if (event.persisted) return;
    unsubscribe();
    app.destroy();
    void session.close().catch(console.error);
  });
}

void boot().catch((error: unknown) => {
  const root = document.getElementById("root");
  if (root) root.textContent = error instanceof Error ? error.message : String(error);
});
