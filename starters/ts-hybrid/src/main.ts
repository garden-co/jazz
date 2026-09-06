import { createDb } from "jazz-tools";
import { mountApp, type AppHandle } from "./app.js";
import { accounts as prepareAccounts } from "./accounts.js";
import { authClient } from "./auth-client.js";
import { getToken } from "./accounts.js";
import { JazzLifecycle } from "./jazz-lifecycle.js";
import "./app.css";

const APP_ID = import.meta.env.VITE_JAZZ_APP_ID as string | undefined;
const SERVER_URL = import.meta.env.VITE_JAZZ_SERVER_URL as string | undefined;

async function boot() {
  const root = document.getElementById("root");
  if (!root) throw new Error("#root not found");
  if (!APP_ID || !SERVER_URL)
    throw new Error("VITE_JAZZ_APP_ID and VITE_JAZZ_SERVER_URL must be set");

  const accounts = await prepareAccounts();
  const lifecycle = new JazzLifecycle(accounts, (account) =>
    createDb({ appId: APP_ID, serverUrl: SERVER_URL, account }),
  );
  let providerLinkError: Error | undefined;
  await lifecycle.attach(async () => {
    const session = await authClient.getSession();
    if (session.data?.session) {
      const retained = accounts.getLoggedIn();
      try {
        await accounts.loginJWT({ getToken });
      } catch (cause) {
        if (retained?.identity.issuer !== "urn:jazz:local-first") throw cause;
        providerLinkError = cause instanceof Error ? cause : new Error(String(cause));
      }
    } else if (!accounts.getLoggedIn()) accounts.createLocalFirst();
  });
  const app: AppHandle = mountApp(root, lifecycle.getClient(), lifecycle, providerLinkError);
  lifecycle.onClientChange((next) => {
    app.setDb(next);
  });
}

void boot();
