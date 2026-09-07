import { createDb } from "jazz-tools";
import { authClient } from "./auth-client.js";
import { accounts, getToken } from "./accounts.js";
import { JazzLifecycle } from "./jazz-lifecycle.js";
import { mountApp } from "./app.js";
import { waitForInitialSession } from "./session-ready.js";
import "./app.css";

const APP_ID = import.meta.env.VITE_JAZZ_APP_ID as string | undefined;
const SERVER_URL = import.meta.env.VITE_JAZZ_SERVER_URL as string | undefined;

async function boot() {
  const root = document.getElementById("root");
  if (!root || !APP_ID || !SERVER_URL) throw new Error("Jazz configuration is missing");
  const manager = await accounts();
  const lifecycle = new JazzLifecycle(manager, (account) =>
    createDb({ appId: APP_ID, serverUrl: SERVER_URL, account }),
  );
  const sessionAtom = authClient.useSession;
  await waitForInitialSession(sessionAtom);
  let registrationError: Error | undefined;
  try {
    await lifecycle.attach(async () => {
      if (sessionAtom.get().data?.session) await manager.loginJWT({ getToken });
    });
  } catch (cause) {
    registrationError = cause instanceof Error ? cause : new Error(String(cause));
  }
  const app = mountApp(root, lifecycle, registrationError);
  lifecycle.onClientChange((db) => app.setDb(db ?? null));
}

void boot().catch((cause) => {
  const root = document.getElementById("root");
  if (root) root.textContent = cause instanceof Error ? cause.message : String(cause);
});
