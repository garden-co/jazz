import { createJazzSession } from "jazz-tools/client";
import { mountApp } from "./app.js";
import "./app.css";

const APP_ID = import.meta.env.VITE_JAZZ_APP_ID as string | undefined;
const SERVER_URL = import.meta.env.VITE_JAZZ_SERVER_URL as string | undefined;

async function boot() {
  const root = document.getElementById("root");
  if (!root || !APP_ID || !SERVER_URL) throw new Error("Jazz configuration is missing");
  const session = await createJazzSession({ appId: APP_ID, serverUrl: SERVER_URL });
  const app = mountApp(root, session);
  const unsubscribe = session.subscribe(() => app.setDb(session.getSnapshot().client?.db ?? null));
  window.addEventListener("pagehide", (event) => {
    if (event.persisted) return;
    unsubscribe();
    app.destroy();
    void session.close().catch(console.error);
  });
}

void boot().catch((cause) => {
  const root = document.getElementById("root");
  if (root) root.textContent = cause instanceof Error ? cause.message : String(cause);
});
