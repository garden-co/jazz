import { betterAuth, createJazzApp } from "jazz-tools/client";
import { authClient } from "./auth-client.js";
import { mountApp } from "./app.js";
import "./app.css";

const APP_ID = import.meta.env.VITE_JAZZ_APP_ID as string | undefined;
const SERVER_URL = import.meta.env.VITE_JAZZ_SERVER_URL as string | undefined;

function boot() {
  const root = document.getElementById("root");
  if (!root || !APP_ID || !SERVER_URL) throw new Error("Jazz configuration is missing");
  const jazz = createJazzApp({
    appId: APP_ID,
    serverUrl: SERVER_URL,
    auth: betterAuth(authClient),
  });
  const app = mountApp(root, jazz);
  window.addEventListener("pagehide", (event) => {
    if (event.persisted) return;
    app.destroy();
    void jazz.dispose().catch(console.error);
  });
}

try {
  boot();
} catch (cause) {
  const root = document.getElementById("root");
  if (root) root.textContent = cause instanceof Error ? cause.message : String(cause);
}
