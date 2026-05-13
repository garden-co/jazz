import { BrowserAuthSecretStore, createDb, type DbConfig } from "jazz-tools";
import { mountApp } from "./app.js";
import "./app.css";

const APP_ID = import.meta.env.VITE_JAZZ_APP_ID as string | undefined;
const SERVER_URL = import.meta.env.VITE_JAZZ_SERVER_URL as string | undefined;

function buildConfig(secret: string): DbConfig {
  if (!APP_ID || !SERVER_URL) {
    const missing = [!APP_ID && "VITE_JAZZ_APP_ID", !SERVER_URL && "VITE_JAZZ_SERVER_URL"]
      .filter((v) => !!v)
      .join(" & ");
    throw new Error(
      `${missing} not set. The jazzPlugin Vite plugin injects these at dev time; in production, set them explicitly in your environment.`,
    );
  }
  return { appId: APP_ID, serverUrl: SERVER_URL, secret };
}

async function boot() {
  const root = document.getElementById("root");
  if (!root) throw new Error("#root not found");
  const secret = await BrowserAuthSecretStore.getOrCreateSecret();
  const db = await createDb(buildConfig(secret));
  mountApp(root, db);
}

boot();
