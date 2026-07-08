import {
  BrowserAuthSecretStore,
  createJazzClient,
  subscribeAll,
  type JazzClient,
  type JazzClientConfig,
} from "jazz-tools/client";
import { mountApp } from "./app.js";
import "./app.css";

const APP_ID = import.meta.env.VITE_JAZZ_APP_ID as string | undefined;
const SERVER_URL = import.meta.env.VITE_JAZZ_SERVER_URL as string | undefined;
const ASYNC_ONLY = import.meta.env.VITE_JAZZ_ASYNC_SUBSCRIPTIONS_ONLY !== "false";

function buildConfig(secret: string): JazzClientConfig<boolean> {
  if (!APP_ID || !SERVER_URL) {
    const missing = [!APP_ID && "VITE_JAZZ_APP_ID", !SERVER_URL && "VITE_JAZZ_SERVER_URL"]
      .filter((v) => !!v)
      .join(" & ");
    throw new Error(
      `${missing} not set. The jazzPlugin Vite plugin injects these at dev time; in production, set them explicitly in your environment.`,
    );
  }
  return { appId: APP_ID, serverUrl: SERVER_URL, secret, asyncSubscriptionsOnly: ASYNC_ONLY };
}

async function boot() {
  const root = document.getElementById("root");
  if (!root) throw new Error("#root not found");
  const secret = await BrowserAuthSecretStore.getOrCreateSecret();
  const client = await createJazzClient(buildConfig(secret));
  mountApp(root, {
    db: client.db,
    subscribeAll: (query, callback, options) =>
      subscribeAll(client as JazzClient<boolean>, query, callback, options),
  });
}

boot();
