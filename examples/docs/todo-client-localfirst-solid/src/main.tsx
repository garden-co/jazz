import { render } from "solid-js/web";
import { App } from "./App.js";

const env = (import.meta as ImportMeta & { env?: Record<string, string | undefined> }).env;
render(
  () => (
    <App
      config={{
        appId: env?.VITE_JAZZ_APP_ID ?? env?.JAZZ_APP_ID ?? "",
        serverUrl: env?.VITE_JAZZ_SERVER_URL ?? env?.JAZZ_SERVER_URL,
        env: "dev",
      }}
    />
  ),
  document.getElementById("app")!,
);
