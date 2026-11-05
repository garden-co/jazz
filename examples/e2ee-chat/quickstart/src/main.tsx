import "../permissions.js";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { JazzProvider } from "jazz-tools/react";
import { app } from "../schema.js";
import { App } from "./App.js";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <JazzProvider
      appId={import.meta.env.VITE_JAZZ_APP_ID}
      serverUrl={import.meta.env.VITE_JAZZ_SERVER_URL}
      initial="local-first"
      e2ee={{ app }}
      loading={<p>Opening encrypted chat…</p>}
    >
      <App />
    </JazzProvider>
  </StrictMode>,
);
