import { JazzInspector } from "jazz-inspector";
import { JazzProvider } from "jazz-react";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App.tsx";
import { apiKey } from "./apiKey.ts";
import "./index.css";
import { JazzAccount } from "./schema.ts";

// We use this to identify the app in the passkey auth
export const APPLICATION_NAME = "Jazz File Stream Example";
const url = new URL(window.location.href);
const peer = url.searchParams.get("peer") as `wss://${string}` | `ws://${string}` | null;
createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <JazzProvider
      sync={{
        peer: peer ?? `wss://cloud.jazz.tools/?key=${apiKey}`,
      }}
      AccountSchema={JazzAccount}
    >
      <JazzInspector />
      <App />
    </JazzProvider>
  </StrictMode>,
);
