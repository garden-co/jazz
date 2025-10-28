import { JazzInspector } from "jazz-tools/inspector";
import { JazzReactProvider } from "jazz-tools/react";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App.tsx";
import { apiKey } from "./apiKey.ts";
import "./index.css";
import { JazzAccount } from "./schema.ts";

// We use this to identify the app in the passkey auth
export const APPLICATION_NAME = "Jazz File Stream Example";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <JazzReactProvider
      sync={{
        peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
      }}
      AccountSchema={JazzAccount}
    >
      <JazzInspector />
      <App />
    </JazzReactProvider>
  </StrictMode>,
);
