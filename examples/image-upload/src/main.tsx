import { JazzInspector } from "jazz-tools/inspector";
import { JazzReactProvider } from "jazz-tools/react";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App.tsx";
import "./index.css";
import { apiKey } from "./apiKey.ts";
import { JazzAccount } from "./schema.ts";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <JazzReactProvider
      authSecretStorageKey="examples/image-upload"
      sync={{
        peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
      }}
      AccountSchema={JazzAccount}
    >
      <App />
      <JazzInspector />
    </JazzReactProvider>
  </StrictMode>,
);
