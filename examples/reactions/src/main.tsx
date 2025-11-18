import { JazzInspector } from "jazz-tools/inspector";
import { JazzReactProvider, PasskeyAuthBasicUI } from "jazz-tools/react";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App.tsx";
import "./index.css";
import { apiKey } from "./apiKey";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <JazzReactProvider
      authSecretStorageKey="examples/reactions"
      sync={{
        peer: `wss://cloud.jazz.tools/?key=${apiKey}`,
      }}
    >
      <PasskeyAuthBasicUI appName="Jazz Reactions Example">
        <App />
      </PasskeyAuthBasicUI>
      <JazzInspector />
    </JazzReactProvider>
  </StrictMode>,
);
